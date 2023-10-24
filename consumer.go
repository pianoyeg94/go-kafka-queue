package kafkaqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/pianoyeg94/go-kafka-queue/timer"
)

type (
	Handler interface {
		Handle(ctx context.Context, msg *Message) error
	}

	HandleFunc func(ctx context.Context, msg *Message) error
)

func (f HandleFunc) Handle(ctx context.Context, msg *Message) error { return f(ctx, msg) }

var _ Handler = HandleFunc(nil)

type (
	HandlerSuccessCallback func(topic string, groupId string, partition int32, offset int64, msg *Message)
	HandlerErrorCallback   func(err error, topic string, groupId string, partition int32, offset int64, msg *Message)
)

const cooperativeRebalanceProtocol = "COOPERATIVE"

const (
	consumerClosing   = 1 << 0
	consumerConsuming = 1 << 1
)

func NewConsumer(ctx context.Context, servers []string, topic string, groupId string, opts *ConsumerOptions) (*Consumer, error) {
	opts = opts.setServers(servers).setGroupId(groupId)
	if err := opts.validate(); err != nil {
		return nil, err
	}

	var libconsumer *kafka.Consumer
	if err := timer.RetryOverTime(ctx, -1, 0, 500*time.Millisecond, 2*time.Second,
		func(ctx context.Context) (err error) {
			libconsumer, err = kafka.NewConsumer(opts.toConfigMap())
			return err
		},
		func(ctx context.Context, err error) bool {
			if onInternalError := opts.InternalErrorCallback; onInternalError != nil {
				onInternalError(fmt.Errorf("kafka: error fetching metadata and creating consumer: %w", err), topic)
			}

			kerr, ok := err.(kafka.Error)
			if !ok {
				return false
			}

			return isKafkalibTransientError(&kerr)
		},
	); err != nil {
		return nil, fmt.Errorf("kafka: error fetching metadata and creating consumer '%s': %w", topic, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := Consumer{
		servers:          servers,
		topic:            topic,
		groupId:          groupId,
		autoOffsetReset:  opts.AutoOffsetReset,
		commitInterval:   opts.getCommitInterval(),
		commitThreshold:  int64(opts.getCommitThreshold()),
		partitionBufSize: opts.getPartitionBufferSize(),

		consumer: libconsumer,

		// we orchestrate partition rebalances manually as well as
		// storing and commiting offsets. At the same time we take
		// special care to skip and discard stale messages.
		// libconsumer.Events() is only deprecated because without
		// manual handling of partition rebalances and offset commits,
		// one can easily receive stale messages after a rebalance occurs.
		events:             libconsumer.Events(),
		partitions:         make(map[int32]struct{}),
		partitionOffsets:   make(map[int32]kafka.Offset),
		partitionConsumers: make(map[int32]*partitionConsumer),

		messageRetriesMaxAttempts:  opts.getMessageRetriesMaxAttempts(),
		messageRetriesInitialDelay: opts.MessageRetriesInitialDelay,
		messageRetriesBackoff:      opts.MessageRetriesBackoff,
		messageRetriesMaxDelay:     opts.MessageRetriesMaxDelay,

		onHandlerSuccess: opts.HandlerSuccessCallback,
		onHandlerError:   opts.HandlerErrorCallback,
		onInternalError:  opts.InternalErrorCallback,

		closeCtx: ctx,
		closeFn:  cancel,
	}

	return &consumer, nil
}

type Consumer struct {
	servers          []string
	topic            string
	groupId          string
	autoOffsetReset  AutoOffsetReset
	commitInterval   time.Duration
	commitThreshold  int64
	partitionBufSize int

	consumer *kafka.Consumer
	handler  Handler

	events             <-chan kafka.Event
	partitions         map[int32]struct{}
	partitionOffsets   map[int32]kafka.Offset
	partitionConsumers map[int32]*partitionConsumer
	processedCount     int64

	messageRetriesMaxAttempts  int
	messageRetriesInitialDelay time.Duration
	messageRetriesBackoff      time.Duration
	messageRetriesMaxDelay     time.Duration

	onHandlerSuccess HandlerSuccessCallback
	onHandlerError   HandlerErrorCallback
	onInternalError  InternalErrorCallback

	state    uint32
	closeCtx context.Context
	closeFn  context.CancelFunc
	closeWg  sync.WaitGroup
}

// Consume starts consuming from the latest commited
// offset or from offset determined by the autoOffsetReset
// policy, depending on whether a partition has offsets commited
// or not (this mimics a typical queue-like behaviour).
func (c *Consumer) Consume(handler Handler) error {
	if handler == nil {
		return fmt.Errorf("kafka: handler for consumer '%s' cannot be nil", c.topic)
	}

	alreadyConsuming, alreadyClosing := c.markConsuming()
	if alreadyConsuming {
		return fmt.Errorf("kafka: consumer '%s' already consuming", c.topic)
	}

	if alreadyClosing {
		return fmt.Errorf("kafka: consumer '%s' already closing or closed", c.topic)
	}

	if err := timer.RetryOverTime(context.Background(), -1, 0, 500*time.Millisecond, 2*time.Second,
		func(ctx context.Context) (err error) { return c.consumer.Subscribe(c.topic, nil) },
		func(ctx context.Context, err error) bool {
			if c.onInternalError != nil {
				c.onInternalError(fmt.Errorf("kafka: error subcribing to topic: %w", err), c.topic)
			}

			kerr, ok := err.(kafka.Error)
			if c.isClosing() || !ok {
				return false
			}

			return isKafkalibTransientError(&kerr)
		},
	); err != nil {
		_ = c.consumer.Close()
		return fmt.Errorf("kafka: error subscribing to topic '%s': %w", c.topic, err)
	}

	c.handler = handler
	c.closeWg.Add(1)
	go c.consume()

	return nil
}

func (c *Consumer) Topic() string {
	return c.topic
}

func (c *Consumer) GroupId() string {
	return c.groupId
}

func (c *Consumer) Close() error {
	if c.markClosing() {
		return nil
	}

	c.closeFn()
	c.closeWg.Wait()
	if err := c.consumer.Close(); err != nil {
		return fmt.Errorf("kafka: error closing consumer '%s': %w", c.topic, err)
	}

	return nil
}

func (c *Consumer) IsConsuming() bool {
	return atomic.LoadUint32(&c.state)&consumerConsuming != 0
}

func (c *Consumer) IsClosed() bool {
	return c.isClosing()
}

func (c *Consumer) consume() {
	var err error
	defer func() {
		if err != nil && c.onInternalError != nil {
			c.onInternalError(err, c.topic)
		}
		c.onClose()
		c.closeWg.Done()
	}()

	commitTimer := timer.NewTimer(c.commitInterval)
	defer commitTimer.Stop()
	for {
		select {
		case event, ok := <-c.events:
			if !ok {
				return
			}

			if err = c.processEvent(event); err != nil || c.isClosing() {
				return
			}

			if atomic.LoadInt64(&c.processedCount) >= c.commitThreshold {
				var commited int
				if commited, err = c.commitOffsets(); err != nil {
					return
				}
				atomic.AddInt64(&c.processedCount, -int64(commited))
				commitTimer.Reset(c.commitInterval)
			}
		case <-commitTimer.C:
			if c.isClosing() {
				return
			}

			if atomic.LoadInt64(&c.processedCount) > 0 {
				var commited int
				if commited, err = c.commitOffsets(); err != nil {
					return
				}
				atomic.AddInt64(&c.processedCount, -int64(commited))
			}

			commitTimer.Reset(c.commitInterval)
		case <-c.closeCtx.Done():
			return
		}
	}
}

func (c *Consumer) processEvent(ev kafka.Event) error {
	switch event := ev.(type) {
	case *kafka.Message:
		if consumer := c.partitionConsumers[event.TopicPartition.Partition]; consumer != nil {
			consumer.queueMessage(event)
		}
	case kafka.AssignedPartitions:
		newPartitions, err := c.assignPartitions(event.Partitions)
		if err != nil {
			return err
		}

		c.startPartitionConsumers(newPartitions)
	case kafka.RevokedPartitions:
		partitions := make([]int32, len(event.Partitions))
		for i := range event.Partitions {
			partitions[i] = event.Partitions[i].Partition
		}
		// we close each partition consumer separately to support
		// cooperative incremental rebalances
		c.closePartitionConsumers(partitions)

		if err := c.revokePartitions(event.Partitions); err != nil {
			return err
		}
	case kafka.Error:
		if !isKafkalibTransientError(&event) {
			return fmt.Errorf("kafka: got consumer fatal error, closing prematurely: %w", event)
		}

		if c.onInternalError != nil {
			c.onInternalError(fmt.Errorf("kafka: got consumer transient error: %w", event), c.topic)
		}
	}

	return nil
}

func (c *Consumer) commitOffsets() (int, error) {
	var commited int
	if err := timer.RetryOverTime(c.closeCtx, -1, 0, 250*time.Millisecond, 2*time.Second,
		func(ctx context.Context) error {
			tp, err := c.consumer.Commit()
			for i := range tp {
				// skip errored out partitions and partitions with no offsets yet commited
				if tp[i].Error != nil || tp[i].Offset == kafka.OffsetInvalid {
					continue
				}
				commited += int(tp[i].Offset - c.partitionOffsets[tp[i].Partition])
				c.partitionOffsets[tp[i].Partition] = tp[i].Offset
			}
			return err
		},
		func(ctx context.Context, err error) bool {
			kerr, ok := err.(kafka.Error)
			if c.isClosing() || !ok {
				return false
			}

			return isKafkalibTransientError(&kerr)
		},
	); err != nil {
		return commited, fmt.Errorf("kafka: error commiting offsets: %w", err)
	}

	return commited, nil
}

func (c *Consumer) assignPartitions(partitions []kafka.TopicPartition) (newPartitions []int32, _ error) {
	assign := c.consumer.Assign
	if c.consumer.GetRebalanceProtocol() == cooperativeRebalanceProtocol {
		assign = c.consumer.IncrementalAssign
	}

	if err := timer.RetryOverTime(c.closeCtx, -1, 0, 250*time.Millisecond, 2*time.Second,
		func(ctx context.Context) error { return assign(partitions) },
		func(ctx context.Context, err error) bool {
			kerr, ok := err.(kafka.Error)
			if c.isClosing() || !ok {
				return false
			}

			return isKafkalibTransientError(&kerr)
		},
	); err != nil {
		return nil, fmt.Errorf("kafka: error assigning partitions: %w", err)
	}

	// get previosuly commited offsets if any
	if err := timer.RetryOverTime(c.closeCtx, -1, 0, 250*time.Millisecond, 2*time.Second,
		func(ctx context.Context) (err error) {
			partitions, err = c.consumer.Committed(partitions, 5000)
			return err
		},
		func(ctx context.Context, err error) bool {
			kerr, ok := err.(kafka.Error)
			if c.isClosing() || !ok {
				return false
			}

			return isKafkalibTransientError(&kerr)
		},
	); err != nil {
		return nil, fmt.Errorf("kafka: error fetching partition offsets: %w", err)
	}

	for i := range partitions {
		if _, ok := c.partitions[partitions[i].Partition]; !ok {
			newPartitions = append(newPartitions, partitions[i].Partition)
			c.partitions[partitions[i].Partition] = struct{}{}
			c.partitionOffsets[partitions[i].Partition] = partitions[i].Offset

			// if there're no offsets previously commited,
			// do some bookkeeping to store correct offsets in
			// the `c.partitionOffsets` map and start consuming
			// from the offsets defined by the AutoOffsetReset
			// policy
			if partitions[i].Offset == kafka.OffsetInvalid {
				var low, high int64
				if err := timer.RetryOverTime(c.closeCtx, -1, 0, 250*time.Millisecond, 2*time.Second,
					func(ctx context.Context) (err error) {
						low, high, err = c.consumer.QueryWatermarkOffsets(c.topic, partitions[i].Partition, 1000)
						return err
					},
					func(ctx context.Context, err error) bool {
						kerr, ok := err.(kafka.Error)
						if c.isClosing() || !ok {
							return false
						}

						return isKafkalibTransientError(&kerr)
					},
				); err != nil {
					return nil, fmt.Errorf("kafka: error querying watermark offsets for partition: %w", err)
				}

				if c.autoOffsetReset == AutoOffsetResetEarliest {
					partitions[i].Offset = kafka.Offset(low)
					c.partitionOffsets[partitions[i].Partition] = kafka.Offset(low)
					continue
				}

				partitions[i].Offset = kafka.Offset(high)
				c.partitionOffsets[partitions[i].Partition] = kafka.Offset(high)
			}

			// start consuming from latest commited offset or offset defined by the AutoOffsetReset policy
			// if none are yet commited
			if err := timer.RetryOverTime(c.closeCtx, -1, 0, 250*time.Millisecond, 2*time.Second,
				func(ctx context.Context) error { return c.consumer.Seek(partitions[i], 0) },
				func(ctx context.Context, err error) bool {
					kerr, ok := err.(kafka.Error)
					if c.isClosing() || !ok {
						return false
					}

					return isKafkalibTransientError(&kerr)
				},
			); err != nil {
				return nil, fmt.Errorf("kafka: error seeking partition: %w", err)
			}
		}
	}

	return newPartitions, nil
}

func (c *Consumer) revokePartitions(partitions []kafka.TopicPartition) error {
	for i := range partitions {
		delete(c.partitions, partitions[i].Partition)
	}

	// commit offsets before revoking partitions
	if atomic.LoadInt64(&c.processedCount) > 0 {
		commited, err := c.commitOffsets()
		if err != nil && c.onInternalError != nil {
			c.onInternalError(err, c.topic)
		}
		atomic.AddInt64(&c.processedCount, -int64(commited))
	}

	for i := range partitions {
		delete(c.partitionOffsets, partitions[i].Partition)
	}

	unassign := func(partitions []kafka.TopicPartition) error { return c.consumer.Unassign() }
	if c.consumer.GetRebalanceProtocol() == cooperativeRebalanceProtocol {
		unassign = c.consumer.IncrementalUnassign
	}

	err := timer.RetryOverTime(c.closeCtx, -1, 0, 250*time.Millisecond, 2*time.Second,
		func(ctx context.Context) error { return unassign(partitions) },
		func(ctx context.Context, err error) bool {
			kerr, ok := err.(kafka.Error)
			if c.isClosing() || !ok {
				return false
			}

			return isKafkalibTransientError(&kerr)
		},
	)
	if err != nil {
		err = fmt.Errorf("kafka: error revoking partitions: %w", err)
	}

	return err
}

func (c *Consumer) startPartitionConsumers(partitions []int32) {
	for _, partition := range partitions {
		c.partitionConsumers[partition] = newPartitionConsumer(partition, c.partitionBufSize, c, c.handler)
		c.partitionConsumers[partition].start()
	}
}

func (c *Consumer) closePartitionConsumers(partitions []int32) {
	var wg sync.WaitGroup
	for _, partition := range partitions {
		if consumer := c.partitionConsumers[partition]; consumer != nil {
			wg.Add(1)
			go func() { defer wg.Done(); consumer.close() }()
			delete(c.partitionConsumers, partition)
		}
	}
	wg.Wait()
}

func (c *Consumer) onClose() {
	c.closePartitionConsumers(c.assignedPartitions())
	if atomic.LoadInt64(&c.processedCount) > 0 {
		if _, err := c.commitOffsets(); err != nil && c.onInternalError != nil {
			c.onInternalError(err, c.topic)
		}
	}
	c.clearPartitions()
	atomic.StoreInt64(&c.processedCount, 0)

	if !c.markClosing() { // consumer closing as a result of a fatal error
		if c.onInternalError != nil {
			c.onInternalError(errors.New("kafka: got fatal error, closing consumer prematurely"), c.topic)
		}

		if err := c.consumer.Close(); err != nil && c.onHandlerError != nil {
			c.onInternalError(fmt.Errorf("kafka: error closing consumer: %w", err), c.topic)
		}
	}
}

func (c *Consumer) clearPartitions() {
	for partition := range c.partitions {
		delete(c.partitions, partition)
		delete(c.partitionOffsets, partition)
	}
}

func (c *Consumer) assignedPartitions() []int32 {
	assigned := make([]int32, 0, len(c.partitions))
	for partition := range c.partitions {
		assigned = append(assigned, partition)
	}

	return assigned
}

// markConsuming uses a spin-lock pattern to atomically determine
// if the consumer is already closed or already consuming, if
// neither hold true - marks the consumer as consuming
func (c *Consumer) markConsuming() (alreadyConsuming bool, alreadyClosing bool) {
	for {
		old := atomic.LoadUint32(&c.state)
		if old&consumerClosing != 0 {
			return false, true
		}

		if old&consumerConsuming != 0 {
			return true, false
		}

		if atomic.CompareAndSwapUint32(&c.state, old, consumerConsuming) {
			return false, false
		}
	}
}

func (c *Consumer) markClosing() (alreadyClosing bool) {
	for {
		old := atomic.LoadUint32(&c.state)
		if old&consumerClosing != 0 {
			return true
		}

		if atomic.CompareAndSwapUint32(&c.state, old, consumerClosing) {
			return false
		}
	}
}

func (c *Consumer) isClosing() bool {
	return atomic.LoadUint32(&c.state)&consumerClosing != 0
}
