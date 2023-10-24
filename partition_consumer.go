package kafkaqueue

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/pianoyeg94/go-kafka-queue/timer"
)

func newPartitionConsumer(partition int32, bufSize int, consumer *Consumer, handler Handler) *partitionConsumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &partitionConsumer{
		partition: partition,
		kafkaId:   fmt.Sprintf("%s_%d_", consumer.topic, partition),

		consumer: consumer,
		handler:  handler,
		msgs:     make(chan *kafka.Message, bufSize),

		closeCtx: ctx,
		closeFn:  cancel,
	}
}

type partitionConsumer struct {
	partition int32
	kafkaId   string

	consumer *Consumer
	handler  Handler
	msgs     chan *kafka.Message

	closing  int32
	closeCtx context.Context
	closeFn  context.CancelFunc
	closeWg  sync.WaitGroup
}

func (c *partitionConsumer) start() {
	c.closeWg.Add(1)
	go c.processMessages()
}

func (c *partitionConsumer) queueMessage(msg *kafka.Message) {
	c.msgs <- msg
}

func (c *partitionConsumer) close() {
	atomic.StoreInt32(&c.closing, 1)
	c.closeFn()
	c.closeWg.Wait()
	close(c.msgs)
	for range c.msgs { // flush stale messages if any
	}
}

func (c *partitionConsumer) processMessages() {
	defer c.closeWg.Done()
	for {
		select {
		case libmsg, ok := <-c.msgs:
			if !ok || c.isClosing() {
				return
			}

			pheaders := convertLibHeaders(libmsg.Headers)
			msg := Message{
				Body:    libmsg.Value,
				KafkaId: c.kafkaId + strconv.Itoa(int(libmsg.TopicPartition.Offset)),
			}
			if pheaders != nil {
				msg.Headers = *pheaders
			}

			if err := timer.RetryOverTime(c.closeCtx,
				c.consumer.messageRetriesMaxAttempts,
				c.consumer.messageRetriesInitialDelay,
				c.consumer.messageRetriesBackoff,
				c.consumer.messageRetriesMaxDelay,
				func(ctx context.Context) error { return c.handler.Handle(ctx, &msg) },
				func(ctx context.Context, err error) bool {
					if c.isClosing() {
						return false
					}

					if c.consumer.onHandlerError != nil {
						c.consumer.onHandlerError(err, c.consumer.topic, c.consumer.groupId, c.partition, int64(libmsg.TopicPartition.Offset), &msg)
					}

					return true
				},
			); err != nil && c.consumer.onHandlerError != nil {
				c.consumer.onHandlerError(err, c.consumer.topic, c.consumer.groupId, c.partition, int64(libmsg.TopicPartition.Offset), &msg)
			} else {
				if c.consumer.onHandlerSuccess != nil {
					c.consumer.onHandlerSuccess(c.consumer.topic, c.consumer.groupId, c.partition, int64(libmsg.TopicPartition.Offset), &msg)
				}
			}

			freeHeaders(pheaders)
			// access to the offset store within librdkafka is guarded by a pthread_mutex_lock
			// including code that commits offsets to the broker
			if _, err := c.consumer.consumer.StoreMessage(libmsg); err != nil && c.consumer.onInternalError != nil {
				c.consumer.onInternalError(
					fmt.Errorf("kafka: unable to store partition offset '%d:%d' in offset store: %w", libmsg.TopicPartition.Partition, libmsg.TopicPartition.Offset, err),
					c.consumer.topic,
				)
				break
			}
			atomic.AddInt64(&c.consumer.processedCount, 1)
		case <-c.closeCtx.Done():
			return
		}
	}
}

func (c *partitionConsumer) isClosing() bool {
	return atomic.LoadInt32(&c.closing) == 1
}
