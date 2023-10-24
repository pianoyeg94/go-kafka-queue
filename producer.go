package kafkaqueue

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/pianoyeg94/go-kafka-queue/goruntime"
	"github.com/pianoyeg94/go-kafka-queue/timer"
)

type (
	DeliverySuccessCallback func(topic string, partition int32, offset int64, msg *Message)
	DeliveryErrorCallback   func(err error, topic string, partition int32, msg *Message)
)

const (
	producerClosed  = 1 << 0
	producerRef     = 1 << 1
	producerRefMask = (1<<62 - 1) << 1
)

var deliveryChansPool = sync.Pool{New: func() interface{} { ch := make(chan kafka.Event, 1); return &ch }}

func NewProducer(ctx context.Context, servers []string, opts *ProducerOptions) (*Producer, error) {
	opts = opts.setServers(servers)
	if err := opts.validate(); err != nil {
		return nil, err
	}

	var libproducer *kafka.Producer
	if err := timer.RetryOverTime(ctx, -1, 0, 500*time.Millisecond, 2*time.Second,
		func(ctx context.Context) (err error) {
			libproducer, err = kafka.NewProducer(opts.toConfigMap())
			return err
		},
		func(ctx context.Context, err error) bool {
			if onInternalError := opts.InternalErrorCallback; onInternalError != nil {
				onInternalError(fmt.Errorf("kafka: error fetching metadata and creating producer: %w", err), "undefined")
			}

			kerr, ok := err.(kafka.Error)
			if !ok {
				return false
			}

			return isKafkalibTransientError(&kerr)
		},
	); err != nil {
		return nil, fmt.Errorf("kafka: error fetching metadata and creating producer '%s': %w", "undefined", err)
	}

	producer := Producer{
		servers:    servers,
		producer:   libproducer,
		deliveries: libproducer.Events(),

		onDeliverySuccess: opts.DeliverySuccessCallback,
		onDeliveryError:   opts.DeliveryErrorCallback,
		onInternalError:   opts.InternalErrorCallback,

		close: make(chan struct{}),
	}
	producer.closeWg.Add(1)
	go producer.watchDeliveries()

	return &producer, nil
}

type Producer struct {
	servers    []string
	producer   *kafka.Producer
	deliveries <-chan kafka.Event

	onDeliverySuccess DeliverySuccessCallback
	onDeliveryError   DeliveryErrorCallback
	onInternalError   InternalErrorCallback

	state   uint64
	close   chan struct{}
	closeWg sync.WaitGroup
}

func (p *Producer) Produce(topic string, key string, msg *Message) error {
	return p.produce(topic, key, msg, nil)
}

func (p *Producer) ProduceSync(topic string, key string, msg *Message) error {
	pdeliveries := deliveryChansPool.Get().(*chan kafka.Event)
	deliveries := *pdeliveries
	defer deliveryChansPool.Put(pdeliveries)

	if err := p.produce(topic, key, msg, deliveries); err != nil {
		return fmt.Errorf("kafka: error producing message to topic '%s': %w", topic, err)
	}

	event := <-deliveries
	delivery, ok := event.(*kafka.Message)
	if !ok {
		return fmt.Errorf("kafka: synchronous produce to topic '%s' received unexpected event type", topic)
	}

	return fmt.Errorf("kafka: error producing message to topic '%s': %w", topic, delivery.TopicPartition.Error)
}

func (p *Producer) Close() {
	if p.markClosed() {
		return
	}

	p.waitClosed()
	close(p.close)
	p.closeWg.Wait()
	p.producer.Close()
}

func (p *Producer) IsClosed() bool {
	return atomic.LoadUint64(&p.state) == producerClosed
}

func (p *Producer) produce(topic string, key string, msg *Message, confirms chan kafka.Event) error {
	if !p.incref() {
		return fmt.Errorf("kafka: producer '%s' already closing or closed", topic)
	}
	defer p.decref()

	var keyBts []byte
	if key != "" {
		keyBts = []byte(key)
	}

	plibheaders := convertHeaders(msg.Headers)
	defer freeLibHeaders(plibheaders)
	libmsg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   keyBts,
		Value: msg.Body,
	}

	return p.producer.Produce(&libmsg, confirms)
}

func (p *Producer) watchDeliveries() {
	defer func() {
		if !p.markClosed() { // if producer should be closed as a result of a fatal error
			p.waitClosed()
			close(p.close)
			p.producer.Close()
		}
		p.closeWg.Done()
	}()

	for {
		select {
		case ev, ok := <-p.deliveries:
			if !ok {
				return
			}

			switch event := ev.(type) {
			case *kafka.Message:
				pheaders := convertLibHeaders(event.Headers)
				msg := Message{Body: event.Value}
				if pheaders != nil {
					msg.Headers = *pheaders
				}

				if err := event.TopicPartition.Error; err != nil && p.onDeliveryError != nil {
					p.onDeliveryError(
						fmt.Errorf("kafka: producer delivery error: %w", err),
						*event.TopicPartition.Topic,
						event.TopicPartition.Partition,
						&msg,
					)
				}

				if err := event.TopicPartition.Error; err == nil && p.onDeliverySuccess != nil {
					p.onDeliverySuccess(
						*event.TopicPartition.Topic,
						event.TopicPartition.Partition,
						int64(event.TopicPartition.Offset),
						&msg,
					)
				}

				freeHeaders(pheaders)
			case kafka.Error:
				if !isKafkalibTransientError(&event) {
					if p.onInternalError != nil {
						p.onInternalError(fmt.Errorf("kafka: got producer fatal error, closing prematurely: %s: %w", event.Code(), event), "unknown")
					}
					return
				}

				if p.onInternalError != nil {
					p.onInternalError(fmt.Errorf("kafka: got producer transient error: %s: %w", event.Code(), event), "unknown")
				}
			}
		case <-p.close:
			return
		}
	}
}

// incref uses a spin-lock pattern
// to check if the producer is
// already closed, if not - increments
// the number of currently active
// produce calls
func (p *Producer) incref() bool {
	for {
		old := atomic.LoadUint64(&p.state)
		if old&producerClosed != 0 {
			return false
		}

		newVal := old + producerRef
		if newVal&producerRefMask == 0 { // overflow
			continue
		}

		if atomic.CompareAndSwapUint64(&p.state, old, newVal) {
			return true
		}
	}
}

func (p *Producer) decref() {
	for {
		old := atomic.LoadUint64(&p.state)
		if old&producerRefMask == 0 {
			panic("kafka: inconsistent producer.incref()")
		}

		newVal := old - producerRef
		if atomic.CompareAndSwapUint64(&p.state, old, newVal) {
			return
		}
	}
}

// refcount returns the number of currently
// active produce calls
func (p *Producer) refcount() int {
	return int(atomic.LoadUint64(&p.state)&producerRefMask) / 2
}

// markClosed uses a spin-lock pattern to check
// if the producer is already closing, otherwise marks
// it as closed.
func (p *Producer) markClosed() (alreadyClosed bool) {
	for {
		old := atomic.LoadUint64(&p.state)
		if old&producerClosed != 0 {
			return true
		}

		newVal := old + producerClosed
		if atomic.CompareAndSwapUint64(&p.state, old, newVal) {
			return false
		}
	}
}

// waitClosed spins and periodically yields
// to the runtime scheduler until there are no
// more active produce calls. Then it waits till
// all outstanding and incoming kafka events are handled properly.
func (p *Producer) waitClosed() {
	// active spinning is never a good idea in user-space,
	// so we cap the spinning time to 5 microseconds
	const yieldDelay = 5 * 1000
	var nextYield int64
	for i := 0; p.refcount() > 0; i++ {
		if i == 0 {
			nextYield = goruntime.Nanotime() + yieldDelay
		}

		if goruntime.Nanotime() < nextYield {
			for x := 0; x < 10 && p.refcount() > 0; x++ {
				goruntime.Procyield(1) // calls the assembly PAUSE instruction once
			}
		} else {
			// switches to g0's OS scheduling stack,
			// puts the current goroutine on to the global run queue
			// and eventually schedules another goroutine to run
			// on the current P's M (OS thread).
			runtime.Gosched()
			nextYield = goruntime.Nanotime() + yieldDelay/2 // spin for another 2.5 microseconds
		}
	}

	p.producer.Flush(int((15 * time.Second).Milliseconds()))
}
