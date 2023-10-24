# go-kafka-queue

#### Create topic
```bash
docker-compose exec kafka-1 kafka-topics \
	--bootstrap-server localhost:29092,kafka-2:29092,kafka-3:29092 \
	--create \
	--topic demo.entity.updated \
	--partitions 3 \
	--replication-factor 3
```

#### Producer main.go
```go
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"
	"unicode/utf8"

	kafka "github.com/pianoyeg94/go-kafka-queue"
)

const (
	topicEntityUpdated = "demo.entity.updated"

	produceEvery = 50 * time.Millisecond
)

var servers = [...]string{"localhost:9092", "localhost:9102", "localhost:9202"}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	logger := log.New(os.Stderr, "producer: ", log.LstdFlags|log.Lmsgprefix)
	if err := run(ctx, logger); err != nil {
		logger.Fatalln(err)
	}
}

func run(ctx context.Context, logger *log.Logger) error {
	var opts kafka.ProducerOptions
	populateProducerOptions(&opts)
	setProducerCallbacks(logger, &opts)
	producer, err := kafka.NewProducer(ctx, servers[:], &opts)
	if err != nil {
		return fmt.Errorf("error starting producer: %w", err)
	}
	defer producer.Close()

	entities := []struct {
		Name   string `json:"name"`
		Amount int    `json:"amount"`
	}{
		{Name: "John"},
		{Name: "Elizabeth"},
		{Name: "Tom"},
	}
	cycle := newIndexCycle(len(entities) - 1)
	for i, idx := 0, cycle.next(); ; i, idx = i+1, cycle.next() {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(produceEvery):
		}

		entities[idx].Amount = i
		body, err := json.Marshal(&entities[idx])
		if err != nil {
			continue
		}

		if err = producer.Produce(
			topicEntityUpdated,
			entities[idx].Name, // key
			&kafka.Message{Body: body},
		); err != nil {
			logger.Printf(`Error delivering message to kafka:
				topic=%s
				key=%s
				body=%s
				headers={}
				err=%s
				`,
				topicEntityUpdated,
				entities[idx].Name,
				string(body),
				err,
			)
		}
	}
}

func populateProducerOptions(opts *kafka.ProducerOptions) {
	opts.SetSecurityProtocol(kafka.SecurityProtocolPlaintext).
		SetRequiredAcks(kafka.AcksRequireAll).
		SetRetries(math.MaxInt32).
		SetMaxInFlight(5).
		SetEnableIdempotence(true).
		SetPartitioner(kafka.PartitionerConsistentRandom).
		SetCompressionType(kafka.CompressionSnappy)
}

func setProducerCallbacks(logger *log.Logger, opts *kafka.ProducerOptions) {
	opts.SetDeliveryErrorCallback(func(err error, topic string, partition int32, msg *kafka.Message) {
		logger.Printf(`Error delivering message to kafka:
			topic=%s
			partition=%d
			headers=%s
			body=%s
			err=%s
			`,
			topic,
			partition,
			messageHeadersToJSON(msg.Headers),
			string(msg.Body),
			err,
		)
	})
	opts.SetDeliverySuccessCallback(func(topic string, partition int32, offset int64, msg *kafka.Message) {
		logger.Printf(`Message delivered successfully to Kafka:
			topic=%s
			partition=%d
			headers=%s
			body=%s
			`,
			topic,
			partition,
			messageHeadersToJSON(msg.Headers),
			string(msg.Body),
		)
	})
	opts.SetInternalErrorCallback(func(err error, topic string) {
		logger.Printf(`Got producer internal error:
			topic=%s
			err=%s`,
			topic,
			err,
		)
	})
}

func messageHeadersToJSON(headers kafka.MessageHeaders) string {
	jheaders := "{}"
	if len(headers) == 0 {
		return jheaders
	}

	hmap := make(map[string][]string, len(headers))
	for k, v := range headers {
		for _, b := range v {
			if utf8.Valid(b) {
				hmap[k] = append(hmap[k], string(b))
				continue
			}
			hmap[k] = append(hmap[k], hex.EncodeToString(b))
		}
	}

	if b, err := json.Marshal(hmap); err == nil {
		jheaders = string(b)
	}

	return jheaders
}

func newIndexCycle(maxIdx int) *indexCycle {
	return &indexCycle{maxIdx: maxIdx}
}

type indexCycle struct {
	maxIdx  int
	currIdx int
}

func (c *indexCycle) next() int {
	if c.currIdx > c.maxIdx {
		c.currIdx = 0
		return c.currIdx
	}

	idx := c.currIdx
	c.currIdx++
	return idx
}
```

#### Consumer main.go
```go
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	kafka "github.com/pianoyeg94/go-kafka-queue"
)

const topicEntityUpdated = "demo.entity.updated"

var (
	groupId         string
	groupInstanceId string

	servers = [...]string{"localhost:9092", "localhost:9102", "localhost:9202"}
)

func init() {
	flag.StringVar(&groupId, "group.id", "demo", "consumer group.id")
	flag.StringVar(&groupInstanceId, "group.instance.id", "", "consumer group.instance.id")
	flag.Parse()
	if groupInstanceId != "" {
		groupInstanceId = fmt.Sprintf("%s-%s", groupId, groupInstanceId)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	logger := log.New(os.Stderr, "consumer: ", log.LstdFlags|log.Lmsgprefix)
	if err := run(ctx, logger); err != nil {
		logger.Fatalln(err)
	}
}

func run(ctx context.Context, logger *log.Logger) error {
	var opts kafka.ConsumerOptions
	populateConsumerOptions(&opts)
	setConsumerCallbacks(logger, &opts)
	consumer, err := kafka.NewConsumer(ctx, servers[:], topicEntityUpdated, groupId, &opts)
	if err != nil {
		return fmt.Errorf("error starting consumer: %w", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Printf("Error closing consumer: %s", err)
		}
	}()

	if err = consumer.Consume(executionTimeMiddlewareFactory(logger, entityUpdated)); err != nil {
		return fmt.Errorf("error starting consumer: %w", err)
	}

	<-ctx.Done()
	return nil
}

func entityUpdated(ctx context.Context, msg *kafka.Message) error {
	topic, partition, offset := parseKafkaId(msg.KafkaId)
	log.Printf(`Processed kafka message:
		topic=%s
		partition=%d
		offset=%d
		headers=%s
		body=%s
		`,
		topic,
		partition,
		offset,
		messageHeadersToJSON(msg.Headers),
		string(msg.Body),
	)
	return nil
}

func executionTimeMiddlewareFactory(logger *log.Logger, h kafka.HandleFunc) kafka.HandleFunc {
	return func(ctx context.Context, msg *kafka.Message) error {
		start := time.Now()
		err := h(ctx, msg)
		logger.Printf(
			"It took %s entityUpdated event handler to process the incoming event corresponding to the following kafkaId: %s",
			time.Since(start),
			msg.KafkaId,
		)
		return err
	}
}

func populateConsumerOptions(opts *kafka.ConsumerOptions) {
	opts.SetSecurityProtocol(kafka.SecurityProtocolPlaintext).
		SetPartitionAssignmentStrategy(kafka.CooperativeStickyAssignmentStrategy).
		SetAutoOffsetReset(kafka.AutoOffsetResetEarliest).
		SetCommitInterval(5 * time.Second).
		SetCommitThreshold(100).
		SetPartitionBufferSize(100).
		SetMessageRetriesMaxAttempts(5).
		SetMessageRetriesInitialDelay(0).
		SetMessageRetriesBackoff(500 * time.Millisecond).
		SetMessageRetriesMaxDelay(2 * time.Second)

	if groupInstanceId != "" {
		opts.SetGroupInstanceId(groupInstanceId)
	}
}

func setConsumerCallbacks(logger *log.Logger, opts *kafka.ConsumerOptions) {
	opts.SetHandlerErrorCallback(func(err error, topic string, groupId string, partition int32, offset int64, msg *kafka.Message) {
		logger.Printf(`Error handling kafka message:
			ntopic=%s
			groupId=%s
			partition=%d
			offset=%d
			headers=%s
			body=%s
			err=%s
			`,
			topic,
			groupId,
			partition,
			offset,
			messageHeadersToJSON(msg.Headers),
			string(msg.Body),
			err,
		)
	})
	opts.SetInternalErrorCallback(func(err error, topic string) {
		logger.Printf(`Got consumer internal error:
			topic=%s
			err=%s
			`,
			topic,
			err,
		)
	})
}

func parseKafkaId(id string) (topic string, partition, offset int) {
	parts := strings.Split(id, "_")
	if len(parts) != 3 {
		return "", 0, 0
	}

	partition, _ = strconv.Atoi(parts[1])
	offset, _ = strconv.Atoi(parts[2])
	return parts[0], partition, offset
}

func messageHeadersToJSON(headers kafka.MessageHeaders) string {
	jheaders := "{}"
	if len(headers) == 0 {
		return jheaders
	}

	hmap := make(map[string][]string, len(headers))
	for k, v := range headers {
		for _, b := range v {
			if utf8.Valid(b) {
				hmap[k] = append(hmap[k], string(b))
				continue
			}
			hmap[k] = append(hmap[k], hex.EncodeToString(b))
		}
	}

	if b, err := json.Marshal(hmap); err == nil {
		jheaders = string(b)
	}

	return jheaders
}
```

#### docker-compose.yaml
```yaml
version: '3.9'

services:
  zookeeper-1:
    image: confluentinc/cp-zookeeper:7.0.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SERVER_ID: 1
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888;zookeeper-2:2888:3888;zookeeper-3:2888:3888
      ZOOKEEPER_TICK_TIME: 2000
      ZOOKEEPER_INIT_LIMIT: 10
      ZOOKEEPER_SYNC_LIMIT: 5
    ports:
      - 22181:2181

  zookeeper-2:
    image: confluentinc/cp-zookeeper:7.0.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SERVER_ID: 2
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888;zookeeper-2:2888:3888;zookeeper-3:2888:3888
      ZOOKEEPER_TICK_TIME: 2000
      ZOOKEEPER_INIT_LIMIT: 10
      ZOOKEEPER_SYNC_LIMIT: 5
    ports:
      - 22182:2181

  zookeeper-3:
    image: confluentinc/cp-zookeeper:7.0.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SERVER_ID: 3
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888;zookeeper-2:2888:3888;zookeeper-3:2888:3888
      ZOOKEEPER_TICK_TIME: 2000
      ZOOKEEPER_INIT_LIMIT: 10
      ZOOKEEPER_SYNC_LIMIT: 5
    ports:
      - 22183:2181

  kafka-1:
    image: confluentinc/cp-kafka:latest
    restart: always
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_LISTENERS: INTERN://0.0.0.0:29092,EXTERN://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: INTERN://kafka-1:29092,EXTERN://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERN:PLAINTEXT,EXTERN:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERN
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_LOG4J_ROOT_LOGLEVEL: INFO
    ports:
      - 9092:9092

  kafka-2:
    image: confluentinc/cp-kafka:latest
    restart: always
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_LISTENERS: INTERN://0.0.0.0:29092,EXTERN://0.0.0.0:9102
      KAFKA_ADVERTISED_LISTENERS: INTERN://kafka-2:29092,EXTERN://localhost:9102
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERN:PLAINTEXT,EXTERN:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERN
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_LOG4J_ROOT_LOGLEVEL: INFO
    ports:
      - 9102:9102

  kafka-3:
    image: confluentinc/cp-kafka:latest
    restart: always
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_LISTENERS: INTERN://0.0.0.0:29092,EXTERN://0.0.0.0:9202
      KAFKA_ADVERTISED_LISTENERS: INTERN://kafka-3:29092,EXTERN://localhost:9202
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERN:PLAINTEXT,EXTERN:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERN
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_LOG4J_ROOT_LOGLEVEL: INFO
    ports:
      - 9202:9202
```