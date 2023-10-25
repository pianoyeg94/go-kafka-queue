# go-kafka-queue

This package allows you to use Kafka as a message queue with at-least-once delivery
semantics out of the box. Each incoming message's `KafkaId` attribute provides a mechanism to achieve idempotance within you consumer handlers (depends on how and where `KafkaId`s will be stored and later retrieved).

All partitions assigned to a single consumer group member are handled concurrently and possibly even in parallel. 

Offsets are committed only after a message is processed by your consumer handler (more precisely messages are commited in batches). It's advisable to set consumer's `AutoOffsetReset` policy to `AutoOffsetResetEarliest`, so that the first member to join a consumer group will be able to read the topic from the start, as if it was a typical queue. After other services join the same
consumer group, they will continue processing messages starting from offsets previously commited by the first member.

NOTE: consumer handler retries shouldn't take to much time, otherwise they will block other upcoming messages from the same partition.

#### Currently lacking
- Although this package is heavily used without a single incident for the last 6 months as a   message bus by a production system with hundreds of microservices, it's still has no test coverage. Thorough unit and integration test coverage is the number one priority to make this package more reliable for end users.

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

#### Create topic
```bash
docker-compose exec kafka-1 kafka-topics \
	--bootstrap-server localhost:29092,kafka-2:29092,kafka-3:29092 \
	--create \
	--topic demo.entity.updated \
	--partitions 3 \
	--replication-factor 3
```

#### Producer options
```go
type ProducerOptions struct {
	// Protocol used to communicate with brokers.
	//
	// Default: SecurityProtocolPlaintext
	SecurityProtocol SecurityProtocol

	// Path to client's private key (PEM) used for authentication.
	SSLKeyLocation string

	// Private key passphrase (for use with SSLCertificateLocation)
	SSLKeyPassword string

	// Client's private key string (PEM format) used for authentication.
	SSLKeyPEM string

	// Path to client's public key (PEM) used for authentication.
	SSLCertificateLocation string

	// Client's public key string (PEM format) used for authentication.
	SSLCertificatePEM string

	// File or directory path to CA certificate(s)
	// for verifying the broker's key.
	// On Linux install the distribution's ca-certificates package.
	// If OpenSSL is statically linked with librdkafka or SSL_CALocation is set to probe
	// a list of standard paths will be probed and the first one found will be used
	// as the default CA certificate location path.
	// If OpenSSL is dynamically linked with librdkafka the OpenSSL library's default
	// path will be used.
	SSLCALocation string

	// CA certificate string (PEM format) for verifying the broker's key.
	SSLCAPEM string

	// SASL mechanism to use for authentication.
	//
	// Default: SASLMechanismNone
	SASLMechanism SASLMechanism
	SASLUsername  string // SASL username for use with SASLMechanismPlain, SASLMechanismScramSHA256 and SASLMechanismScramSHA512.
	SASLPassword  string // SASL password for use with SASLMechanismPlain, SASLMechanismScramSHA256 and SASLMechanismScramSHA512.

	// Check out Partitioner type constants.
	//
	// Default: PartitionerConsistentRandom
	Partitioner Partitioner

	// Delay to wait to assign new sticky partitions for each topic.
	// By default, set to double the time of Linger.
	// The behavior of sticky partitioning affects messages
	// with the key nil in all cases, and messages with key lengths
	// of zero when the PartitionerConsistentRandom partitioner is in use.
	// These messages would otherwise be assigned randomly.
	// A higher value allows for more effective batching of these messages.
	//
	// Default: 10 ms
	StickyPartitioningLinger  time.Duration
	DisableStickyPartitioning bool // Default: false

	// Indicates the number of acknowledgements the leader broker must receive
	// from ISR brokers before responding to the request:
	//   - AcksRequireNone - broker does not send any response/ack to client;
	//   - AcksRequireAll - broker will block until message is committed by all in sync replicas (ISRs).
	// If there are less than min.insync.replicas (broker configuration) in the ISR set
	// the produce request will fail.
	//
	// Default: AcksRequireNone
	RequiredAcks RequiredAcks

	// How many times to retry sending a failing message.
	// Note: retrying may cause reordering unless EnableIdempotence is set to true.
	//
	// Default: 2147483647 (math.MaxInt32)
	Retries int32

	// The backoff time before retrying a protocol request.
	//
	// Default: 100 ms
	RetryBackoff time.Duration

	// Local message timeout.
	// This value is only enforced locally and limits the time
	// a produced message waits for successful delivery. A time of 0 is infinite.
	// This is the maximum time librdkafka may use to deliver a message (including retries).
	// Delivery error occurs when either the retry count or the message timeout are exceeded.
	//
	// Default: 300000 ms
	DeliveryTimeout time.Duration

	// Maximum number of in-flight requests per broker connection.
	// This is a generic property applied to all broker communication,
	// however it is primarily relevant to produce requests.
	// In particular, note that other mechanisms limit the number
	// of outstanding consumer fetch request per broker to one.
	//
	// Default: 1000000
	MaxInFlight int

	// When set to true, the producer will ensure that messages
	// are successfully produced exactly once and in the original produce order.
	// The following configuration properties are adjusted automatically
	// (if not modified by the user) when idempotence is enabled:
	//   - MaxInFlight = 5 (must be less than or equal to 5);
	//   - Retries = math.MaxInt32 (must be greater than 0);
	//   - RequiredAcks = AcksRequireAll.
	// Producer instantation will fail if user-supplied configuration is incompatible.
	//
	// Default: false
	EnableIdempotence bool

	// Compression codec to use for compressing message sets.
	//
	// Default: CompressionNone
	CompressionType CompressionType

	// Delay to wait for messages in the producer queue to accumulate
	// before constructing message batches to transmit to brokers.
	// A higher value allows larger and more effective (less overhead, improved compression)
	// batches of messages to accumulate at the expense of increased message delivery latency.
	//
	// Default: 5 ms
	Linger time.Duration

	// Maximum size (in bytes) of all messages batched in one batch,
	// including protocol framing overhead.
	// This limit is applied after the first message has been added to the batch,
	// regardless of the first message's size, this is to ensure that messages
	// that exceed BatchSize are produced.
	//
	// Default: 16KB
	BatchSize int

	// Maximum total message size sum allowed on the librdkafka producer queue (in bytes).
	// This queue is shared by all partitions.
	//
	// Default: 1048576KB
	QueueBufferingMax int

	// Callback on succesfull delivery.
	//
	// Default: no callback
	DeliverySuccessCallback DeliverySuccessCallback

	// Callback on errored out delivery.
	//
	// Default: no callback
	DeliveryErrorCallback DeliveryErrorCallback

	// Internal error related callback.
	//
	// Default: no callback
	InternalErrorCallback InternalErrorCallback
}
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
			select {
			case <-ctx.Done():
				return nil
			default:
			}
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
		c.currIdx = 1
		return 0
	}

	idx := c.currIdx
	c.currIdx++
	return idx
}
```

#### Consumer options
```go
type ConsumerOptions struct {
	// Protocol used to communicate with brokers.
	//
	// Default: SecurityProtocolPlaintext
	SecurityProtocol SecurityProtocol

	// Path to client's private key (PEM) used for authentication.
	SSLKeyLocation string

	// Private key passphrase (for use with SSLCertificateLocation)
	SSLKeyPassword string

	// Client's private key string (PEM format) used for authentication.
	SSLKeyPEM string

	// Path to client's public key (PEM) used for authentication.
	SSLCertificateLocation string

	// Client's public key string (PEM format) used for authentication.
	SSLCertificatePEM string

	// File or directory path to CA certificate(s)
	// for verifying the broker's key.
	// On Linux install the distribution's ca-certificates package.
	// If OpenSSL is statically linked with librdkafka or SSL_CALocation is set to probe
	// a list of standard paths will be probed and the first one found will be used
	// as the default CA certificate location path.
	// If OpenSSL is dynamically linked with librdkafka the OpenSSL library's default
	// path will be used.
	SSLCALocation string

	// CA certificate string (PEM format) for verifying the broker's key.
	SSLCAPEM string

	// SASL mechanism to use for authentication.
	//
	// Default: SASLMechanismNone
	SASLMechanism SASLMechanism
	SASLUsername  string // SASL username for use with SASLMechanismPlain, SASLMechanismScramSHA256 and SASLMechanismScramSHA512.
	SASLPassword  string // SASL password for use with SASLMechanismPlain, SASLMechanismScramSHA256 and SASLMechanismScramSHA512.

	// Enable static group membership.
	// Static group members are able to leave
	// and rejoin a group within the configured SessionTimeout
	// without prompting a group rebalance.
	// This should be used in combination with a larger SessionTimeout
	// to avoid group rebalances caused by transient unavailability
	// (e.g. process restarts). Requires broker version >= 2.3.0.
	//
	// Default: disabled
	GroupInstanceId string

	// The elected group leader will use a strategy supported by all members
	// of the group to assign partitions to group members.
	// Available strategies: RoundRobinAssignmentStrategy, RangeAssignmentStrategy,
	// CooperativeStickyAssignmentStrategy.
	//
	// Default: RoundRobinAssignmentStrategy
	PartitionAssignmentStrategy PartitionAssignmentStrategy

	// AutoOffsetReset is the action to take when there is no initial offset
	// in offset store or the desired offset is out of range.
	//
	// Default: AutoOffsetResetLatest
	AutoOffsetReset AutoOffsetReset

	// Client group session and failure detection timeout.
	// The consumer sends periodic heartbeats (HeartbeatInterval)
	// to indicate its liveness to the broker.
	// If no hearts are received by the broker for a group member
	// within the session timeout, the broker will remove the consumer
	// from the group and trigger a rebalance.
	// The allowed range is configured with the broker configuration
	// properties group.min.session.timeout.ms and group.max.session.timeout.ms.
	//
	// Default: 45 seconds
	SessionTimeout time.Duration

	// Group session keepalive heartbeat interval.
	// Usually best set to one third of the SessionTimeout.
	// But lower lower values give faster consumer rebalances.
	//
	// Default: 3 seconds
	HeartbeatInterval time.Duration

	// Determines how often we commit offsets.
	//
	// Default: 5 seconds
	CommitInterval time.Duration

	// Determines the number of processed messages
	// before we commit offsets.
	// Can commit earlier if a timer threshold determined
	// by CommitInterval is reached.
	//
	// Default: 100
	CommitThreshold int

	// Size of message queue's buffer
	// per partition.
	//
	// Default: 100
	PartitionBufferSize int

	// Related to consumer handler retries.
	// Values shouldn't be set too high because
	// retries are perfromed in the main loop
	// of a single partition consumer.
	MessageRetriesMaxAttempts  int           // Default: 5
	MessageRetriesInitialDelay time.Duration // Default: 0
	MessageRetriesBackoff      time.Duration // Default: 0
	MessageRetriesMaxDelay     time.Duration // Default: 0

	// Callbacks.
	HandlerSuccessCallback HandlerSuccessCallback // Default: no callback
	HandlerErrorCallback   HandlerErrorCallback   // Default: no callback
	InternalErrorCallback  InternalErrorCallback  // Default: no callback
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
