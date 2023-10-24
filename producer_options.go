package kafkaqueue

import (
	"errors"
	"math"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	configPartitioner                = "partitioner"
	configStickyPartitioningLingerMs = "sticky.partitioning.linger.ms"
	configAcks                       = "acks"
	configRetries                    = "retries"
	configRetryBackoffMs             = "retry.backoff.ms"
	configDeliveryTimeoutMs          = "delivery.timeout.ms"
	configMaxInFlight                = "max.in.flight"
	configEnableIdempotence          = "enable.idempotence"
	configCompressionType            = "compression.type"
	configLingerMs                   = "linger.ms"
	configBatchSize                  = "batch.size"
	configQueueBufferingMaxKbytes    = "queue.buffering.max.kbytes"
)

type Partitioner int8

const (
	// CRC32 hash of key (empty and nil keys are randomly partitioned)
	PartitionerConsistentRandom Partitioner = iota

	// random distribution
	PartitionerRandom

	// CRC32 hash of key (empty and nil keys are mapped to single partition)
	PartitionerConsistent

	// murmur2 hash of key (nil keys are mapped to single partition)
	PartitionerMurmur2

	// murmur2 hash of key (nil keys are randomly partitioned)
	PartitionerMurmur2Random

	// FNV-1a hash of key (nil keys are mapped to single partition)
	PartitionerFNV1a

	// FNV-1a hash of key (nil keys are randomly partitioned)
	PartitionerFNV1aRandom
)

func (p Partitioner) String() string {
	switch p {
	case PartitionerConsistentRandom:
		return "consistent_random"
	case PartitionerRandom:
		return "random"
	case PartitionerConsistent:
		return "consistent"
	case PartitionerMurmur2:
		return "murmur2"
	case PartitionerMurmur2Random:
		return "murmur2_random"
	case PartitionerFNV1a:
		return "fnv1a"
	case PartitionerFNV1aRandom:
		return "fnv1a_random"
	default:
		return "consistent_random"
	}
}

type CompressionType int8

const (
	CompressionNone CompressionType = iota

	CompressionGZIP

	CompressionSnappy

	CompressionLZ4

	CompressionZSTD
)

func (c CompressionType) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionGZIP:
		return "gzip"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	case CompressionZSTD:
		return "zstd"
	default:
		return "none"
	}
}

type RequiredAcks int

const (
	AcksRequireNone RequiredAcks = 0
	AcksRequireOne  RequiredAcks = 1
	AcksRequireAll  RequiredAcks = -1
)

const NoLinger = time.Duration(-1)

func NewProducerOptions() *ProducerOptions {
	return &ProducerOptions{}
}

type ProducerOptions struct {
	// Initial list of bootstrap servers.
	servers []string

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
	// Default: SASLMechanismGSSAPI
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

func (o *ProducerOptions) SetSecurityProtocol(protocol SecurityProtocol) *ProducerOptions {
	o.SecurityProtocol = protocol
	return o
}

func (o *ProducerOptions) SetSSLKeyLocation(path string) *ProducerOptions {
	o.SSLKeyLocation = path
	return o
}

func (o *ProducerOptions) SetSSLKeyPassword(password string) *ProducerOptions {
	o.SSLKeyPassword = password
	return o
}

func (o *ProducerOptions) SetSSLKeyPEM(pem string) *ProducerOptions {
	o.SSLKeyPEM = pem
	return o
}

func (o *ProducerOptions) SetSSLCertificateLocation(path string) *ProducerOptions {
	o.SSLCertificateLocation = path
	return o
}

func (o *ProducerOptions) SetSSLCertificatePEM(pem string) *ProducerOptions {
	o.SSLCertificatePEM = pem
	return o
}

func (o *ProducerOptions) SetSSLCALocation(path string) *ProducerOptions {
	o.SSLCALocation = path
	return o
}

func (o *ProducerOptions) SetSSLCAPEM(pem string) *ProducerOptions {
	o.SSLCAPEM = pem
	return o
}

func (o *ProducerOptions) SetSASLMechanism(mechanism SASLMechanism) *ProducerOptions {
	o.SASLMechanism = mechanism
	return o
}

func (o *ProducerOptions) SetSASLUsername(username string) *ProducerOptions {
	o.SASLUsername = username
	return o
}

func (o *ProducerOptions) SetSASLPassword(password string) *ProducerOptions {
	o.SASLPassword = password
	return o
}

func (o *ProducerOptions) SetPartitioner(partitioner Partitioner) *ProducerOptions {
	o.Partitioner = partitioner
	return o
}

func (o *ProducerOptions) SetStickyPartitioningLinger(linger time.Duration) *ProducerOptions {
	o.StickyPartitioningLinger = linger
	return o
}

func (o *ProducerOptions) SetDisableStickyPartitioning() *ProducerOptions {
	o.DisableStickyPartitioning = true
	return o
}

func (o *ProducerOptions) SetRequiredAcks(acks RequiredAcks) *ProducerOptions {
	o.RequiredAcks = acks
	return o
}

func (o *ProducerOptions) SetRetries(retries int32) *ProducerOptions {
	o.Retries = retries
	return o
}

func (o *ProducerOptions) SetRetryBackoff(backoff time.Duration) *ProducerOptions {
	o.RetryBackoff = backoff
	return o
}

func (o *ProducerOptions) SetDeliveryTimeout(timeout time.Duration) *ProducerOptions {
	o.DeliveryTimeout = timeout
	return o
}

func (o *ProducerOptions) SetMaxInFlight(max int) *ProducerOptions {
	o.MaxInFlight = max
	return o
}

func (o *ProducerOptions) SetEnableIdempotence(enable bool) *ProducerOptions {
	o.EnableIdempotence = enable
	return o
}

func (o *ProducerOptions) SetCompressionType(typ CompressionType) *ProducerOptions {
	o.CompressionType = typ
	return o
}

func (o *ProducerOptions) SetLinger(linger time.Duration) *ProducerOptions {
	o.Linger = linger
	return o
}

func (o *ProducerOptions) SetBatchSize(size int) *ProducerOptions {
	o.BatchSize = size
	return o
}

func (o *ProducerOptions) SetQueueBufferingMax(max int) *ProducerOptions {
	o.QueueBufferingMax = max
	return o
}

func (o *ProducerOptions) SetDeliverySuccessCallback(callback DeliverySuccessCallback) *ProducerOptions {
	o.DeliverySuccessCallback = callback
	return o
}

func (o *ProducerOptions) SetDeliveryErrorCallback(callback DeliveryErrorCallback) *ProducerOptions {
	o.DeliveryErrorCallback = callback
	return o
}

func (o *ProducerOptions) SetInternalErrorCallback(callback InternalErrorCallback) *ProducerOptions {
	o.InternalErrorCallback = callback
	return o
}

func (o *ProducerOptions) setServers(servers []string) *ProducerOptions {
	o.servers = servers
	return o
}

func (o *ProducerOptions) getStickyPartitioningLinger() time.Duration {
	if o.DisableStickyPartitioning {
		return 0
	}

	if o.StickyPartitioningLinger <= 0 {
		return 10 * time.Millisecond
	}

	if o.StickyPartitioningLinger > 900000*time.Millisecond {
		return 900000 * time.Millisecond
	}

	return o.StickyPartitioningLinger
}

func (o *ProducerOptions) getRequiredAcks() RequiredAcks {
	if o.EnableIdempotence {
		return AcksRequireAll
	}

	return o.RequiredAcks
}

func (o *ProducerOptions) getRetries() int {
	if o.EnableIdempotence {
		return int(math.MaxInt32)
	}

	if o.Retries < 0 {
		return 0
	}

	if o.Retries == 0 || o.Retries > 2147483647 {
		return 2147483647
	}

	return int(o.Retries)
}

func (o *ProducerOptions) getRetryBackoff() time.Duration {
	if o.RetryBackoff.Milliseconds() < 1 {
		return 100 * time.Millisecond
	}

	if o.RetryBackoff > 300000*time.Millisecond {
		return 300000 * time.Millisecond
	}

	return o.RetryBackoff
}

func (o *ProducerOptions) getDeliveryTimeout() time.Duration {
	if o.DeliveryTimeout <= 0 {
		return 300000 * time.Millisecond
	}

	if o.DeliveryTimeout > 2147483647*time.Millisecond {
		return 2147483647 * time.Millisecond
	}

	return o.DeliveryTimeout
}

func (o *ProducerOptions) getMaxInFlight() int {
	if o.EnableIdempotence {
		return 5
	}

	if o.MaxInFlight < 1 || o.MaxInFlight > 1000000 {
		return 1000000
	}

	return o.MaxInFlight
}

func (o *ProducerOptions) getLinger() time.Duration {
	if o.Linger < 0 {
		return 0
	}

	if o.Linger == 0 {
		return 5 * time.Millisecond
	}

	if o.Linger > 900000*time.Millisecond {
		return 900000 * time.Millisecond
	}

	return o.Linger
}

func (o *ProducerOptions) getBatchSize() int {
	if o.BatchSize == 0 {
		return 16 << 10 // 16KB
	}

	if o.BatchSize < 0 {
		return 1
	}

	if o.BatchSize > 2147483647 {
		return 2147483647
	}

	return o.BatchSize
}

func (o *ProducerOptions) getQueueBufferingMax() int {
	if o.QueueBufferingMax <= 0 {
		return 1048576 << 10 // 1048576KB
	}

	if o.QueueBufferingMax/(1<<10) < 1 {
		return 1 << 10 // 1KB
	}

	if o.QueueBufferingMax > 2147483647<<10 {
		return 2147483647 << 10 // 2147483647KB
	}

	return o.QueueBufferingMax
}

func (o *ProducerOptions) validate() error {
	if len(o.servers) == 0 {
		return errors.New("kafka: producer requires at least one bootstrap server")
	}

	return nil
}

func (o *ProducerOptions) toConfigMap() *kafka.ConfigMap {
	cm := kafka.ConfigMap{
		configBootstrapServers:           strings.Join(o.servers, ","),
		configPartitioner:                o.Partitioner.String(),
		configStickyPartitioningLingerMs: int(o.getStickyPartitioningLinger().Milliseconds()),
		configAcks:                       int(o.getRequiredAcks()),
		configRetries:                    o.getRetries(),
		configRetryBackoffMs:             int(o.getRetryBackoff().Milliseconds()),
		configDeliveryTimeoutMs:          int(o.getDeliveryTimeout().Milliseconds()),
		configMaxInFlight:                o.getMaxInFlight(),
		configEnableIdempotence:          o.EnableIdempotence,
		configCompressionType:            o.CompressionType.String(),
		configLingerMs:                   int(o.getLinger().Milliseconds()),
		configBatchSize:                  o.getBatchSize(),
		configQueueBufferingMaxKbytes:    o.getQueueBufferingMax() / (1 << 10),
	}

	if proto := o.SecurityProtocol.String(); proto != "" {
		cm.SetKey(configSecurityProtocol, proto)
	}

	if mechanism := o.SASLMechanism.String(); mechanism != "" {
		cm.SetKey(configSASLMechanism, mechanism)
	}

	if o.SSLKeyLocation != "" {
		cm.SetKey(configSSLKeyLocation, o.SSLKeyLocation)
	}

	if o.SSLKeyPassword != "" {
		cm.SetKey(configSSLKeyPassword, o.SSLKeyPassword)
	}

	if o.SSLKeyPEM != "" {
		cm.SetKey(configSSLKeyPEM, o.SSLKeyPEM)
	}

	if o.SSLCertificateLocation != "" {
		cm.SetKey(configSSLCertificateLocation, o.SSLCertificateLocation)
	}

	if o.SSLCertificatePEM != "" {
		cm.SetKey(configSSLCertificatePEM, o.SSLCertificatePEM)
	}

	if o.SSLCALocation != "" {
		cm.SetKey(configSSLCALocation, o.SSLCALocation)
	}

	if o.SSLCAPEM != "" {
		cm.SetKey(configSSLCAPEM, o.SSLCAPEM)
	}

	if o.SASLUsername != "" {
		cm.SetKey(configSASLUsername, o.SASLUsername)
	}

	if o.SASLPassword != "" {
		cm.SetKey(configSASLPassword, o.SASLPassword)
	}

	return &cm
}
