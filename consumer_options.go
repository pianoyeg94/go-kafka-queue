package kafkaqueue

import (
	"errors"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	configGroupId                      = "group.id"
	configGroupInstanceId              = "group.instance.id"
	configPartitionAssignmentStrategy  = "partition.assignment.strategy"
	configAutoOffsetReset              = "auto.offset.reset"
	configSessionTimeoutMs             = "session.timeout.ms"
	configHeartbeatIntervalMs          = "heartbeat.interval.ms"
	configEnableAutoCommit             = "enable.auto.commit"
	configAutoCommitIntervalMs         = "auto.commit.interval.ms"
	configEnableAutoOffsetStore        = "enable.auto.offset.store"
	configGoEventsChannelEnable        = "go.events.channel.enable"
	configGoEventsChannelSize          = "go.events.channel.size"
	configGoApplicationRebalanceEnable = "go.application.rebalance.enable"
)

type PartitionAssignmentStrategy int8

const (
	RoundRobinAssignmentStrategy PartitionAssignmentStrategy = iota
	RangeAssignmentStrategy
	CooperativeStickyAssignmentStrategy
)

func (s PartitionAssignmentStrategy) String() string {
	switch s {
	case RoundRobinAssignmentStrategy:
		return "round-robin"
	case RangeAssignmentStrategy:
		return "range"
	case CooperativeStickyAssignmentStrategy:
		return "cooperative-sticky"
	default:
		return "round-robin"
	}
}

type AutoOffsetReset int8

const (
	AutoOffsetResetLatest AutoOffsetReset = iota
	AutoOffsetResetEarliest
)

func (r AutoOffsetReset) String() string {
	switch r {
	case AutoOffsetResetLatest:
		return "latest"
	case AutoOffsetResetEarliest:
		return "earliest"
	default:
		return "latest"
	}
}

func NewConsumerOptions() *ConsumerOptions {
	return &ConsumerOptions{}
}

type ConsumerOptions struct {
	// Initial list of bootstrap servers
	servers []string

	// groupId is the client group id string.
	// All clients sharing the same groupId
	// belong to the same group.
	groupId string

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

	// Override any options set above, as well as give the ability
	// to additionaly configure your kafka client.
	// Come from https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md.
	// Valid keys are the ones marked with "*" and "C" (C/P column).
	rawOptions kafka.ConfigMap
}

func (o *ConsumerOptions) SetSecurityProtocol(protocol SecurityProtocol) *ConsumerOptions {
	o.SecurityProtocol = protocol
	return o
}

func (o *ConsumerOptions) SetSSLKeyLocation(path string) *ConsumerOptions {
	o.SSLKeyLocation = path
	return o
}

func (o *ConsumerOptions) SetSSLKeyPassword(password string) *ConsumerOptions {
	o.SSLKeyPassword = password
	return o
}

func (o *ConsumerOptions) SetSSLKeyPEM(pem string) *ConsumerOptions {
	o.SSLKeyPEM = pem
	return o
}

func (o *ConsumerOptions) SetSSLCertificateLocation(path string) *ConsumerOptions {
	o.SSLCertificateLocation = path
	return o
}

func (o *ConsumerOptions) SetSSLCertificatePEM(pem string) *ConsumerOptions {
	o.SSLCertificatePEM = pem
	return o
}

func (o *ConsumerOptions) SetSSLCALocation(path string) *ConsumerOptions {
	o.SSLCALocation = path
	return o
}

func (o *ConsumerOptions) SetSSLCAPEM(pem string) *ConsumerOptions {
	o.SSLCAPEM = pem
	return o
}

func (o *ConsumerOptions) SetSASLMechanism(mechanism SASLMechanism) *ConsumerOptions {
	o.SASLMechanism = mechanism
	return o
}

func (o *ConsumerOptions) SetSASLUsername(username string) *ConsumerOptions {
	o.SASLUsername = username
	return o
}

func (o *ConsumerOptions) SetSASLPassword(password string) *ConsumerOptions {
	o.SASLPassword = password
	return o
}

func (o *ConsumerOptions) SetGroupInstanceId(id string) *ConsumerOptions {
	o.GroupInstanceId = id
	return o
}

func (o *ConsumerOptions) SetPartitionAssignmentStrategy(strategy PartitionAssignmentStrategy) *ConsumerOptions {
	o.PartitionAssignmentStrategy = strategy
	return o
}

func (o *ConsumerOptions) SetAutoOffsetReset(mode AutoOffsetReset) *ConsumerOptions {
	o.AutoOffsetReset = mode
	return o
}

func (o *ConsumerOptions) SetSessionTimeout(timeout time.Duration) *ConsumerOptions {
	o.SessionTimeout = timeout
	return o
}

func (o *ConsumerOptions) SetHeartbeatInterval(interval time.Duration) *ConsumerOptions {
	o.HeartbeatInterval = interval
	return o
}

func (o *ConsumerOptions) SetCommitInterval(interval time.Duration) *ConsumerOptions {
	o.CommitInterval = interval
	return o
}

func (o *ConsumerOptions) SetCommitThreshold(threshold int) *ConsumerOptions {
	o.CommitThreshold = threshold
	return o
}

func (o *ConsumerOptions) SetPartitionBufferSize(size int) *ConsumerOptions {
	o.PartitionBufferSize = size
	return o
}

func (o *ConsumerOptions) SetMessageRetriesMaxAttempts(attempts int) *ConsumerOptions {
	o.MessageRetriesMaxAttempts = attempts
	return o
}

func (o *ConsumerOptions) SetMessageRetriesInitialDelay(delay time.Duration) *ConsumerOptions {
	o.MessageRetriesInitialDelay = delay
	return o
}

func (o *ConsumerOptions) SetMessageRetriesBackoff(delay time.Duration) *ConsumerOptions {
	o.MessageRetriesBackoff = delay
	return o
}

func (o *ConsumerOptions) SetMessageRetriesMaxDelay(delay time.Duration) *ConsumerOptions {
	o.MessageRetriesMaxDelay = delay
	return o
}

func (o *ConsumerOptions) SetHandlerSuccessCallback(callback HandlerSuccessCallback) *ConsumerOptions {
	o.HandlerSuccessCallback = callback
	return o
}

func (o *ConsumerOptions) SetHandlerErrorCallback(callback HandlerErrorCallback) *ConsumerOptions {
	o.HandlerErrorCallback = callback
	return o
}

func (o *ConsumerOptions) SetInternalErrorCallback(callback InternalErrorCallback) *ConsumerOptions {
	o.InternalErrorCallback = callback
	return o
}

func (o *ConsumerOptions) SetRawOption(key string, value interface{}) *ConsumerOptions {
	if o.rawOptions == nil {
		o.rawOptions = make(kafka.ConfigMap, 1)
	}
	o.rawOptions.SetKey(key, value)
	return o
}

func (o *ConsumerOptions) setServers(servers []string) *ConsumerOptions {
	o.servers = servers
	return o
}

func (o *ConsumerOptions) setGroupId(id string) *ConsumerOptions {
	o.groupId = id
	return o
}

func (o *ConsumerOptions) getSessionTimeout() time.Duration {
	if o.SessionTimeout <= 0 {
		return 45000 * time.Millisecond
	}

	if dflt := 3600000 * time.Millisecond; o.SessionTimeout > dflt {
		return dflt
	}

	return o.SessionTimeout
}

func (o *ConsumerOptions) getHeartbeatInterval() time.Duration {
	if o.HeartbeatInterval <= 0 {
		return 3000 * time.Millisecond
	}

	if dflt := 3600000 * time.Millisecond; o.HeartbeatInterval > dflt {
		return dflt
	}

	return o.HeartbeatInterval
}

func (o *ConsumerOptions) getCommitInterval() time.Duration {
	if o.CommitInterval <= 0 {
		return 5000 * time.Millisecond
	}

	return o.CommitInterval
}

func (o *ConsumerOptions) getCommitThreshold() int {
	if o.CommitThreshold <= 0 {
		return 100
	}

	return o.CommitThreshold
}

func (o *ConsumerOptions) getPartitionBufferSize() int {
	if o.PartitionBufferSize <= 0 {
		return 100
	}

	return o.PartitionBufferSize
}

func (o *ConsumerOptions) getMessageRetriesMaxAttempts() int {
	if o.MessageRetriesMaxAttempts <= 0 {
		return 5
	}

	return o.MessageRetriesMaxAttempts
}

func (o *ConsumerOptions) validate() error {
	if len(o.servers) == 0 {
		return errors.New("kafka: consumer requires at least one bootstrap server")
	}

	if o.groupId == "" {
		return errors.New("kafka: consumer groupId should not be an empty string")
	}

	return nil
}

func (o *ConsumerOptions) toConfigMap() *kafka.ConfigMap {
	cm := kafka.ConfigMap{
		configBootstrapServers:             strings.Join(o.servers, ","),
		configGroupId:                      o.groupId,
		configEnableAutoCommit:             false,
		configAutoCommitIntervalMs:         0,
		configGoEventsChannelEnable:        true,
		configGoEventsChannelSize:          1,
		configGoApplicationRebalanceEnable: true,
		configEnableAutoOffsetStore:        false,
		configPartitionAssignmentStrategy:  o.PartitionAssignmentStrategy.String(),
		configAutoOffsetReset:              o.AutoOffsetReset.String(),
		configSessionTimeoutMs:             int(o.getSessionTimeout().Milliseconds()),
		configHeartbeatIntervalMs:          int(o.getHeartbeatInterval().Milliseconds()),
	}

	if proto := o.SecurityProtocol.String(); proto != "" {
		cm.SetKey(configSecurityProtocol, proto)
	}

	if mechanism := o.SASLMechanism.String(); mechanism != "" {
		cm.SetKey(configSASLMechanism, o.SASLMechanism.String())
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

	if id := o.GroupInstanceId; id != "" {
		cm.SetKey(configGroupInstanceId, id)
	}

	for key, value := range o.rawOptions {
		cm.SetKey(key, value)
	}

	return &cm
}
