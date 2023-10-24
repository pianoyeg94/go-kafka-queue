package kafkaqueue

import (
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	configBootstrapServers       = "bootstrap.servers"
	configSecurityProtocol       = "security.protocol"
	configSASLMechanism          = "sasl.mechanism"
	configSASLUsername           = "sasl.username"
	configSASLPassword           = "sasl.password"
	configSSLKeyLocation         = "ssl.key.location"
	configSSLKeyPassword         = "ssl.key.password"
	configSSLKeyPEM              = "ssl.key.pem"
	configSSLCertificateLocation = "ssl.certificate.location"
	configSSLCertificatePEM      = "ssl.certificate.pem"
	configSSLCALocation          = "ssl.ca.location"
	configSSLCAPEM               = "ssl.ca.pem"
)

type SecurityProtocol int8

const (
	SecurityProtocolPlaintext SecurityProtocol = iota
	SecurityProtocolSSl
	SecurityProtocolSASLPlaintext
	SecurityProtocolSASL_SSL
)

func (p SecurityProtocol) String() string {
	switch p {
	case SecurityProtocolPlaintext:
		return "plaintext"
	case SecurityProtocolSSl:
		return "ssl"
	case SecurityProtocolSASLPlaintext:
		return "sasl_plaintext"
	case SecurityProtocolSASL_SSL:
		return "sasl_ssl"
	default:
		return ""
	}
}

type SASLMechanism int8

const (
	SASLMechanismNone SASLMechanism = iota - 1
	SASLMechanismGSSAPI
	SASLMechanismPlain
	SASLMechanismScramSHA256
	SASLMechanismScramSHA512
	SASLMechanismOAuthBearer
)

func (s SASLMechanism) String() string {
	switch s {
	case SASLMechanismNone:
		return ""
	case SASLMechanismGSSAPI:
		return "GSSAPI"
	case SASLMechanismPlain:
		return "PLAIN"
	case SASLMechanismScramSHA256:
		return "SCRAM-SHA-256"
	case SASLMechanismScramSHA512:
		return "SCRAM-SHA-512"
	case SASLMechanismOAuthBearer:
		return "OAUTHBEARER"
	default:
		return ""
	}
}

type (
	MessageHeaders map[string][][]byte
	Message        struct {
		Headers MessageHeaders
		Body    []byte
		KafkaId string
	}
)

type InternalErrorCallback func(err error, topic string)

var headersPool = sync.Pool{New: func() interface{} { h := make(MessageHeaders); return &h }}

func convertLibHeaders(libheaders []kafka.Header) (pheaders *MessageHeaders) {
	for i := range libheaders {
		if pheaders == nil {
			pheaders = headersPool.Get().(*MessageHeaders)
		}

		(*pheaders)[libheaders[i].Key] = append((*pheaders)[libheaders[i].Key], libheaders[i].Value)
	}
	return pheaders
}

func freeHeaders(pheaders *MessageHeaders) {
	if pheaders == nil {
		return
	}

	freeHeadersSlow(pheaders) // so the fast path can be inlined
}

func freeHeadersSlow(pheaders *MessageHeaders) {
	for key := range *pheaders {
		delete(*pheaders, key)
	}
	headersPool.Put(pheaders)
}

var libHeadersPool = sync.Pool{New: func() interface{} { lh := make([]kafka.Header, 0); return &lh }}

func convertHeaders(headers MessageHeaders) (plibheaders *[]kafka.Header) {
	for key, value := range headers {
		if plibheaders == nil {
			plibheaders = libHeadersPool.Get().(*[]kafka.Header)
		}

		for _, header := range value {
			*plibheaders = append(*plibheaders, kafka.Header{Key: key, Value: header})
		}
	}
	return plibheaders
}

func freeLibHeaders(plibheaders *[]kafka.Header) {
	if plibheaders == nil {
		return
	}

	freeLibHeadersSlow(plibheaders) // so the fast path can be inlined
}

func freeLibHeadersSlow(plibheaders *[]kafka.Header) {
	for i := range *plibheaders {
		(*plibheaders)[i] = kafka.Header{} // so strings and byte slices can be garbage collected
	}
	*plibheaders = (*plibheaders)[:0] // preserve slice capacity
	libHeadersPool.Put(plibheaders)
}

func isKafkalibTransientError(err *kafka.Error) bool {
	if err.IsRetriable() {
		return true
	}

	switch err.Code() {
	case kafka.ErrFail, kafka.ErrTransport, kafka.ErrAllBrokersDown, kafka.ErrBrokerNotAvailable:
		return true
	default:
		return !err.IsFatal()
	}
}
