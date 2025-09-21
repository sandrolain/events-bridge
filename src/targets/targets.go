package targets

import (
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

const NewMethodName = "NewTarget"

type Target interface {
	Consume(*message.RunnerMessage) error
	Close() error
}

type TargetType string

const (
	TargetTypeNone   TargetType = "none"
	TargetTypeHTTP   TargetType = "http"
	TargetTypeCoAP   TargetType = "coap"
	TargetTypeFile   TargetType = "file"
	TargetTypeKafka  TargetType = "kafka"
	TargetTypeMQTT   TargetType = "mqtt"
	TargetTypeRedis  TargetType = "redis"
	TargetTypeNATS   TargetType = "nats"
	TargetTypeGRPC   TargetType = "grpc"
	TargetTypePlugin TargetType = "plugin"
)

const DefaultTimeout = 5 * time.Second

// Target configurations
type TargetConfig struct {
	Type     TargetType          `yaml:"type" json:"type" validate:"required,oneof=nats redis kafka http coap mqtt grpc plugin"`
	Routines int                 `yaml:"routines" json:"routines" validate:"omitempty,min=1"`
	HTTP     *TargetHTTPConfig   `yaml:"http" json:"http"`
	CoAP     *TargetCoAPConfig   `yaml:"coap" json:"coap"`
	MQTT     *TargetMQTTConfig   `yaml:"mqtt" json:"mqtt"`
	NATS     *TargetNATSConfig   `yaml:"nats" json:"nats"`
	Kafka    *TargetKafkaConfig  `yaml:"kafka" json:"kafka"`
	Redis    *TargetRedisConfig  `yaml:"redis" json:"redis"`
	Plugin   *TargetPluginConfig `yaml:"plugin" json:"plugin" validate:"omitempty"`
	PubSub   *TargetPubSubConfig `yaml:"pubsub" json:"pubsub"`
}

type CoAPProtocol string

const (
	CoAPProtocolUDP CoAPProtocol = "udp"
	CoAPProtocolTCP CoAPProtocol = "tcp"
)

type TargetCoAPConfig struct {
	Protocol CoAPProtocol  `yaml:"protocol" json:"protocol" validate:"required,oneof=udp tcp"`
	Address  string        `yaml:"address" json:"address" validate:"required,hostname_port"`
	Path     string        `yaml:"path" json:"path" validate:"required"`
	Method   string        `yaml:"method" json:"method" validate:"omitempty,oneof=POST PUT GET"`
	Timeout  time.Duration `yaml:"timeout" json:"timeout"`
}

type TargetPluginConfig struct {
	Name    string            `yaml:"name" json:"name" validate:"required"`
	Config  map[string]string `yaml:"config" json:"config"`
	Timeout time.Duration     `yaml:"timeout" json:"timeout"`
}

type TargetMQTTConfig struct {
	Address              string `yaml:"address" json:"address" validate:"required,hostname_port"`
	Topic                string `yaml:"topic" json:"topic" validate:"required"`
	ClientID             string `yaml:"clientID" json:"clientID" validate:"omitempty,alphanum"`
	QoS                  int    `yaml:"qos" json:"qos" validate:"omitempty,min=0,max=2"`
	TopicFromMetadataKey string `yaml:"topicFromMetadataKey" json:"topicFromMetadataKey"`
}

type TargetHTTPConfig struct {
	Method  string            `yaml:"method" json:"method" validate:"omitempty,oneof=POST PUT PATCH"`
	URL     string            `yaml:"url" json:"url" validate:"required"`
	Headers map[string]string `yaml:"headers" json:"headers" validate:"omitempty,dive"`
	Timeout time.Duration     `yaml:"timeout" json:"timeout"`
}

type TargetNATSConfig struct {
	Address                string        `yaml:"address" json:"address" validate:"required,hostname_port"`
	Subject                string        `yaml:"subject" json:"subject" validate:"required"`
	SubjectFromMetadataKey string        `yaml:"subjectFromMetadataKey" json:"subjectFromMetadataKey"`
	Timeout                time.Duration `yaml:"timeout" json:"timeout"`
}

// Kafka target config
// Brokers: list of brokers (e.g., ["localhost:9092"])
// Topic: topic name
// KeyFromMetadataKey: optional, key from metadata
type TargetKafkaConfig struct {
	Brokers           []string `yaml:"brokers" json:"brokers" validate:"required,dive,hostname_port"`
	Topic             string   `yaml:"topic" json:"topic" validate:"required"`
	Partitions        int      `yaml:"partitions" json:"partitions"`
	ReplicationFactor int      `yaml:"replication_factor" json:"replication_factor"`
}

type TargetPubSubConfig struct {
	ProjectID string `yaml:"project_id" json:"project_id" validate:"required"`
	Topic     string `yaml:"topic" json:"topic" validate:"required"`
}

type TargetRedisConfig struct {
	Address string `yaml:"address" json:"address" validate:"required,hostname_port"`
	// PubSub
	Channel                string `yaml:"channel" json:"channel"`
	ChannelFromMetadataKey string `yaml:"channelFromMetadataKey" json:"channelFromMetadataKey"`
	// Stream
	Stream                string        `yaml:"stream" json:"stream"`
	StreamFromMetadataKey string        `yaml:"streamFromMetadataKey" json:"streamFromMetadataKey"`
	ConsumerGroup         string        `yaml:"consumer_group,omitempty" json:"consumer_group,omitempty"`
	ConsumerName          string        `yaml:"consumer_name,omitempty" json:"consumer_name,omitempty"`
	Timeout               time.Duration `yaml:"timeout" json:"timeout"`
	StreamDataKey         string        `yaml:"stream_data_key" json:"stream_data_key"`
}
