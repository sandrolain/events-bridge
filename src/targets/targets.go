package targets

import (
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

type Target interface {
	Consume(<-chan message.Message) error
	Close() error
}

type TargetType string

const (
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

// Configurazioni dei target (target)
type TargetConfig struct {
	Type   TargetType          `yaml:"type" json:"type" validate:"required,oneof=nats redis kafka http coap mqtt grpc plugin"`
	HTTP   *TargetHTTPConfig   `yaml:"http" json:"http"`
	CoAP   *TargetCoAPConfig   `yaml:"coap" json:"coap"`
	MQTT   *TargetMQTTConfig   `yaml:"mqtt" json:"mqtt"`
	Plugin *TargetPluginConfig `yaml:"plugin" json:"plugin"`
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
	Timeout  time.Duration `yaml:"timeout" json:"timeout" validate:"required"`
}

type TargetPluginConfig struct {
	Name   string            `yaml:"name" json:"name" validate:"required"`
	Config map[string]string `yaml:"config" json:"config"`
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
	Timeout time.Duration     `yaml:"timeout" json:"timeout" validate:"required"`
}
