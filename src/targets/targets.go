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
	TargetTypeHTTP   TargetType = "http"
	TargetTypeCoAP   TargetType = "coap"
	TargetTypeKafka  TargetType = "kafka"
	TargetTypeMQTT   TargetType = "mqtt"
	TargetTypeRedis  TargetType = "redis"
	TargetTypeNATS   TargetType = "nats"
	TargetTypeGRPC   TargetType = "grpc"
	TargetTypePubSub TargetType = "pubsub"
	TargetTypePlugin TargetType = "plugin"
	TargetTypeNone   TargetType = "none"
)

var DefaultTimeout = 5 * time.Second

type TargetConfig struct {
	Type     TargetType          `yaml:"type" json:"type" validate:"required,oneof=nats redis kafka http coap mqtt grpc pubsub plugin none"`
	Buffer   int                 `yaml:"buffer" json:"buffer"`
	Routines int                 `yaml:"routines" json:"routines" validate:"omitempty,min=1"`
	Options  map[string]any      `yaml:"options" json:"options"`
	Plugin   *TargetPluginConfig `yaml:"plugin" json:"plugin" validate:"omitempty,required_if=Type plugin"`
}

type TargetPluginConfig struct {
	Name    string            `yaml:"name" json:"name" validate:"required"`
	Timeout time.Duration     `yaml:"timeout" json:"timeout"`
	Config  map[string]string `yaml:"config,omitempty" json:"config,omitempty"`
}
