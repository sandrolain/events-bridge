package targets

import (
	"github.com/sandrolain/events-bridge/src/targets/coaptarget"
	"github.com/sandrolain/events-bridge/src/targets/httptarget"
	"github.com/sandrolain/events-bridge/src/targets/mqtttarget"
	"github.com/sandrolain/events-bridge/src/targets/plugintarget"
)

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
	Type   TargetType                       `yaml:"type" json:"type" validate:"required,oneof=nats redis kafka http coap mqtt grpc plugin"`
	HTTP   *httptarget.TargetHTTPConfig     `yaml:"http" json:"http"`
	CoAP   *coaptarget.TargetCoAPConfig     `yaml:"coap" json:"coap"`
	MQTT   *mqtttarget.TargetMQTTConfig     `yaml:"mqtt" json:"mqtt"`
	Plugin *plugintarget.TargetPluginConfig `yaml:"plugin" json:"plugin"`
}
