package sources

import (
	"github.com/sandrolain/events-bridge/src/sources/coapsource"
	"github.com/sandrolain/events-bridge/src/sources/httpsource"
	"github.com/sandrolain/events-bridge/src/sources/mqttsource"
	"github.com/sandrolain/events-bridge/src/sources/natssource"
	"github.com/sandrolain/events-bridge/src/sources/pluginsource"
)

type SourceType string

const (
	SourceTypeHTTP   SourceType = "http"
	SourceTypeCoAP   SourceType = "coap"
	SourceTypeFile   SourceType = "file"
	SourceTypeKafka  SourceType = "kafka"
	SourceTypeMQTT   SourceType = "mqtt"
	SourceTypeRedis  SourceType = "redis"
	SourceTypeNATS   SourceType = "nats"
	SourceTypeGRPC   SourceType = "grpc"
	SourceTypePlugin SourceType = "plugin"
)

type SourceConfig struct {
	Type   SourceType                       `yaml:"type" json:"type" validate:"required,oneof=nats redis kafka http coap mqtt grpc plugin"`
	Buffer int                              `yaml:"buffer" json:"buffer" validate:"required"`
	HTTP   *httpsource.SourceHTTPConfig     `yaml:"http" json:"http"`
	CoAP   *coapsource.SourceCoAPConfig     `yaml:"coap" json:"coap"`
	NATS   *natssource.SourceNATSConfig     `yaml:"nats" json:"nats"`
	MQTT   *mqttsource.SourceMQTTConfig     `yaml:"mqtt" json:"mqtt"`
	Plugin *pluginsource.SourcePluginConfig `yaml:"plugin" json:"plugin"`
}
