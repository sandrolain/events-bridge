package sources

import (
	"github.com/sandrolain/events-bridge/src/message"
)

const NewMethodName = "NewSource"

type Source interface {
	Produce(int) (<-chan message.Message, error)
	Close() error
}

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
	SourceTypePGSQL  SourceType = "pgsql"
	SourceTypePlugin SourceType = "plugin"
)

type SourceConfig struct {
	Type   SourceType          `yaml:"type" json:"type" validate:"required,oneof=nats redis kafka http coap mqtt grpc pgsql plugin"`
	Buffer int                 `yaml:"buffer" json:"buffer"`
	HTTP   *SourceHTTPConfig   `yaml:"http" json:"http"`
	CoAP   *SourceCoAPConfig   `yaml:"coap" json:"coap"`
	NATS   *SourceNATSConfig   `yaml:"nats" json:"nats"`
	MQTT   *SourceMQTTConfig   `yaml:"mqtt" json:"mqtt"`
	PgSQL  *SourcePGSQLConfig  `yaml:"pgsql" json:"pgsql"`
	Plugin *SourcePluginConfig `yaml:"plugin" json:"plugin"`
	Kafka  *SourceKafkaConfig  `yaml:"kafka" json:"kafka"`
	PubSub *SourcePubSubConfig `yaml:"pubsub" json:"pubsub"`
}

type CoAPProtocol string

const (
	CoAPProtocolUDP CoAPProtocol = "udp"
	CoAPProtocolTCP CoAPProtocol = "tcp"
)

type SourceCoAPConfig struct {
	Protocol CoAPProtocol `yaml:"protocol" json:"protocol" validate:"required,oneof=udp tcp"`
	Address  string       `yaml:"address" json:"address" validate:"required,hostname_port"`
	Path     string       `yaml:"path" json:"path" validate:"required"`
	Method   string       `yaml:"method" json:"method" validate:"omitempty,oneof=POST PUT GET"`
}

type SourceHTTPConfig struct {
	Address string `yaml:"address" json:"address" validate:"required,hostname_port"`
	Method  string `yaml:"method" json:"method" validate:"required,oneof=POST PUT PATCH"`
	Path    string `yaml:"path" json:"path" validate:"required"`
}

type SourceMQTTConfig struct {
	Address       string `yaml:"address" json:"address" validate:"required,hostname_port"`
	Topic         string `yaml:"topic" json:"topic" validate:"required"`
	ClientID      string `yaml:"client_id" json:"client_id" validate:"omitempty"`
	ConsumerGroup string `yaml:"consumer_group" json:"consumer_group"`
}

type SourceNATSConfig struct {
	Address    string `yaml:"address" json:"address" validate:"required"`
	Stream     string `yaml:"stream" json:"stream"`
	Subject    string `yaml:"subject" json:"subject" validate:"required"`
	Consumer   string `yaml:"consumer" json:"consumer"`     // opzionale: consumer name/id per JetStream
	QueueGroup string `yaml:"queueGroup" json:"queueGroup"` // opzionale: queue group per NATS core
}

type SourcePluginConfig struct {
	Name   string            `yaml:"name" json:"name" validate:"required"`
	Config map[string]string `yaml:"config,omitempty" json:"config,omitempty"`
}

type SourcePGSQLConfig struct {
	ConnString string `yaml:"conn_string" json:"conn_string" validate:"required"`
	Table      string `yaml:"table" json:"table"`
}

type SourceKafkaConfig struct {
	Brokers           []string `yaml:"brokers" json:"brokers" validate:"required,dive,hostname_port"`
	GroupID           string   `yaml:"group_id" json:"group_id"`
	Topic             string   `yaml:"topic" json:"topic" validate:"required"`
	Partitions        int      `yaml:"partitions" json:"partitions"`
	ReplicationFactor int      `yaml:"replication_factor" json:"replication_factor"`
}

type SourcePubSubConfig struct {
	ProjectID         string `yaml:"project_id" json:"project_id" validate:"required"`
	Subscription      string `yaml:"subscription" json:"subscription" validate:"required"`
	CreateIfNotExists bool   `yaml:"create_if_not_exists" json:"create_if_not_exists"`
	Topic             string `yaml:"topic" json:"topic"`
	AckDeadline       int    `yaml:"ack_deadline" json:"ack_deadline"`
	RetainAcked       bool   `yaml:"retain_acked" json:"retain_acked"`
	RetentionDuration int    `yaml:"retention_duration" json:"retention_duration"` // secondi
}

type SourceRedisConfig struct {
	Address string `yaml:"address" json:"address" validate:"required,hostname_port"`
	Channel string `yaml:"channel" json:"channel" validate:"required"`
}
