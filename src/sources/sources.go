package sources

import (
	"github.com/sandrolain/events-bridge/src/message"
)

const NewMethodName = "NewSource"

type Source interface {
	Produce(int) (<-chan *message.RunnerMessage, error)
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
	SourceTypeGit    SourceType = "git"
	SourceTypePlugin SourceType = "plugin"
)

type SourceConfig struct {
	Type   SourceType          `yaml:"type" json:"type" validate:"required,oneof=nats redis kafka http coap mqtt grpc pgsql plugin git"`
	Buffer int                 `yaml:"buffer" json:"buffer"`
	HTTP   *SourceHTTPConfig   `yaml:"http" json:"http" validate:"omitempty,required_if=Type http"`
	CoAP   *SourceCoAPConfig   `yaml:"coap" json:"coap" validate:"omitempty,required_if=Type coap"`
	NATS   *SourceNATSConfig   `yaml:"nats" json:"nats" validate:"omitempty,required_if=Type nats"`
	MQTT   *SourceMQTTConfig   `yaml:"mqtt" json:"mqtt" validate:"omitempty,required_if=Type mqtt"`
	PgSQL  *SourcePGSQLConfig  `yaml:"pgsql" json:"pgsql" validate:"omitempty,required_if=Type pgsql"`
	Plugin *SourcePluginConfig `yaml:"plugin" json:"plugin" validate:"omitempty,required_if=Type plugin"`
	Kafka  *SourceKafkaConfig  `yaml:"kafka" json:"kafka" validate:"omitempty,required_if=Type kafka"`
	Redis  *SourceRedisConfig  `yaml:"redis" json:"redis" validate:"omitempty,required_if=Type redis"`
	PubSub *SourcePubSubConfig `yaml:"pubsub" json:"pubsub" validate:"omitempty,required_if=Type pubsub"`
	Git    *SourceGitConfig    `yaml:"git" json:"git" validate:"omitempty,required_if=Type git"`
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
	Consumer   string `yaml:"consumer" json:"consumer"`     // optional: consumer name/id for JetStream
	QueueGroup string `yaml:"queueGroup" json:"queueGroup"` // optional: queue group for NATS core
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
	RetentionDuration int    `yaml:"retention_duration" json:"retention_duration"` // seconds
}

type SourceRedisConfig struct {
	Address string `yaml:"address" json:"address" validate:"required,hostname_port"`
	// PubSub
	Channel string `yaml:"channel" json:"channel"`
	// Stream
	Stream        string `yaml:"stream" json:"stream"`
	ConsumerGroup string `yaml:"consumer_group,omitempty" json:"consumer_group,omitempty"`
	ConsumerName  string `yaml:"consumer_name,omitempty" json:"consumer_name,omitempty"`
	StreamDataKey string `yaml:"stream_data_key" json:"stream_data_key"`
}

type SourceGitConfig struct {
	Path         string `yaml:"path" json:"path"`                                 // local repo path (optional, if empty will use temp dir)
	RemoteURL    string `yaml:"remote_url" json:"remote_url" validate:"required"` // remote repo URL
	Remote       string `yaml:"remote" json:"remote"`                             // remote name, default "origin"
	Branch       string `yaml:"branch" json:"branch" validate:"required"`         // branch name
	Username     string `yaml:"username" json:"username"`                         // optional: username for remote
	Password     string `yaml:"password" json:"password"`                         // optional: password/token for remote
	SubDir       string `yaml:"subdir" json:"subdir"`                             // optional: limit to subdir
	PollInterval int    `yaml:"poll_interval" json:"poll_interval"`               // seconds, default 10
}
