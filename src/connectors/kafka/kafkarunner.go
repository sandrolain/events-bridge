package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/segmentio/kafka-go"
)

// RunnerConfig defines the configuration for a Kafka runner connector.
type RunnerConfig struct {
	// Brokers is the list of Kafka broker addresses.
	// Example: ["localhost:9092", "localhost:9093"]
	Brokers []string `mapstructure:"brokers" validate:"required,min=1"`

	// Topic is the Kafka topic to publish to.
	Topic string `mapstructure:"topic" validate:"required"`

	// Partitions is the number of partitions for the topic.
	// Used when creating the topic if it doesn't exist.
	Partitions int `mapstructure:"partitions" validate:"required,gt=0"`

	// ReplicationFactor is the number of replicas for the topic.
	// Used when creating the topic if it doesn't exist.
	ReplicationFactor int `mapstructure:"replicationFactor" validate:"required,gt=0"`

	// BatchSize is the maximum number of messages to batch before sending.
	// Default: 100
	// Higher values improve throughput but increase latency.
	BatchSize int `mapstructure:"batchSize" default:"100" validate:"min=1"`

	// BatchTimeout is the maximum time to wait for BatchSize messages.
	// Default: 1 second
	// Messages are sent when either BatchSize or BatchTimeout is reached.
	BatchTimeout time.Duration `mapstructure:"batchTimeout" default:"1s"`

	// WriteTimeout is the maximum time to wait for a write operation.
	// Default: 10 seconds
	WriteTimeout time.Duration `mapstructure:"writeTimeout" default:"10s"`

	// RequiredAcks determines the number of broker acknowledgments required.
	// -1 = all in-sync replicas (safest, slowest)
	//  0 = no acknowledgment (fastest, unsafe)
	//  1 = leader only (balanced)
	// Default: -1 (all replicas)
	RequiredAcks int `mapstructure:"requiredAcks" default:"-1" validate:"min=-1,max=1"`

	// Async enables asynchronous writes (fire and forget).
	// Default: false (synchronous, wait for ack)
	// WARNING: Async writes may lose messages on failure.
	Async bool `mapstructure:"async" default:"false"`

	// TLS holds TLS/SSL configuration for secure connections.
	TLS *tlsconfig.Config `mapstructure:"tls"`

	// SASL holds SASL authentication configuration.
	SASL *SASLConfig `mapstructure:"sasl"`
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// NewRunner creates a Kafka runner from options map.
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	l := slog.Default().With("context", "Kafka Runner")

	// Build dialer with TLS and SASL if configured
	dialer, err := buildRunnerDialer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build dialer: %w", err)
	}

	// Create the topic if it does not exist
	err = ensureKafkaTopicWithDialer(l, dialer, cfg.Brokers, cfg.Topic, cfg.Partitions, cfg.ReplicationFactor)
	if err != nil {
		return nil, fmt.Errorf("error creating/verifying topic: %w", err)
	}

	useTLS := cfg.TLS != nil && cfg.TLS.Enabled
	useSASL := cfg.SASL != nil && cfg.SASL.Enabled

	writerConfig := kafka.WriterConfig{
		Brokers:      cfg.Brokers,
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		WriteTimeout: cfg.WriteTimeout,
		RequiredAcks: cfg.RequiredAcks,
		Async:        cfg.Async,
		Dialer:       dialer,
	}

	writer := kafka.NewWriter(writerConfig)

	l.Info("Kafka runner connected",
		"brokers", cfg.Brokers,
		"topic", cfg.Topic,
		"tls", useTLS,
		"sasl", useSASL,
		"batchSize", cfg.BatchSize,
		"async", cfg.Async,
	)

	return &KafkaRunner{
		cfg:    cfg,
		slog:   l,
		writer: writer,
	}, nil
}

// buildRunnerDialer creates a Kafka dialer with TLS and SASL configuration for runners.
func buildRunnerDialer(cfg *RunnerConfig) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// Configure TLS
	if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsConfig, err := cfg.TLS.BuildClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		dialer.TLS = tlsConfig
	}

	// Configure SASL
	if cfg.SASL != nil && cfg.SASL.Enabled {
		mechanism, err := cfg.SASL.BuildSASLMechanism()
		if err != nil {
			return nil, fmt.Errorf("failed to build SASL mechanism: %w", err)
		}
		dialer.SASLMechanism = mechanism
	}

	return dialer, nil
}

type KafkaRunner struct {
	cfg    *RunnerConfig
	slog   *slog.Logger
	writer *kafka.Writer
}

func (r *KafkaRunner) Process(msg *message.RunnerMessage) error {
	metadata, data, err := msg.GetMetadataAndData()
	if err != nil {
		return fmt.Errorf("error getting metadata and data: %w", err)
	}

	r.slog.Debug("publishing Kafka message", "topic", r.cfg.Topic, "bodysize", len(data))

	kmsg := kafka.Message{
		Key:   msg.GetID(),
		Value: data,
	}

	metaLen := len(metadata)
	if metaLen > 0 {
		kmsg.Headers = make([]kafka.Header, 0, metaLen)
		for k, v := range metadata {
			kmsg.Headers = append(kmsg.Headers, kafka.Header{
				Key:   k,
				Value: []byte(v),
			})
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = r.writer.WriteMessages(ctx, kmsg)
	if err != nil {
		return fmt.Errorf("error publishing to Kafka: %w", err)
	}
	r.slog.Debug("Kafka message published", "topic", r.cfg.Topic)
	return nil
}

func (r *KafkaRunner) Close() error {
	if r.writer != nil {
		r.slog.Info("closing Kafka writer")
		if err := r.writer.Close(); err != nil {
			r.slog.Error("error closing Kafka writer", "err", err)
		}
	}
	return nil
}
