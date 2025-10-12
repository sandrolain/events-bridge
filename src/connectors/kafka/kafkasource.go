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

// SourceConfig defines the configuration for a Kafka source connector.
type SourceConfig struct {
	// Brokers is the list of Kafka broker addresses.
	// Example: ["localhost:9092", "localhost:9093"]
	Brokers []string `mapstructure:"brokers" validate:"required,min=1"`

	// GroupID is the consumer group ID for load balancing and offset management.
	// If empty, each consumer will read all partitions independently.
	GroupID string `mapstructure:"groupId"`

	// Topic is the Kafka topic to consume from.
	Topic string `mapstructure:"topic" validate:"required"`

	// Partitions is the number of partitions for the topic.
	// Used when creating the topic if it doesn't exist.
	Partitions int `mapstructure:"partitions" validate:"required,gt=0"`

	// ReplicationFactor is the number of replicas for the topic.
	// Used when creating the topic if it doesn't exist.
	ReplicationFactor int `mapstructure:"replicationFactor" validate:"required,gt=0"`

	// MinBytes is the minimum number of bytes to fetch in a single request.
	// Default: 1 byte (fetch immediately)
	// Higher values can improve throughput but increase latency.
	MinBytes int `mapstructure:"minBytes" default:"1" validate:"min=1"`

	// MaxBytes is the maximum number of bytes to fetch in a single request.
	// Default: 10MB
	// Limits memory usage per fetch request.
	MaxBytes int `mapstructure:"maxBytes" default:"10485760" validate:"min=1"`

	// MaxWait is the maximum time to wait for MinBytes to accumulate.
	// Default: 10 seconds
	MaxWait time.Duration `mapstructure:"maxWait" default:"10s"`

	// TLS holds TLS/SSL configuration for secure connections.
	TLS *tlsconfig.Config `mapstructure:"tls"`

	// SASL holds SASL authentication configuration.
	SASL *SASLConfig `mapstructure:"sasl"`

	// StartOffset determines where to start reading when no offset is committed.
	// Values: "earliest" (from beginning), "latest" (from end)
	// Default: "latest"
	StartOffset string `mapstructure:"startOffset" default:"latest" validate:"omitempty,oneof=earliest latest"`
}

type KafkaSource struct {
	cfg    *SourceConfig
	slog   *slog.Logger
	c      chan *message.RunnerMessage
	reader *kafka.Reader
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

// NewSource creates a Kafka source from options map.
func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	return &KafkaSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "Kafka Source"),
	}, nil
}

func (s *KafkaSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	// Build dialer with TLS and SASL if configured
	dialer, err := s.buildDialer()
	if err != nil {
		return nil, fmt.Errorf("failed to build dialer: %w", err)
	}

	// Create the topic if it does not exist
	err = ensureKafkaTopicWithDialer(s.slog, dialer, s.cfg.Brokers, s.cfg.Topic, s.cfg.Partitions, s.cfg.ReplicationFactor)
	if err != nil {
		s.slog.Error("error creating/verifying topic", "err", err)
		return nil, err
	}

	s.c = make(chan *message.RunnerMessage, buffer)

	useTLS := s.cfg.TLS != nil && s.cfg.TLS.Enabled
	useSASL := s.cfg.SASL != nil && s.cfg.SASL.Enabled

	s.slog.Info("starting Kafka source",
		"brokers", s.cfg.Brokers,
		"topic", s.cfg.Topic,
		"groupID", s.cfg.GroupID,
		"tls", useTLS,
		"sasl", useSASL,
	)

	// Determine start offset
	startOffset := kafka.LastOffset
	if s.cfg.StartOffset == "earliest" {
		startOffset = kafka.FirstOffset
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:     s.cfg.Brokers,
		Topic:       s.cfg.Topic,
		GroupID:     s.cfg.GroupID,
		MinBytes:    s.cfg.MinBytes,
		MaxBytes:    s.cfg.MaxBytes,
		MaxWait:     s.cfg.MaxWait,
		StartOffset: startOffset,
		Dialer:      dialer,
	}

	r := kafka.NewReader(readerConfig)
	s.reader = r

	go func() {
		for {
			m, err := r.FetchMessage(context.Background())
			if err != nil {
				s.slog.Error("error fetching from Kafka, stopping consumer", "err", err)
				break
			}
			msg := &KafkaMessage{
				msg:    &m,
				reader: r,
			}
			s.c <- message.NewRunnerMessage(msg)
		}
	}()

	return s.c, nil
}

// buildDialer creates a Kafka dialer with TLS and SASL configuration.
func (s *KafkaSource) buildDialer() (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// Configure TLS
	if s.cfg.TLS != nil && s.cfg.TLS.Enabled {
		tlsConfig, err := s.cfg.TLS.BuildClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		dialer.TLS = tlsConfig
	}

	// Configure SASL
	if s.cfg.SASL != nil && s.cfg.SASL.Enabled {
		mechanism, err := s.cfg.SASL.BuildSASLMechanism()
		if err != nil {
			return nil, fmt.Errorf("failed to build SASL mechanism: %w", err)
		}
		dialer.SASLMechanism = mechanism
	}

	return dialer, nil
}

func (s *KafkaSource) Close() error {
	if s.reader != nil {
		err := s.reader.Close()
		if err != nil {
			return fmt.Errorf("error closing Kafka reader: %w", err)
		}
	}
	return nil
}
