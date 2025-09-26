package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/segmentio/kafka-go"
)

type SourceConfig struct {
	Brokers           []string `mapstructure:"brokers" validate:"required,min=1"`
	GroupID           string   `mapstructure:"groupId"`
	Topic             string   `mapstructure:"topic" validate:"required"`
	Partitions        int      `mapstructure:"partitions" validate:"required,gt=0"`
	ReplicationFactor int      `mapstructure:"replicationFactor" validate:"required,gt=0"`
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
	// Create the topic if it does not exist
	err := ensureKafkaTopic(s.slog, s.cfg.Brokers, s.cfg.Topic, s.cfg.Partitions, s.cfg.ReplicationFactor)
	if err != nil {
		s.slog.Error("error creating/verifying topic", "err", err)
		return nil, err
	}

	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting Kafka source", "brokers", s.cfg.Brokers, "topic", s.cfg.Topic, "groupID", s.cfg.GroupID)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  s.cfg.Brokers,
		Topic:    s.cfg.Topic,
		GroupID:  s.cfg.GroupID,
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
	})
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

func (s *KafkaSource) Close() error {
	if s.reader != nil {
		err := s.reader.Close()
		if err != nil {
			return fmt.Errorf("error closing Kafka reader: %w", err)
		}
	}
	return nil
}
