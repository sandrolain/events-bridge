package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/segmentio/kafka-go"
)

type SourceConfig struct {
	Brokers           []string `yaml:"brokers" json:"brokers"`
	GroupID           string   `yaml:"group_id" json:"group_id"`
	Topic             string   `yaml:"topic" json:"topic"`
	Partitions        int      `yaml:"partitions" json:"partitions"`
	ReplicationFactor int      `yaml:"replication_factor" json:"replication_factor"`
}

// NewSourceOptions builds a Kafka source config from options map.
// Expected keys: brokers ([]string), topic, group_id, partitions, replication_factor.
func NewSourceOptions(opts map[string]any) (sources.Source, error) {
	cfg := &SourceConfig{}
	if v, ok := opts["brokers"].([]string); ok {
		cfg.Brokers = v
	} else if v, ok := opts["brokers"].([]any); ok {
		bs := make([]string, 0, len(v))
		for _, it := range v {
			if s, ok := it.(string); ok {
				bs = append(bs, s)
			}
		}
		cfg.Brokers = bs
	}
	if v, ok := opts["group_id"].(string); ok {
		cfg.GroupID = v
	}
	if v, ok := opts["topic"].(string); ok {
		cfg.Topic = v
	}
	if v, ok := opts["partitions"].(int); ok {
		cfg.Partitions = v
	}
	if v, ok := opts["partitions"].(int64); ok {
		cfg.Partitions = int(v)
	}
	if v, ok := opts["partitions"].(float64); ok {
		cfg.Partitions = int(v)
	}
	if v, ok := opts["replication_factor"].(int); ok {
		cfg.ReplicationFactor = v
	}
	if v, ok := opts["replication_factor"].(int64); ok {
		cfg.ReplicationFactor = int(v)
	}
	if v, ok := opts["replication_factor"].(float64); ok {
		cfg.ReplicationFactor = int(v)
	}
	return NewSource(cfg)
}

type KafkaSource struct {
	config  *SourceConfig
	slog    *slog.Logger
	c       chan *message.RunnerMessage
	reader  *kafka.Reader
	started bool
}

func NewSource(cfg *SourceConfig) (sources.Source, error) {
	if len(cfg.Brokers) == 0 || cfg.Topic == "" {
		return nil, fmt.Errorf("brokers and topic are required for Kafka source")
	}
	return &KafkaSource{
		config: cfg,
		slog:   slog.Default().With("context", "Kafka"),
	}, nil
}

func (s *KafkaSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	// Create the topic if it does not exist
	err := ensureKafkaTopic(s.slog, s.config.Brokers, s.config.Topic, s.config.Partitions, s.config.ReplicationFactor)
	if err != nil {
		s.slog.Error("error creating/verifying topic", "err", err)
		return nil, err
	}

	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting Kafka source", "brokers", s.config.Brokers, "topic", s.config.Topic, "groupID", s.config.GroupID)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  s.config.Brokers,
		Topic:    s.config.Topic,
		GroupID:  s.config.GroupID,
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

	s.started = true
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
