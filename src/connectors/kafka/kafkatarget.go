package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
	"github.com/sandrolain/events-bridge/src/utils"
	"github.com/segmentio/kafka-go"
)

type TargetConfig struct {
	Brokers           []string `yaml:"brokers" json:"brokers"`
	Topic             string   `yaml:"topic" json:"topic"`
	Partitions        int      `yaml:"partitions" json:"partitions"`
	ReplicationFactor int      `yaml:"replication_factor" json:"replication_factor"`
}

// parseTargetOptions builds a Kafka target config from options map.
// Expected keys: brokers ([]string), topic, partitions, replication_factor.
func parseTargetOptions(opts map[string]any) (*TargetConfig, error) {
	cfg := &TargetConfig{}
	op := &utils.OptsParser{}
	cfg.Brokers = op.OptStringArray(opts, "brokers", nil, utils.StringNonEmpty())
	cfg.Topic = op.OptString(opts, "topic", "", utils.StringNonEmpty())
	cfg.Partitions = op.OptInt(opts, "partitions", 1, utils.IntMin(1))
	cfg.ReplicationFactor = op.OptInt(opts, "replication_factor", 1, utils.IntMin(1))
	if err := op.Error(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// NewTarget creates a Kafka target from options map.
func NewTarget(opts map[string]any) (targets.Target, error) {
	cfg, err := parseTargetOptions(opts)
	if err != nil {
		return nil, err
	}

	l := slog.Default().With("context", "Kafka")

	// Create the topic if it does not exist
	err = ensureKafkaTopic(l, cfg.Brokers, cfg.Topic, cfg.Partitions, cfg.ReplicationFactor)
	if err != nil {
		return nil, fmt.Errorf("error creating/verifying topic: %w", err)
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
	})
	l.Info("Kafka target connected", "brokers", cfg.Brokers, "topic", cfg.Topic)

	return &KafkaTarget{
		config: cfg,
		slog:   l,
		writer: writer,
	}, nil
}

type KafkaTarget struct {
	slog   *slog.Logger
	config *TargetConfig
	writer *kafka.Writer
}

func (t *KafkaTarget) Consume(msg *message.RunnerMessage) error {
	meta, err := msg.GetTargetMetadata()
	if err != nil {
		return fmt.Errorf("error getting metadata: %w", err)
	}

	data, err := msg.GetTargetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	t.slog.Debug("publishing Kafka message", "topic", t.config.Topic, "bodysize", len(data))

	kmsg := kafka.Message{
		Key:   msg.GetID(),
		Value: data,
	}

	metaLen := len(meta)
	if metaLen > 0 {
		kmsg.Headers = make([]kafka.Header, 0, metaLen)
		for k, v := range meta {
			kmsg.Headers = append(kmsg.Headers, kafka.Header{
				Key:   k,
				Value: []byte(v),
			})
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = t.writer.WriteMessages(ctx, kmsg)
	if err != nil {
		return fmt.Errorf("error publishing to Kafka: %w", err)
	}
	t.slog.Debug("Kafka message published", "topic", t.config.Topic)
	return nil
}

func (t *KafkaTarget) Close() error {
	if t.writer != nil {
		t.slog.Info("closing Kafka writer")
		if err := t.writer.Close(); err != nil {
			t.slog.Error("error closing Kafka writer", "err", err)
		}
	}
	return nil
}
