package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/common"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/segmentio/kafka-go"
)

type TargetConfig struct {
	Brokers           []string `mapstructure:"brokers" validate:"required,min=1"`
	Topic             string   `mapstructure:"topic" validate:"required"`
	Partitions        int      `mapstructure:"partitions" validate:"required,gt=0"`
	ReplicationFactor int      `mapstructure:"replicationFactor" validate:"required,gt=0"`
}

// NewTarget creates a Kafka target from options map.
func NewTarget(opts map[string]any) (connectors.Target, error) {
	cfg, err := common.ParseConfig[TargetConfig](opts)
	if err != nil {
		return nil, err
	}

	l := slog.Default().With("context", "Kafka Target")

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
		cfg:    cfg,
		slog:   l,
		writer: writer,
	}, nil
}

type KafkaTarget struct {
	cfg    *TargetConfig
	slog   *slog.Logger
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

	t.slog.Debug("publishing Kafka message", "topic", t.cfg.Topic, "bodysize", len(data))

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
	t.slog.Debug("Kafka message published", "topic", t.cfg.Topic)
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
