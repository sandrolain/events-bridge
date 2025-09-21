package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
	"github.com/segmentio/kafka-go"
)

func NewTarget(cfg *targets.TargetKafkaConfig) (targets.Target, error) {
	if len(cfg.Brokers) == 0 || cfg.Topic == "" {
		return nil, fmt.Errorf("brokers and topic are required for Kafka target")
	}

	l := slog.Default().With("context", "Kafka")

	// Create the topic if it does not exist
	err := ensureKafkaTopic(l, cfg.Brokers, cfg.Topic, cfg.Partitions, cfg.ReplicationFactor)
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
	config *targets.TargetKafkaConfig
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
