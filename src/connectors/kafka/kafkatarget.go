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

	return &KafkaTarget{
		config: cfg,
		slog:   slog.Default().With("context", "Kafka"),
		stopCh: make(chan struct{}),
	}, nil
}

type KafkaTarget struct {
	slog    *slog.Logger
	config  *targets.TargetKafkaConfig
	stopped bool
	stopCh  chan struct{}
	writer  *kafka.Writer
}

func (t *KafkaTarget) Consume(c <-chan message.Message) error {
	// Create the topic if it does not exist
	err := ensureKafkaTopic(t.slog, t.config.Brokers, t.config.Topic, t.config.Partitions, t.config.ReplicationFactor)
	if err != nil {
		t.slog.Error("error creating/verifying topic", "err", err)
		return err
	}

	t.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers: t.config.Brokers,
		Topic:   t.config.Topic,
	})
	t.slog.Info("Kafka target connected", "brokers", t.config.Brokers, "topic", t.config.Topic)

	go func() {
		for {
			select {
			case <-t.stopCh:
				return
			case msg, ok := <-c:
				if !ok {
					return
				}
				err := t.publish(msg)
				if err != nil {
					t.slog.Error("error publishing message", "err", err)
					if err := msg.Nak(); err != nil {
						t.slog.Error("error naking message", "err", err)
					}
				} else {
					if err := msg.Ack(); err != nil {
						t.slog.Error("error acking message", "err", err)
					}
				}
			}
		}
	}()
	return nil
}

func (t *KafkaTarget) publish(msg message.Message) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	t.slog.Debug("publishing Kafka message", "topic", t.config.Topic, "bodysize", len(data))

	kmsg := kafka.Message{
		Key:   msg.GetID(),
		Value: data,
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
	t.stopped = true
	if t.stopCh != nil {
		close(t.stopCh)
	}
	if t.writer != nil {
		if err := t.writer.Close(); err != nil {
			t.slog.Error("error closing Kafka writer", "err", err)
		}
	}
	return nil
}
