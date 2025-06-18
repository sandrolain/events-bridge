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
	if cfg.Brokers == nil || len(cfg.Brokers) == 0 || cfg.Topic == "" {
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
	t.writer = &kafka.Writer{
		Addr:     kafka.TCP(t.config.Brokers...),
		Topic:    t.config.Topic,
		Balancer: &kafka.LeastBytes{},
	}
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
					msg.Nak()
					t.slog.Error("error publishing Kafka message", "err", err)
				} else {
					msg.Ack()
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

	key := []byte{}
	if t.config.KeyFromMetadataKey != "" {
		metadata, _ := msg.GetMetadata()
		if v, ok := metadata[t.config.KeyFromMetadataKey]; ok && len(v) > 0 {
			key = []byte(v[0])
		}
	}

	t.slog.Debug("publishing Kafka message", "topic", t.config.Topic, "bodysize", len(data))

	kmsg := kafka.Message{
		Key:   key,
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
		t.writer.Close()
	}
	return nil
}
