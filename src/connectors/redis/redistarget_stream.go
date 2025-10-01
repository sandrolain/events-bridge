package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/message"
)

func NewStreamTarget(cfg *TargetConfig) (*RedisStreamTarget, error) {
	l := slog.Default().With("context", "RedisStream Target")

	client := redis.NewClient(&redis.Options{
		Addr: cfg.Address,
	})
	l.Info("Redis stream target connected", "address", cfg.Address, "stream", cfg.Stream)

	return &RedisStreamTarget{
		cfg:    cfg,
		slog:   l,
		client: client,
	}, nil
}

type RedisStreamTarget struct {
	cfg    *TargetConfig
	slog   *slog.Logger
	client *redis.Client
}

func (t *RedisStreamTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}
	stream := t.cfg.Stream
	if t.cfg.StreamFromMetadataKey != "" {
		metadata, _ := msg.GetMetadata()
		if v, ok := metadata[t.cfg.StreamFromMetadataKey]; ok && len(v) > 0 {
			stream = v
		}
	}
	dataKey := t.cfg.StreamDataKey
	if dataKey == "" {
		dataKey = "data"
	}
	fields := map[string]interface{}{dataKey: data}
	err = t.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: fields,
	}).Err()
	if err != nil {
		return fmt.Errorf("error publishing to Redis stream: %w", err)
	}
	t.slog.Debug("Redis stream message published", "stream", stream)
	return nil
}

func (t *RedisStreamTarget) Close() error {
	if t.client != nil {
		if err := t.client.Close(); err != nil {
			return fmt.Errorf("error closing Redis client: %w", err)
		}
	}
	return nil
}
