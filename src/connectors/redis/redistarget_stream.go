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

	opts, err := buildRedisTargetOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build Redis options: %w", err)
	}

	client := redis.NewClient(opts)

	// Test connection
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

	tlsEnabled := cfg.TLS != nil && cfg.TLS.Enabled
	hasAuth := cfg.Username != "" || cfg.Password != ""

	l.Info("Redis stream target connected",
		"address", cfg.Address,
		"stream", cfg.Stream,
		"db", cfg.DB,
		"tls", tlsEnabled,
		"auth", hasAuth,
		"strictValidation", cfg.StrictValidation,
	)

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

	// Validate stream name if dynamically resolved
	if t.cfg.StreamFromMetadataKey != "" && stream != t.cfg.Stream {
		if err := validateRedisKey(stream, t.cfg.StrictValidation); err != nil {
			return fmt.Errorf("invalid resolved stream name: %w", err)
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
