package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ connectors.Runner = (*RedisStreamRunner)(nil)

func NewStreamRunner(cfg *RunnerConfig) (connectors.Runner, error) {
	l := slog.Default().With("context", "RedisStream Runner")

	opts, err := buildRedisRunnerOptions(cfg)
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

	l.Info("Redis stream runner connected",
		"address", cfg.Address,
		"stream", cfg.Stream,
		"db", cfg.DB,
		"tls", tlsEnabled,
		"auth", hasAuth,
		"strictValidation", cfg.StrictValidation,
	)

	return &RedisStreamRunner{
		cfg:    cfg,
		slog:   l,
		client: client,
	}, nil
}

type RedisStreamRunner struct {
	cfg    *RunnerConfig
	slog   *slog.Logger
	client *redis.Client
}

func (r *RedisStreamRunner) Process(msg *message.RunnerMessage) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}
	stream := r.cfg.Stream
	if r.cfg.StreamFromMetadataKey != "" {
		metadata, err := msg.GetMetadata()
		if err != nil {
			r.slog.Warn("failed to get metadata for stream resolution", "error", err)
		} else if v, ok := metadata[r.cfg.StreamFromMetadataKey]; ok && len(v) > 0 {
			stream = v
		}
	}

	// Validate stream name if dynamically resolved
	if r.cfg.StreamFromMetadataKey != "" && stream != r.cfg.Stream {
		if err := validateRedisKey(stream, r.cfg.StrictValidation); err != nil {
			return fmt.Errorf("invalid resolved stream name: %w", err)
		}
	}

	dataKey := r.cfg.StreamDataKey
	if dataKey == "" {
		dataKey = "data"
	}
	fields := map[string]interface{}{dataKey: data}
	err = r.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: fields,
	}).Err()
	if err != nil {
		return fmt.Errorf("error publishing to Redis stream: %w", err)
	}
	r.slog.Debug("Redis stream message published", "stream", stream)
	return nil
}

func (r *RedisStreamRunner) Close() error {
	if r.client != nil {
		if err := r.client.Close(); err != nil {
			return fmt.Errorf("error closing Redis client: %w", err)
		}
	}
	return nil
}
