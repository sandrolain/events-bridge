package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
	"github.com/sandrolain/events-bridge/src/utils"
)

func NewTarget(cfg *targets.TargetRedisConfig) (targets.Target, error) {
	if cfg.Stream != "" {
		return NewStreamTarget(cfg)
	}
	if cfg.Channel != "" {
		return NewChannelTarget(cfg)
	}
	return nil, fmt.Errorf("invalid config for Redis target")
}

func NewChannelTarget(cfg *targets.TargetRedisConfig) (targets.Target, error) {
	l := slog.Default().With("context", "Redis")
	client := redis.NewClient(&redis.Options{
		Addr: cfg.Address,
	})
	l.Info("Redis target connected", "address", cfg.Address, "channel", cfg.Channel)

	return &RedisTarget{
		config: cfg,
		slog:   l,
		client: client,
	}, nil
}

type RedisTarget struct {
	slog   *slog.Logger
	config *targets.TargetRedisConfig
	client *redis.Client
}

func (t *RedisTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetTargetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	channel := utils.ResolveFromMetadata(msg, t.config.ChannelFromMetadataKey, t.config.Channel)
	t.slog.Debug("publishing Redis message", "channel", channel, "bodysize", len(data))

	err = t.client.Publish(context.Background(), channel, data).Err()
	if err != nil {
		return fmt.Errorf("error publishing to Redis: %w", err)
	}
	t.slog.Debug("Redis message published", "channel", channel)
	return nil
}

func (t *RedisTarget) Close() error {
	if t.client != nil {
		if err := t.client.Close(); err != nil {
			return fmt.Errorf("error closing Redis client: %w", err)
		}
	}
	return nil
}
