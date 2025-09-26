package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/common"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

type TargetConfig struct {
	Address string `mapstructure:"address" validate:"required"`
	// PubSub
	Channel                string `mapstructure:"channel"`
	ChannelFromMetadataKey string `mapstructure:"channelFromMetadataKey"`
	// Stream
	Stream                string        `mapstructure:"stream" validate:"required"`
	StreamFromMetadataKey string        `mapstructure:"streamFromMetadataKey"`
	ConsumerGroup         string        `mapstructure:"consumerGroup,omitempty" validate:"required_with=ConsumerName,omitempty"`
	ConsumerName          string        `mapstructure:"consumerName,omitempty" validate:"required_with=ConsumerGroup,omitempty"`
	Timeout               time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	StreamDataKey         string        `mapstructure:"streamDataKey"`
}

func NewTargetConfig() any {
	return new(TargetConfig)
}

// NewTarget creates a Redis target from options map.
func NewTarget(anyCfg any) (connectors.Target, error) {
	cfg, ok := anyCfg.(*TargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}
	if cfg.Stream != "" {
		return NewStreamTarget(cfg)
	}
	if cfg.Channel != "" {
		return NewChannelTarget(cfg)
	}
	return nil, fmt.Errorf("invalid config for Redis target")
}

func NewChannelTarget(cfg *TargetConfig) (connectors.Target, error) {
	l := slog.Default().With("context", "RedisChannel Target")
	client := redis.NewClient(&redis.Options{
		Addr: cfg.Address,
	})
	l.Info("Redis target connected", "address", cfg.Address, "channel", cfg.Channel)

	return &RedisTarget{
		cfg:    cfg,
		slog:   l,
		client: client,
	}, nil
}

type RedisTarget struct {
	cfg    *TargetConfig
	slog   *slog.Logger
	client *redis.Client
}

func (t *RedisTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetTargetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	channel := common.ResolveFromMetadata(msg, t.cfg.ChannelFromMetadataKey, t.cfg.Channel)
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
