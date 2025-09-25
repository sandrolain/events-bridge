package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
	"github.com/sandrolain/events-bridge/src/utils"
)

type TargetConfig struct {
	Address string `yaml:"address" json:"address"`
	// PubSub
	Channel                string `yaml:"channel" json:"channel"`
	ChannelFromMetadataKey string `yaml:"channelFromMetadataKey" json:"channelFromMetadataKey"`
	// Stream
	Stream                string        `yaml:"stream" json:"stream"`
	StreamFromMetadataKey string        `yaml:"streamFromMetadataKey" json:"streamFromMetadataKey"`
	ConsumerGroup         string        `yaml:"consumer_group,omitempty" json:"consumer_group,omitempty"`
	ConsumerName          string        `yaml:"consumer_name,omitempty" json:"consumer_name,omitempty"`
	Timeout               time.Duration `yaml:"timeout" json:"timeout"`
	StreamDataKey         string        `yaml:"stream_data_key" json:"stream_data_key"`
}

// parseTargetOptions builds a Redis target config from options map with validation.
// For PubSub: address, channel, channelFromMetadataKey.
// For Stream: address, stream, streamFromMetadataKey, consumer_group, consumer_name, timeout, stream_data_key.
func parseTargetOptions(opts map[string]any) (*TargetConfig, error) {
	op := &utils.OptsParser{}
	cfg := &TargetConfig{}
	cfg.Address = op.OptString(opts, "address", "", utils.StringNonEmpty())
	cfg.Channel = op.OptString(opts, "channel", "")
	cfg.ChannelFromMetadataKey = op.OptString(opts, "channelFromMetadataKey", "")
	cfg.Stream = op.OptString(opts, "stream", "")
	cfg.StreamFromMetadataKey = op.OptString(opts, "streamFromMetadataKey", "")
	cfg.ConsumerGroup = op.OptString(opts, "consumer_group", "")
	cfg.ConsumerName = op.OptString(opts, "consumer_name", "")
	cfg.Timeout = op.OptDuration(opts, "timeout", 0)
	cfg.StreamDataKey = op.OptString(opts, "stream_data_key", "")
	if err := op.Error(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// NewTarget creates a Redis target from options map.
func NewTarget(opts map[string]any) (targets.Target, error) {
	cfg, err := parseTargetOptions(opts)
	if err != nil {
		return nil, err
	}
	if cfg.Stream != "" {
		return NewStreamTarget(cfg)
	}
	if cfg.Channel != "" {
		return NewChannelTarget(cfg)
	}
	return nil, fmt.Errorf("invalid config for Redis target")
}

func NewChannelTarget(cfg *TargetConfig) (targets.Target, error) {
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
	config *TargetConfig
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
