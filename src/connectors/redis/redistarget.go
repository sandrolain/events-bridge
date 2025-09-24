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

// NewTargetOptions builds a Redis target config from options map.
// For PubSub: address, channel, channelFromMetadataKey.
// For Stream: address, stream, streamFromMetadataKey, consumer_group, consumer_name, timeout (ns), stream_data_key.
func NewTargetOptions(opts map[string]any) (targets.Target, error) {
	cfg := &TargetConfig{}
	if v, ok := opts["address"].(string); ok {
		cfg.Address = v
	}
	if v, ok := opts["channel"].(string); ok {
		cfg.Channel = v
	}
	if v, ok := opts["channelFromMetadataKey"].(string); ok {
		cfg.ChannelFromMetadataKey = v
	}
	if v, ok := opts["stream"].(string); ok {
		cfg.Stream = v
	}
	if v, ok := opts["streamFromMetadataKey"].(string); ok {
		cfg.StreamFromMetadataKey = v
	}
	if v, ok := opts["consumer_group"].(string); ok {
		cfg.ConsumerGroup = v
	}
	if v, ok := opts["consumer_name"].(string); ok {
		cfg.ConsumerName = v
	}
	if v, ok := opts["timeout"].(int); ok {
		cfg.Timeout = time.Duration(v)
	}
	if v, ok := opts["timeout"].(int64); ok {
		cfg.Timeout = time.Duration(v)
	}
	if v, ok := opts["timeout"].(float64); ok {
		cfg.Timeout = time.Duration(int64(v))
	}
	if v, ok := opts["stream_data_key"].(string); ok {
		cfg.StreamDataKey = v
	}
	return NewTarget(cfg)
}

func NewTarget(cfg *TargetConfig) (targets.Target, error) {
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
