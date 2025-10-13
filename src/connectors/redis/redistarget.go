package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// TargetConfig defines the configuration for Redis target connector
type TargetConfig struct {
	// Redis server address (host:port)
	Address string `mapstructure:"address" validate:"required"`

	// Authentication
	// Username for Redis ACL authentication (Redis 6+)
	Username string `mapstructure:"username"`
	// Password for authentication
	Password string `mapstructure:"password"`
	// Database number (0-15)
	DB int `mapstructure:"db" default:"0" validate:"min=0,max=15"`

	// TLS configuration for encrypted connections
	TLS *tlsconfig.Config `mapstructure:"tls"`

	// PubSub mode
	// Channel name for publishing messages
	Channel string `mapstructure:"channel"`
	// Metadata key to extract channel name dynamically
	ChannelFromMetadataKey string `mapstructure:"channelFromMetadataKey"`

	// Stream mode
	// Stream name for adding messages
	Stream string `mapstructure:"stream" validate:"required"`
	// Metadata key to extract stream name dynamically
	StreamFromMetadataKey string `mapstructure:"streamFromMetadataKey"`
	ConsumerGroup         string `mapstructure:"consumerGroup,omitempty" validate:"required_with=ConsumerName,omitempty"`
	ConsumerName          string `mapstructure:"consumerName,omitempty" validate:"required_with=ConsumerGroup,omitempty"`
	// Timeout for Redis operations
	Timeout time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	// Key name for data in stream entries
	StreamDataKey string `mapstructure:"streamDataKey"`

	// Enable strict key validation (recommended: true)
	StrictValidation bool `mapstructure:"strictValidation" default:"true"`
}

func NewTargetConfig() any {
	return new(TargetConfig)
}

// buildRedisTargetOptions creates Redis client options with authentication and TLS
func buildRedisTargetOptions(cfg *TargetConfig) (*redis.Options, error) {
	opts := &redis.Options{
		Addr: cfg.Address,
		DB:   cfg.DB,
	}

	// Add authentication if provided
	if cfg.Username != "" {
		opts.Username = cfg.Username
	}
	if cfg.Password != "" {
		opts.Password = cfg.Password
	}

	// Add TLS if configured
	tlsConf, err := buildRedisTLSConfig(cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}
	if tlsConf != nil {
		opts.TLSConfig = tlsConf
	}

	return opts, nil
}

// NewTarget creates a Redis target from options map.
func NewTarget(anyCfg any) (connectors.Target, error) {
	cfg, ok := anyCfg.(*TargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Validate channel/stream names to prevent command injection
	if cfg.Channel != "" {
		if err := validateRedisKey(cfg.Channel, cfg.StrictValidation); err != nil {
			return nil, fmt.Errorf("invalid channel name: %w", err)
		}
	}
	if cfg.Stream != "" {
		if err := validateRedisKey(cfg.Stream, cfg.StrictValidation); err != nil {
			return nil, fmt.Errorf("invalid stream name: %w", err)
		}
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

	l.Info("Redis target connected",
		"address", cfg.Address,
		"channel", cfg.Channel,
		"db", cfg.DB,
		"tls", tlsEnabled,
		"auth", hasAuth,
		"strictValidation", cfg.StrictValidation,
	)

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
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	channel := message.ResolveFromMetadata(msg, t.cfg.ChannelFromMetadataKey, t.cfg.Channel)

	// Validate channel name if dynamically resolved
	if t.cfg.ChannelFromMetadataKey != "" {
		if err := validateRedisKey(channel, t.cfg.StrictValidation); err != nil {
			return fmt.Errorf("invalid resolved channel name: %w", err)
		}
	}

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
