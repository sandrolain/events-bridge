package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/common/secrets"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ connectors.Runner = (*RedisRunner)(nil)

// RunnerConfig defines the configuration for Redis runner connector
type RunnerConfig struct {
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

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// buildRedisRunnerOptions creates Redis client options with authentication and TLS
func buildRedisRunnerOptions(cfg *RunnerConfig) (*redis.Options, error) {
	opts := &redis.Options{
		Addr: cfg.Address,
		DB:   cfg.DB,
	}

	// Add authentication if provided
	if cfg.Username != "" {
		opts.Username = cfg.Username
	}
	if cfg.Password != "" {
		// Resolve password secret
		resolvedPassword, err := secrets.Resolve(cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve password: %w", err)
		}
		opts.Password = resolvedPassword
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

// NewRunner creates a Redis runner from options map.
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
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
		return NewStreamRunner(cfg)
	}
	if cfg.Channel != "" {
		return NewChannelRunner(cfg)
	}
	return nil, fmt.Errorf("invalid config for Redis runner")
}

func NewChannelRunner(cfg *RunnerConfig) (connectors.Runner, error) {
	l := slog.Default().With("context", "RedisChannel Runner")

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

	l.Info("Redis runner connected",
		"address", cfg.Address,
		"channel", cfg.Channel,
		"db", cfg.DB,
		"tls", tlsEnabled,
		"auth", hasAuth,
		"strictValidation", cfg.StrictValidation,
	)

	return &RedisRunner{
		cfg:    cfg,
		slog:   l,
		client: client,
	}, nil
}

type RedisRunner struct {
	cfg    *RunnerConfig
	slog   *slog.Logger
	client *redis.Client
}

func (r *RedisRunner) Process(msg *message.RunnerMessage) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	channel := message.ResolveFromMetadata(msg, r.cfg.ChannelFromMetadataKey, r.cfg.Channel)

	// Validate channel name if dynamically resolved
	if r.cfg.ChannelFromMetadataKey != "" {
		if err := validateRedisKey(channel, r.cfg.StrictValidation); err != nil {
			return fmt.Errorf("invalid resolved channel name: %w", err)
		}
	}

	r.slog.Debug("publishing Redis message", "channel", channel, "bodysize", len(data))

	err = r.client.Publish(context.Background(), channel, data).Err()
	if err != nil {
		return fmt.Errorf("error publishing to Redis: %w", err)
	}
	r.slog.Debug("Redis message published", "channel", channel)
	return nil
}

func (r *RedisRunner) Close() error {
	if r.client != nil {
		if err := r.client.Close(); err != nil {
			return fmt.Errorf("error closing Redis client: %w", err)
		}
	}
	return nil
}
