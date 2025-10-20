package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/common/secrets"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// redisKeyRegex validates Redis keys/channel names to prevent injection
// Allows alphanumeric, colon, dash, underscore, dot, forward slash
var redisKeyRegex = regexp.MustCompile(`^[a-zA-Z0-9:_.\-/]+$`)

// SourceConfig defines the configuration for Redis source connector
type SourceConfig struct {
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
	// Channel name for PubSub subscription
	Channel string `mapstructure:"channel"`

	// Stream mode
	// Stream name for Stream consumption
	Stream        string `mapstructure:"stream"`
	ConsumerGroup string `mapstructure:"consumerGroup,omitempty" validate:"required_with=ConsumerName,omitempty"`
	ConsumerName  string `mapstructure:"consumerName,omitempty" validate:"required_with=ConsumerGroup,omitempty"`
	StreamDataKey string `mapstructure:"streamDataKey" default:"data"`
	// Stream starting position: "0" (beginning), "$" (newest), ">" (consumer group), "+" (end), "-" (start), or specific ID
	LastID string `mapstructure:"lastID" default:"$" validate:"oneof=0 $ > + -"`

	// Enable strict key validation (recommended: true)
	// When true, channel/stream names are validated against a strict pattern
	StrictValidation bool `mapstructure:"strictValidation" default:"true"`
}

type RedisSource struct {
	cfg    *SourceConfig
	slog   *slog.Logger
	c      chan *message.RunnerMessage
	client *redis.Client
	pubsub *redis.PubSub
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

// validateRedisKey checks if a string is a valid Redis key/channel name
// Returns error if the key contains dangerous characters
func validateRedisKey(key string, strict bool) error {
	if key == "" {
		return fmt.Errorf("redis key cannot be empty")
	}

	// Check for control characters and newlines (command injection)
	for _, c := range key {
		if c == '\r' || c == '\n' || c < 32 {
			return fmt.Errorf("redis key contains invalid control characters")
		}
	}

	if strict {
		if !redisKeyRegex.MatchString(key) {
			return fmt.Errorf("invalid Redis key: %s (must contain only alphanumeric, colon, dash, underscore, dot, forward slash)", key)
		}
	}

	return nil
}

// buildTLSConfig creates TLS configuration from source config
func buildRedisTLSConfig(cfg *tlsconfig.Config) (*tls.Config, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}

	return cfg.BuildClientConfig()
}

// buildRedisOptions creates Redis client options with authentication and TLS
func (s *RedisSource) buildRedisOptions() (*redis.Options, error) {
	opts := &redis.Options{
		Addr: s.cfg.Address,
		DB:   s.cfg.DB,
	}

	// Add authentication if provided
	if s.cfg.Username != "" {
		opts.Username = s.cfg.Username
	}
	if s.cfg.Password != "" {
		// Resolve password secret
		resolvedPassword, err := secrets.Resolve(s.cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve password: %w", err)
		}
		opts.Password = resolvedPassword
	}

	// Add TLS if configured
	tlsConf, err := buildRedisTLSConfig(s.cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}
	if tlsConf != nil {
		opts.TLSConfig = tlsConf
	}

	return opts, nil
}

// NewSource creates a Redis source from options map.
func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
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
		return NewStreamSource(cfg)
	}
	if cfg.Channel != "" {
		return NewChannelSource(cfg)
	}
	return nil, fmt.Errorf("invalid config for Redis source")
}

func NewChannelSource(cfg *SourceConfig) (connectors.Source, error) {
	return &RedisSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "RedisChannel Source"),
	}, nil
}

func (s *RedisSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	tlsEnabled := s.cfg.TLS != nil && s.cfg.TLS.Enabled
	hasAuth := s.cfg.Username != "" || s.cfg.Password != ""

	s.slog.Info("starting Redis source",
		"address", s.cfg.Address,
		"channel", s.cfg.Channel,
		"db", s.cfg.DB,
		"tls", tlsEnabled,
		"auth", hasAuth,
		"strictValidation", s.cfg.StrictValidation,
	)

	opts, err := s.buildRedisOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to build Redis options: %w", err)
	}

	s.client = redis.NewClient(opts)

	// Test connection
	if err := s.client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

	s.pubsub = s.client.Subscribe(context.Background(), s.cfg.Channel)
	go s.consume()

	return s.c, nil
}

func (s *RedisSource) consume() {
	ch := s.pubsub.Channel()
	for msg := range ch {
		m := &RedisMessage{msg: msg}
		s.c <- message.NewRunnerMessage(m)
	}
}

func (s *RedisSource) Close() error {
	if s.pubsub != nil {
		if err := s.pubsub.Close(); err != nil {
			return fmt.Errorf("error closing Redis pubsub: %w", err)
		}
	}
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			return fmt.Errorf("error closing Redis client: %w", err)
		}
	}
	return nil
}
