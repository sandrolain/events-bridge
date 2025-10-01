package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

type SourceConfig struct {
	Address string `mapstructure:"address" validate:"required"`
	// PubSub
	Channel string `mapstructure:"channel"`
	// Stream
	Stream        string `mapstructure:"stream"`
	ConsumerGroup string `mapstructure:"consumerGroup,omitempty" validate:"required_with=ConsumerName,omitempty"`
	ConsumerName  string `mapstructure:"consumerName,omitempty" validate:"required_with=ConsumerGroup,omitempty"`
	StreamDataKey string `mapstructure:"streamDataKey" default:"data"`
	LastID        string `mapstructure:"lastID" default:"$" validate:"oneof=0 $ > + -"` // Stream starting position: "0" (beginning), "$" (newest), ">" (consumer group), "+" (end), "-" (start), or specific ID
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

// NewSource creates a Redis source from options map.
func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
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

	s.slog.Info("starting Redis source", "address", s.cfg.Address, "channel", s.cfg.Channel)

	s.client = redis.NewClient(&redis.Options{
		Addr: s.cfg.Address,
	})

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
