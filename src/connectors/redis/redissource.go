package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
)

type RedisSource struct {
	config  *sources.SourceRedisConfig
	slog    *slog.Logger
	c       chan message.Message
	client  *redis.Client
	pubsub  *redis.PubSub
	started bool
}

func NewSource(cfg *sources.SourceRedisConfig) (sources.Source, error) {
	if cfg.Address == "" || cfg.Channel == "" {
		return nil, fmt.Errorf("address and channel are required for Redis source")
	}
	return &RedisSource{
		config: cfg,
		slog:   slog.Default().With("context", "Redis"),
	}, nil
}

func (s *RedisSource) Produce(buffer int) (<-chan message.Message, error) {
	s.c = make(chan message.Message, buffer)

	s.slog.Info("starting Redis source", "address", s.config.Address, "channel", s.config.Channel)

	s.client = redis.NewClient(&redis.Options{
		Addr: s.config.Address,
	})

	s.pubsub = s.client.Subscribe(context.Background(), s.config.Channel)
	go s.consume()

	s.started = true
	return s.c, nil
}

func (s *RedisSource) consume() {
	ch := s.pubsub.Channel()
	for msg := range ch {
		m := &RedisMessage{msg: msg}
		s.c <- m
	}
}

func (s *RedisSource) Close() error {
	if s.c != nil {
		close(s.c)
	}
	if s.pubsub != nil {
		s.pubsub.Close()
	}
	if s.client != nil {
		s.client.Close()
	}
	return nil
}
