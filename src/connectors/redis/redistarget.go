package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
)

func NewTarget(cfg *targets.TargetRedisConfig) (targets.Target, error) {
	return &RedisTarget{
		config: cfg,
		slog:   slog.Default().With("context", "Redis"),
		stopCh: make(chan struct{}),
	}, nil
}

type RedisTarget struct {
	slog    *slog.Logger
	config  *targets.TargetRedisConfig
	stopped bool
	stopCh  chan struct{}
	client  *redis.Client
}

func (t *RedisTarget) Consume(c <-chan message.Message) error {
	t.client = redis.NewClient(&redis.Options{
		Addr: t.config.Address,
	})
	t.slog.Info("Redis target connected", "address", t.config.Address, "channel", t.config.Channel)

	go func() {
		for {
			select {
			case <-t.stopCh:
				return
			case msg, ok := <-c:
				if !ok {
					return
				}
				err := t.publish(msg)
				if err != nil {
					msg.Nak()
					t.slog.Error("error publishing Redis message", "err", err)
				} else {
					msg.Ack()
				}
			}
		}
	}()
	return nil
}

func (t *RedisTarget) publish(msg message.Message) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	channel := t.config.Channel
	if t.config.ChannelFromMetadataKey != "" {
		metadata, _ := msg.GetMetadata()
		if v, ok := metadata[t.config.ChannelFromMetadataKey]; ok && len(v) > 0 {
			channel = v[0]
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
	t.stopped = true
	if t.stopCh != nil {
		close(t.stopCh)
	}
	if t.client != nil {
		t.client.Close()
	}
	return nil
}
