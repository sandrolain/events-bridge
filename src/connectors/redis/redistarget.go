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
	if cfg.Stream != "" {
		return &RedisStreamTarget{
			config: cfg,
			slog:   slog.Default().With("context", "RedisStream"),
			stopCh: make(chan struct{}),
		}, nil
	}
	if cfg.Channel != "" {
		return &RedisTarget{
			config: cfg,
			slog:   slog.Default().With("context", "Redis"),
			stopCh: make(chan struct{}),
		}, nil
	}
	return nil, fmt.Errorf("invalid config for Redis target")
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
					t.slog.Error("error publishing message", "err", err)
					if err := msg.Nak(); err != nil {
						t.slog.Error("error naking message", "err", err)
					}
				} else {
					if err := msg.Ack(); err != nil {
						t.slog.Error("error acking message", "err", err)
					}
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
		if err := t.client.Close(); err != nil {
			return fmt.Errorf("error closing Redis client: %w", err)
		}
	}
	return nil
}

type RedisStreamTarget struct {
	slog    *slog.Logger
	config  *targets.TargetRedisConfig
	stopped bool
	stopCh  chan struct{}
	client  *redis.Client
}

func (t *RedisStreamTarget) Consume(c <-chan message.Message) error {
	t.client = redis.NewClient(&redis.Options{
		Addr: t.config.Address,
	})
	t.slog.Info("Redis stream target connected", "address", t.config.Address, "stream", t.config.Stream)

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
					t.slog.Error("error publishing message", "err", err)
					if err := msg.Nak(); err != nil {
						t.slog.Error("error naking message", "err", err)
					}
				} else {
					if err := msg.Ack(); err != nil {
						t.slog.Error("error acking message", "err", err)
					}
				}
			}
		}
	}()
	return nil
}

func (t *RedisStreamTarget) publish(msg message.Message) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}
	stream := t.config.Stream
	if t.config.StreamFromMetadataKey != "" {
		metadata, _ := msg.GetMetadata()
		if v, ok := metadata[t.config.StreamFromMetadataKey]; ok && len(v) > 0 {
			stream = v[0]
		}
	}
	dataKey := t.config.StreamDataKey
	if dataKey == "" {
		dataKey = "data"
	}
	fields := map[string]interface{}{dataKey: data}
	err = t.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: fields,
	}).Err()
	if err != nil {
		return fmt.Errorf("error publishing to Redis stream: %w", err)
	}
	t.slog.Debug("Redis stream message published", "stream", stream)
	return nil
}

func (t *RedisStreamTarget) Close() error {
	t.stopped = true
	if t.stopCh != nil {
		close(t.stopCh)
	}
	if t.client != nil {
		if err := t.client.Close(); err != nil {
			return fmt.Errorf("error closing Redis client: %w", err)
		}
	}
	return nil
}
