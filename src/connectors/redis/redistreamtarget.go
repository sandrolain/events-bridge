package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
)

type RedisStreamTarget struct {
	slog    *slog.Logger
	config  *targets.TargetRedisStreamConfig
	stopped bool
	stopCh  chan struct{}
	client  *redis.Client
}

func NewStreamTarget(cfg *targets.TargetRedisStreamConfig) (targets.Target, error) {
	return &RedisStreamTarget{
		config: cfg,
		slog:   slog.Default().With("context", "RedisStream"),
		stopCh: make(chan struct{}),
	}, nil
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
					msg.Nak()
					t.slog.Error("error publishing Redis stream message", "err", err)
				} else {
					msg.Ack()
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
	fields := map[string]interface{}{"data": data}
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
		t.client.Close()
	}
	return nil
}
