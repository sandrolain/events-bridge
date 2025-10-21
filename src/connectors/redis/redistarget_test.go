package main

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

const (
	errFmtNewTarget    = "NewTarget returned error: %v"
	errFmtCloseTarget  = "failed to close target: %v"
	errFmtConsume      = "Consume returned error: %v"
	errFmtXRangeFailed = "XRange failed: %v"
)

func newStubRunnerMessage(data string, metadata map[string]string) *message.RunnerMessage {
	stub := testutil.NewAdapter([]byte(data), metadata)
	msg := message.NewRunnerMessage(stub)
	if metadata != nil {
		msg.SetMetadata(metadata)
	}
	msg.SetData([]byte(data))
	return msg
}

func newMiniredis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(func() {
		srv.Close()
	})
	return srv
}

func newRedisClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Fatalf("failed to close redis client: %v", err)
		}
	})
	return client
}

func TestRedisStreamTargetConsumeStoresMessage(t *testing.T) {
	srv := newMiniredis(t)
	cfg := &TargetConfig{
		Address: srv.Addr(),
		Stream:  "events",
	}

	targetAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errFmtNewTarget, err)
	}

	target, ok := targetAny.(*RedisStreamTarget)
	if !ok {
		t.Fatalf("expected RedisStreamTarget, got %T", targetAny)
	}
	t.Cleanup(func() {
		if err := target.Close(); err != nil {
			t.Fatalf(errFmtCloseTarget, err)
		}
	})

	msg := newStubRunnerMessage("hello", nil)

	if err := target.Consume(msg); err != nil {
		t.Fatalf(errFmtConsume, err)
	}

	client := newRedisClient(t, srv.Addr())
	res, err := client.XRange(context.Background(), "events", "-", "+").Result()
	if err != nil {
		t.Fatalf(errFmtXRangeFailed, err)
	}

	if len(res) != 1 {
		t.Fatalf("expected 1 message, got %d", len(res))
	}

	if got := res[0].Values["data"]; got != "hello" {
		t.Fatalf("expected data to be 'hello', got %v", got)
	}
}

func TestRedisStreamTargetMetadataOverride(t *testing.T) {
	srv := newMiniredis(t)
	cfg := &TargetConfig{
		Address:               srv.Addr(),
		Stream:                "default-stream",
		StreamFromMetadataKey: "stream",
	}

	targetAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errFmtNewTarget, err)
	}
	target := targetAny.(*RedisStreamTarget)
	t.Cleanup(func() {
		if err := target.Close(); err != nil {
			t.Fatalf(errFmtCloseTarget, err)
		}
	})

	msg := newStubRunnerMessage("hello", map[string]string{"stream": "custom-stream"})

	if err := target.Consume(msg); err != nil {
		t.Fatalf(errFmtConsume, err)
	}

	client := newRedisClient(t, srv.Addr())

	custom, err := client.XRange(context.Background(), "custom-stream", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange custom failed: %v", err)
	}
	if len(custom) != 1 {
		t.Fatalf("expected 1 message in custom-stream, got %d", len(custom))
	}

	defaultRes, err := client.XRange(context.Background(), "default-stream", "-", "+").Result()
	if err != nil && err != redis.Nil {
		t.Fatalf("unexpected error reading default-stream: %v", err)
	}
	if len(defaultRes) != 0 {
		t.Fatalf("expected empty default-stream, got %d entries", len(defaultRes))
	}
}

func TestRedisStreamTargetCustomDataKey(t *testing.T) {
	srv := newMiniredis(t)
	cfg := &TargetConfig{
		Address:       srv.Addr(),
		Stream:        "events",
		StreamDataKey: "payload",
	}

	targetAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errFmtNewTarget, err)
	}
	target := targetAny.(*RedisStreamTarget)
	t.Cleanup(func() {
		if err := target.Close(); err != nil {
			t.Fatalf(errFmtCloseTarget, err)
		}
	})

	msg := newStubRunnerMessage("payload-data", nil)

	if err := target.Consume(msg); err != nil {
		t.Fatalf(errFmtConsume, err)
	}

	client := newRedisClient(t, srv.Addr())
	res, err := client.XRange(context.Background(), "events", "-", "+").Result()
	if err != nil {
		t.Fatalf(errFmtXRangeFailed, err)
	}

	if val, ok := res[0].Values["payload"]; !ok || val != "payload-data" {
		t.Fatalf("expected payload 'payload-data', got %v", res[0].Values)
	}
}

func TestRedisStreamTargetClose(t *testing.T) {
	srv := newMiniredis(t)
	cfg := &TargetConfig{
		Address: srv.Addr(),
		Stream:  "events",
	}

	targetAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errFmtNewTarget, err)
	}
	target := targetAny.(*RedisStreamTarget)

	if err := target.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	if err := target.client.Ping(context.Background()).Err(); err == nil {
		t.Fatalf("expected ping after close to fail")
	}
}

func TestRedisChannelTargetPublishesMessage(t *testing.T) {
	srv := newMiniredis(t)
	cfg := &TargetConfig{
		Address: "",
		Channel: "notifications",
	}
	cfg.Address = srv.Addr()

	targetAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errFmtNewTarget, err)
	}
	target := targetAny.(*RedisTarget)
	t.Cleanup(func() {
		if err := target.Close(); err != nil {
			t.Fatalf(errFmtCloseTarget, err)
		}
	})

	client := newRedisClient(t, srv.Addr())
	pubsub := client.Subscribe(context.Background(), "notifications")
	defer func() {
		if err := pubsub.Close(); err != nil {
			t.Logf("failed to close pubsub: %v", err)
		}
	}()

	msg := newStubRunnerMessage("notify", nil)

	if err := target.Consume(msg); err != nil {
		t.Fatalf(errFmtConsume, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	received, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("ReceiveMessage failed: %v", err)
	}

	if received.Payload != "notify" {
		t.Fatalf("expected payload 'notify', got %q", received.Payload)
	}
}

func TestRedisChannelTargetMetadataOverride(t *testing.T) {
	srv := newMiniredis(t)
	cfg := &TargetConfig{
		Address:                srv.Addr(),
		Channel:                "default",
		ChannelFromMetadataKey: "channel",
	}

	targetAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errFmtNewTarget, err)
	}
	target := targetAny.(*RedisTarget)
	t.Cleanup(func() {
		if err := target.Close(); err != nil {
			t.Fatalf(errFmtCloseTarget, err)
		}
	})

	client := newRedisClient(t, srv.Addr())
	pubsub := client.Subscribe(context.Background(), "custom")
	defer func() {
		if err := pubsub.Close(); err != nil {
			t.Logf("failed to close pubsub: %v", err)
		}
	}()

	msg := newStubRunnerMessage("notify", map[string]string{"channel": "custom"})

	if err := target.Consume(msg); err != nil {
		t.Fatalf(errFmtConsume, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	received, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("ReceiveMessage failed: %v", err)
	}

	if received.Payload != "notify" {
		t.Fatalf("expected payload 'notify', got %q", received.Payload)
	}
}
