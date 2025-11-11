package main

import (
	"context"
	"testing"
	"time"
)

const testMessageHello = "hello"

func TestRedisStreamEndToEnd(t *testing.T) {
	srv := newMiniredis(t)

	targetCfg := &RunnerConfig{
		Address: srv.Addr(),
		Stream:  "events",
	}

	targetAny, err := NewRunner(targetCfg)
	if err != nil {
		t.Fatalf(errFmtNewRunner, err)
	}
	target, ok := targetAny.(*RedisStreamRunner)
	if !ok {
		t.Fatal("failed to cast target to RedisStreamRunner")
	}
	t.Cleanup(func() {
		if err := target.Close(); err != nil {
			t.Fatalf(errFmtCloseRunner, err)
		}
	})

	sourceCfg := &SourceConfig{
		Address: srv.Addr(),
		Stream:  "events",
		LastID:  "0",
	}

	sourceAny, err := NewSource(sourceCfg)
	if err != nil {
		t.Fatalf(errFmtNewSource, err)
	}
	streamSource, ok := sourceAny.(*RedisStreamSource)
	if !ok {
		t.Fatal("failed to cast source to RedisStreamSource")
	}
	t.Cleanup(func() {
		if err := streamSource.Close(); err != nil {
			t.Fatalf(errFmtCloseStreamSrc, err)
		}
	})

	messages, err := streamSource.Produce(1)
	if err != nil {
		t.Fatalf(errFmtProduce, err)
	}

	msg := newStubRunnerMessage("payload", nil)
	if err := target.Process(msg); err != nil {
		t.Fatalf(errFmtConsume, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	select {
	case event := <-messages:
		data, err := event.GetData()
		if err != nil {
			t.Fatalf(errFmtGetData, err)
		}
		if string(data) != "payload" {
			t.Fatalf("expected payload 'payload', got %q", string(data))
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for stream message")
	}
}

func TestRedisChannelEndToEnd(t *testing.T) {
	srv := newMiniredis(t)

	targetCfg := &RunnerConfig{
		Address: srv.Addr(),
		Channel: "updates",
	}

	targetAny, err := NewRunner(targetCfg)
	if err != nil {
		t.Fatalf(errFmtNewRunner, err)
	}
	channelTarget, ok := targetAny.(*RedisRunner)
	if !ok {
		t.Fatal("failed to cast target to RedisRunner")
	}
	t.Cleanup(func() {
		if err := channelTarget.Close(); err != nil {
			t.Fatalf(errFmtCloseRunner, err)
		}
	})

	sourceCfg := &SourceConfig{
		Address: srv.Addr(),
		Channel: "updates",
	}

	sourceAny, err := NewSource(sourceCfg)
	if err != nil {
		t.Fatalf(errFmtNewSource, err)
	}
	source, ok := sourceAny.(*RedisSource)
	if !ok {
		t.Fatal("failed to cast source to RedisSource")
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Fatalf(errFmtCloseSource, err)
		}
	})

	messages, err := source.Produce(1)
	if err != nil {
		t.Fatalf(errFmtProduce, err)
	}

	// Wait for subscription to be ready
	time.Sleep(50 * time.Millisecond)

	if err := channelTarget.Process(newStubRunnerMessage(testMessageHello, nil)); err != nil {
		t.Fatalf(errFmtConsume, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	select {
	case msg := <-messages:
		data, err := msg.GetData()
		if err != nil {
			t.Fatalf(errFmtGetData, err)
		}
		if string(data) != testMessageHello {
			t.Fatalf("expected data 'hello', got %q", string(data))
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for pubsub message")
	}
}
