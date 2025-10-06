package main

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	errFmtNewSource      = "NewSource returned error: %v"
	errFmtCloseSource    = "failed to close source: %v"
	errFmtProduce        = "Produce returned error: %v"
	errFmtPublish        = "Publish returned error: %v"
	errFmtGetData        = "GetData returned error: %v"
	errFmtGetMetadata    = "GetMetadata returned error: %v"
	errFmtCloseStreamSrc = "failed to close stream source: %v"
	errFmtXAdd           = "XAdd returned error: %v"
)

func TestRedisChannelSourceReceivesMessage(t *testing.T) {
	srv := newMiniredis(t)

	cfg := &SourceConfig{
		Address: srv.Addr(),
		Channel: "updates",
	}

	sourceAny, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(errFmtNewSource, err)
	}
	source := sourceAny.(*RedisSource)
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Fatalf(errFmtCloseSource, err)
		}
	})

	messages, err := source.Produce(1)
	if err != nil {
		t.Fatalf(errFmtProduce, err)
	}

	// Ensure subscription is ready before publishing the message.
	time.Sleep(10 * time.Millisecond)

	client := newRedisClient(t, srv.Addr())
	if err := client.Publish(context.Background(), "updates", "hello").Err(); err != nil {
		t.Fatalf(errFmtPublish, err)
	}

	select {
	case msg := <-messages:
		data, err := msg.GetData()
		if err != nil {
			t.Fatalf(errFmtGetData, err)
		}
		if string(data) != "hello" {
			t.Fatalf("expected data 'hello', got %q", string(data))
		}

		metadata, err := msg.GetMetadata()
		if err != nil {
			t.Fatalf(errFmtGetMetadata, err)
		}
		if channel := metadata["channel"]; channel != "updates" {
			t.Fatalf("expected metadata channel 'updates', got %q", channel)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestRedisStreamSourceReceivesMessage(t *testing.T) {
	srv := newMiniredis(t)

	cfg := &SourceConfig{
		Address:       srv.Addr(),
		Stream:        "events",
		StreamDataKey: "payload",
		LastID:        "0",
	}

	sourceAny, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(errFmtNewSource, err)
	}
	streamSource := sourceAny.(*RedisStreamSource)
	t.Cleanup(func() {
		if err := streamSource.Close(); err != nil {
			t.Fatalf(errFmtCloseStreamSrc, err)
		}
	})

	messages, err := streamSource.Produce(2)
	if err != nil {
		t.Fatalf(errFmtProduce, err)
	}

	client := newRedisClient(t, srv.Addr())
	if err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: "events",
		Values: map[string]any{"payload": "data"},
	}).Err(); err != nil {
		t.Fatalf(errFmtXAdd, err)
	}

	select {
	case msg := <-messages:
		data, err := msg.GetData()
		if err != nil {
			t.Fatalf(errFmtGetData, err)
		}
		if string(data) != "data" {
			t.Fatalf("expected payload 'data', got %q", string(data))
		}

		metadata, err := msg.GetMetadata()
		if err != nil {
			t.Fatalf(errFmtGetMetadata, err)
		}
		if metadata["payload"] != "data" {
			t.Fatalf("expected metadata payload 'data', got %q", metadata["payload"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for stream message")
	}
}
