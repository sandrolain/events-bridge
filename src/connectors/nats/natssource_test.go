package main

import (
	"testing"
	"time"

	natsc "github.com/nats-io/nats.go"

	"github.com/sandrolain/events-bridge/src/utils"
)

const errUnexpected = "unexpected error: %v"

func TestNATSSourceNewSourceValidation(t *testing.T) {
	if err := utils.ParseConfig(map[string]any{"address": "", "subject": "s", "queueGroup": "g"}, new(SourceConfig)); err == nil {
		t.Fatal("expected error when address is empty")
	}
	if err := utils.ParseConfig(map[string]any{"address": "nats://127.0.0.1:4222", "subject": "", "queueGroup": "g"}, new(SourceConfig)); err == nil {
		t.Fatal("expected error when subject is empty")
	}
}

func TestNATSSourceCloseWithoutStart(t *testing.T) {
	src := mustNewNATSSource(t, map[string]any{"address": "nats://127.0.0.1:4222", "subject": "s", "queueGroup": "g"})
	if err := src.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestNATSQueueGroupBasic(t *testing.T) {
	addr, cleanup := startNATSServer(t)
	defer cleanup()

	s1 := mustNewNATSSource(t, map[string]any{"address": addr, "subject": "share.*", "queueGroup": "grp"})
	ch1, err := s1.Produce(10)
	if err != nil {
		t.Fatalf("s1 produce: %v", err)
	}
	defer s1.Close() //nolint:errcheck

	s2 := mustNewNATSSource(t, map[string]any{"address": addr, "subject": "share.*", "queueGroup": "grp"})
	ch2, err := s2.Produce(10)
	if err != nil {
		t.Fatalf("s2 produce: %v", err)
	}
	defer s2.Close() //nolint:errcheck

	time.Sleep(200 * time.Millisecond)

	// publisher
	nc, err := natsc.Connect(addr)
	if err != nil {
		t.Fatalf("publisher connect: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := nc.Publish("share.x", []byte("m")); err != nil {
			t.Fatalf("publish err: %v", err)
		}
	}
	nc.Flush() //nolint:errcheck
	nc.Close()

	got1, got2 := 0, 0
	timeout := time.After(5 * time.Second)
	for got1+got2 < 10 {
		select {
		case m := <-ch1:
			if err := m.Ack(); err != nil {
				t.Logf("failed to ack message: %v", err)
			}
			got1++
		case m := <-ch2:
			if err := m.Ack(); err != nil {
				t.Logf("failed to ack message: %v", err)
			}
			got2++
		case <-timeout:
			t.Fatalf("timeout waiting queue messages, got1=%d got2=%d", got1, got2)
		}
	}
	if got1 == 0 || got2 == 0 {
		t.Fatalf("expected distribution across consumers, got1=%d got2=%d", got1, got2)
	}
}

func mustNewNATSSource(t *testing.T, opts map[string]any) *NATSSource {
	t.Helper()
	cfg := new(SourceConfig)
	if err := utils.ParseConfig(opts, cfg); err != nil {
		t.Fatalf("failed to parse source config: %v", err)
	}
	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}
	natsSrc, ok := src.(*NATSSource)
	if !ok {
		t.Fatalf("expected *NATSSource, got %T", src)
	}
	return natsSrc
}
