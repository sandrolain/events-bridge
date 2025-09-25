package main

import (
	"testing"
	"time"

	natsc "github.com/nats-io/nats.go"
)

const errUnexpected = "unexpected error: %v"

func TestNATSSourceNewSourceValidation(t *testing.T) {
	if _, err := NewSource(map[string]any{"address": "", "subject": "s"}); err == nil {
		t.Fatal("expected error when address is empty")
	}
	if _, err := NewSource(map[string]any{"address": "nats://127.0.0.1:4222", "subject": ""}); err == nil {
		t.Fatal("expected error when subject is empty")
	}
}

func TestNATSSourceCloseWithoutStart(t *testing.T) {
	src, err := NewSource(map[string]any{"address": "nats://127.0.0.1:4222", "subject": "s"})
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestNATSQueueGroupBasic(t *testing.T) {
	addr, cleanup := startNATSServer(t)
	defer cleanup()

	s1, _ := NewSource(map[string]any{"address": addr, "subject": "share.*", "queueGroup": "grp"})
	ch1, err := s1.Produce(10)
	if err != nil {
		t.Fatalf("s1 produce: %v", err)
	}
	defer s1.Close()

	s2, _ := NewSource(map[string]any{"address": addr, "subject": "share.*", "queueGroup": "grp"})
	ch2, err := s2.Produce(10)
	if err != nil {
		t.Fatalf("s2 produce: %v", err)
	}
	defer s2.Close()

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
	nc.Flush()
	nc.Close()

	got1, got2 := 0, 0
	timeout := time.After(5 * time.Second)
	for got1+got2 < 10 {
		select {
		case m := <-ch1:
			_ = m.Ack()
			got1++
		case m := <-ch2:
			_ = m.Ack()
			got2++
		case <-timeout:
			t.Fatalf("timeout waiting queue messages, got1=%d got2=%d", got1, got2)
		}
	}
	if got1 == 0 || got2 == 0 {
		t.Fatalf("expected distribution across consumers, got1=%d got2=%d", got1, got2)
	}
}
