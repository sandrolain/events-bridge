package main

import (
	"strings"
	"testing"

	"log/slog"
)

func TestEnsureKafkaTopicDialError(t *testing.T) {
	// Use an unroutable address to avoid hanging
	brokers := []string{"127.0.0.1:1"}
	err := ensureKafkaTopic(slog.Default(), brokers, "test-topic", 1, 1)
	if err == nil {
		t.Fatal("expected error when dialing unreachable broker")
	}
	if !strings.Contains(err.Error(), "error connecting to Kafka") {
		t.Fatalf("unexpected error: %v", err)
	}
}
