package main

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/sources"
)

func TestKafkaSourceNewSourceValidation(t *testing.T) {
	// missing brokers
	_, err := NewSource(&sources.SourceKafkaConfig{Brokers: nil, Topic: "t"})
	if err == nil {
		t.Fatal("expected error when brokers are empty")
	}
	// missing topic
	_, err = NewSource(&sources.SourceKafkaConfig{Brokers: []string{"localhost:9092"}, Topic: ""})
	if err == nil {
		t.Fatal("expected error when topic is empty")
	}
}

func TestKafkaSourceCloseWithoutStart(t *testing.T) {
	src, err := NewSource(&sources.SourceKafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "t"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
