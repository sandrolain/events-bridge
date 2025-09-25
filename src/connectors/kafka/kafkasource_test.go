package main

import (
	"testing"
)

func TestKafkaSourceNewSourceValidation(t *testing.T) {
	// missing brokers
	_, err := NewSource(map[string]any{"brokers": []string{}, "topic": "t"})
	if err == nil {
		t.Fatal("expected error when brokers are empty")
	}
	// missing topic
	_, err = NewSource(map[string]any{"brokers": []string{"localhost:9092"}, "topic": ""})
	if err == nil {
		t.Fatal("expected error when topic is empty")
	}
}

func TestKafkaSourceCloseWithoutStart(t *testing.T) {
	src, err := NewSource(map[string]any{"brokers": []string{"localhost:9092"}, "topic": "t"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
