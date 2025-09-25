package main

import (
	"testing"
)

const (
	testBrokerAddr = "localhost:9092"
)

func TestKafkaSourceNewSourceValidation(t *testing.T) {
	_, err := NewSource(map[string]any{"brokers": testBrokerAddr, "topic": "t"})
	if err == nil {
		t.Fatal("expected error when brokers option has invalid type")
	}
	_, err = NewSource(map[string]any{"brokers": []string{testBrokerAddr}, "topic": 42})
	if err == nil {
		t.Fatal("expected error when topic option has invalid type")
	}
}

func TestKafkaSourceCloseWithoutStart(t *testing.T) {
	src, err := NewSource(map[string]any{"brokers": []string{testBrokerAddr}, "topic": "t"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
