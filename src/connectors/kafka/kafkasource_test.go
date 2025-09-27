package main

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/utils"
)

const (
	testBrokerAddr = "localhost:9092"
)

func TestKafkaSourceNewSourceValidation(t *testing.T) {
	cfg := new(SourceConfig)
	if err := utils.ParseConfig(map[string]any{"brokers": []string{}, "topic": "t", "partitions": 1, "replicationFactor": 1}, cfg); err == nil {
		t.Fatal("expected error when brokers list is empty")
	}
	if err := utils.ParseConfig(map[string]any{"brokers": []string{testBrokerAddr}, "topic": "", "partitions": 1, "replicationFactor": 1}, cfg); err == nil {
		t.Fatal("expected error when topic is empty")
	}
	if err := utils.ParseConfig(map[string]any{"brokers": []string{testBrokerAddr}, "topic": "t", "partitions": 0, "replicationFactor": 1}, cfg); err == nil {
		t.Fatal("expected error when partitions is zero")
	}
}

func TestKafkaSourceCloseWithoutStart(t *testing.T) {
	cfg := new(SourceConfig)
	if err := utils.ParseConfig(map[string]any{"brokers": []string{testBrokerAddr}, "topic": "t", "partitions": 1, "replicationFactor": 1}, cfg); err != nil {
		t.Fatalf("unexpected error parsing config: %v", err)
	}
	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
