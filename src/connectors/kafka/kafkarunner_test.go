package main

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/utils"
)

func TestKafkaRunnerNewRunnerValidation(t *testing.T) {
	cfg := new(RunnerConfig)
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

func TestKafkaRunnerCloseWithoutStart(t *testing.T) {
	// We cannot instantiate a real target without a broker due to ensureKafkaTopic; this test ensures Close on zero-value struct is safe.
	tgt := &KafkaRunner{}
	if err := tgt.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
