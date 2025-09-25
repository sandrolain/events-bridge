package main

import (
	"testing"
)

func TestKafkaTargetNewTargetValidation(t *testing.T) {
	// missing brokers
	_, err := NewTarget(map[string]any{"brokers": []string{}, "topic": "t"})
	if err == nil {
		t.Fatal("expected error when brokers are empty")
	}
	// missing topic
	_, err = NewTarget(map[string]any{"brokers": []string{"localhost:9092"}, "topic": ""})
	if err == nil {
		t.Fatal("expected error when topic is empty")
	}
}

func TestKafkaTargetCloseWithoutStart(t *testing.T) {
	// We cannot instantiate a real target without a broker due to ensureKafkaTopic; this test ensures Close on zero-value struct is safe.
	tgt := &KafkaTarget{}
	if err := tgt.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
