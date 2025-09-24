package main

import (
	"testing"
)

func TestKafkaTargetNewTargetValidation(t *testing.T) {
	// missing brokers
	_, err := NewTarget(&TargetConfig{Brokers: nil, Topic: "t"})
	if err == nil {
		t.Fatal("expected error when brokers are empty")
	}
	// missing topic
	_, err = NewTarget(&TargetConfig{Brokers: []string{"localhost:9092"}, Topic: ""})
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
