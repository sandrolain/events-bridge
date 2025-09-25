package main

import (
	"testing"
)

func TestKafkaTargetNewTargetValidation(t *testing.T) {
	_, err := NewTarget(map[string]any{"brokers": testBrokerAddr, "topic": "t"})
	if err == nil {
		t.Fatal("expected error when brokers option has invalid type")
	}
	_, err = NewTarget(map[string]any{"brokers": []string{testBrokerAddr}, "topic": 99})
	if err == nil {
		t.Fatal("expected error when topic option has invalid type")
	}
}

func TestKafkaTargetCloseWithoutStart(t *testing.T) {
	// We cannot instantiate a real target without a broker due to ensureKafkaTopic; this test ensures Close on zero-value struct is safe.
	tgt := &KafkaTarget{}
	if err := tgt.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
