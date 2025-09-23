package main

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/targets"
)

func TestMQTTTargetNewTargetValidation(t *testing.T) {
	// missing address
	if _, err := NewTarget(&targets.TargetMQTTConfig{Address: "", Topic: "t"}); err == nil {
		t.Fatal("expected error when address is empty")
	}
	// missing topic
	if _, err := NewTarget(&targets.TargetMQTTConfig{Address: "localhost:1883", Topic: ""}); err == nil {
		t.Fatal("expected error when topic is empty")
	}
}

func TestMQTTTargetCloseWithoutStart(t *testing.T) {
	// Construct target with minimal config but don't connect a real broker
	tgt := &MQTTTarget{}
	if err := tgt.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
