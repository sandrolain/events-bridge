package main

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/sources"
)

func TestMQTTSourceNewSourceValidation(t *testing.T) {
	// missing address
	if _, err := NewSource(&sources.SourceMQTTConfig{Address: "", Topic: "t"}); err == nil {
		t.Fatal("expected error when address is empty")
	}
	// missing topic
	if _, err := NewSource(&sources.SourceMQTTConfig{Address: "localhost:1883", Topic: ""}); err == nil {
		t.Fatal("expected error when topic is empty")
	}
}

func TestMQTTSourceCloseWithoutStart(t *testing.T) {
	src, err := NewSource(&sources.SourceMQTTConfig{Address: "localhost:1883", Topic: "t"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
