package main

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestNewRunnerConfig(t *testing.T) {
	cfg := NewRunnerConfig()
	if cfg == nil {
		t.Fatal("NewRunnerConfig returned nil")
	}
}

func TestTemplateOperation(t *testing.T) {
	cfg := &FormatRunnerConfig{
		Timeout: 30 * time.Second,
		Operations: []FormatOperation{
			{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Hello {{ .data }}",
					"maxTemplateSize": 1024,
				},
			},
		},
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer runner.Close()

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("World"), nil))

	if err := runner.Process(msg); err != nil {
		t.Fatalf("failed to process message: %v", err)
	}

	result, err := msg.GetData()
	if err != nil {
		t.Fatalf("failed to get result: %v", err)
	}

	expected := "Hello World"
	if string(result) != expected {
		t.Errorf("expected %q, got %q", expected, string(result))
	}
}

func TestEncodeOperation(t *testing.T) {
	cfg := &FormatRunnerConfig{
		Timeout: 30 * time.Second,
		Operations: []FormatOperation{
			{
				Type:   "encode",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"encoding":  "base64",
					"operation": "encode",
				},
			},
		},
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer runner.Close()

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("hello"), nil))

	if err := runner.Process(msg); err != nil {
		t.Fatalf("failed to process message: %v", err)
	}

	result, err := msg.GetData()
	if err != nil {
		t.Fatalf("failed to get result: %v", err)
	}

	expected := "aGVsbG8="
	if string(result) != expected {
		t.Errorf("expected %q, got %q", expected, string(result))
	}
}
