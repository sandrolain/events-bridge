package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestNewRunner(t *testing.T) {
	// Test with Logic
	cfg := &RunnerConfig{
		Logic:   `{"==": [{"var": "data.value"}, 42]}`,
		Timeout: 5 * time.Second,
	}
	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	if runner == nil {
		t.Fatal("runner is nil")
	}

	// Test with Path - but since it's test, maybe skip or mock
	// For simplicity, test with Logic
}

func TestProcess(t *testing.T) {
	cfg := &RunnerConfig{
		Logic:           `{"==": [{"var": "data.value"}, 42]}`,
		PreservePayload: false,
		Timeout:         5 * time.Second,
	}
	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer runner.Close()

	// Create a test message
	stub := testutil.NewAdapter([]byte(`{"value": 42}`), map[string]string{"source": "test"})
	stub.ID = []byte("test-id")
	msg := message.NewRunnerMessage(stub)

	err = runner.Process(msg)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}

	// Since preservePayload is false, result should be true
	data, err := msg.GetData()
	if err != nil {
		t.Fatalf("failed to get data: %v", err)
	}
	var res bool
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	if !res {
		t.Fatal("expected true, got false")
	}
}

func TestProcessWithPreservePayload(t *testing.T) {
	cfg := &RunnerConfig{
		Logic:           `{"==": [{"var": "data.value"}, 42]}`,
		PreservePayload: true,
		Timeout:         5 * time.Second,
	}
	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer runner.Close()

	stub := testutil.NewAdapter([]byte(`{"value": 42}`), map[string]string{"source": "test"})
	stub.ID = []byte("test-id")
	msg := message.NewRunnerMessage(stub)

	err = runner.Process(msg)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}

	data, err := msg.GetData()
	if err != nil {
		t.Fatalf("failed to get data: %v", err)
	}
	var res map[string]interface{}
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	if _, ok := res["payload"]; !ok {
		t.Fatal("expected payload in result")
	}
	if _, ok := res["result"]; !ok {
		t.Fatal("expected result in result")
	}
}

func TestProcessTimeout(t *testing.T) {
	// A logic that might take time, but for simplicity, use a short timeout
	cfg := &RunnerConfig{
		Logic:   `{"==": [{"var": "data.value"}, 42]}`,
		Timeout: 1 * time.Nanosecond, // very short
	}
	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer runner.Close()

	stub := testutil.NewAdapter([]byte(`{"value": 42}`), map[string]string{})
	stub.ID = []byte("test-id")
	msg := message.NewRunnerMessage(stub)

	err = runner.Process(msg)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "jsonlogic execution timeout") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClose(t *testing.T) {
	cfg := &RunnerConfig{
		Logic:   `{"==": [{"var": "data.value"}, 42]}`,
		Timeout: 5 * time.Second,
	}
	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}

	err = runner.Close()
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Close again should not error
	err = runner.Close()
	if err != nil {
		t.Fatalf("second close failed: %v", err)
	}
}
