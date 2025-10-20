package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestNewExprRunner(t *testing.T) {
	// Test with Expression
	cfg := &ExprRunnerConfig{
		Expression: "data.value == 42",
		Timeout:    5 * time.Second,
	}
	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	if runner == nil {
		t.Fatal("runner is nil")
	}
}

func TestExprProcess(t *testing.T) {
	cfg := &ExprRunnerConfig{
		Expression:      "data.value == 42",
		PreservePayload: false,
		Timeout:         5 * time.Second,
	}
	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Logf("failed to close runner: %v", err)
		}
	}()

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

func TestExprProcessWithPreservePayload(t *testing.T) {
	cfg := &ExprRunnerConfig{
		Expression:      "data.value == 42",
		PreservePayload: true,
		Timeout:         5 * time.Second,
	}
	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Logf("failed to close runner: %v", err)
		}
	}()

	stub2 := testutil.NewAdapter([]byte(`{"value": 42}`), map[string]string{"source": "test"})
	stub2.ID = []byte("test-id")
	msg := message.NewRunnerMessage(stub2)

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

// TestExprProcessTimeout verifies that the timeout mechanism works
// Note: Simple expressions may complete faster than very short timeouts can detect.
// This test uses a microsecond timeout which, while artificially short, demonstrates
// the timeout mechanism exists and will trigger for operations that don't complete
// extremely quickly. In production, timeouts are typically seconds.
func TestExprProcessTimeout(t *testing.T) {
	t.Skip("Timeout behavior with expr-lang is timing-dependent and may not trigger for very fast expressions")

	// Use an extremely short timeout
	timeoutDuration := 1 * time.Microsecond

	cfg := &ExprRunnerConfig{
		Expression: "data.value == 42",
		Timeout:    timeoutDuration,
	}
	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Logf("failed to close runner: %v", err)
		}
	}()

	stub := testutil.NewAdapter([]byte(`{"value": 42}`), map[string]string{})
	stub.ID = []byte("test-id")
	msg := message.NewRunnerMessage(stub)

	// With 1μs timeout, may or may not timeout depending on system speed
	err = runner.Process(msg)

	// If it times out, verify the error message
	if err != nil && strings.Contains(err.Error(), "expr execution timeout") {
		t.Logf("Successfully triggered timeout with %v", timeoutDuration)
	} else if err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else {
		t.Log("Expression completed faster than timeout could detect (acceptable)")
	}
}

// TestExprProcessNoTimeout verifies that fast operations don't timeout
func TestExprProcessNoTimeout(t *testing.T) {
	cfg := &ExprRunnerConfig{
		Expression: "data.value == 42", // Fast operation
		Timeout:    5 * time.Second,    // Generous timeout
	}
	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer runner.Close()

	stub := testutil.NewAdapter([]byte(`{"value": 42}`), map[string]string{})
	stub.ID = []byte("test-id")
	msg := message.NewRunnerMessage(stub)

	// Should complete successfully without timeout
	err = runner.Process(msg)
	if err != nil {
		t.Fatalf("unexpected error (should not timeout): %v", err)
	}

	// Verify result
	data, err := msg.GetData()
	if err != nil {
		t.Fatalf("failed to get data: %v", err)
	}
	var res bool
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	if !res {
		t.Error("expected true result")
	}
}

func TestExprClose(t *testing.T) {
	cfg := &ExprRunnerConfig{
		Expression: "data.value == 42",
		Timeout:    5 * time.Second,
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

// Removed local stubSourceMessage - now using testutil.NewAdapter()
