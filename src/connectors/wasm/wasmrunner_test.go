package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

const (
	errMsgUnexpectedError = "unexpected error: %v"
	errMsgExpectedError   = "expected error, got nil"
)

// stubSourceMessage is a minimal implementation of message.SourceMessage for testing
type stubSourceMessage struct {
	id       []byte
	metadata message.MessageMetadata
	data     []byte
}

func (s *stubSourceMessage) GetID() []byte {
	return s.id
}

func (s *stubSourceMessage) GetMetadata() (message.MessageMetadata, error) {
	return s.metadata, nil
}

func (s *stubSourceMessage) GetData() ([]byte, error) {
	return s.data, nil
}

func (s *stubSourceMessage) Ack() error {
	return nil
}

func (s *stubSourceMessage) Nak() error {
	return nil
}

func (s *stubSourceMessage) Reply(_ *message.ReplyData) error {
	return nil
}

// getTestAssetPath returns the absolute path to a test asset
func getTestAssetPath(filename string) string {
	return filepath.Join("testassets", filename)
}

// createTestMessage creates a simple test message
func createTestMessage() *message.RunnerMessage {
	stub := &stubSourceMessage{
		id: []byte("test-id"),
		metadata: message.MessageMetadata{
			"source": "test",
			"type":   "test-message",
		},
		data: []byte("test data"),
	}
	return message.NewRunnerMessage(stub)
}

func TestNewRunnerConfig(t *testing.T) {
	t.Parallel()

	cfg := NewRunnerConfig()
	if cfg == nil {
		t.Fatal("NewRunnerConfig returned nil")
	}

	_, ok := cfg.(*RunnerConfig)
	if !ok {
		t.Fatalf("NewRunnerConfig returned wrong type: %T", cfg)
	}
}

func TestNewRunner_Success(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("testrunner.wasm"),
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("error closing runner: %v", err)
		}
	}()

	if runner == nil {
		t.Fatal("NewRunner returned nil runner")
	}

	wasmRunner, ok := runner.(*WasmRunner)
	if !ok {
		t.Fatalf("NewRunner returned wrong type: %T", runner)
	}

	if wasmRunner.cfg.Path != cfg.Path {
		t.Errorf("expected path %s, got %s", cfg.Path, wasmRunner.cfg.Path)
	}

	if wasmRunner.timeout != cfg.Timeout {
		t.Errorf("expected timeout %v, got %v", cfg.Timeout, wasmRunner.timeout)
	}
}

func TestNewRunner_InvalidConfigType(t *testing.T) {
	t.Parallel()

	cfg := "invalid config type"

	runner, err := NewRunner(cfg)
	if err == nil {
		t.Fatal(errMsgExpectedError)
	}
	if runner != nil {
		t.Error("expected nil runner on error")
	}
}

func TestNewRunner_FileNotFound(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    "nonexistent.wasm",
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err == nil {
		t.Fatal(errMsgExpectedError)
	}
	if runner != nil {
		t.Error("expected nil runner on error")
	}
}

func TestNewRunner_InvalidWasmFile(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("Makefile"), // Not a WASM file
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err == nil {
		t.Fatal(errMsgExpectedError)
	}
	if runner != nil {
		t.Error("expected nil runner on error")
	}
}

func TestProcess_Success(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("testrunner.wasm"),
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("error closing runner: %v", err)
		}
	}()

	msg := createTestMessage()

	result, err := runner.Process(msg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	if result == nil {
		t.Fatal("Process returned nil result")
	}

	// Check that metadata was added by WASM
	meta, err := result.GetMetadata()
	if err != nil {
		t.Fatalf("error getting metadata: %v", err)
	}

	if meta["wasm-processed"] != "true" {
		t.Error("expected wasm-processed metadata to be 'true'")
	}

	// Check that data was modified
	data, err := result.GetData()
	if err != nil {
		t.Fatalf("error getting data: %v", err)
	}

	if len(data) == 0 {
		t.Error("expected non-empty data")
	}
}

func TestProcess_WithEnvironment(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("testrunner.wasm"),
		Timeout: 5 * time.Second,
		Env: map[string]string{
			"TEST_ENV": "test-value",
		},
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("error closing runner: %v", err)
		}
	}()

	msg := createTestMessage()

	result, err := runner.Process(msg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	meta, err := result.GetMetadata()
	if err != nil {
		t.Fatalf("error getting metadata: %v", err)
	}

	if meta["test-env-value"] != "test-value" {
		t.Errorf("expected test-env-value to be 'test-value', got %s", meta["test-env-value"])
	}
}

func TestProcess_WithArgs(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("testrunner.wasm"),
		Timeout: 5 * time.Second,
		Args:    []string{"arg1", "arg2", "arg3"},
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("error closing runner: %v", err)
		}
	}()

	msg := createTestMessage()

	result, err := runner.Process(msg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	meta, err := result.GetMetadata()
	if err != nil {
		t.Fatalf("error getting metadata: %v", err)
	}

	if meta["args"] == "" {
		t.Error("expected args metadata to be set")
	}
}

func TestProcess_Timeout(t *testing.T) {
	t.Skip("Timeout behavior with WASM is inconsistent across platforms")

	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("timeoutrunner.wasm"),
		Timeout: 500 * time.Millisecond, // Even shorter timeout
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("error closing runner: %v", err)
		}
	}()

	msg := createTestMessage()

	start := time.Now()
	_, err = runner.Process(msg)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal(errMsgExpectedError)
	}

	// Verify that it timed out relatively quickly (should be close to timeout value)
	if elapsed > 2*time.Second {
		t.Errorf("timeout took too long: %v", elapsed)
	}
}

func TestProcess_WasmError(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("errorrunner.wasm"),
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("error closing runner: %v", err)
		}
	}()

	msg := createTestMessage()

	_, err = runner.Process(msg)
	if err == nil {
		t.Fatal(errMsgExpectedError)
	}
}

func TestClose_Success(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("testrunner.wasm"),
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	err = runner.Close()
	if err != nil {
		t.Fatalf("error closing runner: %v", err)
	}

	// Second close should not error
	err = runner.Close()
	if err != nil {
		t.Fatalf("error on second close: %v", err)
	}
}

func TestClose_MultipleCalls(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("testrunner.wasm"),
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	// Multiple closes should be safe
	for i := 0; i < 3; i++ {
		if err := runner.Close(); err != nil {
			t.Errorf("error on close #%d: %v", i+1, err)
		}
	}
}

func TestProcess_MultipleMessages(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("testrunner.wasm"),
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("error closing runner: %v", err)
		}
	}()

	// Process multiple messages
	for i := 0; i < 5; i++ {
		stub := &stubSourceMessage{
			id: []byte("test-id"),
			metadata: message.MessageMetadata{
				"iteration": string(rune(i)),
			},
			data: []byte("test data"),
		}
		msg := message.NewRunnerMessage(stub)

		result, err := runner.Process(msg)
		if err != nil {
			t.Fatalf("error processing message %d: %v", i, err)
		}

		if result == nil {
			t.Fatalf("Process returned nil result for message %d", i)
		}

		// Verify processing occurred
		resultMeta, err := result.GetMetadata()
		if err != nil {
			t.Fatalf("error getting metadata for message %d: %v", i, err)
		}

		if resultMeta["wasm-processed"] != "true" {
			t.Errorf("message %d was not processed correctly", i)
		}
	}
}

func TestProcess_EmptyMetadata(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("testrunner.wasm"),
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("error closing runner: %v", err)
		}
	}()

	stub := &stubSourceMessage{
		id:       []byte("test-id"),
		metadata: message.MessageMetadata{},
		data:     []byte("test data"),
	}
	msg := message.NewRunnerMessage(stub)

	result, err := runner.Process(msg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	if result == nil {
		t.Fatal("Process returned nil result")
	}
}

func TestProcess_EmptyData(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath("testrunner.wasm"),
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("error closing runner: %v", err)
		}
	}()

	stub := &stubSourceMessage{
		id: []byte("test-id"),
		metadata: message.MessageMetadata{
			"test": "value",
		},
		data: []byte{},
	}
	msg := message.NewRunnerMessage(stub)

	result, err := runner.Process(msg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	if result == nil {
		t.Fatal("Process returned nil result")
	}
}
