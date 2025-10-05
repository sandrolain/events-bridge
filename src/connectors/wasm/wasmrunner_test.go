package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

const (
	errMsgUnexpectedError = "unexpected error: %v"
	errMsgExpectedError   = "expected error, got nil"
	testWasmFile          = "testrunner.wasm"
	msgErrClosingRunner   = "error closing runner: %v"
	msgProcessReturnedNil = "Process returned nil result"
	msgErrGettingMetadata = "error getting metadata: %v"
	testMsgID             = "test-id"
	testMsgData           = "test data"
	msgExpectedNilRunner  = "expected nil runner on error"
)

var sharedRunner *WasmRunner

// getSharedRunner returns the shared runner instance (panics if not initialized)
func getSharedRunner() *WasmRunner {
	if sharedRunner == nil {
		panic("sharedRunner not initialized")
	}
	return sharedRunner
}

func TestMain(m *testing.M) {
	// Initialize a shared runner for tests that only need default config
	cfg := &RunnerConfig{
		Path:    getTestAssetPath("testrunner.wasm"),
		Timeout: 5 * time.Second,
		Format:  "json",
	}
	r, err := NewRunner(cfg)
	if err != nil {
		panic(err)
	}
	sharedRunner = r.(*WasmRunner)

	code := m.Run()

	// Close shared runner once after all tests
	if sharedRunner != nil {
		_ = sharedRunner.Close()
	}
	os.Exit(code)
}

// stubSourceMessage is a minimal implementation of message.SourceMessage for testing
type stubSourceMessage struct {
	id       []byte
	metadata map[string]string
	data     []byte
}

func (s *stubSourceMessage) GetID() []byte {
	return s.id
}

func (s *stubSourceMessage) GetMetadata() (map[string]string, error) {
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
		id: []byte(testMsgID),
		metadata: map[string]string{
			"source": "test",
			"type":   "test-message",
		},
		data: []byte(testMsgData),
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

func TestNewRunnerSuccess(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath(testWasmFile),
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf(msgErrClosingRunner, err)
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

func TestNewRunnerInvalidConfigType(t *testing.T) {
	t.Parallel()

	cfg := "invalid config type"

	runner, err := NewRunner(cfg)
	if err == nil {
		t.Fatal(errMsgExpectedError)
	}
	if runner != nil {
		t.Error(msgExpectedNilRunner)
	}
}

func TestNewRunnerFileNotFound(t *testing.T) {
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
		t.Error(msgExpectedNilRunner)
	}
}

func TestNewRunnerInvalidWasmFile(t *testing.T) {
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
		t.Error(msgExpectedNilRunner)
	}
}

func TestProcessSuccess(t *testing.T) {
	t.Parallel()
	runner := getSharedRunner()

	msg := createTestMessage()

	err := runner.Process(msg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	// Check that metadata was added by WASM
	meta, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(msgErrGettingMetadata, err)
	}

	if meta["wasm-processed"] != "true" {
		t.Error("expected wasm-processed metadata to be 'true'")
	}

	// Check that data was modified
	data, err := msg.GetData()
	if err != nil {
		t.Fatalf("error getting data: %v", err)
	}

	if len(data) == 0 {
		t.Error("expected non-empty data")
	}
}

func TestProcessWithEnvironment(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath(testWasmFile),
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
			t.Errorf(msgErrClosingRunner, err)
		}
	}()

	msg := createTestMessage()

	err = runner.Process(msg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	meta, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(msgErrGettingMetadata, err)
	}

	if meta["test-env-value"] != "test-value" {
		t.Errorf("expected test-env-value to be 'test-value', got %s", meta["test-env-value"])
	}
}

func TestProcessWithArgs(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath(testWasmFile),
		Timeout: 5 * time.Second,
		Args:    []string{"arg1", "arg2", "arg3"},
	}
	fmt.Printf("cfg: %v\n", cfg)

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf(msgErrClosingRunner, err)
		}
	}()

	msg := createTestMessage()

	err = runner.Process(msg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	meta, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf("error getting metadata: %v", err)
	}

	if meta["args"] == "" {
		t.Error("expected args metadata to be set")
	}
}

func TestProcessTimeout(t *testing.T) {
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
			t.Errorf(msgErrClosingRunner, err)
		}
	}()

	msg := createTestMessage()

	start := time.Now()
	err = runner.Process(msg)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal(errMsgExpectedError)
	}

	// Verify that it timed out relatively quickly (should be close to timeout value)
	if elapsed > 2*time.Second {
		t.Errorf("timeout took too long: %v", elapsed)
	}
}

func TestProcessWasmError(t *testing.T) {
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
			t.Errorf(msgErrClosingRunner, err)
		}
	}()

	msg := createTestMessage()

	err = runner.Process(msg)
	if err == nil {
		t.Fatal(errMsgExpectedError)
	}
}

func TestCloseSuccess(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath(testWasmFile),
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

func TestCloseMultipleCalls(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    getTestAssetPath(testWasmFile),
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

func TestProcessMultipleMessages(t *testing.T) {
	t.Parallel()
	runner := getSharedRunner()

	// Process multiple messages
	for i := 0; i < 5; i++ {
		stub := &stubSourceMessage{
			id: []byte(testMsgID),
			metadata: map[string]string{
				"iteration": string(rune(i)),
			},
			data: []byte("test data"),
		}
		msg := message.NewRunnerMessage(stub)

		err := runner.Process(msg)
		if err != nil {
			t.Fatalf("error processing message %d: %v", i, err)
		}

		// Verify processing occurred
		resultMeta, err := msg.GetMetadata()
		if err != nil {
			t.Fatalf("error getting metadata for message %d: %v", i, err)
		}

		if resultMeta["wasm-processed"] != "true" {
			t.Errorf("message %d was not processed correctly", i)
		}
	}
}

func TestProcessEmptyMetadata(t *testing.T) {
	t.Parallel()
	runner := getSharedRunner()

	stub := &stubSourceMessage{
		id:       []byte(testMsgID),
		metadata: map[string]string{},
		data:     []byte(testMsgData),
	}
	msg := message.NewRunnerMessage(stub)

	err := runner.Process(msg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	if msg == nil {
		t.Fatal(msgProcessReturnedNil)
	}
}

func TestProcessEmptyData(t *testing.T) {
	t.Parallel()
	runner := getSharedRunner()

	stub := &stubSourceMessage{
		id: []byte(testMsgID),
		metadata: map[string]string{
			"test": "value",
		},
		data: []byte{},
	}
	msg := message.NewRunnerMessage(stub)

	err := runner.Process(msg)
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
}
