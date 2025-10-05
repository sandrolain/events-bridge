package main

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

// Test for CLI Runner functions (0% coverage)
func TestCLIRunner(t *testing.T) {
	// Test NewRunnerConfig
	cfg := NewRunnerConfig()
	runnerCfg, ok := cfg.(*RunnerConfig)
	if !ok {
		t.Fatal("NewRunnerConfig should return *RunnerConfig")
	}
	if runnerCfg == nil {
		t.Fatal("NewRunnerConfig should not return nil")
	}

	// Test NewRunner with valid config
	runnerCfg.Command = "echo"
	runnerCfg.Args = []string{"test"}
	runnerCfg.Timeout = 5 * time.Second
	runnerCfg.Format = "cli"

	runner, err := NewRunner(runnerCfg)
	if err != nil {
		t.Fatalf("NewRunner error: %v", err)
	}

	// Test Close (skip Process test as it requires specific input format)
	err = runner.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}
}

// mockSourceMessage implements SourceMessage for testing
type mockSourceMessage struct {
	id       []byte
	metadata map[string]string
	data     []byte
}

func (m *mockSourceMessage) GetID() []byte {
	return m.id
}

func (m *mockSourceMessage) GetMetadata() (map[string]string, error) {
	return m.metadata, nil
}

func (m *mockSourceMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *mockSourceMessage) Ack() error {
	return nil
}

func (m *mockSourceMessage) Nak() error {
	return nil
}

func (m *mockSourceMessage) Reply(data *message.ReplyData) error {
	return nil
}

func TestCLIRunnerProcess(t *testing.T) {
	// Test successful process with cat command
	runnerCfg := &RunnerConfig{
		Command: "cat",
		Timeout: 5 * time.Second,
		Format:  "cli",
	}

	runner, err := NewRunner(runnerCfg)
	if err != nil {
		t.Fatalf("NewRunner error: %v", err)
	}
	defer runner.Close()

	originalMeta := map[string]string{"key1": "value1", "key2": "value2"}
	originalData := []byte("test data")

	mockSource := &mockSourceMessage{
		id:       []byte("test-id"),
		metadata: originalMeta,
		data:     originalData,
	}

	msg := message.NewRunnerMessage(mockSource)

	err = runner.Process(msg)
	if err != nil {
		t.Fatalf("Process error: %v", err)
	}

	resultMeta, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf("GetMetadata error: %v", err)
	}

	if len(resultMeta) != len(originalMeta) {
		t.Fatalf("Metadata length mismatch: expected %d, got %d", len(originalMeta), len(resultMeta))
	}

	for k, v := range originalMeta {
		if resultMeta[k] != v {
			t.Fatalf("Metadata mismatch for key %s: expected %s, got %s", k, v, resultMeta[k])
		}
	}

	resultData, err := msg.GetData()
	if err != nil {
		t.Fatalf("GetData error: %v", err)
	}

	if string(resultData) != string(originalData) {
		t.Fatalf("Data mismatch: expected %s, got %s", string(originalData), string(resultData))
	}
}

func TestCLIRunnerProcessError(t *testing.T) {
	// Test process with failing command
	runnerCfg := &RunnerConfig{
		Command: "false", // Command that always fails
		Timeout: 5 * time.Second,
		Format:  "cli",
	}

	runner, err := NewRunner(runnerCfg)
	if err != nil {
		t.Fatalf("NewRunner error: %v", err)
	}
	defer runner.Close()

	originalMeta := map[string]string{"key": "value"}
	originalData := []byte("data")

	mockSource := &mockSourceMessage{
		id:       []byte("test-id"),
		metadata: originalMeta,
		data:     originalData,
	}

	msg := message.NewRunnerMessage(mockSource)

	err = runner.Process(msg)
	if err == nil {
		t.Fatal("Expected error from failing command, but got none")
	}
}

func TestCLIRunnerProcessDecodeError(t *testing.T) {
	// Test process with command that produces invalid output
	runnerCfg := &RunnerConfig{
		Command: "echo", // echo will output the input as text, not binary frame
		Args:    []string{"invalid output"},
		Timeout: 5 * time.Second,
		Format:  "cli",
	}

	runner, err := NewRunner(runnerCfg)
	if err != nil {
		t.Fatalf("NewRunner error: %v", err)
	}
	defer runner.Close()

	originalMeta := map[string]string{"key": "value"}
	originalData := []byte("data")

	mockSource := &mockSourceMessage{
		id:       []byte("test-id"),
		metadata: originalMeta,
		data:     originalData,
	}

	msg := message.NewRunnerMessage(mockSource)

	err = runner.Process(msg)
	if err == nil {
		t.Fatal("Expected decode error, but got none")
	}
}
