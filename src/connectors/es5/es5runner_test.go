package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

func TestNewRunner_InvalidConfigType(t *testing.T) {
	t.Parallel()

	_, err := NewRunner(struct{}{})
	if err == nil {
		t.Fatalf("expected error for invalid config type, got nil")
	}

	if !strings.Contains(err.Error(), "invalid config type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewRunner_ReadFileError(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{Path: filepath.Join(t.TempDir(), "missing.js"), Timeout: time.Second}
	_, err := NewRunner(cfg)
	if err == nil {
		t.Fatalf("expected error when reading missing file, got nil")
	}

	if !strings.Contains(err.Error(), "failed to read js file") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestES5RunnerProcessSuccess(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "script.js")
	script := `
// Simply set new data without reading the old data
message.SetData(new Uint8Array([72, 69, 76, 76, 79])); // "HELLO" in ASCII
message.AddMetadata("processed", "true");
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf("failed to write script file: %v", err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: time.Second,
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	runner := runnerAny.(*ES5Runner)

	msg := message.NewRunnerMessage(&stubSourceMessage{
		id:       []byte("id"),
		data:     []byte("hello"),
		metadata: map[string]string{"source": "test"},
	})

	if err := runner.Process(msg); err != nil {
		t.Fatalf("process returned error: %v", err)
	}

	resultData, err := msg.GetData()
	if err != nil {
		t.Fatalf("failed to get message data: %v", err)
	}
	if string(resultData) != "HELLO" {
		t.Fatalf("unexpected message data: %s", resultData)
	}

	meta, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}
	if meta["processed"] != "true" {
		t.Fatalf("expected processed metadata to be true, got %q", meta["processed"])
	}
}

func TestES5RunnerProcessRuntimeError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "script.js")
	script := `throw new Error("boom");`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf("failed to write script file: %v", err)
	}

	cfg := &RunnerConfig{Path: scriptPath, Timeout: time.Second}
	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	runner := runnerAny.(*ES5Runner)

	msg := message.NewRunnerMessage(&stubSourceMessage{data: []byte("payload")})
	err = runner.Process(msg)
	if err == nil {
		t.Fatalf("expected runtime error, got nil")
	}

	if !strings.Contains(err.Error(), "js execution error") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestES5RunnerProcessInvalidServiceMethod(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "script.js")
	script := `message.invalidMethod("test");`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf("failed to write script file: %v", err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: time.Second,
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	runner := runnerAny.(*ES5Runner)

	msg := message.NewRunnerMessage(&stubSourceMessage{data: []byte("payload")})
	err = runner.Process(msg)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if !strings.Contains(err.Error(), "js execution error") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type stubSourceMessage struct {
	id       []byte
	data     []byte
	metadata map[string]string
}

func (s *stubSourceMessage) GetID() []byte {
	if s.id != nil {
		return s.id
	}
	return []byte("stub-id")
}

func (s *stubSourceMessage) GetMetadata() (map[string]string, error) {
	if s.metadata == nil {
		return map[string]string{}, nil
	}
	return s.metadata, nil
}

func (s *stubSourceMessage) GetData() ([]byte, error) {
	return s.data, nil
}

func (s *stubSourceMessage) Ack() error { return nil }

func (s *stubSourceMessage) Nak() error { return nil }

func (s *stubSourceMessage) Reply(data *message.ReplyData) error { return nil }

func (s *stubSourceMessage) ReplySource() error { return nil }
