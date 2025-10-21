package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

const (
	scriptFileName        = "script.js"
	errMsgWriteScript     = "failed to write script file: %v"
	errMsgCreateRunner    = "failed to create runner: %v"
	errMsgProcessReturned = "process returned error: %v"
	benchID               = "bench-id"
	benchData             = "benchmark test data"
	benchDataComplex      = "benchmark test data for complex processing"
	benchSource           = "bench"
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

	cfg := &RunnerConfig{
		Path:             filepath.Join(t.TempDir(), "missing.js"),
		Timeout:          time.Second,
		MaxCallStackSize: 1000000,
	}
	_, err := NewRunner(cfg)
	if err == nil {
		t.Fatalf("expected error when reading missing file, got nil")
	}

	if !strings.Contains(err.Error(), "failed to read js file") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestES5RunnerProcessSuccess(t *testing.T) {
	t.Skip("FIXME: goja error handling issue - returns pointer instead of error message")
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `// Access message object and set metadata
if (typeof message !== 'undefined') {
	message.AddMetadata("processed", "true");
	message.AddMetadata("status", "ok");
}`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          time.Second,
		MaxCallStackSize: 1000000,
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgCreateRunner, err)
	}
	runner, ok := runnerAny.(*ES5Runner)
	if !ok {
		t.Fatal("failed to cast to ES5Runner")
	}

	stub := testutil.NewAdapter([]byte("hello"), map[string]string{"source": "test"})
	stub.ID = []byte("id")
	msg := message.NewRunnerMessage(stub)

	if err := runner.Process(msg); err != nil {
		t.Fatalf("process returned error: %v", err)
	}

	meta, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}
	if meta["processed"] != "true" {
		t.Fatalf("expected processed metadata to be true, got %q", meta["processed"])
	}
	if meta["status"] != "ok" {
		t.Fatalf("expected status metadata to be ok, got %q", meta["status"])
	}
}

func TestES5RunnerProcessRuntimeError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `throw new Error("boom");`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          time.Second,
		MaxCallStackSize: 1000000,
	}
	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgCreateRunner, err)
	}
	runner := runnerAny.(*ES5Runner)

	stub := testutil.NewAdapter([]byte("payload"), nil)
	msg := message.NewRunnerMessage(stub)
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
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `message.invalidMethod("test");`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          time.Second,
		MaxCallStackSize: 1000000,
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgCreateRunner, err)
	}
	runner := runnerAny.(*ES5Runner)

	stub := testutil.NewAdapter([]byte("payload"), nil)
	msg := message.NewRunnerMessage(stub)
	err = runner.Process(msg)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if !strings.Contains(err.Error(), "js execution error") {
		t.Fatalf("unexpected error: %v", err)
	}
}
