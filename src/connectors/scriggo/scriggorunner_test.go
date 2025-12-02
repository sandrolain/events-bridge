package main

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

const (
	scriptFileName        = "script.go"
	errMsgWriteScript     = "failed to write script file: %v"
	errMsgCreateRunner    = "failed to create runner: %v"
	errMsgProcessReturned = "process returned error: %v"
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
		Path:    filepath.Join(t.TempDir(), "missing.go"),
		Timeout: time.Second,
	}
	_, err := NewRunner(cfg)
	if err == nil {
		t.Fatalf("expected error when reading missing file, got nil")
	}

	if !strings.Contains(err.Error(), "failed to read go file") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScriggoRunnerProcessSuccess(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `package main

import "events"

func main() {
	events.Message.AddMetadata("processed", "true")
	events.Message.AddMetadata("status", "ok")
}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: 5 * time.Second,
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgCreateRunner, err)
	}
	runner, ok := runnerAny.(*ScriggoRunner)
	if !ok {
		t.Fatal("failed to cast to ScriggoRunner")
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

func TestScriggoRunnerProcessWithDataTransform(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `package main

import "events"

func main() {
	data, _ := events.Message.GetData()
	newData := append([]byte("processed: "), data...)
	events.Message.SetData(newData)
	events.Message.AddMetadata("transformed", "true")
}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: 5 * time.Second,
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgCreateRunner, err)
	}
	runner, ok := runnerAny.(*ScriggoRunner)
	if !ok {
		t.Fatal("failed to cast to ScriggoRunner")
	}

	stub := testutil.NewAdapter([]byte("hello"), nil)
	stub.ID = []byte("id")
	msg := message.NewRunnerMessage(stub)

	if err := runner.Process(msg); err != nil {
		t.Fatalf("process returned error: %v", err)
	}

	data, err := msg.GetData()
	if err != nil {
		t.Fatalf("failed to get data: %v", err)
	}
	expected := "processed: hello"
	if string(data) != expected {
		t.Fatalf("expected data to be %q, got %q", expected, string(data))
	}
}

func TestScriggoRunnerProcessBuildError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	// Invalid Go code - syntax error
	script := `package main

func main() {
	invalid syntax here {{{{
}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: time.Second,
	}
	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgCreateRunner, err)
	}
	runner, ok := runnerAny.(*ScriggoRunner)
	if !ok {
		t.Fatal("failed to cast runner to ScriggoRunner")
	}

	stub := testutil.NewAdapter([]byte("payload"), nil)
	msg := message.NewRunnerMessage(stub)
	err = runner.Process(msg)
	if err == nil {
		t.Fatalf("expected build error, got nil")
	}

	if !strings.Contains(err.Error(), "scriggo build error") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScriggoRunnerProcessRuntimeError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `package main

func main() {
	panic("intentional panic")
}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: time.Second,
	}
	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgCreateRunner, err)
	}
	runner, ok := runnerAny.(*ScriggoRunner)
	if !ok {
		t.Fatal("failed to cast runner to ScriggoRunner")
	}

	stub := testutil.NewAdapter([]byte("payload"), nil)
	msg := message.NewRunnerMessage(stub)
	err = runner.Process(msg)
	if err == nil {
		t.Fatalf("expected runtime error, got nil")
	}

	if !strings.Contains(err.Error(), "scriggo execution error") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScriggoRunnerTimeout(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	// Script with infinite loop
	script := `package main

func main() {
	for {
		// Infinite loop
	}
}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: 100 * time.Millisecond, // Short timeout
	}
	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgCreateRunner, err)
	}
	runner, ok := runnerAny.(*ScriggoRunner)
	if !ok {
		t.Fatal("failed to cast runner to ScriggoRunner")
	}

	stub := testutil.NewAdapter([]byte("payload"), nil)
	msg := message.NewRunnerMessage(stub)

	start := time.Now()
	err = runner.Process(msg)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}

	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected timeout error, got: %v", err)
	}

	// Verify it actually timed out (with some margin)
	if elapsed > 500*time.Millisecond {
		t.Errorf("timeout took too long: %v", elapsed)
	}
}

func TestScriggoRunnerScriptIntegritySuccess(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `package main

func main() {}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf(errMsgWriteScript, err)
	}

	// Calculate hash
	hash := sha256.Sum256([]byte(script))
	expectedHash := hex.EncodeToString(hash[:])

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          time.Second,
		VerifyScriptHash: true,
		ExpectedSHA256:   expectedHash,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgCreateRunner, err)
	}
	if runner == nil {
		t.Fatal("expected runner, got nil")
	}
	if err := runner.Close(); err != nil {
		t.Logf("failed to close runner: %v", err)
	}
}

func TestScriggoRunnerScriptIntegrityFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `package main

func main() {}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          time.Second,
		VerifyScriptHash: true,
		ExpectedSHA256:   "0000000000000000000000000000000000000000000000000000000000000000",
	}

	_, err := NewRunner(cfg)
	if err == nil {
		t.Fatal("expected error for hash mismatch, got nil")
	}

	if !strings.Contains(err.Error(), "script integrity") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScriggoRunnerClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `package main

func main() {}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errMsgCreateRunner, err)
	}

	// Test Close doesn't error
	err = runner.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	// Test multiple Close calls don't error
	err = runner.Close()
	if err != nil {
		t.Errorf("second Close returned error: %v", err)
	}
}

func TestNewRunnerConfig(t *testing.T) {
	t.Parallel()

	cfgAny := NewRunnerConfig()
	cfg, ok := cfgAny.(*RunnerConfig)
	if !ok {
		t.Fatal("NewRunnerConfig did not return *RunnerConfig")
	}

	if cfg == nil {
		t.Fatal("NewRunnerConfig returned nil")
	}

	// Verify it's an empty struct ready for population
	if cfg.Path != "" {
		t.Errorf("expected empty Path, got %s", cfg.Path)
	}
}

func TestVerifyScriptIntegrity(t *testing.T) {
	t.Parallel()

	script := []byte("test script content")
	hash := sha256.Sum256(script)
	correctHash := hex.EncodeToString(hash[:])

	// Test with correct hash
	err := VerifyScriptIntegrity(script, correctHash)
	if err != nil {
		t.Errorf("expected no error for correct hash, got: %v", err)
	}

	// Test with incorrect hash
	err = VerifyScriptIntegrity(script, "0000000000000000000000000000000000000000000000000000000000000000")
	if err == nil {
		t.Error("expected error for incorrect hash, got nil")
	}
}

func init() {
	// Suppress logs during tests
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	})))
}
