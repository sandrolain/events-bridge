package main

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
)

const (
	testScriptName        = "test.go"
	errWriteScript        = "failed to write test script: %v"
	errCreateRunnerSec    = "failed to create runner: %v"
	securityTestTimeout   = 5 * time.Second
	testDataPayloadSecure = "test data"
)

// stubSecuritySourceMessage is a simple implementation for testing
type stubSecuritySourceMessage struct {
	id       []byte
	data     []byte
	metadata map[string]string
}

func (s *stubSecuritySourceMessage) GetID() []byte {
	if s.id != nil {
		return s.id
	}
	return []byte("security-test-id")
}

func (s *stubSecuritySourceMessage) GetMetadata() (map[string]string, error) {
	if s.metadata == nil {
		return map[string]string{}, nil
	}
	return s.metadata, nil
}

func (s *stubSecuritySourceMessage) GetData() ([]byte, error) {
	return s.data, nil
}

func (s *stubSecuritySourceMessage) GetFilesystem() (fsutil.Filesystem, error) {
	return nil, nil
}

func (s *stubSecuritySourceMessage) Ack(data *message.ReplyData) error {
	return nil
}

func (s *stubSecuritySourceMessage) Nak() error { return nil }

func TestSecurityRunnerConfigDefaults(t *testing.T) {
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

	if cfg.AllowGoStmt {
		t.Error("expected AllowGoStmt to be false by default")
	}
}

func TestSecurityScriptIntegrityVerificationSuccess(t *testing.T) {
	t.Parallel()

	// Create a temporary test script
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, testScriptName)
	scriptContent := []byte(`package main

func main() {}
`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	// Calculate actual hash
	hash := sha256.Sum256(scriptContent)
	expectedHash := hex.EncodeToString(hash[:])

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          securityTestTimeout,
		VerifyScriptHash: true,
		ExpectedSHA256:   expectedHash,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	if runner == nil {
		t.Error("expected runner but got nil")
	}

	if runner != nil {
		if err := runner.Close(); err != nil {
			t.Logf("failed to close runner: %v", err)
		}
	}
}

func TestSecurityScriptIntegrityVerificationFailure(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, testScriptName)
	scriptContent := []byte(`package main

func main() {}
`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          securityTestTimeout,
		VerifyScriptHash: true,
		ExpectedSHA256:   "0000000000000000000000000000000000000000000000000000000000000000",
	}

	_, err := NewRunner(cfg)
	if err == nil {
		t.Error("expected error but got nil")
	}
}

func TestSecurityScriptIntegrityVerificationDisabled(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, testScriptName)
	scriptContent := []byte(`package main

func main() {}
`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          securityTestTimeout,
		VerifyScriptHash: false,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	if runner != nil {
		if err := runner.Close(); err != nil {
			t.Logf("failed to close runner: %v", err)
		}
	}
}

func TestSecurityRunnerTimeout(t *testing.T) {
	t.Parallel()

	// Create a script with infinite loop
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "infinite.go")
	scriptContent := []byte(`package main

func main() {
	for {
		// Infinite loop
	}
}
`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: 100 * time.Millisecond, // Short timeout
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errCreateRunnerSec, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Logf("failed to close runner: %v", err)
		}
	}()

	msg := message.NewRunnerMessage(&stubSecuritySourceMessage{
		data: []byte(testDataPayloadSecure),
	})

	start := time.Now()
	err = runner.Process(msg)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("expected timeout error but got nil")
	}

	// Verify it actually timed out (with some margin)
	if elapsed > 500*time.Millisecond {
		t.Errorf("timeout took too long: %v", elapsed)
	}
}

func TestSecurityRunnerPanicRecovery(t *testing.T) {
	t.Parallel()

	// Create a script that panics
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "panic.go")
	scriptContent := []byte(`package main

func main() {
	panic("intentional error")
}
`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: securityTestTimeout,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errCreateRunnerSec, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Logf("failed to close runner: %v", err)
		}
	}()

	msg := message.NewRunnerMessage(&stubSecuritySourceMessage{
		data: []byte(testDataPayloadSecure),
	})

	// Should recover from panic
	err = runner.Process(msg)
	if err == nil {
		t.Error("expected error but got nil")
	}
}

func TestSecurityRunnerInvalidConfigType(t *testing.T) {
	t.Parallel()

	_, err := NewRunner("invalid")
	if err == nil {
		t.Error("expected error for invalid config type")
	}
}

func TestSecurityRunnerNonexistentFile(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{
		Path:    "/nonexistent/file.go",
		Timeout: securityTestTimeout,
	}

	_, err := NewRunner(cfg)
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestSecurityRunnerInvalidGo(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "invalid.go")
	scriptContent := []byte(`this is not valid go code {{{`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: securityTestTimeout,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		// Script is loaded but not compiled until Process
		t.Logf("runner creation error (expected at process time): %v", err)
	}

	if runner == nil {
		return
	}

	msg := message.NewRunnerMessage(&stubSecuritySourceMessage{
		data: []byte(testDataPayloadSecure),
	})

	err = runner.Process(msg)
	if err == nil {
		t.Error("expected compilation error for invalid Go")
	}
}

func TestSecurityRunnerClose(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, testScriptName)
	scriptContent := []byte(`package main

func main() {}
`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: securityTestTimeout,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errCreateRunnerSec, err)
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

func TestSecurityNoUnauthorizedPackageAccess(t *testing.T) {
	t.Parallel()

	// Create a script that tries to import a package not provided
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "unauthorized.go")
	scriptContent := []byte(`package main

import "os"

func main() {
	os.Exit(0)
}
`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: securityTestTimeout,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errCreateRunnerSec, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Logf("failed to close runner: %v", err)
		}
	}()

	msg := message.NewRunnerMessage(&stubSecuritySourceMessage{
		data: []byte(testDataPayloadSecure),
	})

	// Should fail because "os" package is not provided
	err = runner.Process(msg)
	if err == nil {
		t.Error("expected error for unauthorized package import")
	}
}

func TestSecurityGoStmtDisabledByDefault(t *testing.T) {
	t.Parallel()

	// Create a script that uses go statement
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "gostmt.go")
	scriptContent := []byte(`package main

func main() {
	go func() {}()
}
`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:        scriptPath,
		Timeout:     securityTestTimeout,
		AllowGoStmt: false, // Explicitly disabled
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errCreateRunnerSec, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Logf("failed to close runner: %v", err)
		}
	}()

	msg := message.NewRunnerMessage(&stubSecuritySourceMessage{
		data: []byte(testDataPayloadSecure),
	})

	// Should fail because go statement is not allowed
	err = runner.Process(msg)
	if err == nil {
		t.Error("expected error for go statement when disabled")
	}
}

func init() {
	// Suppress logs during tests
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	})))
}
