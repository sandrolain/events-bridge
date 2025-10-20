package main

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

const (
	testScriptName      = "test.js"
	errWriteScript      = "failed to write test script: %v"
	errCreateRunner     = "failed to create runner: %v"
	securityTestTimeout = 5 * time.Second
	securityStackSize   = 1000000
	testDataPayload     = "test data"
)

// stubSourceMessage is a simple implementation for testing
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

func (s *stubSecuritySourceMessage) Ack() error { return nil }

func (s *stubSecuritySourceMessage) Nak() error { return nil }

func (s *stubSecuritySourceMessage) Reply(data *message.ReplyData) error { return nil }

func TestRunnerConfigDefaults(t *testing.T) {
	cfg := NewRunnerConfig().(*RunnerConfig)

	if cfg == nil {
		t.Fatal("NewRunnerConfig returned nil")
	}

	// Verify it's an empty struct ready for population
	if cfg.Path != "" {
		t.Errorf("expected empty Path, got %s", cfg.Path)
	}
}

func TestScriptIntegrityVerificationSuccess(t *testing.T) {
	// Create a temporary test script
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, testScriptName)
	scriptContent := []byte(`message.data = "processed";`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	// Calculate actual hash
	hash := sha256.Sum256(scriptContent)
	expectedHash := hex.EncodeToString(hash[:])

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          securityTestTimeout,
		MaxCallStackSize: securityStackSize,
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
		_ = runner.Close()
	}
}

func TestScriptIntegrityVerificationFailure(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, testScriptName)
	scriptContent := []byte(`message.data = "processed";`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          securityTestTimeout,
		MaxCallStackSize: securityStackSize,
		VerifyScriptHash: true,
		ExpectedSHA256:   "0000000000000000000000000000000000000000000000000000000000000000",
	}

	_, err := NewRunner(cfg)
	if err == nil {
		t.Error("expected error but got nil")
	}
}

func TestScriptIntegrityVerificationDisabled(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, testScriptName)
	scriptContent := []byte(`message.data = "processed";`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          securityTestTimeout,
		MaxCallStackSize: securityStackSize,
		VerifyScriptHash: false,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	if runner != nil {
		_ = runner.Close()
	}
}

func TestRunnerWithCallStackLimit(t *testing.T) {
	// Create a script with deep recursion
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "recursive.js")
	scriptContent := []byte(`
		function recursive(n) {
			if (n > 0) {
				return recursive(n - 1);
			}
			return n;
		}
		recursive(1000);
	`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          securityTestTimeout,
		MaxCallStackSize: 10, // Very low limit
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errCreateRunner, err)
	}
	defer runner.Close()

	msg := message.NewRunnerMessage(&stubSecuritySourceMessage{
		data: []byte(testDataPayload),
	})

	// Should hit stack limit
	err = runner.Process(msg)
	if err == nil {
		t.Error("expected stack overflow error but got nil")
	}
}

func TestRunnerTimeout(t *testing.T) {
	// Create a script with infinite loop
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "infinite.js")
	scriptContent := []byte(`
		while(true) {
			// Infinite loop
		}
	`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          100 * time.Millisecond, // Short timeout
		MaxCallStackSize: securityStackSize,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errCreateRunner, err)
	}
	defer runner.Close()

	msg := message.NewRunnerMessage(&stubSecuritySourceMessage{
		data: []byte(testDataPayload),
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

func TestRunnerPanicRecovery(t *testing.T) {
	// Create a script that throws an error
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "panic.js")
	scriptContent := []byte(`
		throw new Error("intentional error");
	`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          securityTestTimeout,
		MaxCallStackSize: securityStackSize,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errCreateRunner, err)
	}
	defer runner.Close()

	msg := message.NewRunnerMessage(&stubSecuritySourceMessage{
		data: []byte(testDataPayload),
	})

	// Should recover from panic
	err = runner.Process(msg)
	if err == nil {
		t.Error("expected error but got nil")
	}
}

func TestRunnerWithAllowedGlobals(t *testing.T) {
	t.Skip("FIXME: goja error handling issue - returns pointer instead of error message")

	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, testScriptName)
	scriptContent := []byte(`(function() { return 42; })();`) // IIFE that returns a value

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          securityTestTimeout,
		MaxCallStackSize: securityStackSize,
		AllowedGlobals:   []string{"console", "JSON"},
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errCreateRunner, err)
	}
	defer runner.Close()

	msg := message.NewRunnerMessage(&stubSecuritySourceMessage{
		data: []byte(testDataPayload),
	})

	err = runner.Process(msg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunnerInvalidConfigType(t *testing.T) {
	_, err := NewRunner("invalid")
	if err == nil {
		t.Error("expected error for invalid config type")
	}
}

func TestRunnerNonexistentFile(t *testing.T) {
	cfg := &RunnerConfig{
		Path:             "/nonexistent/file.js",
		Timeout:          securityTestTimeout,
		MaxCallStackSize: securityStackSize,
	}

	_, err := NewRunner(cfg)
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestRunnerInvalidJavaScript(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "invalid.js")
	scriptContent := []byte(`this is not valid javascript {{{`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          securityTestTimeout,
		MaxCallStackSize: securityStackSize,
	}

	_, err := NewRunner(cfg)
	if err == nil {
		t.Error("expected compilation error for invalid JavaScript")
	}
}

func TestRunnerClose(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, testScriptName)
	scriptContent := []byte(`// test script`)

	if err := os.WriteFile(scriptPath, scriptContent, 0600); err != nil {
		t.Fatalf(errWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:             scriptPath,
		Timeout:          securityTestTimeout,
		MaxCallStackSize: securityStackSize,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errCreateRunner, err)
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

func init() {
	// Suppress logs during tests
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	})))
}
