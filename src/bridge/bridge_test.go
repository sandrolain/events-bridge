package bridge

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/config"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

// Test helpers

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

func newTestConfig() *config.Config {
	return &config.Config{
		Source: connectors.SourceConfig{
			Type:   "cli",
			Buffer: 10,
			Options: map[string]any{
				"command": "echo",
				"args":    []string{"test"},
			},
		},
		Runners: []connectors.RunnerConfig{
			{
				Type:     "pass",
				Routines: 1,
			},
		},
	}
}

// Test connectorPath

func TestConnectorPath(t *testing.T) {
	tests := []struct {
		name     string
		connType string
		want     string
	}{
		{
			name:     "lowercase type",
			connType: "http",
			want:     "./connectors/http.so",
		},
		{
			name:     "uppercase type",
			connType: "HTTP",
			want:     "./connectors/http.so",
		},
		{
			name:     "mixed case type",
			connType: "MqTt",
			want:     "./connectors/mqtt.so",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := connectorPath(tt.connType)
			if got != tt.want {
				t.Errorf("connectorPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test EventsBridge message handling methods

func TestEventsBridgeHandleSuccess(t *testing.T) {
	cfg := newTestConfig()
	logger := newTestLogger()
	bridge := &EventsBridge{
		cfg:    cfg,
		logger: logger,
	}

	// Create test message
	adapter := testutil.NewAdapter([]byte("test"), nil)
	msg := message.NewRunnerMessage(adapter)

	// Handle success
	bridge.HandleSuccess(msg, "test operation", "key", "value")

	// Verify message was acknowledged
	if adapter.AckCalls != 1 {
		t.Errorf("HandleSuccess() AckCalls = %d, want 1", adapter.AckCalls)
	}
	if adapter.NakCalls != 0 {
		t.Errorf("HandleSuccess() NakCalls = %d, want 0", adapter.NakCalls)
	}
}

func TestEventsBridgeHandleError(t *testing.T) {
	cfg := newTestConfig()
	logger := newTestLogger()
	bridge := &EventsBridge{
		cfg:    cfg,
		logger: logger,
	}

	// Create test message
	adapter := testutil.NewAdapter([]byte("test"), nil)
	msg := message.NewRunnerMessage(adapter)

	// Handle error
	testErr := errors.New("test error")
	bridge.HandleError(msg, testErr, "test operation", "key", "value")

	// Verify message was naked
	if adapter.NakCalls != 1 {
		t.Errorf("HandleError() NakCalls = %d, want 1", adapter.NakCalls)
	}
	if adapter.AckCalls != 0 {
		t.Errorf("HandleError() AckCalls = %d, want 0", adapter.AckCalls)
	}
}

func TestEventsBridgeHandleRunnerError(t *testing.T) {
	cfg := newTestConfig()
	logger := newTestLogger()
	bridge := &EventsBridge{
		cfg:    cfg,
		logger: logger,
	}

	// Create test message
	adapter := testutil.NewAdapter([]byte("test"), nil)
	msg := message.NewRunnerMessage(adapter)

	// Handle runner error
	testErr := errors.New("runner error")
	retMsg, ok, err := bridge.HandleRunnerError(msg, testErr, "runner operation")

	// Verify return values are correct for rill pipeline
	if retMsg != nil {
		t.Error("HandleRunnerError() should return nil message")
	}
	if ok {
		t.Error("HandleRunnerError() should return false for ok")
	}
	if err != nil {
		t.Error("HandleRunnerError() should return nil error")
	}

	// Verify message was naked
	if adapter.NakCalls != 1 {
		t.Errorf("HandleRunnerError() NakCalls = %d, want 1", adapter.NakCalls)
	}
}

// Test EventsBridge message handling error paths

func TestEventsBridgeHandleSuccess_AckError(t *testing.T) {
	cfg := newTestConfig()
	logger := newTestLogger()
	bridge := &EventsBridge{
		cfg:    cfg,
		logger: logger,
	}

	// Create adapter that returns error on Ack
	adapter := testutil.NewAdapter([]byte("test"), nil)
	adapter.AckErr = errors.New("ack failed")
	msg := message.NewRunnerMessage(adapter)

	// Should not panic even if Ack fails
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("HandleSuccess() panicked with Ack error: %v", r)
		}
	}()

	bridge.HandleSuccess(msg, "test operation", "key", "value")

	// Should still attempt to Ack
	if adapter.AckCalls != 1 {
		t.Errorf("HandleSuccess() AckCalls = %d, want 1", adapter.AckCalls)
	}
}

func TestEventsBridgeHandleSuccess_NilMessage(t *testing.T) {
	cfg := newTestConfig()
	logger := newTestLogger()
	bridge := &EventsBridge{
		cfg:    cfg,
		logger: logger,
	}

	// Should not panic with nil message
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("HandleSuccess() panicked with nil message: %v", r)
		}
	}()

	bridge.HandleSuccess(nil, "test operation", "key", "value")
}

func TestEventsBridgeHandleError_NakError(t *testing.T) {
	cfg := newTestConfig()
	logger := newTestLogger()
	bridge := &EventsBridge{
		cfg:    cfg,
		logger: logger,
	}

	// Create adapter that returns error on Nak
	adapter := testutil.NewAdapter([]byte("test"), nil)
	adapter.NakErr = errors.New("nak failed")
	msg := message.NewRunnerMessage(adapter)

	// Should not panic even if Nak fails
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("HandleError() panicked with Nak error: %v", r)
		}
	}()

	testErr := errors.New("test error")
	bridge.HandleError(msg, testErr, "test operation", "key", "value")

	// Should still attempt to Nak
	if adapter.NakCalls != 1 {
		t.Errorf("HandleError() NakCalls = %d, want 1", adapter.NakCalls)
	}
}

func TestEventsBridgeHandleError_NilMessage(t *testing.T) {
	cfg := newTestConfig()
	logger := newTestLogger()
	bridge := &EventsBridge{
		cfg:    cfg,
		logger: logger,
	}

	// Should not panic with nil message
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("HandleError() panicked with nil message: %v", r)
		}
	}()

	testErr := errors.New("test error")
	bridge.HandleError(nil, testErr, "test operation", "key", "value")
}

func TestEventsBridgeHandleError_NilError(t *testing.T) {
	cfg := newTestConfig()
	logger := newTestLogger()
	bridge := &EventsBridge{
		cfg:    cfg,
		logger: logger,
	}

	adapter := testutil.NewAdapter([]byte("test"), nil)
	msg := message.NewRunnerMessage(adapter)

	// Should handle nil error gracefully
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("HandleError() panicked with nil error: %v", r)
		}
	}()

	bridge.HandleError(msg, nil, "test operation", "key", "value")

	// Should still Nak the message
	if adapter.NakCalls != 1 {
		t.Errorf("HandleError() NakCalls = %d, want 1", adapter.NakCalls)
	}
}

func TestEventsBridgeHandleRunnerError_NilMessage(t *testing.T) {
	cfg := newTestConfig()
	logger := newTestLogger()
	bridge := &EventsBridge{
		cfg:    cfg,
		logger: logger,
	}

	// Should not panic with nil message
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("HandleRunnerError() panicked with nil message: %v", r)
		}
	}()

	testErr := errors.New("runner error")
	retMsg, ok, err := bridge.HandleRunnerError(nil, testErr, "runner operation")

	// Should return expected values
	if retMsg != nil {
		t.Error("HandleRunnerError() should return nil message")
	}
	if ok {
		t.Error("HandleRunnerError() should return false")
	}
	if err != nil {
		t.Error("HandleRunnerError() should return nil error")
	}
}

func TestEventsBridgeHandleRunnerError_NakError(t *testing.T) {
	cfg := newTestConfig()
	logger := newTestLogger()
	bridge := &EventsBridge{
		cfg:    cfg,
		logger: logger,
	}

	// Create adapter that returns error on Nak
	adapter := testutil.NewAdapter([]byte("test"), nil)
	adapter.NakErr = errors.New("nak failed")
	msg := message.NewRunnerMessage(adapter)

	// Should not panic even if Nak fails
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("HandleRunnerError() panicked with Nak error: %v", r)
		}
	}()

	testErr := errors.New("runner error")
	retMsg, ok, err := bridge.HandleRunnerError(msg, testErr, "runner operation")

	// Should still return expected values
	if retMsg != nil {
		t.Error("HandleRunnerError() should return nil message")
	}
	if ok {
		t.Error("HandleRunnerError() should return false")
	}
	if err != nil {
		t.Error("HandleRunnerError() should return nil error")
	}

	// Should still attempt to Nak
	if adapter.NakCalls != 1 {
		t.Errorf("HandleRunnerError() NakCalls = %d, want 1", adapter.NakCalls)
	}
}

// Test NewEventsBridge

func TestNewEventsBridge_NilConfig(t *testing.T) {
	logger := newTestLogger()

	bridge, err := NewEventsBridge(nil, logger)

	if bridge != nil {
		t.Error("NewEventsBridge() should return nil bridge for nil config")
	}
	if err == nil {
		t.Fatal("NewEventsBridge() should return error for nil config")
	}
	if err.Error() != "config cannot be nil" {
		t.Errorf("NewEventsBridge() error = %v, want 'config cannot be nil'", err)
	}
}

func TestNewEventsBridge_NilLogger(t *testing.T) {
	cfg := newTestConfig()

	bridge, err := NewEventsBridge(cfg, nil)

	if bridge != nil {
		t.Error("NewEventsBridge() should return nil bridge for nil logger")
	}
	if err == nil {
		t.Fatal("NewEventsBridge() should return error for nil logger")
	}
	if err.Error() != "logger cannot be nil" {
		t.Errorf("NewEventsBridge() error = %v, want 'logger cannot be nil'", err)
	}
}

func TestNewEventsBridge_InvalidSourceType(t *testing.T) {
	cfg := newTestConfig()
	cfg.Source.Type = "nonexistent"
	logger := newTestLogger()

	bridge, err := NewEventsBridge(cfg, logger)

	if bridge != nil {
		t.Error("NewEventsBridge() should return nil bridge for invalid source")
	}
	if err == nil {
		t.Fatal("NewEventsBridge() should return error for invalid source")
	}
}

func TestNewEventsBridge_InvalidRunnerType(t *testing.T) {
	cfg := newTestConfig()
	cfg.Runners = []connectors.RunnerConfig{
		{
			Type:     "nonexistent",
			Routines: 1,
		},
	}
	logger := newTestLogger()

	bridge, err := NewEventsBridge(cfg, logger)

	if bridge != nil {
		t.Error("NewEventsBridge() should return nil bridge for invalid runner")
	}
	if err == nil {
		t.Fatal("NewEventsBridge() should return error for invalid runner")
	}
}

// Target-related tests are obsolete since Target has been removed in favor of Runners
/*
func TestNewEventsBridge_InvalidTargetType(t *testing.T) {
	cfg := newTestConfig()
	cfg.Target.Type = "nonexistent"
	logger := newTestLogger()

	bridge, err := NewEventsBridge(cfg, logger)

	if bridge != nil {
		t.Error("NewEventsBridge() should return nil bridge for invalid target")
	}
	if err == nil {
		t.Fatal("NewEventsBridge() should return error for invalid target")
	}
}
*/

func TestNewEventsBridge_NoRunners(t *testing.T) {
	t.Skip("Skipping test that requires CLI plugin - tested in integration tests")
	cfg := newTestConfig()
	cfg.Runners = []connectors.RunnerConfig{}
	logger := newTestLogger()

	bridge, err := NewEventsBridge(cfg, logger)

	if err != nil {
		t.Fatalf("NewEventsBridge() unexpected error = %v", err)
	}
	if bridge == nil {
		t.Fatal("NewEventsBridge() returned nil bridge")
	}
	if len(bridge.runners) != 0 {
		t.Errorf("NewEventsBridge() runners length = %d, want 0", len(bridge.runners))
	}
}

/*
func TestNewEventsBridge_NoTarget(t *testing.T) {
	t.Skip("Skipping test that requires CLI plugin - tested in integration tests")
	cfg := newTestConfig()
	cfg.Target.Type = "none"
	logger := newTestLogger()

	bridge, err := NewEventsBridge(cfg, logger)

	if err != nil {
		t.Fatalf("NewEventsBridge() unexpected error = %v", err)
	}
	if bridge == nil {
		t.Fatal("NewEventsBridge() returned nil bridge")
	}
	if bridge.target != nil {
		t.Error("NewEventsBridge() should have nil target for type 'none'")
	}
}

func TestNewEventsBridge_EmptyTargetType(t *testing.T) {
	t.Skip("Skipping test that requires CLI plugin - tested in integration tests")
	cfg := newTestConfig()
	cfg.Target.Type = ""
	logger := newTestLogger()

	bridge, err := NewEventsBridge(cfg, logger)

	if err != nil {
		t.Fatalf("NewEventsBridge() unexpected error = %v", err)
	}
	if bridge == nil {
		t.Fatal("NewEventsBridge() returned nil bridge")
	}
	if bridge.target != nil {
		t.Error("NewEventsBridge() should have nil target for empty type")
	}
}
*/

// Test Close

func TestEventsBridge_Close(t *testing.T) {
	t.Skip("Skipping test that requires CLI plugin - tested in integration tests")
	cfg := newTestConfig()
	logger := newTestLogger()

	bridge, err := NewEventsBridge(cfg, logger)
	if err != nil {
		t.Fatalf("NewEventsBridge() unexpected error = %v", err)
	}

	err = bridge.Close()
	if err != nil {
		t.Errorf("Close() unexpected error = %v", err)
	}
}

func TestEventsBridge_Close_NilSource(t *testing.T) {
	t.Skip("Skipping test that requires CLI plugin - tested in integration tests")
	cfg := newTestConfig()
	logger := newTestLogger()

	bridge, err := NewEventsBridge(cfg, logger)
	if err != nil {
		t.Fatalf("NewEventsBridge() unexpected error = %v", err)
	}

	bridge.source = nil
	err = bridge.Close()
	if err != nil {
		t.Errorf("Close() should handle nil source, got error = %v", err)
	}
}

/*
func TestEventsBridge_Close_NilTarget(t *testing.T) {
	t.Skip("Skipping test that requires CLI plugin - tested in integration tests")
	cfg := newTestConfig()
	logger := newTestLogger()

	bridge, err := NewEventsBridge(cfg, logger)
	if err != nil {
		t.Fatalf("NewEventsBridge() unexpected error = %v", err)
	}

	bridge.target = nil
	err = bridge.Close()
	if err != nil {
		t.Errorf("Close() should handle nil target, got error = %v", err)
	}
}
*/

func TestEventsBridge_Close_NilRunners(t *testing.T) {
	t.Skip("Skipping test that requires CLI plugin - tested in integration tests")
	cfg := newTestConfig()
	cfg.Runners = []connectors.RunnerConfig{}
	logger := newTestLogger()

	bridge, err := NewEventsBridge(cfg, logger)
	if err != nil {
		t.Fatalf("NewEventsBridge() unexpected error = %v", err)
	}

	err = bridge.Close()
	if err != nil {
		t.Errorf("Close() should handle empty runners, got error = %v", err)
	}
}

// Test closeWithRetry

func TestCloseWithRetry_Success(t *testing.T) {
	callCount := 0
	closeFunc := func() error {
		callCount++
		return nil
	}

	err := closeWithRetry(closeFunc, 3, time.Millisecond)

	if err != nil {
		t.Errorf("closeWithRetry() unexpected error = %v", err)
	}
	if callCount != 1 {
		t.Errorf("closeWithRetry() called %d times, want 1", callCount)
	}
}

func TestCloseWithRetry_SuccessAfterRetries(t *testing.T) {
	callCount := 0
	closeFunc := func() error {
		callCount++
		if callCount < 2 {
			return errors.New("temporary error")
		}
		return nil
	}

	err := closeWithRetry(closeFunc, 3, time.Millisecond)

	if err != nil {
		t.Errorf("closeWithRetry() unexpected error = %v", err)
	}
	if callCount != 2 {
		t.Errorf("closeWithRetry() called %d times, want 2", callCount)
	}
}

func TestCloseWithRetry_Failure(t *testing.T) {
	callCount := 0
	testErr := errors.New("persistent error")
	closeFunc := func() error {
		callCount++
		return testErr
	}

	err := closeWithRetry(closeFunc, 3, time.Millisecond)

	if err == nil {
		t.Fatal("closeWithRetry() should return error after all retries")
	}
	if err != testErr {
		t.Errorf("closeWithRetry() error = %v, want %v", err, testErr)
	}
	if callCount != 3 {
		t.Errorf("closeWithRetry() called %d times, want 3", callCount)
	}
}

// Test min

func TestMin(t *testing.T) {
	tests := []struct {
		name string
		a    int
		b    int
		want int
	}{
		{
			name: "a less than 1",
			a:    0,
			b:    5,
			want: 5,
		},
		{
			name: "a negative",
			a:    -1,
			b:    5,
			want: 5,
		},
		{
			name: "a greater than or equal to 1",
			a:    3,
			b:    5,
			want: 3,
		},
		{
			name: "a equals 1",
			a:    1,
			b:    5,
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := min(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("min(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// Test Run - basic flow test
func TestEventsBridge_Run_Cancelled(t *testing.T) {
	t.Skip("Skipping test that requires CLI plugin - tested in integration tests")
	cfg := newTestConfig()
	logger := newTestLogger()

	bridge, err := NewEventsBridge(cfg, logger)
	if err != nil {
		t.Fatalf("NewEventsBridge() unexpected error = %v", err)
	}
	defer func() {
		if err := bridge.Close(); err != nil {
			t.Logf("failed to close bridge: %v", err)
		}
	}()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Run should handle cancelled context gracefully
	err = bridge.Run(ctx)
	// We expect some error since context is cancelled immediately
	// The exact error depends on timing and implementation
	if err != nil {
		t.Logf("Run() with cancelled context returned error (expected): %v", err)
	}
}

// Test RunnerItem struct
func TestRunnerItem(t *testing.T) {
	item := RunnerItem{
		Config: connectors.RunnerConfig{
			Type:     "test",
			Routines: 1,
		},
		Runner: nil,
	}

	if item.Config.Type != "test" {
		t.Errorf("RunnerItem.Config.Type = %s, want test", item.Config.Type)
	}
	if item.Runner != nil {
		t.Error("RunnerItem.Runner should be nil")
	}
}
