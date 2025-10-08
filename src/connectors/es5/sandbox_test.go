package main

import (
	"log/slog"
	"os"
	"testing"

	"github.com/dop251/goja"
)

func TestVerifyScriptIntegrity(t *testing.T) {
	script := []byte(`console.log("test");`)

	tests := []struct {
		name        string
		script      []byte
		hash        string
		expectError bool
	}{
		{
			name:        "valid_hash",
			script:      script,
			hash:        "b80112c06818d86d16f0185c023439b4364af3c61971b8eaeb90d6f094dc8a6b",
			expectError: false,
		},
		{
			name:        "invalid_hash",
			script:      script,
			hash:        "0000000000000000000000000000000000000000000000000000000000000000",
			expectError: true,
		},
		{
			name:        "empty_hash",
			script:      script,
			hash:        "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := VerifyScriptIntegrity(tt.script, tt.hash)
			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestSetupSandbox(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	tests := []struct {
		name   string
		config SandboxConfig
	}{
		{
			name: "basic_sandbox",
			config: SandboxConfig{
				MaxIterations:    100000,
				MaxCallStackSize: 1000000,
				Logger:           logger,
			},
		},
		{
			name: "with_globals",
			config: SandboxConfig{
				MaxIterations:    50000,
				MaxCallStackSize: 500000,
				AllowedGlobals:   []string{"console", "JSON"},
				Logger:           logger,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vm := goja.New()
			err := SetupSandbox(vm, tt.config)
			if err != nil {
				t.Errorf("SetupSandbox failed: %v", err)
			}
		})
	}
}

func TestCreateInterruptChannel(t *testing.T) {
	ch := CreateInterruptChannel()
	if ch == nil {
		t.Error("CreateInterruptChannel returned nil")
	}

	// Verify it's a valid channel
	select {
	case <-ch:
		t.Error("channel should not be closed initially")
	default:
		// Expected: channel is open and not ready to receive
	}

	// Close and verify
	close(ch)
	select {
	case <-ch:
		// Expected: channel is now closed
	default:
		t.Error("channel should be closed after close()")
	}
}

func TestSandboxCallStackLimit(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	vm := goja.New()
	cfg := SandboxConfig{
		MaxCallStackSize: 10, // Very low limit for testing
		Logger:           logger,
	}

	if err := SetupSandbox(vm, cfg); err != nil {
		t.Fatalf("SetupSandbox failed: %v", err)
	}

	// This recursive function should hit the stack limit
	script := `
		function recursive(n) {
			if (n > 0) {
				return recursive(n - 1);
			}
			return n;
		}
		recursive(100);
	`

	_, err := vm.RunString(script)
	if err == nil {
		t.Error("expected stack overflow error but got nil")
	}
}

func TestSandboxInterrupt(t *testing.T) {
	vm := goja.New()
	interrupt := CreateInterruptChannel()
	vm.Interrupt(interrupt)

	// Close interrupt immediately
	close(interrupt)

	// This should be interrupted
	script := `
		while(true) {
			// Infinite loop
		}
	`

	_, err := vm.RunString(script)
	if err == nil {
		t.Error("expected interrupt error but got nil")
	}
}
