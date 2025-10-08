// Package main implements the ES5/JavaScript connector for the Events Bridge.
//
// The ES5 connector enables execution of JavaScript (ECMAScript 5.1) code
// as message processors using the goja runtime.
//
// Key features:
//   - ECMAScript 5.1 compatible JavaScript execution
//   - Isolated VM instances per message (no shared state)
//   - Configurable execution timeout
//   - Call stack size limits
//   - Optional script integrity verification via SHA256
//
// Security architecture:
//   - Each message runs in a fresh goja VM instance
//   - Stack overflow protection via MaxCallStackSize
//   - Timeout enforcement via interrupt mechanism
//   - Panic recovery prevents process crashes
//   - Script integrity verification (optional)
//
// Security limitations:
//   - Goja is NOT a complete sandbox (has access to Go runtime)
//   - No filesystem or network isolation
//   - Iteration limits not yet enforced
//   - Global object filtering not yet implemented
//   - Use with TRUSTED scripts only in production
//
// Example configuration:
//
//	runners:
//	  - name: js-processor
//	    type: es5
//	    config:
//	      path: ./processor.js
//	      timeout: 5s
//	      maxCallStackSize: 1000000
//	      verifyScriptHash: true
//	      expectedSHA256: "abc123..."
//
// For script integrity verification, generate hash with:
//
//	sha256sum processor.js
//
// WARNING: This connector executes arbitrary JavaScript code. Only use with
// scripts you trust. Consider using the WASM connector for untrusted code.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/dop251/goja"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure ES5Runner implements models.Runner
var _ connectors.Runner = &ES5Runner{}

// RunnerConfig defines the configuration for the ES5/JavaScript runner with security enhancements.
//
// Security features:
//   - Script integrity verification via SHA256 hash
//   - Call stack size limits to prevent stack overflow
//   - Iteration limits to prevent infinite loops (future)
//   - Global API whitelisting (future)
type RunnerConfig struct {
	// Path is the filesystem path to the JavaScript file
	Path string `mapstructure:"path" validate:"required"`

	// Timeout is the maximum execution time for scripts
	Timeout time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`

	// MaxCallStackSize limits recursion depth (default: 1000000)
	MaxCallStackSize int `mapstructure:"maxCallStackSize" default:"1000000" validate:"gt=0"`

	// MaxIterations limits loop iterations (default: 100000)
	// Note: Currently not enforced, requires custom implementation
	MaxIterations int `mapstructure:"maxIterations" default:"100000" validate:"gt=0"`

	// VerifyScriptHash enables script integrity verification
	VerifyScriptHash bool `mapstructure:"verifyScriptHash" default:"false"`

	// ExpectedSHA256 is the expected hash of the script (required if VerifyScriptHash is true)
	ExpectedSHA256 string `mapstructure:"expectedSHA256" validate:"required_if=VerifyScriptHash true"`

	// AllowedGlobals is a whitelist of allowed global objects/functions
	// Note: Currently not enforced, requires custom implementation
	AllowedGlobals []string `mapstructure:"allowedGlobals"`
}

// ES5Runner executes JavaScript code to process messages.
//
// Security considerations:
//   - Scripts run in isolated goja VM instances
//   - Call stack size is limited to prevent stack overflow
//   - Execution timeout prevents runaway scripts
//   - Optional script integrity verification via SHA256
//
// Known limitations:
//   - No complete sandbox (goja has access to Go runtime)
//   - Iteration limits not yet enforced
//   - Global object filtering not yet implemented
type ES5Runner struct {
	cfg     *RunnerConfig
	slog    *slog.Logger
	program *goja.Program
}

// NewRunnerConfig creates a new RunnerConfig instance with security defaults.
//
// Default values:
//   - Timeout: 5s
//   - MaxCallStackSize: 1000000
//   - MaxIterations: 100000
//   - VerifyScriptHash: false
//   - AllowedGlobals: empty (all globals allowed)
//
// Returns:
//   - any: A pointer to RunnerConfig that can be populated via mapstructure
func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// NewRunner creates a new ES5Runner instance from the provided configuration.
// It loads and compiles the JavaScript program, optionally verifying its integrity.
//
// Security features:
//   - Script integrity verification via SHA256 (if enabled)
//   - Program compilation before execution
//   - Validation of all configuration parameters
//
// Parameters:
//   - anyCfg: Configuration object (must be *RunnerConfig)
//
// Returns:
//   - connectors.Runner: The configured ES5Runner
//   - error: Configuration errors, file read errors, or compilation errors
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	log := slog.Default().With("context", "ES5 Runner")
	log.Info("loading es5 program", "path", cfg.Path)

	src, err := os.ReadFile(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read js file: %w", err)
	}

	// Verify script integrity if enabled
	if cfg.VerifyScriptHash {
		if err := VerifyScriptIntegrity(src, cfg.ExpectedSHA256); err != nil {
			log.Error("script integrity verification failed", "error", err)
			return nil, fmt.Errorf("script integrity verification failed: %w", err)
		}
		log.Info("script integrity verified", "hash", cfg.ExpectedSHA256)
	}

	name := filepath.Base(cfg.Path)

	prog, err := goja.Compile(name, string(src), false)
	if err != nil {
		return nil, fmt.Errorf("failed to compile js: %w", err)
	}

	return &ES5Runner{
		cfg:     cfg,
		slog:    log,
		program: prog,
	}, nil
}

// Process executes the JavaScript program to process a message.
//
// Security measures:
//   - Each message runs in a new isolated VM instance
//   - Call stack size is limited via MaxCallStackSize
//   - Hard timeout enforced via interrupt mechanism
//   - Panic recovery prevents crashes
//
// Execution flow:
//  1. Create new VM with security restrictions
//  2. Set up interrupt channel for timeout
//  3. Inject message into VM context
//  4. Execute program in goroutine with panic recovery
//  5. Wait for completion or timeout
//
// Parameters:
//   - msg: The message to process
//
// Returns:
//   - error: Timeout, execution errors, or panic errors
func (e *ES5Runner) Process(msg *message.RunnerMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.Timeout)
	defer cancel()

	// Create new VM instance for isolation
	vm := goja.New()

	// Apply security restrictions
	sandboxCfg := SandboxConfig{
		MaxIterations:    e.cfg.MaxIterations,
		MaxCallStackSize: e.cfg.MaxCallStackSize,
		AllowedGlobals:   e.cfg.AllowedGlobals,
		Logger:           e.slog,
	}

	if err := SetupSandbox(vm, sandboxCfg); err != nil {
		return fmt.Errorf("failed to setup sandbox: %w", err)
	}

	// Setup interrupt channel for hard timeout
	interrupt := CreateInterruptChannel()
	vm.Interrupt(interrupt)

	// Close interrupt channel when context is done
	go func() {
		<-ctx.Done()
		close(interrupt)
	}()

	// Inject message into VM
	result := msg
	if err := vm.Set("message", result); err != nil {
		return fmt.Errorf("failed to set message: %w", err)
	}

	// Execute in goroutine with panic recovery
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				e.slog.Error("js panic recovered", "panic", r)
				done <- fmt.Errorf("js panic: %v", r)
			}
		}()
		_, err := vm.RunProgram(e.program)
		if err != nil {
			e.slog.Error("js execution failed", "error", err)
		}
		done <- err
	}()

	// Wait for completion or timeout
	select {
	case <-ctx.Done():
		e.slog.Warn("js execution timeout", "timeout", e.cfg.Timeout)
		return fmt.Errorf("js execution timeout after %v", e.cfg.Timeout)
	case err := <-done:
		if err != nil {
			return fmt.Errorf("js execution error: %w", err)
		}
	}

	return nil
}

// Close performs cleanup when the runner is no longer needed.
//
// Currently, ES5Runner has no resources to release, but this method
// is required by the connectors.Runner interface.
//
// Returns:
//   - error: Always nil
func (e *ES5Runner) Close() error {
	e.slog.Info("closing es5 runner")
	return nil
}
