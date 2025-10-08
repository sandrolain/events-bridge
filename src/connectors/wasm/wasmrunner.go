// Package main implements the WASM connector for the Events Bridge.
//
// The WASM connector enables execution of WebAssembly modules as message processors,
// providing a secure, sandboxed environment for custom transformation logic.
//
// Key features:
//   - Sandboxed execution using wazero (no CGo dependencies)
//   - Configurable memory limits (MaxMemoryPages)
//   - Secure filesystem access via SafeFS with path whitelisting
//   - Environment variable filtering to prevent sensitive data leakage
//   - Read-only filesystem mounting by default
//   - Configurable execution timeouts
//
// Security architecture:
//   - WASM modules run in isolated instances (no shared state)
//   - Path traversal attacks prevented by SafeFS
//   - Resource exhaustion mitigated via memory limits
//   - Environment variables filtered using deny list
//   - All filesystem access goes through security checks
//
// Example configuration:
//
//	runners:
//	  - name: wasm-processor
//	    type: wasm
//	    config:
//	      path: ./processor.wasm
//	      maxMemoryPages: 512    # 32MB
//	      readOnlyMount: true
//	      allowedPaths:
//	        - data
//	        - public
//	      denyEnvVars:
//	        - AWS_SECRET_ACCESS_KEY
//	        - DATABASE_PASSWORD
//
// For more details on security implementation, see SECURITY.md and IMPLEMENTATION.md.
package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/common/encdec"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// Ensure WasmRunner implements connectors.Runner
var _ connectors.Runner = &WasmRunner{}

// RunnerConfig defines the configuration for the WASM runner with security enhancements.
// It provides options for sandboxing, resource limits, and access control.
type RunnerConfig struct {
	// Path is the filesystem path to the WASM module file
	Path string `mapstructure:"path" validate:"required"`

	// MountPath is the host directory to mount as filesystem for WASM module.
	// If empty, no filesystem is mounted.
	MountPath string `mapstructure:"mountPath"`

	// Timeout is the maximum execution time for processing a single message
	Timeout time.Duration `mapstructure:"timeout" default:"5s" validate:"required"`

	// Env contains environment variables to pass to the WASM module.
	// Variables in DenyEnvVars will be filtered out.
	Env map[string]string `mapstructure:"env"`

	// Args contains command-line arguments to pass to the WASM module
	Args []string `mapstructure:"args"`

	// Format specifies the message encoding format (json, cbor, or cli)
	Format string `mapstructure:"format" default:"cli" validate:"required,oneof=json cbor cli"`

	// MetadataKey is the key name for metadata in json/cbor formats
	MetadataKey string `mapstructure:"metadataKey" default:"metadata" validate:"required"`

	// DataKey is the key name for data payload in json/cbor formats
	DataKey string `mapstructure:"dataKey" default:"data" validate:"required"`

	// Security enhancements

	// MaxMemoryPages limits the WASM module's memory usage.
	// Each page is 64KB. Default: 256 pages (16MB). Max: 65536 pages (4GB)
	MaxMemoryPages uint32 `mapstructure:"maxMemoryPages" default:"256" validate:"max=65536"`

	// ReadOnlyMount when true, mounts the filesystem in read-only mode.
	// Default: true (recommended for security)
	ReadOnlyMount bool `mapstructure:"readOnlyMount" default:"true"`

	// AllowedPaths is a whitelist of paths accessible within MountPath.
	// If empty, all paths under MountPath are accessible (subject to ReadOnlyMount).
	// Paths are relative to MountPath.
	AllowedPaths []string `mapstructure:"allowedPaths"`

	// DenyEnvVars is a blacklist of environment variable names to exclude.
	// Use this to prevent sensitive data (passwords, keys) from being exposed to WASM.
	DenyEnvVars []string `mapstructure:"denyEnvVars"`
}

// WasmRunner executes WebAssembly modules to process messages.
//
// It provides:
//   - Sandboxed execution environment
//   - Configurable resource limits (memory, timeout)
//   - Secure filesystem access via SafeFS
//   - Environment variable filtering
//   - Thread-safe message processing
//
// The runner compiles WASM modules once and creates new instances for each
// message to ensure isolation between executions.
type WasmRunner struct {
	cfg       *RunnerConfig
	timeout   time.Duration         // Timeout for processing messages
	slog      *slog.Logger          // Logger instance
	rt        wazero.Runtime        // WASM runtime
	ctx       context.Context       // Background context
	mu        sync.Mutex            // Mutex for thread-safe operations
	wasmBytes []byte                // Compiled WASM bytecode
	module    wazero.CompiledModule // Compiled module for instantiation
	decoder   encdec.MessageDecoder // Message encoder/decoder
	stopCh    chan struct{}         // Stop channel for graceful shutdown
}

// NewRunnerConfig creates a new RunnerConfig instance with default values.
//
// Default values:
//   - MaxMemoryPages: 256 (16MB)
//   - ReadOnlyMount: true
//   - AllowedPaths: empty (all paths allowed if whitelist disabled)
//   - DenyEnvVars: empty (no filtering)
//
// Returns:
//   - any: A pointer to RunnerConfig that can be populated via mapstructure
func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// runtimeCreateMu guards creation of new wazero runtimes to prevent
// excessive parallel allocations during tests or high-concurrency scenarios.
//
// Note: This mutex protects runtime creation only, not module instantiation.
// Each module instance is created from a compiled module without additional locking.
var runtimeCreateMu sync.Mutex

// NewRunner creates a new WasmRunner instance from the provided configuration.
// It loads and compiles the WASM module, applies security restrictions, and prepares
// the runtime for message processing.
//
// Security features:
//   - Memory limits based on MaxMemoryPages configuration
//   - Filesystem sandboxing with SafeFS
//   - Environment variable filtering
//
// Returns an error if:
//   - Configuration is invalid
//   - WASM file cannot be read
//   - WASM module compilation fails
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	log := slog.Default().With("context", "WASM Runner")
	log.Info("loading wasm module", "path", cfg.Path)

	decoder, err := encdec.NewMessageDecoder(cfg.Format, cfg.MetadataKey, cfg.DataKey)
	if err != nil {
		return nil, fmt.Errorf("invalid format: %w", err)
	}

	wasmBytes, err := os.ReadFile(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read wasm file: %w", err)
	}

	ctx := context.Background()

	// Create runtime configuration with memory limits
	runtimeConfig := wazero.NewRuntimeConfig()
	if cfg.MaxMemoryPages > 0 {
		runtimeConfig = runtimeConfig.WithMemoryLimitPages(cfg.MaxMemoryPages)
	}

	runtimeCreateMu.Lock()
	rt := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)
	runtimeCreateMu.Unlock()

	// Instantiate WASI before loading the module
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		return nil, fmt.Errorf("failed to instantiate WASI: %w", err)
	}

	cmod, err := rt.CompileModule(ctx, wasmBytes)
	if err != nil {
		_ = rt.Close(ctx)
		return nil, fmt.Errorf("failed to compile wasm module: %w", err)
	}

	return &WasmRunner{
		cfg:       cfg,
		slog:      log,
		ctx:       ctx,
		timeout:   cfg.Timeout,
		rt:        rt,
		wasmBytes: wasmBytes,
		module:    cmod,
		decoder:   decoder,
		stopCh:    make(chan struct{}),
	}, nil
}

// Process executes the WASM module to process a single message.
// The message data is encoded and passed to the module via stdin,
// and the processed result is read from stdout.
//
// Security measures applied:
//   - Timeout enforcement to prevent infinite loops
//   - Environment variable filtering via filterEnvVars
//   - Filesystem access control via SafeFS
//   - Memory limits enforced by runtime configuration
//
// The execution flow:
//  1. Encode input message using configured format
//  2. Create module configuration with stdin/stdout
//  3. Apply security restrictions (env vars, filesystem)
//  4. Instantiate and execute WASM module with timeout
//  5. Decode output and update message
//
// Returns an error if:
//   - Message encoding/decoding fails
//   - WASM module execution fails
//   - Timeout is exceeded
func (w *WasmRunner) Process(msg *message.RunnerMessage) error {
	inData, err := w.decoder.EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("error encoding input data: %w", err)
	}

	w.slog.Debug("processing message", "timeout", w.timeout)

	stdin := bytes.NewReader(inData)
	stout := bytes.NewBuffer(nil)

	config := wazero.NewModuleConfig().
		WithStdin(stdin).
		WithStdout(stout).
		WithStderr(os.Stderr)

	if len(w.cfg.Args) > 0 {
		config = config.WithArgs(w.cfg.Args...)
	}

	// Filter environment variables based on DenyEnvVars
	if len(w.cfg.Env) > 0 {
		safeEnv := w.filterEnvVars(w.cfg.Env)
		for k, v := range safeEnv {
			config = config.WithEnv(k, v)
		}
	}

	// Setup secure filesystem if MountPath is specified
	if w.cfg.MountPath != "" {
		baseFS := os.DirFS(w.cfg.MountPath)
		safeFS := NewSafeFS(baseFS, w.cfg.AllowedPaths, w.cfg.ReadOnlyMount)
		config = config.WithFS(safeFS)
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	module, err := w.rt.InstantiateModule(ctx, w.module, config)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			w.slog.Warn("wasm execution timeout")
		}
		return fmt.Errorf("failed to instantiate wasm module: %w", err)
	}
	defer func() {
		if err := module.Close(ctx); err != nil {
			w.slog.Error("failed to close wasm module", "err", err)
		}
	}()

	res, err := w.decoder.DecodeMessage(stout.Bytes())
	if err != nil {
		return fmt.Errorf("error decoding output data: %w", err)
	}

	err = msg.SetFromSourceMessage(res)
	if err != nil {
		return fmt.Errorf("error updating message: %w", err)
	}

	return nil
}

// filterEnvVars filters environment variables based on the deny list.
// Variables listed in DenyEnvVars configuration are excluded from the result.
// This prevents sensitive data (API keys, passwords, etc.) from being exposed to WASM.
//
// The filtering is case-sensitive and uses exact matching.
// If DenyEnvVars is empty or nil, all variables are passed through unchanged.
//
// Filtered variables are logged at debug level for audit purposes.
func (w *WasmRunner) filterEnvVars(env map[string]string) map[string]string {
	if len(w.cfg.DenyEnvVars) == 0 {
		return env
	}

	// Create a set of denied variables for faster lookup
	denied := make(map[string]bool, len(w.cfg.DenyEnvVars))
	for _, key := range w.cfg.DenyEnvVars {
		denied[key] = true
	}

	// Filter out denied variables
	safeEnv := make(map[string]string)
	for k, v := range env {
		if !denied[k] {
			safeEnv[k] = v
		} else if w.slog != nil {
			w.slog.Debug("filtered out denied environment variable", "key", k)
		}
	}

	return safeEnv
}

// Close performs cleanup of the WASM runtime and associated resources.
// It closes the wazero runtime and marks the runner as stopped.
// This method is safe to call multiple times.
//
// Returns an error if the runtime fails to close cleanly.
func (w *WasmRunner) Close() error {
	w.slog.Info("closing wasm runner")
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case <-w.stopCh:
		// already closed
		return nil
	default:
		close(w.stopCh)
	}
	return w.rt.Close(w.ctx)
}
