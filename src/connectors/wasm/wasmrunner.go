package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/common/cliformat"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// Ensure WasmRunner implements connectors.Runner
var _ connectors.Runner = &WasmRunner{}

type RunnerConfig struct {
	Path      string            `mapstructure:"path" validate:"required"`
	MountPath string            `mapstructure:"mountPath"`
	Timeout   time.Duration     `mapstructure:"timeout" default:"5s" validate:"required"`
	Env       map[string]string `mapstructure:"env"`
	Args      []string          `mapstructure:"args"`
}

type WasmRunner struct {
	cfg       *RunnerConfig
	timeout   time.Duration // Timeout for processing messages
	slog      *slog.Logger
	rt        wazero.Runtime
	ctx       context.Context
	mu        sync.Mutex
	wasmBytes []byte
	module    wazero.CompiledModule // optional: keep the compiled module
	stopCh    chan struct{}         // stop channel
	cached    bool                  // true if using shared cached module/runtime
	cacheKey  string                // path used as cache key
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// New creates a new instance of WasmRunner
// runtimeCreateMu guards creation of new wazero runtimes (not instantiation) to avoid
// excessive parallel allocations during tests. Module compilation is expensive so we
// also add a simple in-process cache keyed by the wasm file path.
var runtimeCreateMu sync.Mutex

type cachedModule struct {
	rt        wazero.Runtime
	cmod      wazero.CompiledModule
	wasmBytes []byte
	refs      int
}

var (
	moduleCache   = map[string]*cachedModule{}
	moduleCacheMu sync.Mutex
)

func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	log := slog.Default().With("context", "WASM Runner")
	log.Info("loading wasm module", "path", cfg.Path)

	ctx := context.Background()
	cacheKey := cfg.Path

	// Attempt to reuse a compiled module/runtime from cache
	moduleCacheMu.Lock()
	if cached, ok := moduleCache[cacheKey]; ok {
		cached.refs++
		moduleCacheMu.Unlock()
		return &WasmRunner{
			cfg:       cfg,
			slog:      log,
			ctx:       ctx,
			timeout:   cfg.Timeout,
			rt:        cached.rt,
			wasmBytes: cached.wasmBytes,
			module:    cached.cmod,
			stopCh:    make(chan struct{}),
			cached:    true,
			cacheKey:  cacheKey,
		}, nil
	}
	moduleCacheMu.Unlock()

	// Not cached: read and compile
	wasmBytes, err := os.ReadFile(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read wasm file: %w", err)
	}

	runtimeCreateMu.Lock()
	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig())
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

	// Store in cache
	moduleCacheMu.Lock()
	moduleCache[cacheKey] = &cachedModule{rt: rt, cmod: cmod, wasmBytes: wasmBytes, refs: 1}
	moduleCacheMu.Unlock()

	return &WasmRunner{
		cfg:       cfg,
		slog:      log,
		ctx:       ctx,
		timeout:   cfg.Timeout,
		rt:        rt,
		wasmBytes: wasmBytes,
		module:    cmod,
		stopCh:    make(chan struct{}),
		cached:    true,
		cacheKey:  cacheKey,
	}, nil
}

// Process handles the logic for a single message
func (w *WasmRunner) Process(msg *message.RunnerMessage) (*message.RunnerMessage, error) {
	data, err := msg.GetData()
	if err != nil {
		return nil, fmt.Errorf("error getting data from message: %w", err)
	}

	meta, err := msg.GetMetadata()
	if err != nil {
		return nil, fmt.Errorf("error getting metadata from message: %w", err)
	}

	inData, err := cliformat.Encode(meta, data)
	if err != nil {
		return nil, fmt.Errorf("error encoding input data: %w", err)
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

	if len(w.cfg.Env) > 0 {
		for k, v := range w.cfg.Env {
			config = config.WithEnv(k, v)
		}
	}

	if w.cfg.MountPath != "" {
		config = config.WithFS(os.DirFS(w.cfg.MountPath))
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	module, err := w.rt.InstantiateModule(ctx, w.module, config)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			w.slog.Warn("wasm execution timeout")
		}
		return nil, fmt.Errorf("failed to instantiate wasm module: %w", err)
	}
	defer func() {
		if err := module.Close(ctx); err != nil {
			w.slog.Error("failed to close wasm module", "err", err)
		}
	}()

	outMeta, outData, err := cliformat.Decode(stout.Bytes())
	if err != nil {
		return nil, fmt.Errorf("error decoding output data: %w", err)
	}

	msg.MergeMetadata(outMeta)
	msg.SetData(outData)

	return msg, nil
}

func (w *WasmRunner) Close() error {
	w.slog.Info("closing wasm runner")
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case <-w.stopCh:
		// already closed
	default:
		close(w.stopCh)
	}
	if w.cached {
		moduleCacheMu.Lock()
		if entry, ok := moduleCache[w.cacheKey]; ok {
			entry.refs--
			if entry.refs <= 0 {
				delete(moduleCache, w.cacheKey)
				moduleCacheMu.Unlock()
				return entry.rt.Close(w.ctx)
			}
		}
		moduleCacheMu.Unlock()
		return nil
	}
	// Fallback (should not happen with current logic)
	return w.rt.Close(w.ctx)
}
