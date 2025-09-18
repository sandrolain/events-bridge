package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/cliformat"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/runners"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// Ensure WasmRunner implements runners.Runner
var _ runners.Runner = &WasmRunner{}

type WasmRunner struct {
	cfg       *runners.RunnerWASMConfig
	timeout   time.Duration // Timeout for processing messages
	slog      *slog.Logger
	rt        wazero.Runtime
	ctx       context.Context
	mu        sync.Mutex
	wasmBytes []byte
	module    wazero.CompiledModule // optional: keep the compiled module
	stopCh    chan struct{}         // stop channel
}

// New creates a new instance of WasmRunner
func New(cfg *runners.RunnerWASMConfig) (runners.Runner, error) {
	if cfg == nil {
		return nil, fmt.Errorf("wasm runner configuration cannot be nil")
	}

	if cfg.Path == "" {
		return nil, fmt.Errorf("wasm module path is required")
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second // Default timeout if not set
	}

	log := slog.Default().With("context", "WASM")
	log.Info("loading wasm module", "path", cfg.Path)

	ctx := context.Background()

	wasmBytes, err := os.ReadFile(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read wasm file: %w", err)
	}

	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig())

	// Instantiate WASI before loading the module
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		return nil, fmt.Errorf("failed to instantiate WASI: %w", err)
	}

	cmod, err := rt.CompileModule(ctx, wasmBytes)
	if err != nil {
		defer func() {
			if err := rt.Close(ctx); err != nil {
				log.Error("failed to close wasm runtime", "err", err)
			}
		}()
		return nil, fmt.Errorf("failed to compile wasm module: %w", err)
	}

	return &WasmRunner{
		cfg:       cfg,
		slog:      log,
		ctx:       ctx,
		timeout:   timeout,
		rt:        rt,
		wasmBytes: wasmBytes,
		module:    cmod,
		stopCh:    make(chan struct{}),
	}, nil
}

// Process handles the logic for a single message
func (w *WasmRunner) Process(msg *message.RunnerMessage) (*message.RunnerMessage, error) {
	data, err := msg.GetSourceData()
	if err != nil {
		return nil, fmt.Errorf("error getting data from message: %w", err)
	}

	metadata, err := msg.GetSourceMetadata()
	if err != nil {
		return nil, fmt.Errorf("error getting metadata from message: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	inData := cliformat.Encode(metadata, data)

	w.slog.Debug("processing message", "timeout", w.timeout)

	stdin := bytes.NewReader(inData)
	stout := bytes.NewBuffer(nil)

	module, err := w.rt.InstantiateModule(ctx, w.module, wazero.NewModuleConfig().
		WithStdin(stdin).
		WithStdout(stout).
		WithStderr(os.Stderr))
	if err != nil {
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
	return w.rt.Close(w.ctx)
}
