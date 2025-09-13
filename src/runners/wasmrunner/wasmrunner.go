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
	module    wazero.CompiledModule // opzionale, se vuoi tenere il modulo compilato
	stopCh    chan struct{}         // canale di stop
}

// New crea una nuova istanza di WasmRunner
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

	// Istanzia WASI prima di caricare il modulo
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		return nil, fmt.Errorf("failed to instantiate WASI: %w", err)
	}

	cmod, err := rt.CompileModule(ctx, wasmBytes)
	if err != nil {
		rt.Close(ctx)
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

// Process gestisce la logica di un singolo messaggio, riducendo la complessità di Ingest
func (w *WasmRunner) Process(msg message.Message) (message.Message, error) {
	data, err := msg.GetData()
	if err != nil {
		msg.Nak()
		return nil, fmt.Errorf("error getting data from message: %w", err)
	}

	metadata, err := msg.GetMetadata()
	if err != nil {
		msg.Nak()
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
		msg.Nak()
		return nil, fmt.Errorf("failed to instantiate wasm module: %w", err)
	}
	defer module.Close(ctx)

	outMeta, outData, err := cliformat.Decode(stout.Bytes())
	if err != nil {
		msg.Nak()
		return nil, fmt.Errorf("error decoding output data: %w", err)
	}

	processed := &WasmMessage{
		original: msg,
		data:     outData,
		metadata: outMeta,
	}

	return processed, nil
}

func (w *WasmRunner) Close() error {
	w.slog.Info("closing wasm runner")
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case <-w.stopCh:
		// già chiuso
	default:
		close(w.stopCh)
	}
	return w.rt.Close(w.ctx)
}
