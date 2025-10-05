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

type RunnerConfig struct {
	Path        string            `mapstructure:"path" validate:"required"`
	MountPath   string            `mapstructure:"mountPath"`
	Timeout     time.Duration     `mapstructure:"timeout" default:"5s" validate:"required"`
	Env         map[string]string `mapstructure:"env"`
	Args        []string          `mapstructure:"args"`
	Format      string            `mapstructure:"format" validate:"required,oneof=json cbor cli"`
	MetadataKey string            `mapstructure:"metadataKey" validate:"required_if=Format json cbor"`
	DataKey     string            `mapstructure:"dataKey" validate:"required_if=Format json cbor"`
}

type WasmRunner struct {
	cfg       *RunnerConfig
	timeout   time.Duration // Timeout for processing messages
	slog      *slog.Logger
	rt        wazero.Runtime
	ctx       context.Context
	mu        sync.Mutex
	wasmBytes []byte
	module    wazero.CompiledModule // keep the compiled module for instantiation
	decoder   encdec.MessageDecoder
	stopCh    chan struct{} // stop channel
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// New creates a new instance of WasmRunner
// runtimeCreateMu guards creation of new wazero runtimes (not instantiation) to avoid
// excessive parallel allocations during tests.
var runtimeCreateMu sync.Mutex

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

// Process handles the logic for a single message
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
