package wasmrunner

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/runners/runner"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// Ensure WasmRunner implements runners.Runner
var _ runner.Runner = &WasmRunner{}

type RunnerWASMConfig struct {
	Path     string        `yaml:"path" json:"module_path" validate:"required,filepath"`
	Function string        `yaml:"function" json:"function" validate:"required"`
	Timeout  time.Duration `yaml:"timeout" json:"timeout" validate:"required"`
}

type WasmRunner struct {
	cfg       *RunnerWASMConfig
	slog      *slog.Logger
	rt        wazero.Runtime
	ctx       context.Context
	mu        sync.Mutex
	wasmBytes []byte
	module    wazero.CompiledModule // opzionale, se vuoi tenere il modulo compilato
	stopCh    chan struct{}         // canale di stop
}

// New crea una nuova istanza di WasmRunner
func New(cfg *RunnerWASMConfig) (runner.Runner, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("wasm module path is required")
	}

	if cfg.Function == "" {
		return nil, fmt.Errorf("function name is required in wasm runner configuration")
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
		rt:        rt,
		wasmBytes: wasmBytes,
		module:    cmod,
		stopCh:    make(chan struct{}),
	}, nil
}

// Ingest riceve i messaggi, li processa tramite la runtime wasm e restituisce i messaggi processati
func (w *WasmRunner) Ingest(in <-chan message.Message) (<-chan message.Message, error) {
	w.slog.Info("starting wasm ingestion", "function", w.cfg.Function)

	out := make(chan message.Message)
	go func() {
		defer close(out)
		for {
			select {
			case msg, ok := <-in:
				if !ok {
					return
				}
				if err := w.processMessage(msg, out); err != nil {
					w.slog.Error("wasm ingest error", "error", err)
				}
			case <-w.stopCh:
				w.slog.Info("wasm runner stopped via stopCh")
				return
			}
		}
	}()
	return out, nil
}

// processMessage gestisce la logica di un singolo messaggio, riducendo la complessità di Ingest
func (w *WasmRunner) processMessage(msg message.Message, out chan<- message.Message) error {
	data, err := msg.GetData()
	if err != nil {
		msg.Nak()
		return fmt.Errorf("error getting data from message: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.Timeout)
	defer cancel()

	dataSize := len(data)

	w.slog.Debug("processing message", "function", w.cfg.Function, "data_size", dataSize, "timeout", w.cfg.Timeout)

	stdin := bytes.NewReader(data)
	stout := bytes.NewBuffer(nil)

	module, err := w.rt.InstantiateModule(ctx, w.module, wazero.NewModuleConfig().
		WithStdin(stdin).
		WithStdout(stout).
		WithStderr(os.Stderr))
	if err != nil {
		msg.Nak()
		return fmt.Errorf("failed to instantiate wasm module: %w", err)
	}
	defer module.Close(ctx)

	resultData := stout.Bytes()

	processed := &WasmMessage{original: msg, data: resultData}
	out <- processed
	return nil
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
