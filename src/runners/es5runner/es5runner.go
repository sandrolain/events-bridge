package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/dop251/goja"
	"github.com/fxamacker/cbor/v2"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/runners"
)

// Ensure ES5Runner implements models.Runner
var _ runners.Runner = &ES5Runner{}

type ES5Runner struct {
	cfg     *runners.RunnerES5Config
	slog    *slog.Logger
	program *goja.Program
	mu      sync.Mutex
	timeout time.Duration
	stopCh  chan struct{} // stop channel
}

// New creates a new instance of ES5Runner
func New(cfg *runners.RunnerES5Config) (runners.Runner, error) {
	if cfg == nil {
		return nil, fmt.Errorf("es5runner configuration cannot be nil")
	}
	if cfg.Path == "" {
		return nil, fmt.Errorf("js program path is required")
	}

	log := slog.Default().With("context", "ES5")
	log.Info("loading es5 program", "path", cfg.Path)

	src, err := os.ReadFile(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read js file: %w", err)
	}

	name := filepath.Base(cfg.Path)

	prog, err := goja.Compile(name, string(src), true)
	if err != nil {
		return nil, fmt.Errorf("failed to compile js: %w", err)
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	return &ES5Runner{
		cfg:     cfg,
		slog:    log,
		program: prog,
		timeout: timeout,
		stopCh:  make(chan struct{}),
	}, nil
}

// processMessage handles the logic for a single message
func (e *ES5Runner) Process(msg message.Message) (message.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	vm := goja.New()
	//vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))

	result := &ES5Message{original: msg}
	if err := vm.Set("message", result); err != nil {
		return nil, fmt.Errorf("failed to set message: %w", err)
	}

	// TODO: define ES5 context functions

	// Expose EncodeJSON/DecodeJSON
	err := vm.Set("EncodeJSON", func(call goja.FunctionCall) goja.Value {
		rt := vm
		data := call.Argument(0).Export()
		fmt.Printf("data: %v\n", data)
		b, err := sonic.Marshal(data)
		if err != nil {
			panic(rt.ToValue(fmt.Sprintf("EncodeJSON error: %s", err.Error())))
		}
		return rt.ToValue(b)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to set EncodeJSON: %w", err)
	}

	err = vm.Set("DecodeJSON", func(call goja.FunctionCall) goja.Value {
		rt := vm
		arg := call.Argument(0).Export()
		var data []byte
		switch v := arg.(type) {
		case string:
			data = []byte(v)
		case []byte:
			data = v
		case goja.ArrayBuffer:
			data = v.Bytes()
		default:
			panic(rt.ToValue("DecodeJSON expects a Buffer, ArrayBuffer or string"))
		}
		var out interface{}
		err := sonic.Unmarshal(data, &out)
		if err != nil {
			panic(rt.ToValue(fmt.Sprintf("DecodeJSON error: %s", err.Error())))
		}
		return rt.ToValue(out)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to set DecodeJSON: %w", err)
	}

	// Expose EncodeCBOR/DecodeCBOR
	err = vm.Set("EncodeCBOR", func(call goja.FunctionCall) goja.Value {
		rt := vm
		b, err := cbor.Marshal(call.Argument(0).Export())
		if err != nil {
			panic(rt.ToValue(fmt.Sprintf("EncodeCBOR error: %s", err.Error())))
		}
		return rt.ToValue(b)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to set EncodeCBOR: %w", err)
	}

	err = vm.Set("DecodeCBOR", func(call goja.FunctionCall) goja.Value {
		rt := vm
		arg := call.Argument(0).Export()
		var data []byte
		switch v := arg.(type) {
		case string:
			data = []byte(v)
		case []byte:
			data = v
		case goja.ArrayBuffer:
			data = v.Bytes()
		default:
			panic(rt.ToValue("DecodeCBOR expects a Buffer, ArrayBuffer or string"))
		}
		var out interface{}

		err := cbor.Unmarshal(data, &out)
		if err != nil {
			panic(rt.ToValue(fmt.Sprintf("DecodeCBOR error: %s", err.Error())))
		}
		return rt.ToValue(out)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to set DecodeCBOR: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := vm.RunProgram(e.program)
		done <- err
	}()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("js execution timeout")
	case err := <-done:
		if err != nil {
			return nil, fmt.Errorf("js execution error: %w", err)
		}
	}

	return result, nil
}

func (e *ES5Runner) Close() error {
	e.slog.Info("closing es5 runner")
	e.mu.Lock()
	defer e.mu.Unlock()
	select {
	case <-e.stopCh:
		// already closed
	default:
		close(e.stopCh)
	}
	return nil
}
