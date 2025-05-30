package es5runner

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
	"github.com/sandrolain/events-bridge/src/models"
)

// Assicura che ES5Runner implementi models.Runner
var _ models.Runner = &ES5Runner{}

type ES5Runner struct {
	cfg     *models.RunnerES5Config
	slog    *slog.Logger
	program *goja.Program
	mu      sync.Mutex
	timeout time.Duration
}

// New crea una nuova istanza di ES5Runner
func New(cfg *models.RunnerES5Config) (models.Runner, error) {
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
	}, nil
}

// Ingest riceve i messaggi, li processa tramite la VM JS e restituisce i messaggi processati
func (e *ES5Runner) Ingest(in <-chan message.Message) (<-chan message.Message, error) {
	e.slog.Info("starting es5 ingestion")
	out := make(chan message.Message)
	go func() {
		defer close(out)
		for msg := range in {
			if err := e.processMessage(msg, out); err != nil {
				e.slog.Error("es5 ingest error", "error", err)
			}
		}
	}()
	return out, nil
}

// processMessage gestisce la logica di un singolo messaggio
func (e *ES5Runner) processMessage(msg message.Message, out chan<- message.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	vm := goja.New()
	//vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))

	result := &ES5Message{original: msg}
	if err := vm.Set("message", result); err != nil {
		msg.Nak()
		return fmt.Errorf("failed to set message: %w", err)
	}

	// Espone EncodeJSON/DecodeJSON
	vm.Set("EncodeJSON", func(call goja.FunctionCall) goja.Value {
		rt := vm
		data := call.Argument(0).Export()
		fmt.Printf("data: %v\n", data)
		b, err := sonic.Marshal(data)
		if err != nil {
			panic(rt.ToValue(fmt.Sprintf("EncodeJSON error: %s", err.Error())))
		}
		return rt.ToValue(b)
	})
	vm.Set("DecodeJSON", func(call goja.FunctionCall) goja.Value {
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

	// Espone EncodeCBOR/DecodeCBOR
	vm.Set("EncodeCBOR", func(call goja.FunctionCall) goja.Value {
		rt := vm
		b, err := cbor.Marshal(call.Argument(0).Export())
		if err != nil {
			panic(rt.ToValue(fmt.Sprintf("EncodeCBOR error: %s", err.Error())))
		}
		return rt.ToValue(b)
	})
	vm.Set("DecodeCBOR", func(call goja.FunctionCall) goja.Value {
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

	done := make(chan error, 1)
	go func() {
		_, err := vm.RunProgram(e.program)
		done <- err
	}()

	select {
	case <-ctx.Done():
		msg.Nak()
		return fmt.Errorf("js execution timeout")
	case err := <-done:
		if err != nil {
			msg.Nak()
			return fmt.Errorf("js execution error: %w", err)
		}
	}

	out <- result
	return nil
}

func (e *ES5Runner) Close() error {
	e.slog.Info("closing es5 runner")
	return nil
}

// ES5Message implementa message.Message per i dati processati
// Espone metodi JS-friendly per data e metadata

type ES5Message struct {
	original message.Message
	data     []byte
	metadata map[string][]string
}

func (m *ES5Message) GetMetadata() (map[string][]string, error) {
	if m.metadata != nil {
		return m.metadata, nil
	}
	return m.original.GetMetadata()
}

func (m *ES5Message) SetMetadata(key string, value string) {
	if m.metadata == nil {
		orig, _ := m.original.GetMetadata()
		m.metadata = make(map[string][]string, len(orig))
		for k, v := range orig {
			vv := make([]string, len(v))
			copy(vv, v)
			m.metadata[k] = vv
		}
	}
	m.metadata[key] = []string{value}
}

func (m *ES5Message) AddMetadata(key string, value string) {
	if m.metadata == nil {
		orig, _ := m.original.GetMetadata()
		m.metadata = make(map[string][]string, len(orig))
		for k, v := range orig {
			vv := make([]string, len(v))
			copy(vv, v)
			m.metadata[k] = vv
		}
	}
	m.metadata[key] = append(m.metadata[key], value)
}

func (m *ES5Message) GetData() ([]byte, error) {
	if m.data != nil {
		return m.data, nil
	}
	return m.original.GetData()
}

func (m *ES5Message) SetData(data []byte) {
	m.data = data
}

func (m *ES5Message) Ack() error {
	return m.original.Ack()
}

func (m *ES5Message) Nak() error {
	return m.original.Nak()
}
