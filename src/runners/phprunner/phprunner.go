package phprunner

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/runners/clirunner"
	"github.com/sandrolain/events-bridge/src/runners/runner"
)

// Assicura che PHPRunner implementi runner.Runner
var _ runner.Runner = &PHPRunner{}

type RunnerPHPConfig struct {
	Path    string        `yaml:"path" json:"path" validate:"required,filepath"`
	Timeout time.Duration `yaml:"timeout" json:"timeout"`
}

type PHPRunner struct {
	cfg     *RunnerPHPConfig
	slog    *slog.Logger
	mu      sync.Mutex
	timeout time.Duration
	stopCh  chan struct{}
}

func New(cfg *RunnerPHPConfig) (runner.Runner, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("php script path is required")
	}
	log := slog.Default().With("context", "PHP")
	log.Info("loading php script", "path", cfg.Path)
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &PHPRunner{
		cfg:     cfg,
		slog:    log,
		timeout: cfg.Timeout,
		stopCh:  make(chan struct{}),
	}, nil
}

func (p *PHPRunner) Ingest(in <-chan message.Message) (<-chan message.Message, error) {
	p.slog.Info("starting php ingestion")
	out := make(chan message.Message)
	go func() {
		defer close(out)
		for {
			select {
			case msg, ok := <-in:
				if !ok {
					return
				}
				if err := p.processMessage(msg, out); err != nil {
					p.slog.Error("php ingest error", "error", err)
				}
			case <-p.stopCh:
				p.slog.Info("php runner stopped via stopCh")
				return
			}
		}
	}()
	return out, nil
}

func (p *PHPRunner) processMessage(msg message.Message, out chan<- message.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	meta, err := msg.GetMetadata()
	if err != nil {
		msg.Nak()
		return fmt.Errorf("failed to get metadata: %w", err)
	}
	data, err := msg.GetData()
	if err != nil {
		msg.Nak()
		return fmt.Errorf("failed to get data: %w", err)
	}

	// Usa la funzione Encode di clirunner
	stdin := bytes.NewReader(clirunner.Encode(meta, data))

	cmd := exec.CommandContext(ctx, "php", p.cfg.Path)
	cmd.Stdin = stdin

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		msg.Nak()
		return fmt.Errorf("php execution error: %w, stderr: %s", err, stderr.String())
	}

	// Usa la funzione Decode di clirunner
	outMeta, outData, err := clirunner.Decode(stdout.Bytes())
	if err != nil {
		msg.Nak()
		return fmt.Errorf("failed to decode php output: %w", err)
	}

	out <- &phpMessage{original: msg, meta: outMeta, data: outData}
	return nil
}

type phpMessage struct {
	original message.Message
	meta     map[string][]string
	data     []byte
}

func (m *phpMessage) GetMetadata() (map[string][]string, error) {
	return m.meta, nil
}

func (m *phpMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *phpMessage) Ack() error {
	return m.original.Ack()
}

func (m *phpMessage) Nak() error {
	return m.original.Nak()
}

func (p *PHPRunner) Close() error {
	p.slog.Info("closing php runner")
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.stopCh:
		// già chiuso
	default:
		close(p.stopCh)
	}
	return nil
}
