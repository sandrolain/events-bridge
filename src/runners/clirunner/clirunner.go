package clirunner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os/exec"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/runners/runner"
)

// Assicura che CLIRunner implementi runner.Runner
var _ runner.Runner = &CLIRunner{}

type RunnerCLIConfig struct {
	Command string            `yaml:"command" json:"command" validate:"required"`
	Timeout time.Duration     `yaml:"timeout" json:"timeout"`
	Args    []string          `yaml:"args" json:"args"`
	Envs    map[string]string `yaml:"envs" json:"envs"`
}

type CLIRunner struct {
	cfg     *RunnerCLIConfig
	slog    *slog.Logger
	mu      sync.Mutex
	timeout time.Duration
	stopCh  chan struct{}
}

func New(cfg *RunnerCLIConfig) (runner.Runner, error) {
	if cfg.Command == "" {
		return nil, fmt.Errorf("cli command is required")
	}
	log := slog.Default().With("context", "CLI")
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &CLIRunner{
		cfg:     cfg,
		slog:    log,
		timeout: cfg.Timeout,
		stopCh:  make(chan struct{}),
	}, nil
}

func (c *CLIRunner) Ingest(in <-chan message.Message) (<-chan message.Message, error) {
	c.slog.Info("starting cli ingestion")
	out := make(chan message.Message)
	go func() {
		defer close(out)
		for {
			select {
			case msg, ok := <-in:
				if !ok {
					return
				}
				if err := c.processMessage(msg, out); err != nil {
					c.slog.Error("cli ingest error", "error", err)
				}
			case <-c.stopCh:
				c.slog.Info("cli runner stopped via stopCh")
				return
			}
		}
	}()
	return out, nil
}

func (c *CLIRunner) processMessage(msg message.Message, out chan<- message.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
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

	stdin := bytes.NewReader(encode(meta, data))
	cmd := exec.CommandContext(ctx, c.cfg.Command, c.cfg.Args...)
	cmd.Stdin = stdin
	if len(c.cfg.Envs) > 0 {
		env := make([]string, 0, len(c.cfg.Envs))
		for k, v := range c.cfg.Envs {
			env = append(env, k+"="+v)
		}
		cmd.Env = append(cmd.Env, env...)
	}

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		msg.Nak()
		return fmt.Errorf("cli execution error: %w, stderr: %s", err, stderr.String())
	}

	outMeta, outData, err := decode(stdout.Bytes())
	if err != nil {
		msg.Nak()
		return fmt.Errorf("failed to decode cli output: %w", err)
	}

	out <- &cliMessage{original: msg, meta: outMeta, data: outData}
	return nil
}

type cliMessage struct {
	original message.Message
	meta     map[string][]string
	data     []byte
}

func (m *cliMessage) GetMetadata() (map[string][]string, error) {
	return m.meta, nil
}

func (m *cliMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *cliMessage) Ack() error {
	return m.original.Ack()
}

func (m *cliMessage) Nak() error {
	return m.original.Nak()
}

func (c *CLIRunner) Close() error {
	c.slog.Info("closing cli runner")
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.stopCh:
		// già chiuso
	default:
		close(c.stopCh)
	}
	return nil
}

// rendo le funzioni Encode/Decode non in un file separato ma come funzioni private in clirunner.go per evitare import cycle

func encode(meta map[string][]string, data []byte) []byte {
	q := url.Values{}
	for k, vals := range meta {
		for _, v := range vals {
			q.Add(k, v)
		}
	}
	var buf bytes.Buffer
	buf.WriteString(q.Encode())
	buf.WriteByte('\n')
	buf.Write(data)
	return buf.Bytes()
}

func decode(input []byte) (map[string][]string, []byte, error) {
	i := bytes.IndexByte(input, '\n')
	if i < 0 {
		return nil, nil, errors.New("invalid format: missing newline separator")
	}
	metaStr := string(input[:i])
	data := input[i+1:]
	m, err := url.ParseQuery(metaStr)
	if err != nil {
		return nil, nil, err
	}
	return m, data, nil
}
