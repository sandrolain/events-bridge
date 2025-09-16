package clirunner

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/cliformat"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/runners"
)

// Ensure CLIRunner implements runner.Runner
var _ runners.Runner = &CLIRunner{}

type CLIRunner struct {
	cfg     *runners.RunnerCLIConfig
	slog    *slog.Logger
	mu      sync.Mutex
	timeout time.Duration
	stopCh  chan struct{}
}

func New(cfg *runners.RunnerCLIConfig) (runners.Runner, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cli runner config cannot be nil")
	}
	if cfg.Command == "" {
		return nil, fmt.Errorf("cli command is required")
	}
	log := slog.Default().With("context", "CLI")

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second // Default timeout if not set
	}
	return &CLIRunner{
		cfg:     cfg,
		slog:    log,
		timeout: timeout,
		stopCh:  make(chan struct{}),
	}, nil
}

func (c *CLIRunner) Process(msg message.Message) (message.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	meta, err := msg.GetMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}
	data, err := msg.GetData()
	if err != nil {
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	stdin := bytes.NewReader(cliformat.Encode(meta, data))
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
		return nil, fmt.Errorf("cli execution error: %w, stderr: %s", err, stderr.String())
	}

	outMeta, outData, err := cliformat.Decode(stdout.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to decode cli output: %w", err)
	}

	processed := &cliMessage{
		original: msg,
		meta:     outMeta,
		data:     outData,
	}

	return processed, nil
}
