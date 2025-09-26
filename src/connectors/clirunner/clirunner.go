package clirunner

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"time"

	"github.com/sandrolain/events-bridge/src/cliformat"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/common"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure CLIRunner implements runner.Runner
var _ connectors.Runner = &CLIRunner{}

type CLIRunner struct {
	cfg     *RunnerConfig
	slog    *slog.Logger
	timeout time.Duration
}

type RunnerConfig struct {
	Command string            `mapstructure:"command" validate:"required"`
	Timeout time.Duration     `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	Args    []string          `mapstructure:"args"`
	Envs    map[string]string `mapstructure:"envs"`
}

func NewRunner(opts map[string]any) (connectors.Runner, error) {
	cfg, err := common.ParseConfig[RunnerConfig](opts)
	if err != nil {
		return nil, err
	}
	log := slog.Default().With("context", "CLI Runner")

	return &CLIRunner{
		cfg:     cfg,
		slog:    log,
		timeout: cfg.Timeout,
	}, nil
}

func (c *CLIRunner) Process(msg *message.RunnerMessage) (*message.RunnerMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	meta, err := msg.GetSourceMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}
	data, err := msg.GetSourceData()
	if err != nil {
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	d, err := cliformat.Encode(meta, data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode input data: %w", err)
	}

	stdin := bytes.NewReader(d)
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

	msg.MergeMetadata(outMeta)
	msg.SetData(outData)

	return msg, nil
}

func (c *CLIRunner) Close() error {
	return nil
}
