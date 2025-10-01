package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/cliformat"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure CLIRunner implements runner.Runner
var _ connectors.Runner = &CLIRunner{}

type CLIRunner struct {
	cfg      *RunnerConfig
	executor *CommandExecutor
	slog     *slog.Logger
	timeout  time.Duration
}

type RunnerConfig struct {
	Command string            `mapstructure:"command" validate:"required"`
	Timeout time.Duration     `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	Args    []string          `mapstructure:"args"`
	Envs    map[string]string `mapstructure:"envs"`
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Validate using common validation
	baseConfig := runnerToBaseConfig(cfg)
	if err := validateBaseConfig(baseConfig); err != nil {
		return nil, err
	}

	executor, err := NewCommandExecutor(baseConfig, slog.Default().With("context", "CLI Runner"))
	if err != nil {
		return nil, err
	}

	return &CLIRunner{
		cfg:      cfg,
		executor: executor,
		slog:     executor.slog,
		timeout:  cfg.Timeout,
	}, nil
}

func (c *CLIRunner) Process(msg *message.RunnerMessage) (*message.RunnerMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	meta, err := msg.GetTargetMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	data, err := msg.GetTargetData()
	if err != nil {
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	d, err := cliformat.Encode(meta, data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode input data: %w", err)
	}

	stdin := bytes.NewReader(d)
	cmd := c.executor.CreateCommand(ctx)
	cmd.Stdin = stdin

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
	if c.executor != nil {
		return c.executor.Close()
	}
	return nil
}
