package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/common/encdec"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure CLIRunner implements runner.Runner
var _ connectors.Runner = &CLIRunner{}

type RunnerConfig struct {
	Command     string            `mapstructure:"command" validate:"required"`
	Timeout     time.Duration     `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	Args        []string          `mapstructure:"args"`
	Envs        map[string]string `mapstructure:"envs"`
	Format      string            `mapstructure:"format" default:"cli" validate:"required,oneof=json cbor cli"`
	MetadataKey string            `mapstructure:"metadataKey" default:"metadata" validate:"required"`
	DataKey     string            `mapstructure:"dataKey" default:"data" validate:"required"`
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

	decoder, err := encdec.NewMessageDecoder(cfg.Format, cfg.MetadataKey, cfg.DataKey)
	if err != nil {
		return nil, fmt.Errorf("invalid format: %w", err)
	}

	executor, err := NewCommandExecutor(baseConfig, slog.Default().With("context", "CLI Runner"))
	if err != nil {
		return nil, err
	}

	return &CLIRunner{
		cfg:      cfg,
		slog:     executor.slog,
		executor: executor,
		decoder:  decoder,
	}, nil
}

type CLIRunner struct {
	cfg      *RunnerConfig
	slog     *slog.Logger
	decoder  encdec.MessageDecoder
	executor *CommandExecutor
}

func (c *CLIRunner) Process(msg *message.RunnerMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.Timeout)
	defer cancel()

	d, err := c.decoder.EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
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
		return fmt.Errorf("cli execution error: %w, stderr: %s", err, stderr.String())
	}

	res, err := c.decoder.DecodeMessage(stdout.Bytes())
	if err != nil {
		return fmt.Errorf("failed to decode message: %w", err)
	}

	err = msg.SetFromSourceMessage(res)
	if err != nil {
		return fmt.Errorf("failed to update message: %w", err)
	}

	return nil
}

func (c *CLIRunner) Close() error {
	if c.executor != nil {
		return c.executor.Close()
	}
	return nil
}
