package clirunner

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"time"

	"github.com/sandrolain/events-bridge/src/cliformat"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/runners"
	"github.com/sandrolain/events-bridge/src/utils"
)

// Ensure CLIRunner implements runner.Runner
var _ runners.Runner = &CLIRunner{}

type Config struct {
	Command string
	Timeout time.Duration
	Args    []string
	Envs    map[string]string
}

type CLIRunner struct {
	cfg     *Config
	slog    *slog.Logger
	timeout time.Duration
}

func parseConfig(opts map[string]any) (*Config, error) {
	parser := &utils.OptsParser{}
	command := parser.OptString(opts, "command", "", utils.StringNonEmpty())
	args := parser.OptStringArray(opts, "args", nil)
	envs := parser.OptStringMap(opts, "envs", nil)
	timeout := parser.OptDuration(opts, "timeout", 0)
	if err := parser.Error(); err != nil {
		return nil, err
	}
	if command == "" {
		return nil, fmt.Errorf("cli command is required")
	}
	if timeout <= 0 {
		timeout = runners.DefaultTimeout
	}
	return &Config{
		Command: command,
		Timeout: timeout,
		Args:    args,
		Envs:    envs,
	}, nil
}

func New(opts map[string]any) (runners.Runner, error) {
	cfg, err := parseConfig(opts)
	if err != nil {
		return nil, err
	}
	log := slog.Default().With("context", "CLI")

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
