package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/common/encdec"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

type TargetConfig struct {
	Command         string            `mapstructure:"command" validate:"required"`
	Timeout         time.Duration     `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	Args            []string          `mapstructure:"args"`
	Envs            map[string]string `mapstructure:"envs"`
	Format          string            `mapstructure:"format" default:"cli" validate:"required,oneof=json cbor cli"`
	MetadataKey     string            `mapstructure:"metadataKey" default:"metadata" validate:"required"`
	DataKey         string            `mapstructure:"dataKey" default:"data" validate:"required"`
	AllowedCommands []string          `mapstructure:"allowedCommands"`                 // Whitelist of allowed commands
	WorkDir         string            `mapstructure:"workDir"`                         // Working directory for command execution
	MaxOutputSize   int64             `mapstructure:"maxOutputSize" default:"1048576"` // Max output size in bytes (default 1MB)
	DenyEnvVars     []string          `mapstructure:"denyEnvVars"`                     // Blacklist of environment variables to filter
	UseShell        bool              `mapstructure:"useShell" default:"false"`        // Allow shell interpretation (dangerous, disabled by default)
}

func NewTargetConfig() any {
	return new(TargetConfig)
}

func NewTarget(anyCfg any) (connectors.Target, error) {
	cfg, ok := anyCfg.(*TargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Validate using common validation
	baseConfig := targetToBaseConfig(cfg)
	if err := validateBaseConfig(baseConfig); err != nil {
		return nil, err
	}

	decoder, err := encdec.NewMessageDecoder(cfg.Format, cfg.MetadataKey, cfg.DataKey)
	if err != nil {
		return nil, fmt.Errorf("invalid format: %w", err)
	}

	executor, err := NewCommandExecutor(baseConfig, slog.Default().With("context", "CLI Target"))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	cmd := executor.CreateCommand(ctx)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to open stdin: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to open stdout: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to open stderr: %w", err)
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to start command: %w", err)
	}

	target := &CLITarget{
		cfg:      cfg,
		slog:     executor.slog,
		decoder:  decoder,
		executor: executor,
		ctx:      ctx,
		cancel:   cancel,
		cmd:      cmd,
		stdin:    stdin,
		done:     make(chan struct{}),
	}

	target.waitDone = make(chan error, 1)

	go target.waitCommand()
	go PipeLogger(target.slog, stdout, "stdout", ctx)
	go PipeLogger(target.slog, stderr, "stderr", ctx)

	target.slog.Info("started CLI target", "command", cfg.Command, "args", cfg.Args, "format", cfg.Format)

	return target, nil
}

type CLITarget struct {
	cfg      *TargetConfig
	decoder  encdec.MessageDecoder
	executor *CommandExecutor
	slog     *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	cmd   *exec.Cmd
	stdin io.WriteCloser
	done  chan struct{}

	waitOnce sync.Once
	waitDone chan error

	exitErrMu sync.RWMutex
	exitErr   error

	writeMu sync.Mutex
}

func (t *CLITarget) Consume(msg *message.RunnerMessage) error {
	select {
	case <-t.done:
		if err := t.commandExitError(); err != nil {
			return err
		}
		return errors.New("cli command already exited")
	default:
	}

	encoded, err := t.decoder.EncodeMessage(msg)
	if err != nil {
		return err
	}

	if t.stdin == nil {
		return errors.New("stdin pipe not available")
	}

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	if _, err := t.stdin.Write(encoded); err != nil {
		return fmt.Errorf("failed to write to CLI stdin: %w", err)
	}

	return nil
}

func (t *CLITarget) Close() error {
	if t.cancel == nil {
		return nil
	}

	if t.stdin != nil {
		if err := t.stdin.Close(); err != nil {
			t.slog.Warn("failed to close stdin", "error", err)
		}
		t.stdin = nil
	}

	waitErr := t.waitForExit(t.cfg.Timeout)
	ctxErr := t.ctx.Err()

	t.cancel()
	t.cancel = nil

	// Close the executor as well
	if t.executor != nil {
		if err := t.executor.Close(); err != nil {
			t.slog.Warn("failed to close executor", "error", err)
		}
	}

	if waitErr != nil && ctxErr == nil {
		return fmt.Errorf("cli command exited with error: %w", waitErr)
	}

	return nil
}

func (t *CLITarget) waitForExit(timeout time.Duration) error {
	if t.waitDone == nil {
		<-t.done
		return t.commandExitError()
	}

	select {
	case err, ok := <-t.waitDone:
		if ok {
			return err
		}
		return t.commandExitError()
	case <-time.After(timeout):
		if t.cmd != nil && t.cmd.Process != nil {
			if err := t.cmd.Process.Kill(); err != nil {
				t.slog.Warn("failed to kill process", "error", err)
			}
		}
		if err, ok := <-t.waitDone; ok {
			return err
		}
		return t.commandExitError()
	}
}

func (t *CLITarget) waitCommand() {
	err := t.cmd.Wait()
	if err != nil && t.ctx.Err() != nil {
		err = nil
	}

	t.exitErrMu.Lock()
	t.exitErr = err
	t.exitErrMu.Unlock()

	t.waitOnce.Do(func() {
		t.waitDone <- err
		close(t.waitDone)
	})

	close(t.done)
}

func (t *CLITarget) commandExitError() error {
	t.exitErrMu.RLock()
	defer t.exitErrMu.RUnlock()
	return t.exitErr
}
