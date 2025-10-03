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

	"github.com/sandrolain/events-bridge/src/common"
	"github.com/sandrolain/events-bridge/src/common/encdec"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

type TargetConfig struct {
	// Command to execute.
	Command string `mapstructure:"command" validate:"required"`
	// Timeout to wait for graceful command termination.
	Timeout time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	// Arguments passed to the command.
	Args []string `mapstructure:"args"`
	// Environment variables for the command.
	Envs map[string]string `mapstructure:"envs"`
	// Format of the message. Allowed values: json, cbor, cli.
	// When format is json or cbor both MetadataKey and DataKey MUST be provided.
	Format string `mapstructure:"format" validate:"required,oneof=json cbor cli"`
	// Key under which metadata is encoded (required if format is json or cbor).
	MetadataKey string `mapstructure:"metadataKey" validate:"required_if=Format json cbor"`
	// Key under which data is encoded (required if format is json or cbor).
	DataKey string `mapstructure:"dataKey" validate:"required_if=Format json cbor"`
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
	format   string
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
		_ = t.stdin.Close()
		t.stdin = nil
	}

	waitErr := t.waitForExit(t.cfg.Timeout)
	ctxErr := t.ctx.Err()

	t.cancel()
	t.cancel = nil

	// Close the executor as well
	if t.executor != nil {
		_ = t.executor.Close()
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
			_ = t.cmd.Process.Kill()
		}
		if err, ok := <-t.waitDone; ok {
			return err
		}
		return t.commandExitError()
	}
}

func (t *CLITarget) buildPayload(metadata map[string]string, data []byte) (any, error) {
	if t.cfg.DataKey == "" {
		if len(data) == 0 {
			return nil, errors.New("data key not specified and message data is empty")
		}
		return data, nil
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("data key %q specified but message has no data", t.cfg.DataKey)
	}

	payload := make(map[string]any, 2)

	if t.cfg.MetadataKey != "" {
		payload[t.cfg.MetadataKey] = copyMetadata(metadata)
	}

	payload[t.cfg.DataKey] = data

	return payload, nil
}

func copyMetadata(src map[string]string) map[string]string {
	if len(src) == 0 {
		return map[string]string{}
	}
	return common.CopyMap(src, nil)
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
