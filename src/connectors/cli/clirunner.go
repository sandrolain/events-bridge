package main

import (
	"bytes"
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

// Ensure CLIRunner implements runner.Runner
var _ connectors.Runner = &CLIRunner{}

type RunnerConfig struct {
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

	// LongRunning enables target-like behavior: start process once and pipe messages to stdin
	// When false (default), runs command once per message and returns stdout as result
	LongRunning bool `mapstructure:"longRunning" default:"false"`
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

	runner := &CLIRunner{
		cfg:      cfg,
		slog:     executor.slog,
		executor: executor,
		decoder:  decoder,
	}

	// If configured as long-running, start the process now and keep pipes
	if cfg.LongRunning {
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

		runner.ctx = ctx
		runner.cancel = cancel
		runner.cmd = cmd
		runner.stdin = stdin
		runner.done = make(chan struct{})
		runner.waitDone = make(chan error, 1)

		go runner.waitCommand()
		go PipeLogger(runner.slog, stdout, "stdout", ctx)
		go PipeLogger(runner.slog, stderr, "stderr", ctx)

		runner.slog.Info("started CLI runner (long-running)", "command", cfg.Command, "args", cfg.Args, "format", cfg.Format)
	}

	return runner, nil
}

type CLIRunner struct {
	cfg      *RunnerConfig
	slog     *slog.Logger
	decoder  encdec.MessageDecoder
	executor *CommandExecutor
	// Long-running process fields (used when cfg.LongRunning == true)
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

func (c *CLIRunner) Process(msg *message.RunnerMessage) error {
	// If configured as long-running, write to the already-running process stdin
	if c.cfg.LongRunning {
		// Check process state
		select {
		case <-c.done:
			if err := c.commandExitError(); err != nil {
				return err
			}
			return errors.New("cli command already exited")
		default:
		}

		encoded, err := c.decoder.EncodeMessage(msg)
		if err != nil {
			return err
		}

		if c.stdin == nil {
			return errors.New("stdin pipe not available")
		}

		c.writeMu.Lock()
		defer c.writeMu.Unlock()

		if _, err := c.stdin.Write(encoded); err != nil {
			return fmt.Errorf("failed to write to CLI stdin: %w", err)
		}

		return nil
	}

	// Default per-message execution
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
		return fmt.Errorf("cli execution error: %w, stderr: %s, stdout: %s", err, stderr.String(), stdout.String())
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
	// If long-running, attempt graceful shutdown
	if c.cfg.LongRunning {
		if c.cancel == nil {
			// nothing to do
		} else {
			if c.stdin != nil {
				if err := c.stdin.Close(); err != nil {
					c.slog.Warn("failed to close stdin", "error", err)
				}
				c.stdin = nil
			}

			waitErr := c.waitForExit(c.cfg.Timeout)
			ctxErr := c.ctx.Err()

			c.cancel()
			c.cancel = nil

			if c.executor != nil {
				if err := c.executor.Close(); err != nil {
					c.slog.Warn("failed to close executor", "error", err)
				}
			}

			if waitErr != nil && ctxErr == nil {
				return fmt.Errorf("cli command exited with error: %w", waitErr)
			}
		}

		return nil
	}

	if c.executor != nil {
		return c.executor.Close()
	}
	return nil
}

// waitForExit waits for the long-running command to exit or kills it after timeout
func (c *CLIRunner) waitForExit(timeout time.Duration) error {
	if c.waitDone == nil {
		<-c.done
		return c.commandExitError()
	}

	select {
	case err, ok := <-c.waitDone:
		if ok {
			return err
		}
		return c.commandExitError()
	case <-time.After(timeout):
		if c.cmd != nil && c.cmd.Process != nil {
			if err := c.cmd.Process.Kill(); err != nil {
				c.slog.Warn("failed to kill process", "error", err)
			}
		}
		if err, ok := <-c.waitDone; ok {
			return err
		}
		return c.commandExitError()
	}
}

func (c *CLIRunner) waitCommand() {
	err := c.cmd.Wait()
	if err != nil && c.ctx.Err() != nil {
		err = nil
	}

	c.exitErrMu.Lock()
	c.exitErr = err
	c.exitErrMu.Unlock()

	c.waitOnce.Do(func() {
		c.waitDone <- err
		close(c.waitDone)
	})

	close(c.done)
}

func (c *CLIRunner) commandExitError() error {
	c.exitErrMu.RLock()
	defer c.exitErrMu.RUnlock()
	return c.exitErr
}
