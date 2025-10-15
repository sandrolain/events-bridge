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

type SourceConfig struct {
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

func NewSourceConfig() any {
	return new(SourceConfig)
}

func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Validate using common validation
	baseConfig := sourceToBaseConfig(cfg)
	if err := validateBaseConfig(baseConfig); err != nil {
		return nil, err
	}

	decoder, err := encdec.NewMessageDecoder(cfg.Format, cfg.MetadataKey, cfg.DataKey)
	if err != nil {
		return nil, fmt.Errorf("invalid format: %w", err)
	}

	executor, err := NewCommandExecutor(baseConfig, slog.Default().With("context", "CLI Source"))
	if err != nil {
		return nil, err
	}

	return &CLISource{
		cfg:      cfg,
		slog:     executor.slog,
		executor: executor,
		decoder:  decoder,
	}, nil
}

type CLISource struct {
	cfg      *SourceConfig
	slog     *slog.Logger
	decoder  encdec.MessageDecoder
	executor *CommandExecutor
	ctx      context.Context
	cancel   context.CancelFunc
	cmd      *exec.Cmd
	waitOnce sync.Once
	waitDone chan error
	stdout   io.ReadCloser
	c        chan *message.RunnerMessage
}

func (s *CLISource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	if s.c != nil {
		return nil, errors.New("produce already called")
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel

	cmd := s.executor.CreateCommand(ctx)

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

	s.cmd = cmd
	s.stdout = stdout
	s.c = make(chan *message.RunnerMessage, buffer)
	s.waitDone = make(chan error, 1)

	s.slog.Info("started CLI source", "command", s.cfg.Command, "args", s.cfg.Args, "format", s.cfg.Format)

	go s.waitCommand()
	go PipeLogger(s.slog, stderr, "stderr", s.ctx)
	go func() {
		err := s.consumeStream(stdout)
		if err != nil && s.ctx.Err() == nil {
			s.slog.Error("error consuming stream", "error", err)
		}
	}()

	return s.c, nil
}

func (s *CLISource) Close() error {
	if s.cancel == nil {
		return nil
	}

	s.cancel()
	if s.stdout != nil {
		_ = s.stdout.Close()
	}

	// Close the executor as well
	if s.executor != nil {
		_ = s.executor.Close()
	}

	if s.waitDone != nil {
		select {
		case err, ok := <-s.waitDone:
			if ok {
				if err != nil && s.ctx.Err() == nil {
					return fmt.Errorf("cli command exited with error: %w", err)
				}
			}
		case <-time.After(s.cfg.Timeout):
			if s.cmd != nil && s.cmd.Process != nil {
				_ = s.cmd.Process.Kill()
			}
			if err, ok := <-s.waitDone; ok && err != nil && s.ctx.Err() == nil {
				return fmt.Errorf("cli command exited with error: %w", err)
			}
		}
	}

	close(s.c)

	return nil
}

func (s *CLISource) consumeStream(r io.Reader) error {
	stream := s.decoder.DecodeStream(r)
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case payload, ok := <-stream:
			if !ok {
				s.slog.Debug("json stream closed")
				return nil
			}
			if payload.Error != nil {
				s.slog.Error("error decoding JSON payload", "error", payload.Error)
				continue
			}
			s.c <- message.NewRunnerMessage(payload.Value)
		}
	}
}

func (s *CLISource) waitCommand() {
	err := s.cmd.Wait()
	if err != nil && s.ctx.Err() != nil {
		err = nil
	}
	s.waitOnce.Do(func() {
		s.waitDone <- err
		close(s.waitDone)
	})
}
