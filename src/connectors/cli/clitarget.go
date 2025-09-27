package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/encdec"
	"github.com/sandrolain/events-bridge/src/message"
)

type TargetConfig struct {
	Command     string            `mapstructure:"command" validate:"required"`
	Timeout     time.Duration     `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	Args        []string          `mapstructure:"args"`
	Envs        map[string]string `mapstructure:"envs"`
	Format      string            `mapstructure:"format" validate:"required"`
	MetadataKey string            `mapstructure:"metadataKey"`
	DataKey     string            `mapstructure:"dataKey"`
}

func NewTargetConfig() any {
	return new(TargetConfig)
}

func NewTarget(anyCfg any) (connectors.Target, error) {
	cfg, ok := anyCfg.(*TargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	format, err := parseFormat(cfg.Format)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	cmd := exec.CommandContext(ctx, cfg.Command, cfg.Args...)
	if len(cfg.Envs) > 0 {
		env := make([]string, 0, len(cfg.Envs))
		for k, v := range cfg.Envs {
			env = append(env, k+"="+v)
		}
		cmd.Env = append(cmd.Env, env...)
	}

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
		cfg:         cfg,
		format:      format,
		slog:        slog.Default().With("context", "CLI Target"),
		timeout:     cfg.Timeout,
		ctx:         ctx,
		cancel:      cancel,
		cmd:         cmd,
		stdin:       stdin,
		done:        make(chan struct{}),
		metadataKey: cfg.MetadataKey,
		dataKey:     cfg.DataKey,
	}

	target.waitDone = make(chan error, 1)

	go target.waitCommand()
	go target.pipeLogger(stdout, "stdout")
	go target.pipeLogger(stderr, "stderr")

	target.slog.Info("started CLI target", "command", cfg.Command, "args", cfg.Args, "format", format)

	return target, nil
}

type CLITarget struct {
	cfg     *TargetConfig
	format  CLIFormat
	slog    *slog.Logger
	timeout time.Duration

	metadataKey string
	dataKey     string

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

	metadata, err := msg.GetTargetMetadata()
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}

	data, err := msg.GetTargetData()
	if err != nil {
		return fmt.Errorf("failed to get data: %w", err)
	}

	payload, err := t.buildPayload(metadata, data)
	if err != nil {
		return err
	}

	encoded, err := t.encodePayload(payload)
	if err != nil {
		return err
	}

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	if t.stdin == nil {
		return errors.New("stdin pipe not available")
	}

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

	waitErr := t.waitForExit(t.timeout)
	ctxErr := t.ctx.Err()

	t.cancel()
	t.cancel = nil

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

func (t *CLITarget) encodePayload(payload any) ([]byte, error) {
	switch t.format {
	case FormatJSON:
		return encodeJSONPayload(payload)
	case FormatCBOR:
		return encodeCBORPayload(payload)
	default:
		return nil, fmt.Errorf("unsupported format: %s", t.format)
	}
}

func encodeJSONPayload(payload any) ([]byte, error) {
	switch v := payload.(type) {
	case []byte:
		dataCopy := v
		encoded, err := encdec.EncodeJSON(&dataCopy)
		if err != nil {
			return nil, fmt.Errorf("failed to encode JSON payload: %w", err)
		}
		encoded = append(encoded, '\n')
		return encoded, nil
	case map[string]any:
		dataCopy := v
		encoded, err := encdec.EncodeJSON(&dataCopy)
		if err != nil {
			return nil, fmt.Errorf("failed to encode JSON payload: %w", err)
		}
		encoded = append(encoded, '\n')
		return encoded, nil
	default:
		return nil, fmt.Errorf("unsupported payload type %T for JSON format", payload)
	}
}

func encodeCBORPayload(payload any) ([]byte, error) {
	switch v := payload.(type) {
	case []byte:
		dataCopy := v
		encoded, err := encdec.EncodeCBOR(&dataCopy)
		if err != nil {
			return nil, fmt.Errorf("failed to encode CBOR payload: %w", err)
		}
		return encoded, nil
	case map[string]any:
		dataCopy := v
		encoded, err := encdec.EncodeCBOR(&dataCopy)
		if err != nil {
			return nil, fmt.Errorf("failed to encode CBOR payload: %w", err)
		}
		return encoded, nil
	default:
		return nil, fmt.Errorf("unsupported payload type %T for CBOR format", payload)
	}
}

func (t *CLITarget) buildPayload(metadata message.MessageMetadata, data []byte) (any, error) {
	if t.dataKey == "" {
		if len(data) == 0 {
			return nil, errors.New("data key not specified and message data is empty")
		}
		return data, nil
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("data key %q specified but message has no data", t.dataKey)
	}

	payload := make(map[string]any, 2)

	if t.metadataKey != "" {
		payload[t.metadataKey] = copyMetadata(metadata)
	}

	payload[t.dataKey] = data

	return payload, nil
}

func copyMetadata(src message.MessageMetadata) map[string]string {
	if len(src) == 0 {
		return map[string]string{}
	}
	res := make(map[string]string, len(src))
	for k, v := range src {
		res[k] = v
	}
	return res
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

func (t *CLITarget) pipeLogger(r io.Reader, stream string) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		t.slog.Warn("cli output", "stream", stream, "line", scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.slog.Error("error reading cli output", "stream", stream, "error", err)
	}
}
