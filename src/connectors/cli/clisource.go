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

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/encdec"
	"github.com/sandrolain/events-bridge/src/message"
)

type SourceConfig struct {
	Command     string            `mapstructure:"command" validate:"required"`
	Timeout     time.Duration     `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	Args        []string          `mapstructure:"args"`
	Envs        map[string]string `mapstructure:"envs"`
	Format      string            `mapstructure:"format" validate:"required"`
	MetadataKey string            `mapstructure:"metadataKey"`
	DataKey     string            `mapstructure:"dataKey"`
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

	format, err := parseFormat(cfg.Format)
	if err != nil {
		return nil, err
	}

	executor, err := NewCommandExecutor(baseConfig, slog.Default().With("context", "CLI Source"))
	if err != nil {
		return nil, err
	}

	return &CLISource{
		cfg:         cfg,
		format:      format,
		executor:    executor,
		timeout:     cfg.Timeout,
		slog:        executor.slog,
		metadataKey: cfg.MetadataKey,
		dataKey:     cfg.DataKey,
	}, nil
}

type CLISource struct {
	cfg      *SourceConfig
	format   CLIFormat
	executor *CommandExecutor
	timeout  time.Duration
	slog     *slog.Logger

	metadataKey string
	dataKey     string

	ctx      context.Context
	cancel   context.CancelFunc
	cmd      *exec.Cmd
	waitOnce sync.Once
	waitDone chan error
	stdout   io.ReadCloser

	c chan *message.RunnerMessage
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

	s.slog.Info("started CLI source", "command", s.cfg.Command, "args", s.cfg.Args, "format", s.format)

	go s.waitCommand()
	go PipeLogger(s.slog, stderr, "stderr", s.ctx)
	go s.consumeStream(stdout)

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
		case <-time.After(s.timeout):
			if s.cmd != nil && s.cmd.Process != nil {
				_ = s.cmd.Process.Kill()
			}
			if err, ok := <-s.waitDone; ok && err != nil && s.ctx.Err() == nil {
				return fmt.Errorf("cli command exited with error: %w", err)
			}
		}
	}

	return nil
}

func (s *CLISource) consumeStream(r io.Reader) {
	defer close(s.c)

	var err error
	switch s.format {
	case FormatJSON:
		err = s.consumeJSON(r)
	case FormatCBOR:
		err = s.consumeCBOR(r)
	default:
		err = fmt.Errorf("unsupported format: %s", s.format)
	}

	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
		s.slog.Error("error consuming CLI stream", "error", err)
	}
}

func (s *CLISource) consumeJSON(r io.Reader) error {
	stream := encdec.DecodeJSONStream[map[string]any](r)
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
			if err := s.handlePayload(payload.Value); err != nil {
				s.slog.Error("failed to process CLI payload", "error", err)
			}
		}
	}
}

func (s *CLISource) consumeCBOR(r io.Reader) error {
	stream := encdec.DecodeCBORStream[map[string]any](r)
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case payload, ok := <-stream:
			if !ok {
				s.slog.Debug("cbor stream closed")
				return nil
			}
			if payload.Error != nil {
				s.slog.Error("error decoding CBOR payload", "error", payload.Error)
				continue
			}
			if err := s.handlePayload(payload.Value); err != nil {
				s.slog.Error("failed to process CLI payload", "error", err)
			}
		}
	}
}

func (s *CLISource) handlePayload(payload map[string]any) error {
	if payload == nil {
		return errors.New("payload is nil")
	}

	metadata, data, err := s.processPayload(payload)
	if err != nil {
		return err
	}

	msg := newCLISourceMessage(metadata, data)
	runnerMsg := message.NewRunnerMessage(msg)
	if len(metadata) > 0 {
		runnerMsg.MergeMetadata(metadata)
	}
	if len(data) > 0 {
		runnerMsg.SetData(data)
	}

	select {
	case s.c <- runnerMsg:
	case <-s.ctx.Done():
	}

	return nil
}

func (s *CLISource) processPayload(payload map[string]any) (message.MessageMetadata, []byte, error) {
	metadata, err := s.extractMetadata(payload)
	if err != nil {
		return nil, nil, err
	}

	dataValue, err := s.extractData(payload)
	if err != nil {
		return nil, nil, err
	}

	dataBytes, err := s.encodeData(dataValue)
	if err != nil {
		return nil, nil, err
	}

	return metadata, dataBytes, nil
}

func (s *CLISource) extractMetadata(payload map[string]any) (message.MessageMetadata, error) {
	if s.metadataKey == "" {
		return nil, nil
	}

	raw, ok := payload[s.metadataKey]
	if !ok {
		return nil, fmt.Errorf("metadata key %q not found", s.metadataKey)
	}

	meta, err := convertToStringMap(raw)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func convertToStringMap(value any) (message.MessageMetadata, error) {
	switch typed := value.(type) {
	case map[string]string:
		return copyStringStringMap(typed), nil
	case map[string]any:
		return convertMapStringAny(typed)
	case map[any]any:
		return convertMapInterfaceAny(typed)
	default:
		return nil, fmt.Errorf("metadata must be map[string]string (got %T)", value)
	}
}

func copyStringStringMap(src map[string]string) message.MessageMetadata {
	res := make(message.MessageMetadata, len(src))
	for k, v := range src {
		res[k] = v
	}
	return res
}

func convertMapStringAny(src map[string]any) (message.MessageMetadata, error) {
	res := make(message.MessageMetadata, len(src))
	for k, v := range src {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("metadata value for key %q is not string (got %T)", k, v)
		}
		res[k] = str
	}
	return res, nil
}

func convertMapInterfaceAny(src map[any]any) (message.MessageMetadata, error) {
	res := make(message.MessageMetadata, len(src))
	for k, v := range src {
		key, ok := k.(string)
		if !ok {
			return nil, fmt.Errorf("metadata key is not string (got %T)", k)
		}
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("metadata value for key %q is not string (got %T)", key, v)
		}
		res[key] = str
	}
	return res, nil
}

func (s *CLISource) extractData(payload map[string]any) (any, error) {
	if s.dataKey != "" {
		raw, ok := payload[s.dataKey]
		if !ok {
			return nil, fmt.Errorf("data key %q not found", s.dataKey)
		}
		return raw, nil
	}

	dataCopy := make(map[string]any, len(payload))
	for k, v := range payload {
		if s.metadataKey != "" && k == s.metadataKey {
			continue
		}
		dataCopy[k] = v
	}
	return dataCopy, nil
}

func (s *CLISource) encodeData(value any) ([]byte, error) {
	switch s.format {
	case FormatJSON:
		tmp := value
		encoded, err := encdec.EncodeJSON(&tmp)
		if err != nil {
			return nil, fmt.Errorf("failed to encode JSON data: %w", err)
		}
		return encoded, nil
	case FormatCBOR:
		tmp := value
		encoded, err := encdec.EncodeCBOR(&tmp)
		if err != nil {
			return nil, fmt.Errorf("failed to encode CBOR data: %w", err)
		}
		return encoded, nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", s.format)
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

type CLISourceMessage struct {
	metadata message.MessageMetadata
	data     []byte
}

var _ message.SourceMessage = (*CLISourceMessage)(nil)

func newCLISourceMessage(metadata message.MessageMetadata, data []byte) *CLISourceMessage {
	msg := &CLISourceMessage{}
	if len(metadata) > 0 {
		msg.metadata = make(message.MessageMetadata, len(metadata))
		for k, v := range metadata {
			msg.metadata[k] = v
		}
	}
	if len(data) > 0 {
		msg.data = make([]byte, len(data))
		copy(msg.data, data)
	}
	return msg
}

func (m *CLISourceMessage) GetID() []byte {
	return nil
}

func (m *CLISourceMessage) GetMetadata() (message.MessageMetadata, error) {
	if m.metadata == nil {
		return message.MessageMetadata{}, nil
	}
	res := make(message.MessageMetadata, len(m.metadata))
	for k, v := range m.metadata {
		res[k] = v
	}
	return res, nil
}

func (m *CLISourceMessage) GetData() ([]byte, error) {
	if len(m.data) == 0 {
		return nil, nil
	}
	res := make([]byte, len(m.data))
	copy(res, m.data)
	return res, nil
}

func (m *CLISourceMessage) Ack() error {
	return nil
}

func (m *CLISourceMessage) Nak() error {
	return nil
}

func (m *CLISourceMessage) Reply(*message.ReplyData) error {
	return nil
}
