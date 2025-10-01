package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/encdec"
	"github.com/sandrolain/events-bridge/src/message"
)

type CLIFormat string

const (
	FormatJSON CLIFormat = "JSON"
	FormatCBOR CLIFormat = "CBOR"
)

func parseFormat(value string) (CLIFormat, error) {
	v := CLIFormat(strings.ToUpper(strings.TrimSpace(value)))
	switch v {
	case FormatJSON, FormatCBOR:
		return v, nil
	default:
		return "", fmt.Errorf("unsupported format %q", value)
	}
}

// validateCommand validates the command and arguments to prevent command injection
func validateCommand(command string, args []string) error {
	// Check if command is a valid executable name or absolute path
	if strings.Contains(command, ";") || strings.Contains(command, "&") ||
		strings.Contains(command, "|") || strings.Contains(command, "$") ||
		strings.Contains(command, "`") || strings.Contains(command, ">") ||
		strings.Contains(command, "<") {
		return fmt.Errorf("command contains potentially dangerous characters: %s", command)
	}

	// Validate command is not empty and doesn't start with suspicious patterns
	command = strings.TrimSpace(command)
	if command == "" {
		return fmt.Errorf("command cannot be empty")
	}

	// Check for shell metacharacters in arguments
	dangerousChars := regexp.MustCompile(`[;&|$\x60<>]`)
	for _, arg := range args {
		if dangerousChars.MatchString(arg) {
			return fmt.Errorf("argument contains potentially dangerous characters: %s", arg)
		}
	}

	return nil
}

// sanitizeEnvVars validates environment variable keys and values
func sanitizeEnvVars(envs map[string]string) error {
	validKeyPattern := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

	for k, v := range envs {
		// Validate environment variable key
		if !validKeyPattern.MatchString(k) {
			return fmt.Errorf("invalid environment variable key: %s", k)
		}

		// Check for dangerous patterns in values
		if strings.Contains(v, "$") && strings.Contains(v, "(") {
			return fmt.Errorf("environment variable value contains potentially dangerous pattern: %s", k)
		}
	}

	return nil
}

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

	// Validate command and arguments for security
	if err := validateCommand(cfg.Command, cfg.Args); err != nil {
		return nil, fmt.Errorf("command validation failed: %w", err)
	}

	// Validate environment variables
	if err := sanitizeEnvVars(cfg.Envs); err != nil {
		return nil, fmt.Errorf("environment variable validation failed: %w", err)
	}

	format, err := parseFormat(cfg.Format)
	if err != nil {
		return nil, err
	}

	return &CLISource{
		cfg:         cfg,
		format:      format,
		timeout:     cfg.Timeout,
		slog:        slog.Default().With("context", "CLI Source"),
		metadataKey: cfg.MetadataKey,
		dataKey:     cfg.DataKey,
	}, nil
}

type CLISource struct {
	cfg     *SourceConfig
	format  CLIFormat
	slog    *slog.Logger
	timeout time.Duration

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

	cmd := exec.CommandContext(ctx, s.cfg.Command, s.cfg.Args...)
	if len(s.cfg.Envs) > 0 {
		// Additional runtime validation (defense in depth)
		if err := sanitizeEnvVars(s.cfg.Envs); err != nil {
			cancel()
			return nil, fmt.Errorf("runtime environment variable validation failed: %w", err)
		}

		env := make([]string, 0, len(s.cfg.Envs))
		for k, v := range s.cfg.Envs {
			env = append(env, k+"="+v)
		}
		cmd.Env = append(os.Environ(), env...)
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

	s.cmd = cmd
	s.stdout = stdout
	s.c = make(chan *message.RunnerMessage, buffer)
	s.waitDone = make(chan error, 1)

	s.slog.Info("started CLI source", "command", s.cfg.Command, "args", s.cfg.Args, "format", s.format)

	go s.waitCommand()
	go s.pipeLogger(stderr, "stderr")
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

func (s *CLISource) pipeLogger(r io.Reader, stream string) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		s.slog.Warn("cli output", "stream", stream, "line", scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) {
			return
		}
		s.slog.Error("error reading cli output", "stream", stream, "error", err)
	}
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
