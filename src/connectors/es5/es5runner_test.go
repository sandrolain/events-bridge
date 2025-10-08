package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

func TestNewRunner_InvalidConfigType(t *testing.T) {
	t.Parallel()

	_, err := NewRunner(struct{}{})
	if err == nil {
		t.Fatalf("expected error for invalid config type, got nil")
	}

	if !strings.Contains(err.Error(), "invalid config type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewRunner_ReadFileError(t *testing.T) {
	t.Parallel()

	cfg := &RunnerConfig{Path: filepath.Join(t.TempDir(), "missing.js"), Timeout: time.Second}
	_, err := NewRunner(cfg)
	if err == nil {
		t.Fatalf("expected error when reading missing file, got nil")
	}

	if !strings.Contains(err.Error(), "failed to read js file") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestES5RunnerProcessSuccess(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "script.js")
	script := `console.log("processing");
var upper = svc.call("Uppercase", message.getDataString());
message.setDataString(util.bytesToString(upper));
message.setMetadata("processed", "true");
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf("failed to write script file: %v", err)
	}

	svc := newMockService()
	svc.register("Uppercase", func(args []any) ([]byte, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("unexpected args length: %d", len(args))
		}
		str, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected arg type: %T", args[0])
		}
		return []byte(strings.ToUpper(str)), nil
	})

	cfg := &RunnerConfig{
		Path:     scriptPath,
		Timeout:  time.Second,
		Services: map[string]connectors.Service{"svc": svc},
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	runner := runnerAny.(*ES5Runner)

	msg := message.NewRunnerMessage(&stubSourceMessage{
		id:       []byte("id"),
		data:     []byte("hello"),
		metadata: map[string]string{"source": "test"},
	})

	if err := runner.Process(msg); err != nil {
		t.Fatalf("process returned error: %v", err)
	}

	resultData, err := msg.GetData()
	if err != nil {
		t.Fatalf("failed to get message data: %v", err)
	}
	if string(resultData) != "HELLO" {
		t.Fatalf("unexpected message data: %s", resultData)
	}

	meta, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}
	if meta["processed"] != "true" {
		t.Fatalf("expected processed metadata to be true, got %q", meta["processed"])
	}

	if len(svc.calls) != 1 {
		t.Fatalf("expected one service call, got %d", len(svc.calls))
	}
	if svc.calls[0].method != "Uppercase" {
		t.Fatalf("unexpected method called: %s", svc.calls[0].method)
	}
	if len(svc.calls[0].args) != 1 || svc.calls[0].args[0] != "hello" {
		t.Fatalf("unexpected call args: %#v", svc.calls[0].args)
	}
}

func TestES5RunnerProcessRuntimeError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "script.js")
	script := `throw new Error("boom");`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf("failed to write script file: %v", err)
	}

	cfg := &RunnerConfig{Path: scriptPath, Timeout: time.Second}
	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	runner := runnerAny.(*ES5Runner)

	msg := message.NewRunnerMessage(&stubSourceMessage{data: []byte("payload")})
	err = runner.Process(msg)
	if err == nil {
		t.Fatalf("expected runtime error, got nil")
	}

	if !strings.Contains(err.Error(), "js execution error") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestES5RunnerProcessInvalidServiceMethod(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "script.js")
	script := `svc.call("Unknown");`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf("failed to write script file: %v", err)
	}

	svc := newMockService()

	cfg := &RunnerConfig{
		Path:     scriptPath,
		Timeout:  time.Second,
		Services: map[string]connectors.Service{"svc": svc},
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	runner := runnerAny.(*ES5Runner)

	msg := message.NewRunnerMessage(&stubSourceMessage{data: []byte("payload")})
	err = runner.Process(msg)
	if err == nil {
		t.Fatalf("expected panic error, got nil")
	}

	if !strings.Contains(err.Error(), "invalid method") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type serviceCall struct {
	method string
	args   []any
}

type mockService struct {
	methods map[string]func([]any) ([]byte, error)
	calls   []serviceCall
}

func newMockService() *mockService {
	return &mockService{methods: make(map[string]func([]any) ([]byte, error))}
}

func (m *mockService) register(name string, fn func([]any) ([]byte, error)) {
	m.methods[name] = fn
}

func (m *mockService) List() ([]string, error) {
	names := make([]string, 0, len(m.methods))
	for name := range m.methods {
		names = append(names, name)
	}
	return names, nil
}

func (m *mockService) IsValidMethod(name string, args []any) bool {
	_, ok := m.methods[name]
	return ok
}

func (m *mockService) Call(name string, args []any) ([]byte, error) {
	fn, ok := m.methods[name]
	if !ok {
		return nil, errors.New("unknown method")
	}
	m.calls = append(m.calls, serviceCall{method: name, args: args})
	return fn(args)
}

func (m *mockService) Close() error {
	return nil
}

type stubSourceMessage struct {
	id       []byte
	data     []byte
	metadata map[string]string
}

func (s *stubSourceMessage) GetID() []byte {
	if s.id != nil {
		return s.id
	}
	return []byte("stub-id")
}

func (s *stubSourceMessage) GetMetadata() (map[string]string, error) {
	if s.metadata == nil {
		return map[string]string{}, nil
	}
	return s.metadata, nil
}

func (s *stubSourceMessage) GetData() ([]byte, error) {
	return s.data, nil
}

func (s *stubSourceMessage) Ack() error { return nil }

func (s *stubSourceMessage) Nak() error { return nil }

func (s *stubSourceMessage) Reply(data *message.ReplyData) error { return nil }

func (s *stubSourceMessage) ReplySource() error { return nil }
