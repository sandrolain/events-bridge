package utils_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/utils"
)

const unexpectedParserError = "unexpected parser error: %v"

type mockSourceMessage struct {
	metadata message.MessageMetadata
}

func (m *mockSourceMessage) GetID() []byte {
	return nil
}

func (m *mockSourceMessage) GetMetadata() (message.MessageMetadata, error) {
	return m.metadata, nil
}

func (m *mockSourceMessage) GetData() ([]byte, error) {
	return nil, nil
}

func (m *mockSourceMessage) Ack() error {
	return nil
}

func (m *mockSourceMessage) Nak() error {
	return nil
}

func (m *mockSourceMessage) Reply(data *message.ReplyData) error {
	return nil
}

func TestResolveFromMetadata(t *testing.T) {
	base := message.MessageMetadata{"color": "blue"}
	msg := message.NewRunnerMessage(&mockSourceMessage{metadata: base})

	if got := utils.ResolveFromMetadata(msg, "color", "fallback"); got != "blue" {
		t.Fatalf("expected metadata value to be returned, got %q", got)
	}

	if got := utils.ResolveFromMetadata(msg, "missing", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback for missing key, got %q", got)
	}

	if got := utils.ResolveFromMetadata(msg, "", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback for empty key, got %q", got)
	}

	msg.SetMetadata("color", "green")
	if got := utils.ResolveFromMetadata(msg, "color", "fallback"); got != "green" {
		t.Fatalf("expected overridden metadata value, got %q", got)
	}
}

func TestAwaitReplyOrStatusReply(t *testing.T) {
	done := make(chan message.ResponseStatus, 1)
	reply := make(chan *message.ReplyData, 1)
	reply <- &message.ReplyData{Data: []byte("hello")}

	r, status, timeout := utils.AwaitReplyOrStatus(50*time.Millisecond, done, reply)
	if timeout {
		t.Fatal("unexpected timeout")
	}
	if status != nil {
		t.Fatal("expected nil status when reply received")
	}
	if r == nil || string(r.Data) != "hello" {
		t.Fatalf("unexpected reply data: %+v", r)
	}
}

func TestAwaitReplyOrStatusStatus(t *testing.T) {
	done := make(chan message.ResponseStatus, 1)
	reply := make(chan *message.ReplyData, 1)
	done <- message.ResponseStatusAck

	r, status, timeout := utils.AwaitReplyOrStatus(50*time.Millisecond, done, reply)
	if timeout {
		t.Fatal("unexpected timeout")
	}
	if r != nil {
		t.Fatal("expected nil reply when status received")
	}
	if status == nil || *status != message.ResponseStatusAck {
		t.Fatalf("unexpected status: %v", status)
	}
}

func TestAwaitReplyOrStatusTimeout(t *testing.T) {
	done := make(chan message.ResponseStatus)
	reply := make(chan *message.ReplyData)

	r, status, timeout := utils.AwaitReplyOrStatus(10*time.Millisecond, done, reply)
	if !timeout {
		t.Fatal("expected timeout to be true")
	}
	if r != nil {
		t.Fatal("expected nil reply on timeout")
	}
	if status != nil {
		t.Fatal("expected nil status on timeout")
	}
}

func TestLoadPluginMissingFile(t *testing.T) {
	value, err := utils.LoadPlugin[map[string]any, int]("/non/existent/plugin.so", "Constructor", nil)
	if err == nil {
		t.Fatal("expected error when plugin file is missing")
	}
	if !strings.Contains(err.Error(), "failed to open plugin") {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != 0 {
		t.Fatalf("expected zero value for generic type, got %d", value)
	}
}

func TestOptsParserOptIntConversionAndValidation(t *testing.T) {
	parser := &utils.OptsParser{}
	opts := map[string]any{"port": float64(8123)}

	val := parser.OptInt(opts, "port", 1000, utils.IntMin(1000), utils.IntMax(9000))
	if val != 8123 {
		t.Fatalf("expected 8123, got %d", val)
	}
	if err := parser.Error(); err != nil {
		t.Fatalf(unexpectedParserError, err)
	}
}

func TestOptsParserOptIntErrors(t *testing.T) {
	parser := &utils.OptsParser{}
	opts := map[string]any{
		"typeMismatch": "oops",
		"tooSmall":     1,
	}

	_ = parser.OptInt(opts, "typeMismatch", 0)
	_ = parser.OptInt(opts, "tooSmall", 0, utils.IntGreaterThan(5))

	err := parser.Error()
	if err == nil {
		t.Fatal("expected aggregated error")
	}
	optsErr, ok := err.(*utils.OptsError)
	if !ok {
		t.Fatalf("expected *utils.OptsError, got %T", err)
	}
	if len(optsErr.Errors) != 2 {
		t.Fatalf("expected 2 errors, got %d", len(optsErr.Errors))
	}
}

func TestOptsParserOptString(t *testing.T) {
	parser := &utils.OptsParser{}
	opts := map[string]any{"name": "bridge"}

	val := parser.OptString(opts, "name", "default", utils.StringNonEmpty())
	if val != "bridge" {
		t.Fatalf("expected string value, got %q", val)
	}
	if err := parser.Error(); err != nil {
		t.Fatalf(unexpectedParserError, err)
	}
}

func TestOptsParserOptStringArray(t *testing.T) {
	parser := &utils.OptsParser{}
	opts := map[string]any{"topics": []any{"one", "two"}}

	arr := parser.OptStringArray(opts, "topics", []string{"fallback"})
	expected := []string{"one", "two"}
	if !reflect.DeepEqual(arr, expected) {
		t.Fatalf("expected %v, got %v", expected, arr)
	}
	if err := parser.Error(); err != nil {
		t.Fatalf(unexpectedParserError, err)
	}
}

func TestOptsParserOptStringMap(t *testing.T) {
	parser := &utils.OptsParser{}
	opts := map[string]any{"headers": "k1:v1; k2:v2"}

	m := parser.OptStringMap(opts, "headers", nil)
	if len(m) != 2 || m["k1"] != "v1" || m["k2"] != "v2" {
		t.Fatalf("unexpected map: %v", m)
	}
}

func TestOptsParserOptBoolAndDuration(t *testing.T) {
	parser := &utils.OptsParser{}
	opts := map[string]any{
		"enabled":  "true",
		"timeout":  "150ms",
		"interval": "invalid",
	}

	if val := parser.OptBool(opts, "enabled", true); val != true {
		t.Fatalf("expected default bool when type mismatch, got %v", val)
	}

	if d := parser.OptDuration(opts, "timeout", time.Second); d != 150*time.Millisecond {
		t.Fatalf("expected parsed duration, got %v", d)
	}

	_ = parser.OptDuration(opts, "interval", time.Second)

	err := parser.Error()
	if err == nil {
		t.Fatal("expected aggregated error")
	}
	if !strings.Contains(err.Error(), "options validation errors") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestOptsErrorError(t *testing.T) {
	aggregate := &utils.OptsError{Errors: []error{
		errors.New("first"),
		errors.New("second"),
	}}

	msg := aggregate.Error()
	if !strings.Contains(msg, "first") || !strings.Contains(msg, "second") {
		t.Fatalf("expected both errors in message, got %q", msg)
	}
}
