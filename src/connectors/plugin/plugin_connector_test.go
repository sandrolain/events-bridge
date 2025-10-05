package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/sandrolain/events-bridge/src/connectors/plugin/manager"
	"github.com/sandrolain/events-bridge/src/message"
)

// locateTestPlugin returns the path to the pre-built test plugin binary.
// It expects the asset to be built beforehand via: task build-test-plugin-connector
// or by setting TEST_PLUGIN_CONNECTOR_BIN environment variable.
func locateTestPlugin(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("TEST_PLUGIN_CONNECTOR_BIN"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	name := "test-plugin-connector"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	// From package path src/connectors/plugin reach repo root ../../..
	candidates := []string{
		filepath.Clean(filepath.Join("..", "..", "..", "bin", name)),
		filepath.Clean(filepath.Join("..", "..", "bin", name)), // fallback
		filepath.Clean(filepath.Join("testassets", name)),      // local task output
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	t.Skipf("test plugin binary not found (looked in %v) - run 'task build-test-plugin-connector' first", candidates)
	return "" // unreachable
}

func newPluginConfig(execPath string, name string) manager.PluginConfig {
	return manager.PluginConfig{
		Name:           name,
		Exec:           execPath,
		Protocol:       "unix",
		Output:         true,
		Retry:          10,
		Delay:          100 * time.Millisecond,
		StatusInterval: 500 * time.Millisecond,
		Timeout:        2 * time.Second,
	}
}

func TestPluginSource(t *testing.T) {
	execPath := locateTestPlugin(t)
	name := "tp-src-" + sanitizeName(t.Name())
	cfg := &SourceConfig{Plugin: newPluginConfig(execPath, name)}
	srcAny, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("NewSource error: %v", err)
	}
	src := srcAny.(*PluginSource)

	ch, err := src.Produce(10)
	if err != nil {
		t.Fatalf("Produce error: %v", err)
	}

	received := 0
	timeout := time.After(2 * time.Second)
	for received < 3 { // expected messages from test plugin
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for messages, got %d", received)
		case m := <-ch:
			if m == nil {
				continue
			}
			received++
		}
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
}

func TestPluginRunner(t *testing.T) {
	execPath := locateTestPlugin(t)
	name := "tp-run-" + sanitizeName(t.Name())
	cfg := &RunnerConfig{Plugin: newPluginConfig(execPath, name), Timeout: 2 * time.Second}
	rAny, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner error: %v", err)
	}
	r := rAny.(*PluginRunner)

	// JSON payload the runner will convert to CBOR
	payload := map[string]any{"hello": "world", "num": 42}
	data, _ := json.Marshal(payload)
	// Compose a RunnerMessage from a stub source message
	sm := &stubSourceMessage{meta: map[string]string{"content-type": "application/json"}, data: data, id: []byte("id1")}
	msg := message.NewRunnerMessage(sm)

	err = r.Process(msg)
	if err != nil {
		t.Fatalf("Process error: %v", err)
	}
	md, _ := msg.GetMetadata()
	if md == nil || md["processed"] != "true" {
		t.Fatalf("expected processed metadata, got %#v", md)
	}

	// Decode CBOR back to map
	var decoded map[string]any
	outData, err := msg.GetData()
	if err != nil {
		t.Fatalf("GetData error: %v", err)
	}
	if err := cbor.Unmarshal(outData, &decoded); err != nil {
		t.Fatalf("failed to unmarshal CBOR: %v", err)
	}
	if decoded["hello"] != "world" {
		t.Fatalf("unexpected decoded data: %#v", decoded)
	}
	// number may decode as uint64 or float64 depending on cbor settings
	switch v := decoded["num"].(type) {
	case uint64:
		if v != 42 {
			t.Fatalf("unexpected num: %v", v)
		}
	case float64:
		if int(v) != 42 {
			t.Fatalf("unexpected num: %v", v)
		}
	default:
		t.Fatalf("unexpected num type: %T", v)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Runner Close error: %v", err)
	}
}

func TestPluginTarget(t *testing.T) {
	execPath := locateTestPlugin(t)
	name := "tp-tgt-" + sanitizeName(t.Name())
	cfg := &TargetConfig{Plugin: newPluginConfig(execPath, name), Timeout: 2 * time.Second}
	tAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf("NewTarget error: %v", err)
	}
	tgt := tAny.(*PluginTarget)

	sm := &stubSourceMessage{meta: map[string]string{"k": "v"}, data: []byte("data"), id: []byte("id2")}
	msg := message.NewRunnerMessage(sm)
	if err := tgt.Consume(msg); err != nil {
		t.Fatalf("Consume error: %v", err)
	}
	if err := tgt.Close(); err != nil {
		t.Fatalf("Target Close error: %v", err)
	}
}

// stubSourceMessage implements message.SourceMessage for tests
type stubSourceMessage struct {
	id   []byte
	meta map[string]string
	data []byte
}

func (s *stubSourceMessage) GetID() []byte                           { return s.id }
func (s *stubSourceMessage) GetMetadata() (map[string]string, error) { return s.meta, nil }
func (s *stubSourceMessage) GetData() ([]byte, error)                { return s.data, nil }
func (s *stubSourceMessage) Ack() error                              { return nil }
func (s *stubSourceMessage) Nak() error                              { return errors.New("nak not implemented in stub") }
func (s *stubSourceMessage) Reply(d *message.ReplyData) error        { return nil }

// sanitizeName replaces problematic characters in test names
func sanitizeName(s string) string {
	b := make([]rune, 0, len(s))
	for _, r := range s {
		switch r {
		case '/', ' ', ':':
			b = append(b, '-')
		default:
			b = append(b, r)
		}
	}
	return string(b)
}
