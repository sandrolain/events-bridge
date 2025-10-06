package main

import (
	"bytes"
	"net"
	"testing"
	"time"

	coap "github.com/plgd-dev/go-coap/v3"
	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
	coapmux "github.com/plgd-dev/go-coap/v3/mux"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/utils"
)

const coapRunnerErrCreate = "unexpected error creating coap runner: %v"

// stubSourceMessage reused (duplicated minimal) from http runner tests for isolation.
type stubSourceMessage struct {
	data     []byte
	metadata map[string]string
}

func (s *stubSourceMessage) GetID() []byte                           { return []byte("id") }
func (s *stubSourceMessage) GetMetadata() (map[string]string, error) { return s.metadata, nil }
func (s *stubSourceMessage) GetData() ([]byte, error)                { return s.data, nil }
func (s *stubSourceMessage) Ack() error                              { return nil }
func (s *stubSourceMessage) Nak() error                              { return nil }
func (s *stubSourceMessage) Reply(d *message.ReplyData) error        { return nil }

func mustParseCoAPRunnerConfig(t *testing.T, opts map[string]any) *CoAPRunnerConfig {
	t.Helper()
	cfg := new(CoAPRunnerConfig)
	if err := utils.ParseConfig(opts, cfg); err != nil {
		t.Fatalf("failed to parse coap runner config: %v", err)
	}
	return cfg
}

func startTestCoAPServer(t *testing.T, path string, handler coapmux.Handler) string {
	t.Helper()
	router := coapmux.NewRouter()
	if err := router.Handle(path, handler); err != nil {
		t.Fatalf("failed to register coap path: %v", err)
	}
	// Reserve a UDP port
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve udp port: %v", err)
	}
	addr := pc.LocalAddr().String()
	_ = pc.Close()
	go func() {
		if e := coap.ListenAndServe("udp", addr, router); e != nil {
			// cannot t.Fatalf in goroutine safely; best effort logging via testing logger
			t.Logf("coap test server error: %v", e)
		}
	}()
	// Brief wait to let server start
	time.Sleep(50 * time.Millisecond)
	return addr
}

func TestCoAPRunnerSuccess(t *testing.T) {
	path := "/test"
	addr := startTestCoAPServer(t, path, coapmux.HandlerFunc(func(w coapmux.ResponseWriter, m *coapmux.Message) {
		_ = w.SetResponse(coapcodes.Content, coapmessage.AppJSON, bytes.NewReader([]byte(`{"ok":true}`)))
	}))

	cfg := mustParseCoAPRunnerConfig(t, map[string]any{"protocol": "udp", "address": addr, "path": path, "method": "POST", "timeout": "1s"})
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(coapRunnerErrCreate, err)
	}
	runner := r.(*CoAPRunner)

	msg := message.NewRunnerMessage(&stubSourceMessage{data: []byte(`{"in":1}`), metadata: map[string]string{"Content-Type": "application/json"}})
	if err := runner.Process(msg); err != nil {
		t.Fatalf("unexpected process error: %v", err)
	}
	meta, data, err := msg.GetMetadataAndData()
	if err != nil {
		t.Fatalf("unexpected get data error: %v", err)
	}
	if string(data) != `{"ok":true}` {
		t.Fatalf("expected body overwritten, got %s", string(data))
	}
	if meta["eb-status"] == "" {
		t.Fatalf("expected eb-status metadata")
	}
}

func TestCoAPRunnerNon2XX(t *testing.T) {
	path := "/test"
	addr := startTestCoAPServer(t, path, coapmux.HandlerFunc(func(w coapmux.ResponseWriter, m *coapmux.Message) {
		_ = w.SetResponse(coapcodes.InternalServerError, coapmessage.TextPlain, nil)
	}))
	cfg := mustParseCoAPRunnerConfig(t, map[string]any{"protocol": "udp", "address": addr, "path": path, "method": "GET", "timeout": "1s"})
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(coapRunnerErrCreate, err)
	}
	runner := r.(*CoAPRunner)
	orig := []byte(`{"in":2}`)
	msg := message.NewRunnerMessage(&stubSourceMessage{data: orig, metadata: map[string]string{"Content-Type": "application/json"}})
	if err := runner.Process(msg); err == nil {
		t.Fatalf("expected error for non-2.xx code")
	}
	_, data, err := msg.GetMetadataAndData()
	if err != nil {
		t.Fatalf("unexpected get data error: %v", err)
	}
	if string(data) != string(orig) {
		t.Fatalf("expected payload unchanged")
	}
}

func TestCoAPRunnerTimeout(t *testing.T) {
	path := "/test"
	addr := startTestCoAPServer(t, path, coapmux.HandlerFunc(func(w coapmux.ResponseWriter, m *coapmux.Message) {
		time.Sleep(300 * time.Millisecond)
		_ = w.SetResponse(coapcodes.Content, coapmessage.TextPlain, bytes.NewReader([]byte("late")))
	}))
	cfg := mustParseCoAPRunnerConfig(t, map[string]any{"protocol": "udp", "address": addr, "path": path, "method": "GET", "timeout": "100ms"})
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(coapRunnerErrCreate, err)
	}
	runner := r.(*CoAPRunner)
	msg := message.NewRunnerMessage(&stubSourceMessage{data: []byte("{}"), metadata: map[string]string{}})
	// Process already has its own timeout; just invoke
	if err := runner.Process(msg); err == nil {
		t.Fatalf("expected timeout error")
	}
}
