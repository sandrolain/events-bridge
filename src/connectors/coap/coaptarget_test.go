package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
	coapmux "github.com/plgd-dev/go-coap/v3/mux"
	coapnet "github.com/plgd-dev/go-coap/v3/net"
	coapoptions "github.com/plgd-dev/go-coap/v3/options"
	coaptcp "github.com/plgd-dev/go-coap/v3/tcp"
	coapudp "github.com/plgd-dev/go-coap/v3/udp"

	"github.com/sandrolain/events-bridge/src/message"
)

type mockMessage struct {
	data     []byte
	ack, nak bool
}

func (m *mockMessage) GetID() []byte                                 { return []byte("mock-id") }
func (m *mockMessage) GetMetadata() (message.MessageMetadata, error) { return nil, nil }
func (m *mockMessage) GetData() ([]byte, error)                      { return m.data, nil }
func (m *mockMessage) Ack() error                                    { m.ack = true; return nil }
func (m *mockMessage) Nak() error                                    { m.nak = true; return nil }
func (m *mockMessage) Reply(data *message.ReplyData) error           { return nil }

// Dummy client to avoid real network calls
// You can expand this with a build tag for integration tests
const addrLocal = "localhost:5683"

func TestSendUnsupportedProtocol(t *testing.T) {
	_, err := NewTarget(map[string]any{
		"protocol": "invalid",
		"address":  addrLocal,
		"path":     "/test",
		"method":   "POST",
	})
	if err == nil || !strings.Contains(err.Error(), "option protocol") {
		t.Fatalf("expected validation error for protocol, got: %v", err)
	}
}

func TestSendUnsupportedMethod(t *testing.T) {
	_, err := NewTarget(map[string]any{
		"protocol": "udp",
		"address":  addrLocal,
		"path":     "/test",
		"method":   "DELETE",
	})
	if err == nil || !strings.Contains(err.Error(), "option method") {
		t.Fatalf("expected validation error for method, got: %v", err)
	}
}

func TestSendErrorGettingData(t *testing.T) {
	target := mustNewTarget(t, map[string]any{
		"protocol": "udp",
		"address":  addrLocal,
		"path":     "/test",
		"method":   "POST",
		"timeout":  "1s",
	})
	msg := &mockMessageWithError{}
	err := target.Consume(message.NewRunnerMessage(msg))
	if err == nil || !strings.Contains(err.Error(), "error getting data") {
		t.Errorf("expected error getting data, got: %v", err)
	}
}

const errUnexpected = "unexpected error: %v"

func mustNewTarget(t *testing.T, opts map[string]any) *CoAPTarget {
	t.Helper()
	tgt, err := NewTarget(opts)
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}
	coapTarget, ok := tgt.(*CoAPTarget)
	if !ok {
		t.Fatalf("expected *CoAPTarget, got %T", tgt)
	}
	return coapTarget
}

func TestSendSuccessUnsupportedNetworkUDP(t *testing.T) {
	target := mustNewTarget(t, map[string]any{
		"protocol": "udp",
		"address":  "127.0.0.1:9999",
		"path":     "/test",
		"method":   "POST",
		"timeout":  "1s",
	})
	msg := &mockMessage{data: []byte("test-data")}
	err := target.Consume(message.NewRunnerMessage(msg))
	if err == nil {
		t.Error("expected error dialing coap server, got nil")
	}
}

func TestSendSuccessUnsupportedNetworkTCP(t *testing.T) {
	target := mustNewTarget(t, map[string]any{
		"protocol": "tcp",
		"address":  "127.0.0.1:9999",
		"path":     "/test",
		"method":   "POST",
		"timeout":  "1s",
	})
	msg := &mockMessage{data: []byte("test-data")}
	err := target.Consume(message.NewRunnerMessage(msg))
	if err == nil {
		t.Error("expected error dialing coap server, got nil")
	}
}

// NOTE: To test a real "success" send, a CoAP server must be listening.
// Here we only simulate that the function is called and handles network errors.
// For real integration tests, use a test CoAP server.

type mockMessageWithError struct{}

func (m *mockMessageWithError) GetID() []byte                                 { return []byte("mock-id-error") }
func (m *mockMessageWithError) GetMetadata() (message.MessageMetadata, error) { return nil, nil }
func (m *mockMessageWithError) GetData() ([]byte, error)                      { return nil, errors.New("fail") }
func (m *mockMessageWithError) Ack() error                                    { return nil }
func (m *mockMessageWithError) Nak() error                                    { return nil }
func (m *mockMessageWithError) Reply(data *message.ReplyData) error           { return nil }

// --- Moved integration tests from coaptarget_integration_test.go ---

type dummyMessage struct {
	data         []byte
	acked, naked bool
}

func (m *dummyMessage) GetID() []byte                                 { return []byte("dummy-id") }
func (m *dummyMessage) GetMetadata() (message.MessageMetadata, error) { return nil, nil }
func (m *dummyMessage) GetData() ([]byte, error)                      { return m.data, nil }
func (m *dummyMessage) Ack() error                                    { m.acked = true; return nil }
func (m *dummyMessage) Nak() error                                    { m.naked = true; return nil }
func (m *dummyMessage) Reply(data *message.ReplyData) error           { return nil }

// UDP test server that uses mux logic like coapsource
func startUDPServer(t *testing.T, addr string, onMsg func()) {
	l, err := coapnet.NewListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("failed to listen udp: %v", err)
	}
	router := coapmux.NewRouter()
	err = router.Handle("/test", coapmux.HandlerFunc(func(w coapmux.ResponseWriter, r *coapmux.Message) {
		onMsg()
		err := w.SetResponse(coapcodes.Changed, coapmessage.TextPlain, nil)
		if err != nil {
			t.Fatalf("failed to set response: %v", err)
		}
	}))
	if err != nil {
		t.Fatalf("failed to handle /test: %v", err)
	}
	s := coapudp.NewServer(coapoptions.WithMux(router))
	go func() {
		_ = s.Serve(l)
	}()
	t.Cleanup(func() { s.Stop() })
	time.Sleep(100 * time.Millisecond)
}

// TCP test server that uses mux logic like coapsource
func startTCPServer(t *testing.T, addr string, onMsg func()) {
	ln, err := coapnet.NewTCPListener("tcp", addr)
	if err != nil {
		t.Fatalf("failed to listen tcp: %v", err)
	}
	router := coapmux.NewRouter()
	err = router.Handle("/test", coapmux.HandlerFunc(func(w coapmux.ResponseWriter, r *coapmux.Message) {
		onMsg()
		err := w.SetResponse(coapcodes.Changed, coapmessage.TextPlain, nil)
		if err != nil {
			t.Fatalf("failed to set response: %v", err)
		}
	}))
	if err != nil {
		t.Fatalf("failed to handle /test: %v", err)
	}
	s := coaptcp.NewServer(coapoptions.WithMux(router))
	go func() {
		_ = s.Serve(ln)
	}()
	t.Cleanup(func() { s.Stop() })
	time.Sleep(100 * time.Millisecond)
}

// Exported only for tests: sendTest is a test helper to invoke the unexported send method
func sendTest(tgt *CoAPTarget, msg message.SourceMessage) error {
	return tgt.Consume(message.NewRunnerMessage(msg))
}

func TestIntegrationSendUDP(t *testing.T) {
	addr := "127.0.0.1:56831"
	receivedCh := make(chan struct{}, 1)
	startUDPServer(t, addr, func() {
		select {
		case receivedCh <- struct{}{}:
		default:
		}
	})
	target := mustNewTarget(t, map[string]any{
		"protocol": "udp",
		"address":  addr,
		"path":     "/test",
		"method":   "POST",
		"timeout":  "2s",
	})
	msg := &dummyMessage{data: []byte("hello udp")}
	err := sendTest(target, msg)
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	select {
	case <-receivedCh:
		// ok
	case <-time.After(2 * time.Second):
		t.Error("UDP server did not receive message")
	}
}

func TestIntegrationSendTCP(t *testing.T) {
	addr := "127.0.0.1:56832"
	receivedCh := make(chan struct{}, 1)
	startTCPServer(t, addr, func() {
		select {
		case receivedCh <- struct{}{}:
		default:
		}
	})
	target := mustNewTarget(t, map[string]any{
		"protocol": "tcp",
		"address":  addr,
		"path":     "/test",
		"method":   "POST",
		"timeout":  "2s",
	})
	msg := &dummyMessage{data: []byte("hello tcp")}
	err := sendTest(target, msg)
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	select {
	case <-receivedCh:
		// ok
	case <-time.After(2 * time.Second):
		t.Error("TCP server did not receive message")
	}
}
