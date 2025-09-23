package main_test

import (
	"testing"
	"time"

	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
	coapmux "github.com/plgd-dev/go-coap/v3/mux"
	coapnet "github.com/plgd-dev/go-coap/v3/net"
	coapoptions "github.com/plgd-dev/go-coap/v3/options"
	coaptcp "github.com/plgd-dev/go-coap/v3/tcp"
	coapudp "github.com/plgd-dev/go-coap/v3/udp"

	coaptarget "github.com/sandrolain/events-bridge/src/connectors/coap"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
)

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
func sendTest(tgt *coaptarget.CoAPTarget, msg message.SourceMessage) error {
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
	cfg := &targets.TargetCoAPConfig{
		Protocol: "udp",
		Address:  addr,
		Path:     "/test",
		Method:   "POST",
	}
	target, err := coaptarget.NewTarget(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msg := &dummyMessage{data: []byte("hello udp")}
	err = sendTest(target.(*coaptarget.CoAPTarget), msg)
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
	cfg := &targets.TargetCoAPConfig{
		Protocol: "tcp",
		Address:  addr,
		Path:     "/test",
		Method:   "POST",
	}
	target, err := coaptarget.NewTarget(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msg := &dummyMessage{data: []byte("hello tcp")}
	err = sendTest(target.(*coaptarget.CoAPTarget), msg)
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
