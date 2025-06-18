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

func (m *dummyMessage) GetID() []byte                             { return []byte("dummy-id") }
func (m *dummyMessage) GetMetadata() (map[string][]string, error) { return nil, nil }
func (m *dummyMessage) GetData() ([]byte, error)                  { return m.data, nil }
func (m *dummyMessage) Ack() error                                { m.acked = true; return nil }
func (m *dummyMessage) Nak() error                                { m.naked = true; return nil }

// Server UDP di test che usa la logica mux come coapsource
func startUDPServer(t *testing.T, addr string, onMsg func()) {
	l, err := coapnet.NewListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("failed to listen udp: %v", err)
	}
	router := coapmux.NewRouter()
	router.Handle("/test", coapmux.HandlerFunc(func(w coapmux.ResponseWriter, r *coapmux.Message) {
		onMsg()
		w.SetResponse(coapcodes.Changed, coapmessage.TextPlain, nil)
	}))
	s := coapudp.NewServer(coapoptions.WithMux(router))
	go func() {
		_ = s.Serve(l)
	}()
	t.Cleanup(func() { s.Stop() })
	time.Sleep(100 * time.Millisecond)
}

// Server TCP di test che usa la logica mux come coapsource
func startTCPServer(t *testing.T, addr string, onMsg func()) {
	ln, err := coapnet.NewTCPListener("tcp", addr)
	if err != nil {
		t.Fatalf("failed to listen tcp: %v", err)
	}
	router := coapmux.NewRouter()
	router.Handle("/test", coapmux.HandlerFunc(func(w coapmux.ResponseWriter, r *coapmux.Message) {
		onMsg()
		w.SetResponse(coapcodes.Changed, coapmessage.TextPlain, nil)
	}))
	s := coaptcp.NewServer(coapoptions.WithMux(router))
	go func() {
		_ = s.Serve(ln)
	}()
	t.Cleanup(func() { s.Stop() })
	time.Sleep(100 * time.Millisecond)
}

// Exported only for test: SendTest è una funzione di test per invocare il metodo non esportato send
func SendTest(tgt *coaptarget.CoAPTarget, msg message.Message) error {
	return tgt.Send(msg)
}

func TestIntegration_SendUDP(t *testing.T) {
	addr := "127.0.0.1:56831"
	var received bool
	startUDPServer(t, addr, func() { received = true })
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
	err = SendTest(target.(*coaptarget.CoAPTarget), msg)
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	if !received {
		t.Error("UDP server did not receive message")
	}
}

func TestIntegration_SendTCP(t *testing.T) {
	addr := "127.0.0.1:56832"
	var received bool
	startTCPServer(t, addr, func() { received = true })
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
	err = SendTest(target.(*coaptarget.CoAPTarget), msg)
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	if !received {
		t.Error("TCP server did not receive message")
	}
}
