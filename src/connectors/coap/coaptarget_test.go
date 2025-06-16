package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/sandrolain/events-bridge/src/targets"
)

type mockMessage struct {
	data     []byte
	ack, nak bool
}

func (m *mockMessage) GetMetadata() (map[string][]string, error) { return nil, nil }
func (m *mockMessage) GetData() ([]byte, error)                  { return m.data, nil }
func (m *mockMessage) Ack() error                                { m.ack = true; return nil }
func (m *mockMessage) Nak() error                                { m.nak = true; return nil }

// Dummy client to avoid real network calls
// You can expand this with a build tag for integration tests
func TestSend_UnsupportedProtocol(t *testing.T) {
	cfg := &targets.TargetCoAPConfig{
		Protocol: "invalid",
		Address:  "localhost:5683",
		Path:     "/test",
		Method:   "POST",
	}
	target, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msg := &mockMessage{data: []byte("test")}
	err = target.(*CoAPTarget).send(msg)
	if err == nil || !strings.Contains(err.Error(), "unsupported coap protocol") {
		t.Errorf("expected unsupported protocol error, got: %v", err)
	}
}

func TestSend_UnsupportedMethod(t *testing.T) {
	cfg := &targets.TargetCoAPConfig{
		Protocol: "udp",
		Address:  "localhost:5683",
		Path:     "/test",
		Method:   "DELETE",
	}
	target, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msg := &mockMessage{data: []byte("test")}

	// Patch coapudp.Dial to return a dummy client that implements Close
	// and methods returning error for unsupported method
	// This is a placeholder for a more advanced mocking framework
	// For now, just check the error from send
	err = target.(*CoAPTarget).send(msg)
	if err == nil || !strings.Contains(err.Error(), "unsupported coap method") {
		t.Errorf("expected unsupported method error, got: %v", err)
	}
}

func TestSend_ErrorGettingData(t *testing.T) {
	cfg := &targets.TargetCoAPConfig{
		Protocol: "udp",
		Address:  "localhost:5683",
		Path:     "/test",
		Method:   "POST",
	}
	target, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msg := &mockMessageWithError{}
	err = target.(*CoAPTarget).send(msg)
	if err == nil || !strings.Contains(err.Error(), "error getting data") {
		t.Errorf("expected error getting data, got: %v", err)
	}
}

const errUnexpected = "unexpected error: %v"

func TestSendSuccessUnsupportedNetworkUDP(t *testing.T) {
	cfg := &targets.TargetCoAPConfig{
		Protocol: "udp",
		Address:  "127.0.0.1:9999", // porta non usata, nessun server
		Path:     "/test",
		Method:   "POST",
	}
	target, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}
	msg := &mockMessage{data: []byte("test-data")}
	err = target.(*CoAPTarget).send(msg)
	if err == nil {
		t.Error("expected error dialing coap server, got nil")
	}
}

func TestSendSuccessUnsupportedNetworkTCP(t *testing.T) {
	cfg := &targets.TargetCoAPConfig{
		Protocol: "tcp",
		Address:  "127.0.0.1:9999", // porta non usata, nessun server
		Path:     "/test",
		Method:   "POST",
	}
	target, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}
	msg := &mockMessage{data: []byte("test-data")}
	err = target.(*CoAPTarget).send(msg)
	if err == nil {
		t.Error("expected error dialing coap server, got nil")
	}
}

// NOTA: Per testare un invio "successo" reale serve un server CoAP in ascolto.
// Qui simuliamo solo che la funzione venga chiamata e gestisca errori di rete.
// Per test di integrazione reali, usare un server CoAP di test.

type mockMessageWithError struct{}

func (m *mockMessageWithError) GetMetadata() (map[string][]string, error) { return nil, nil }
func (m *mockMessageWithError) GetData() ([]byte, error)                  { return nil, errors.New("fail") }
func (m *mockMessageWithError) Ack() error                                { return nil }
func (m *mockMessageWithError) Nak() error                                { return nil }
