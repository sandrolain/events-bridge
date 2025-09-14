package main

import (
	"errors"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
)

// metaErrorMock returns an error from GetMetadata
type metaErrorMock struct{ mockMessage }

func (m *metaErrorMock) GetMetadata() (map[string][]string, error) {
	return nil, errors.New("fail meta")
}

// dataErrorMock returns an error from GetData
type dataErrorMock struct{ mockMessage }

func (m *dataErrorMock) GetData() ([]byte, error) {
	return nil, errors.New("fail data")
}
func (m *dataErrorMock) GetMetadata() (map[string][]string, error) {
	return map[string][]string{}, nil
}

// mockMessage implements message.Message for testing
type mockMessage struct {
	metadata map[string][]string
	data     []byte
	ack      bool
	nak      bool
}

func (m *mockMessage) GetID() []byte                             { return []byte("id") }
func (m *mockMessage) GetMetadata() (map[string][]string, error) { return m.metadata, nil }
func (m *mockMessage) GetData() ([]byte, error)                  { return m.data, nil }
func (m *mockMessage) Ack() error                                { m.ack = true; return nil }
func (m *mockMessage) Nak() error                                { m.nak = true; return nil }

func TestNewTargetDefaultTimeout(t *testing.T) {
	const errMsg = "unexpected error: %v"
	cfg := &targets.TargetHTTPConfig{Timeout: 0}
	tgt, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errMsg, err)
	}
	if tgt == nil {
		t.Fatal("expected non-nil target")
	}
}

func TestHTTPTargetConsumeAndClose(t *testing.T) {
	const errMsg = "unexpected error: %v"
	cfg := &targets.TargetHTTPConfig{URL: "http://localhost:12345", Method: "POST", Headers: map[string]string{}, Timeout: time.Millisecond * 100}
	tgt, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errMsg, err)
	}
	httpTgt, ok := tgt.(*HTTPTarget)
	if !ok {
		t.Fatal("expected *HTTPTarget type")
	}
	ch := make(chan message.Message, 1)
	msg := &mockMessage{metadata: map[string][]string{"X-Test": {"v"}}, data: []byte("payload")}
	ch <- msg
	close(ch)
	err = httpTgt.Consume(ch)
	if err != nil {
		t.Fatalf(errMsg, err)
	}
	if err := httpTgt.Close(); err != nil {
		t.Fatalf("unexpected error on close: %v", err)
	}
}

func TestHTTPTargetSendErrorMetadata(t *testing.T) {
	httpTgt := &HTTPTarget{config: &targets.TargetHTTPConfig{URL: "http://localhost", Method: "POST", Headers: map[string]string{}}, client: nil}
	msg := &metaErrorMock{}
	err := httpTgt.send(msg)
	if err == nil || err.Error() != "error getting metadata: fail meta" {
		t.Fatalf("expected metadata error, got: %v", err)
	}
}

func TestHTTPTargetSendErrorData(t *testing.T) {
	httpTgt := &HTTPTarget{config: &targets.TargetHTTPConfig{URL: "http://localhost", Method: "POST", Headers: map[string]string{}}, client: nil}
	msg := &dataErrorMock{}
	err := httpTgt.send(msg)
	if err == nil || err.Error() != "error getting data: fail data" {
		t.Fatalf("expected data error, got: %v", err)
	}
}
