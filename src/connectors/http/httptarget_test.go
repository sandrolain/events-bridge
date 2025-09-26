package main

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/valyala/fasthttp"
)

// metaErrorMock returns an error from GetMetadata
type metaErrorMock struct{ mockMessage }

func (m *metaErrorMock) GetMetadata() (message.MessageMetadata, error) {
	return nil, errors.New("fail meta")
}

// dataErrorMock returns an error from GetData
type dataErrorMock struct{ mockMessage }

func (m *dataErrorMock) GetData() ([]byte, error) {
	return nil, errors.New("fail data")
}
func (m *dataErrorMock) GetMetadata() (message.MessageMetadata, error) {
	return message.MessageMetadata{}, nil
}

// mockMessage implements message.Message for testing
type mockMessage struct {
	metadata message.MessageMetadata
	data     []byte
	ack      bool
	nak      bool
}

func (m *mockMessage) GetID() []byte                                 { return []byte("id") }
func (m *mockMessage) GetMetadata() (message.MessageMetadata, error) { return m.metadata, nil }
func (m *mockMessage) GetData() ([]byte, error)                      { return m.data, nil }
func (m *mockMessage) Ack() error                                    { m.ack = true; return nil }
func (m *mockMessage) Nak() error                                    { m.nak = true; return nil }
func (m *mockMessage) Reply(data *message.ReplyData) error           { return nil }

func TestNewTargetDefaultTimeout(t *testing.T) {
	const errMsg = "unexpected error: %v"
	tgt, err := NewTarget(map[string]any{"timeout": 0})
	if err != nil {
		t.Fatalf(errMsg, err)
	}
	if tgt == nil {
		t.Fatal("expected non-nil target")
	}
}

func TestHTTPTargetConsumeAndClose(t *testing.T) {
	const errMsg = "unexpected error: %v"

	// Start a local fasthttp server on an ephemeral port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf(errMsg, err)
	}
	done := make(chan struct{})
	go func() {
		_ = fasthttp.Serve(ln, func(ctx *fasthttp.RequestCtx) {
			if string(ctx.Method()) != "POST" || string(ctx.Path()) != "/test" {
				ctx.SetStatusCode(405)
				return
			}
			ctx.SetStatusCode(200)
		})
		close(done)
	}()

	url := "http://" + ln.Addr().String() + "/test"
	tgt, err := NewTarget(map[string]any{"url": url, "method": "POST", "headers": map[string]string{}, "timeout": int64(250 * time.Millisecond)})
	if err != nil {
		t.Fatalf(errMsg, err)
	}
	httpTgt, ok := tgt.(*HTTPTarget)
	if !ok {
		t.Fatal("expected *HTTPTarget type")
	}
	m := &mockMessage{metadata: message.MessageMetadata{"X-Test": "v"}, data: []byte("payload")}
	msg := message.NewRunnerMessage(m)
	if err = httpTgt.Consume(msg); err != nil {
		t.Fatalf(errMsg, err)
	}
	// Close listener and target
	_ = ln.Close()
	<-done
	if err := httpTgt.Close(); err != nil {
		t.Fatalf("unexpected error on close: %v", err)
	}
}

func TestHTTPTargetSendErrorMetadata(t *testing.T) {
	httpTgt := &HTTPTarget{cfg: &TargetConfig{URL: "http://localhost", Method: "POST", Headers: map[string]string{}}, client: nil}
	msg := &metaErrorMock{}
	m := message.NewRunnerMessage(msg)
	err := httpTgt.Consume(m)
	if err == nil || err.Error() != "error getting metadata: fail meta" {
		t.Fatalf("expected metadata error, got: %v", err)
	}
}

func TestHTTPTargetSendErrorData(t *testing.T) {
	httpTgt := &HTTPTarget{cfg: &TargetConfig{URL: "http://localhost", Method: "POST", Headers: map[string]string{}}, client: nil}
	msg := &dataErrorMock{}
	m := message.NewRunnerMessage(msg)
	err := httpTgt.Consume(m)
	if err == nil || err.Error() != "error getting data: fail data" {
		t.Fatalf("expected data error, got: %v", err)
	}
}
