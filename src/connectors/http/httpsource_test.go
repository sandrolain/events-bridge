package main

import (
	"net"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/sources"
)

type dummyLogger struct{}

// Info is a no-op for dummyLogger (used for testing only)
func (d *dummyLogger) Info(msg string, args ...any) {}

// Debug is a no-op for dummyLogger (used for testing only)
func (d *dummyLogger) Debug(msg string, args ...any) {}

func TestNewSource(t *testing.T) {
	cfg := &sources.SourceHTTPConfig{Address: "127.0.0.1:0", Method: "POST", Path: "/test"}
	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src == nil {
		t.Fatal("expected non-nil source")
	}
}

func TestHTTPSourceProduceAndClose(t *testing.T) {
	cfg := &sources.SourceHTTPConfig{Address: "127.0.0.1:0", Method: "POST", Path: "/test"}
	src, _ := NewSource(cfg)
	httpSrc, ok := src.(*HTTPSource)
	if !ok {
		t.Fatal("expected *HTTPSource type")
	}
	ch, err := httpSrc.Produce(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
	time.Sleep(10 * time.Millisecond)
	err = httpSrc.Close()
	if err != nil {
		t.Fatalf("unexpected error on close: %v", err)
	}
}

func TestHTTPSourceProduce_ErrorOnListen(t *testing.T) {
	// Use an invalid address to force net.Listen to fail
	cfg := &sources.SourceHTTPConfig{Address: "invalid:address", Method: "POST", Path: "/test"}
	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	httpSrc, ok := src.(*HTTPSource)
	if !ok {
		t.Fatal("expected *HTTPSource type")
	}
	_, err = httpSrc.Produce(1)
	if err == nil {
		t.Fatal("expected error from Produce with invalid address")
	}
}

func TestHTTPSourceProduceMethodNotAllowed(t *testing.T) {
	const addr = "127.0.0.1:0"
	const errMsg = "unexpected error: %v"
	const dialMsg = "failed to dial: %v"
	cfg := &sources.SourceHTTPConfig{Address: addr, Method: "POST", Path: "/test"}
	src, _ := NewSource(cfg)
	httpSrc := src.(*HTTPSource)
	ch, err := httpSrc.Produce(1)
	if err != nil {
		t.Fatalf(errMsg, err)
	}
	// Simulate a request with wrong method
	conn, err := net.Dial("tcp", httpSrc.listener.Addr().String())
	if err != nil {
		t.Fatalf(dialMsg, err)
	}
	defer conn.Close()
	conn.Write([]byte("GET /test HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	time.Sleep(10 * time.Millisecond)
	_ = ch // just to keep channel in scope
	httpSrc.Close()
}

func TestHTTPSourceProducePathNotFound(t *testing.T) {
	const addr = "127.0.0.1:0"
	const errMsg = "unexpected error: %v"
	const dialMsg = "failed to dial: %v"
	cfg := &sources.SourceHTTPConfig{Address: addr, Method: "POST", Path: "/test"}
	src, _ := NewSource(cfg)
	httpSrc := src.(*HTTPSource)
	ch, err := httpSrc.Produce(1)
	if err != nil {
		t.Fatalf(errMsg, err)
	}
	// Simulate a request with wrong path
	conn, err := net.Dial("tcp", httpSrc.listener.Addr().String())
	if err != nil {
		t.Fatalf(dialMsg, err)
	}
	defer conn.Close()
	conn.Write([]byte("POST /wrong HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	time.Sleep(10 * time.Millisecond)
	_ = ch
	httpSrc.Close()
}

func TestHTTPSourceProduceTimeout(t *testing.T) {
	const addr = "127.0.0.1:0"
	const errMsg = "unexpected error: %v"
	const dialMsg = "failed to dial: %v"
	cfg := &sources.SourceHTTPConfig{Address: addr, Method: "POST", Path: "/test"}
	src, _ := NewSource(cfg)
	httpSrc := src.(*HTTPSource)
	ch, err := httpSrc.Produce(1)
	if err != nil {
		t.Fatalf(errMsg, err)
	}
	// Simulate a request with correct method and path, but do not Ack/Nak
	conn, err := net.Dial("tcp", httpSrc.listener.Addr().String())
	if err != nil {
		t.Fatalf(dialMsg, err)
	}
	defer conn.Close()
	conn.Write([]byte("POST /test HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	time.Sleep(8 * time.Second) // let the timeout branch trigger
	_ = ch
	httpSrc.Close()
}
