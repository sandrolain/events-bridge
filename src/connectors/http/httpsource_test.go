package main

import (
	"net"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/utils"
)

const (
	httpTestAddr           = "127.0.0.1:0"
	httpErrUnexpected      = "unexpected error: %v"
	httpErrUnexpectedClose = "unexpected error on close: %v"
	httpErrFailedDial      = "failed to dial: %v"
	httpErrConnClose       = "error closing connection: %v"
	httpErrWriteConn       = "failed to write to connection: %v"
)

func TestNewSource(t *testing.T) {
	cfg := mustParseSourceConfig(t, map[string]any{"address": httpTestAddr, "method": "POST", "path": "/test"})
	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(httpErrUnexpected, err)
	}
	if src == nil {
		t.Fatal("expected non-nil source")
	}
}

func TestHTTPSourceProduceAndClose(t *testing.T) {
	httpSrc := mustNewHTTPSource(t, map[string]any{"address": httpTestAddr, "method": "POST", "path": "/test"})
	ch, err := httpSrc.Produce(1)
	if err != nil {
		t.Fatalf(httpErrUnexpected, err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
	time.Sleep(10 * time.Millisecond)
	if err := httpSrc.Close(); err != nil {
		t.Fatalf(httpErrUnexpectedClose, err)
	}
}

func TestHTTPSourceProduceErrorOnListen(t *testing.T) {
	// Use an invalid address to force net.Listen to fail
	httpSrc := mustNewHTTPSource(t, map[string]any{"address": "invalid:address", "method": "POST", "path": "/test"})
	if _, err := httpSrc.Produce(1); err == nil {
		t.Fatal("expected error from Produce with invalid address")
	}
}

func TestHTTPSourceProduceMethodNotAllowed(t *testing.T) {
	httpSrc := mustNewHTTPSource(t, map[string]any{"address": httpTestAddr, "method": "POST", "path": "/test"})
	ch, err := httpSrc.Produce(1)
	if err != nil {
		t.Fatalf(httpErrUnexpected, err)
	}
	conn, err := net.Dial("tcp", httpSrc.listener.Addr().String())
	if err != nil {
		t.Fatalf(httpErrFailedDial, err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf(httpErrConnClose, err)
		}
	}()
	if _, err = conn.Write([]byte("GET /test HTTP/1.1\r\nHost: localhost\r\n\r\n")); err != nil {
		t.Fatalf(httpErrWriteConn, err)
	}
	time.Sleep(10 * time.Millisecond)
	_ = ch
	if err := httpSrc.Close(); err != nil {
		t.Fatalf(httpErrUnexpectedClose, err)
	}
}

func TestHTTPSourceProducePathNotFound(t *testing.T) {
	httpSrc := mustNewHTTPSource(t, map[string]any{"address": httpTestAddr, "method": "POST", "path": "/test"})
	ch, err := httpSrc.Produce(1)
	if err != nil {
		t.Fatalf(httpErrUnexpected, err)
	}
	conn, err := net.Dial("tcp", httpSrc.listener.Addr().String())
	if err != nil {
		t.Fatalf(httpErrFailedDial, err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf(httpErrConnClose, err)
		}
	}()
	if _, err = conn.Write([]byte("POST /wrong HTTP/1.1\r\nHost: localhost\r\n\r\n")); err != nil {
		t.Fatalf(httpErrWriteConn, err)
	}
	time.Sleep(10 * time.Millisecond)
	_ = ch
	if err := httpSrc.Close(); err != nil {
		t.Fatalf(httpErrUnexpectedClose, err)
	}
}

func TestHTTPSourceProduceTimeout(t *testing.T) {
	httpSrc := mustNewHTTPSource(t, map[string]any{"address": httpTestAddr, "method": "POST", "path": "/test", "timeout": "200ms"})
	ch, err := httpSrc.Produce(1)
	if err != nil {
		t.Fatalf(httpErrUnexpected, err)
	}
	conn, err := net.Dial("tcp", httpSrc.listener.Addr().String())
	if err != nil {
		t.Fatalf(httpErrFailedDial, err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf(httpErrConnClose, err)
		}
	}()
	if _, err = conn.Write([]byte("POST /test HTTP/1.1\r\nHost: localhost\r\n\r\n")); err != nil {
		t.Fatalf(httpErrWriteConn, err)
	}
	time.Sleep(500 * time.Millisecond)
	_ = ch
	if err := httpSrc.Close(); err != nil {
		t.Fatalf(httpErrUnexpectedClose, err)
	}
}

func mustParseSourceConfig(t *testing.T, opts map[string]any) *SourceConfig {
	t.Helper()
	cfg := new(SourceConfig)
	if err := utils.ParseConfig(opts, cfg); err != nil {
		t.Fatalf("failed to parse source config: %v", err)
	}
	return cfg
}

func mustNewHTTPSource(t *testing.T, opts map[string]any) *HTTPSource {
	cfg := mustParseSourceConfig(t, opts)
	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	httpSrc, ok := src.(*HTTPSource)
	if !ok {
		t.Fatalf("expected *HTTPSource type, got %T", src)
	}
	return httpSrc
}
