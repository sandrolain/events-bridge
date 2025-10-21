package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
	"github.com/sandrolain/events-bridge/src/utils"
)

const (
	httpRunnerErrCreate = "unexpected error creating runner: %v"
)

func mustParseRunnerConfig(t *testing.T, opts map[string]any) *HTTPRunnerConfig {
	t.Helper()
	cfg := new(HTTPRunnerConfig)
	if err := utils.ParseConfig(opts, cfg); err != nil {
		t.Fatalf("failed to parse runner config: %v", err)
	}
	return cfg
}

func TestHTTPRunnerSuccess(t *testing.T) {
	// Create test server returning JSON with headers
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always set echo headers (simplify deterministic test)
		w.Header().Set("X-Echo", "ok")
		w.Header().Set("X-Another", "value")
		w.WriteHeader(http.StatusCreated)
		if _, err := w.Write([]byte(`{"ok":true}`)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	cfg := mustParseRunnerConfig(t, map[string]any{
		"method":  "POST",
		"url":     ts.URL,
		"timeout": "1s",
	})

	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(httpRunnerErrCreate, err)
	}
	runner, ok := r.(*HTTPRunner)
	if !ok {
		t.Fatalf("expected *HTTPRunner got %T", r)
	}

	stub := testutil.NewAdapter([]byte(`{"in":1}`), map[string]string{"X-Test-Meta": "meta-val"})
	msg := message.NewRunnerMessage(stub)

	if err := runner.Process(msg); err != nil {
		t.Fatalf("unexpected error processing: %v", err)
	}

	meta, data, err := msg.GetMetadataAndData()
	if err != nil {
		t.Fatalf("unexpected error getting message data: %v", err)
	}

	if string(data) != `{"ok":true}` {
		t.Fatalf("expected response body to overwrite payload, got %s", string(data))
	}

	if meta["eb-status"] != fmt.Sprintf("%d", http.StatusCreated) {
		t.Fatalf("expected eb-status %d got %s", http.StatusCreated, meta["eb-status"])
	}
	if meta["x-echo"] != "ok" {
		t.Fatalf("expected x-echo header metadata, got %s", meta["x-echo"])
	}
	if meta["x-another"] != "value" {
		t.Fatalf("expected x-another header metadata, got %s", meta["x-another"])
	}
}

func TestHTTPRunnerNon2XX(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer ts.Close()

	cfg := mustParseRunnerConfig(t, map[string]any{
		"method":  "GET",
		"url":     ts.URL,
		"timeout": "1s",
	})
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(httpRunnerErrCreate, err)
	}
	runner, ok := r.(*HTTPRunner)
	if !ok {
		t.Fatal("failed to cast to HTTPRunner")
	}

	origPayload := []byte(`{"in":2}`)
	stub2 := testutil.NewAdapter(origPayload, map[string]string{"A": "b"})
	msg := message.NewRunnerMessage(stub2)
	if err := runner.Process(msg); err == nil {
		t.Fatalf("expected error for non-2xx response")
	}

	// Ensure payload unchanged
	_, data, err := msg.GetMetadataAndData()
	if err != nil {
		t.Fatalf("unexpected error getting message data: %v", err)
	}
	if string(data) != string(origPayload) {
		t.Fatalf("expected original payload unchanged, got %s", string(data))
	}
}

func TestHTTPRunnerTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cfg := mustParseRunnerConfig(t, map[string]any{
		"method":  "GET",
		"url":     ts.URL,
		"timeout": "100ms",
	})
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(httpRunnerErrCreate, err)
	}
	runner, ok := r.(*HTTPRunner)
	if !ok {
		t.Fatal("failed to cast to HTTPRunner")
	}

	stub3 := testutil.NewAdapter([]byte("{}"), map[string]string{"A": "b"})
	msg := message.NewRunnerMessage(stub3)
	if err := runner.Process(msg); err == nil {
		t.Fatalf("expected timeout error")
	}
}
