package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

// newRunnerMsg is a helper that builds a RunnerMessage from raw JSON and metadata.
func newRunnerMsg(data []byte, meta map[string]string, id string) *message.RunnerMessage {
	stub := testutil.NewAdapter(data, meta)
	stub.ID = []byte(id)
	return message.NewRunnerMessage(stub)
}

// --------------------------------------------------------------------------
// NewRunnerConfig
// --------------------------------------------------------------------------

func TestNewRunnerConfig(t *testing.T) {
	cfg := NewRunnerConfig()
	if cfg == nil {
		t.Fatal("NewRunnerConfig returned nil")
	}
	if _, ok := cfg.(*RunnerConfig); !ok {
		t.Fatalf("expected *RunnerConfig, got %T", cfg)
	}
}

// --------------------------------------------------------------------------
// NewRunner — creation & validation
// --------------------------------------------------------------------------

func TestNewRunnerInlineExpression(t *testing.T) {
	cfg := &RunnerConfig{Expression: "$.name", Timeout: 5 * time.Second}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r == nil {
		t.Fatal("runner is nil")
	}
	_ = r.Close()
}

func TestNewRunnerFromFile(t *testing.T) {
	dir := t.TempDir()
	exprFile := filepath.Join(dir, "transform.jsonata")
	if err := os.WriteFile(exprFile, []byte("$.value * 2"), 0o600); err != nil {
		t.Fatalf("failed to write expression file: %v", err)
	}

	cfg := &RunnerConfig{Path: exprFile, Timeout: 5 * time.Second}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r == nil {
		t.Fatal("runner is nil")
	}
	_ = r.Close()
}

func TestNewRunnerFileNotFound(t *testing.T) {
	cfg := &RunnerConfig{Path: "/nonexistent/transform.jsonata", Timeout: 5 * time.Second}
	_, err := NewRunner(cfg)
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestNewRunnerInvalidExpression(t *testing.T) {
	cfg := &RunnerConfig{Expression: "$$$$invalid{{{{", Timeout: 5 * time.Second}
	_, err := NewRunner(cfg)
	if err == nil {
		t.Fatal("expected compile error for invalid expression")
	}
}

func TestNewRunnerExpressionSizeExceeded(t *testing.T) {
	longExpr := strings.Repeat("a", 10)
	cfg := &RunnerConfig{Expression: longExpr, Timeout: 5 * time.Second, MaxExprSize: 5}
	_, err := NewRunner(cfg)
	if err == nil {
		t.Fatal("expected error when expression exceeds maxExprSize")
	}
}

func TestNewRunnerWrongConfigType(t *testing.T) {
	_, err := NewRunner("not-a-config")
	if err == nil {
		t.Fatal("expected error for wrong config type")
	}
}

// --------------------------------------------------------------------------
// Process — basic transformations
// --------------------------------------------------------------------------

func TestProcessFieldExtraction(t *testing.T) {
	cfg := &RunnerConfig{Expression: "$.name", Timeout: 5 * time.Second}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newRunnerMsg([]byte(`{"name":"Alice","age":30}`), map[string]string{}, "id-1")
	if err := r.Process(msg); err != nil {
		t.Fatalf("Process error: %v", err)
	}

	data, _ := msg.GetData()
	var result string
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result != "Alice" {
		t.Fatalf("expected 'Alice', got %q", result)
	}
}

func TestProcessObjectConstruction(t *testing.T) {
	cfg := &RunnerConfig{
		Expression: `{"id": $.orderId, "total": $sum($.items.price)}`,
		Timeout:    5 * time.Second,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	payload := `{"orderId":"ORD-1","items":[{"price":10},{"price":20},{"price":30}]}`
	msg := newRunnerMsg([]byte(payload), map[string]string{}, "id-2")
	if err := r.Process(msg); err != nil {
		t.Fatalf("Process error: %v", err)
	}

	data, _ := msg.GetData()
	var res map[string]interface{}
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if res["id"] != "ORD-1" {
		t.Fatalf("expected id 'ORD-1', got %v", res["id"])
	}
	if res["total"] != float64(60) {
		t.Fatalf("expected total 60, got %v", res["total"])
	}
}

func TestProcessArrayFilter(t *testing.T) {
	cfg := &RunnerConfig{
		Expression: `$.items[price > 15]`,
		Timeout:    5 * time.Second,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	payload := `{"items":[{"name":"a","price":10},{"name":"b","price":20},{"name":"c","price":30}]}`
	msg := newRunnerMsg([]byte(payload), map[string]string{}, "id-3")
	if err := r.Process(msg); err != nil {
		t.Fatalf("Process error: %v", err)
	}

	data, _ := msg.GetData()
	var res []interface{}
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 items with price > 15, got %d", len(res))
	}
}

func TestProcessWithMetadataBinding(t *testing.T) {
	cfg := &RunnerConfig{
		Expression:     `{"name": $.name, "source": $metadata.source}`,
		ExposeMetadata: true,
		Timeout:        5 * time.Second,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newRunnerMsg([]byte(`{"name":"Bob"}`), map[string]string{"source": "kafka-topic-1"}, "id-4")
	if err := r.Process(msg); err != nil {
		t.Fatalf("Process error: %v", err)
	}

	data, _ := msg.GetData()
	var res map[string]interface{}
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if res["source"] != "kafka-topic-1" {
		t.Fatalf("expected source 'kafka-topic-1', got %v", res["source"])
	}
}

func TestProcessMetadataNotExposed(t *testing.T) {
	// When ExposeMetadata is false, $metadata should be undefined / empty.
	cfg := &RunnerConfig{
		Expression:     `$exists($metadata)`,
		ExposeMetadata: false,
		Timeout:        5 * time.Second,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newRunnerMsg([]byte(`{}`), map[string]string{"x": "y"}, "id-5")
	if err := r.Process(msg); err != nil {
		t.Fatalf("Process error: %v", err)
	}

	data, _ := msg.GetData()
	var res bool
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if res {
		t.Fatal("expected $metadata to be undefined when ExposeMetadata=false")
	}
}

func TestProcessPreservePayload(t *testing.T) {
	cfg := &RunnerConfig{
		Expression:      `$.value * 2`,
		PreservePayload: true,
		Timeout:         5 * time.Second,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newRunnerMsg([]byte(`{"value":21}`), map[string]string{}, "id-6")
	if err := r.Process(msg); err != nil {
		t.Fatalf("Process error: %v", err)
	}

	data, _ := msg.GetData()
	var res map[string]interface{}
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if _, ok := res["payload"]; !ok {
		t.Fatal("expected 'payload' key in result")
	}
	if _, ok := res["result"]; !ok {
		t.Fatal("expected 'result' key in result")
	}
	if res["result"] != float64(42) {
		t.Fatalf("expected result 42, got %v", res["result"])
	}
}

func TestProcessStringTransformation(t *testing.T) {
	cfg := &RunnerConfig{
		Expression: `$uppercase($.name) & " " & $string($.age)`,
		Timeout:    5 * time.Second,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newRunnerMsg([]byte(`{"name":"alice","age":30}`), map[string]string{}, "id-7")
	if err := r.Process(msg); err != nil {
		t.Fatalf("Process error: %v", err)
	}

	data, _ := msg.GetData()
	var res string
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if res != "ALICE 30" {
		t.Fatalf("expected 'ALICE 30', got %q", res)
	}
}

// --------------------------------------------------------------------------
// Process — error cases
// --------------------------------------------------------------------------

func TestProcessInvalidJSON(t *testing.T) {
	cfg := &RunnerConfig{Expression: "$.name", Timeout: 5 * time.Second}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newRunnerMsg([]byte(`not json`), map[string]string{}, "id-err-1")
	if err := r.Process(msg); err == nil {
		t.Fatal("expected error for non-JSON payload")
	}
}

func TestProcessInputSizeExceeded(t *testing.T) {
	cfg := &RunnerConfig{Expression: "$.x", Timeout: 5 * time.Second, MaxInputSize: 5}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newRunnerMsg([]byte(`{"x":1,"y":2}`), map[string]string{}, "id-err-2")
	if err := r.Process(msg); err == nil {
		t.Fatal("expected error for input size exceeded")
	}
}

func TestProcessOutputSizeExceeded(t *testing.T) {
	// Build a 20-element array and limit output to 10 bytes.
	cfg := &RunnerConfig{Expression: "$.items", Timeout: 5 * time.Second, MaxOutputSize: 10}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	items := `{"items":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]}`
	msg := newRunnerMsg([]byte(items), map[string]string{}, "id-err-3")
	if err := r.Process(msg); err == nil {
		t.Fatal("expected error for output size exceeded")
	}
}

func TestProcessTimeout(t *testing.T) {
	cfg := &RunnerConfig{Expression: "$.name", Timeout: 1 * time.Nanosecond}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newRunnerMsg([]byte(`{"name":"Alice"}`), map[string]string{}, "id-timeout")
	err = r.Process(msg)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// --------------------------------------------------------------------------
// Close
// --------------------------------------------------------------------------

func TestCloseIdempotent(t *testing.T) {
	cfg := &RunnerConfig{Expression: "$.x", Timeout: 5 * time.Second}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("first Close error: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close error: %v", err)
	}
}

// --------------------------------------------------------------------------
// normaliseResult
// --------------------------------------------------------------------------

func TestNormaliseResultNull(t *testing.T) {
	from := normaliseResult(nil)
	if from != nil {
		t.Fatalf("expected nil, got %v", from)
	}
}

func TestNormaliseResultNestedMap(t *testing.T) {
	input := map[string]interface{}{
		"a": "hello",
		"b": nil,
	}
	out := normaliseResult(input)
	m, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", out)
	}
	if m["b"] != nil {
		t.Fatalf("expected nil for key b, got %v", m["b"])
	}
}

func TestNormaliseResultArray(t *testing.T) {
	input := []interface{}{1, nil, "x"}
	out := normaliseResult(input)
	arr, ok := out.([]interface{})
	if !ok {
		t.Fatalf("expected slice, got %T", out)
	}
	if arr[1] != nil {
		t.Fatalf("expected nil at index 1, got %v", arr[1])
	}
}
