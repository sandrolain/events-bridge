package main

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

const (
	benchMeta   = "benchmark"
	benchID     = "bench-id"
	benchSource = "bench-source"
)

// benchPayload is a realistic e-commerce order payload used across benchmarks.
const benchPayload = `{
	"orderId": "ORD-12345",
	"customerId": "CUST-99",
	"items": [
		{"sku": "A1", "name": "Widget", "qty": 2, "price": 9.99},
		{"sku": "B2", "name": "Gadget", "qty": 1, "price": 49.99},
		{"sku": "C3", "name": "Gizmo",  "qty": 5, "price": 4.99}
	],
	"shipping": {"address": "123 Main St", "city": "Springfield", "zip": "12345"},
	"status": "pending"
}`

// newBenchMsg creates a fresh RunnerMessage for each benchmark iteration.
func newBenchMsg() *message.RunnerMessage {
	stub := testutil.NewAdapter([]byte(benchPayload), map[string]string{"source": benchSource, "env": "prod"})
	stub.ID = []byte(benchID)
	return message.NewRunnerMessage(stub)
}

// BenchmarkProcessSimpleField measures throughput for a trivial field extraction.
func BenchmarkProcessSimpleField(b *testing.B) {
	cfg := &RunnerConfig{Expression: "$.orderId", Timeout: 5 * time.Second}
	r, err := NewRunner(cfg)
	if err != nil {
		b.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newBenchMsg()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := r.Process(msg); err != nil {
			b.Fatalf("Process error: %v", err)
		}
	}
}

// BenchmarkProcessObjectConstruction measures throughput for building a new object
// with aggregations — representative of a typical ETL mapping step.
func BenchmarkProcessObjectConstruction(b *testing.B) {
	cfg := &RunnerConfig{
		Expression: `{
			"id":       $.orderId,
			"customer": $.customerId,
			"total":    $sum($.items.(qty * price)),
			"itemCount": $count($.items),
			"status":   $uppercase($.status)
		}`,
		Timeout: 5 * time.Second,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		b.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newBenchMsg()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := r.Process(msg); err != nil {
			b.Fatalf("Process error: %v", err)
		}
	}
}

// BenchmarkProcessWithMetadata measures overhead when metadata bindings are enabled.
func BenchmarkProcessWithMetadata(b *testing.B) {
	cfg := &RunnerConfig{
		Expression:     `{"id": $.orderId, "source": $metadata.source, "env": $metadata.env}`,
		ExposeMetadata: true,
		Timeout:        5 * time.Second,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		b.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newBenchMsg()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := r.Process(msg); err != nil {
			b.Fatalf("Process error: %v", err)
		}
	}
}

// BenchmarkProcessArrayFilter measures throughput for predicate-based array filtering.
func BenchmarkProcessArrayFilter(b *testing.B) {
	cfg := &RunnerConfig{
		Expression: `$.items[price > 5].name`,
		Timeout:    5 * time.Second,
	}
	r, err := NewRunner(cfg)
	if err != nil {
		b.Fatalf("failed to create runner: %v", err)
	}
	defer func() { _ = r.Close() }()

	msg := newBenchMsg()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := r.Process(msg); err != nil {
			b.Fatalf("Process error: %v", err)
		}
	}
}
