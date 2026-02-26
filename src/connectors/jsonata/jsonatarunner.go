// Package main implements the JSONata connector for the Events Bridge.
//
// The JSONata connector enables data transformation using the JSONata 2.1.0+
// expression language — a powerful JSON query and transformation language
// ported from the reference JavaScript implementation.
//
// Key features:
//   - Full JSONata 2.1.0+ conformance (1273/1273 official test cases)
//   - Expression pre-compilation at startup for optimal throughput
//   - Thread-safe reusable evaluator across concurrent goroutines
//   - Optional LRU expression cache
//   - Optional exposure of message metadata as JSONata bindings
//   - Configurable execution timeout via context cancellation
//   - Input and output size limits
//
// JSONata expression context:
//   - The message payload (parsed JSON) is the root context ($)
//   - Metadata fields are accessible via the $metadata binding (if enabled)
//
// Example configuration:
//
//	runners:
//	  - type: jsonata
//	    options:
//	      expression: |
//	        {
//	          "id":    $.orderId,
//	          "total": $sum($.items.price),
//	          "user":  $metadata.userId
//	        }
//	      timeout: 5s
//	      exposeMetadata: true
//
//	  # Load expression from file
//	  - type: jsonata
//	    options:
//	      path: ./transforms/normalize.jsonata
//	      preservePayload: false
//	      timeout: 10s
//
// Security considerations:
//   - JSONata expressions run in a pure-Go evaluator with no filesystem or
//     network access.
//   - Custom functions are not supported in this connector to limit the attack
//     surface. Use the ES5 or WASM connectors when arbitrary callouts are needed.
//   - Always set MaxInputSize and MaxOutputSize to prevent memory exhaustion.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	gosonata "github.com/sandrolain/gosonata"
	"github.com/sandrolain/gosonata/pkg/evaluator"
	"github.com/sandrolain/gosonata/pkg/types"
)

// Ensure JSONataRunner implements connectors.Runner at compile time.
var _ connectors.Runner = &JSONataRunner{}

// RunnerConfig holds the configuration for the JSONata runner connector.
type RunnerConfig struct {
	// Expression is an inline JSONata expression string.
	// Exactly one of Expression or Path must be provided.
	Expression string `mapstructure:"expression" validate:"excluded_with=Path|required_without=Path"`
	// Path is the file path to a JSONata expression file.
	// Exactly one of Expression or Path must be provided.
	Path string `mapstructure:"path" validate:"excluded_with=Expression|required_without=Expression"`

	// PreservePayload wraps the output as {"payload": <original>, "result": <output>}
	// when true, so that the original message data is preserved alongside the result.
	PreservePayload bool `mapstructure:"preservePayload"`

	// ExposeMetadata controls whether message metadata is made available as the
	// $metadata binding inside the JSONata expression. Defaults to true.
	ExposeMetadata bool `mapstructure:"exposeMetadata" default:"true"`

	// Timeout is the maximum allowed duration for a single expression evaluation.
	Timeout time.Duration `mapstructure:"timeout" default:"5s" validate:"required,gt=0,lte=60s"`

	// MaxExprSize limits the byte size of the expression source (inline or file).
	MaxExprSize int `mapstructure:"maxExprSize" default:"100000" validate:"omitempty,gt=0,lte=1000000"`

	// MaxInputSize limits the byte size of the incoming JSON payload.
	MaxInputSize int `mapstructure:"maxInputSize" default:"1048576" validate:"omitempty,gt=0"` // 1 MB

	// MaxOutputSize limits the byte size of the serialised output.
	MaxOutputSize int `mapstructure:"maxOutputSize" default:"10485760" validate:"omitempty,gt=0"` // 10 MB

	// EnableCaching enables an LRU cache of compiled expressions inside the evaluator.
	EnableCaching bool `mapstructure:"enableCaching" default:"true"`

	// CacheSize sets the maximum number of entries in the expression cache.
	CacheSize int `mapstructure:"cacheSize" default:"256" validate:"omitempty,gt=0"`

	// EnableConcurrency enables concurrent sub-expression evaluation via goroutines.
	EnableConcurrency bool `mapstructure:"enableConcurrency" default:"true"`
}

// JSONataRunner transforms messages using a pre-compiled JSONata expression.
type JSONataRunner struct {
	cfg  *RunnerConfig
	slog *slog.Logger
	expr *types.Expression
	eval *evaluator.Evaluator
	mu   sync.Mutex
	stop chan struct{}
}

// NewRunnerConfig returns a new RunnerConfig instance for plugin loading conventions.
func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// NewRunner creates and initialises a JSONataRunner from the supplied configuration.
// The expression is compiled once at startup so that per-message processing is as
// fast as possible.
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	log := slog.Default().With("context", "JSONata Runner")

	// Load the expression source.
	var src string
	if cfg.Expression != "" {
		src = cfg.Expression
	} else {
		b, err := os.ReadFile(cfg.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to read expression file: %w", err)
		}
		src = string(b)
	}

	// Enforce expression size limit.
	if cfg.MaxExprSize > 0 && len(src) > cfg.MaxExprSize {
		return nil, fmt.Errorf("expression size (%d bytes) exceeds maxExprSize (%d bytes)", len(src), cfg.MaxExprSize)
	}

	// Pre-compile the expression once.
	expr, err := gosonata.Compile(src)
	if err != nil {
		return nil, fmt.Errorf("failed to compile jsonata expression: %w", err)
	}

	// Report any non-fatal compilation warnings.
	if errs := expr.Errors(); len(errs) > 0 {
		for _, e := range errs {
			log.Warn("jsonata compile warning", "error", e)
		}
	}

	// Build the reusable evaluator.
	ev := evaluator.New(
		evaluator.WithCaching(cfg.EnableCaching),
		evaluator.WithCacheSize(cfg.CacheSize),
		evaluator.WithConcurrency(cfg.EnableConcurrency),
		evaluator.WithTimeout(cfg.Timeout),
	)

	log.Info("jsonata expression compiled",
		"source", describeSource(cfg),
		"len", len(src),
		"caching", cfg.EnableCaching,
		"concurrency", cfg.EnableConcurrency,
	)

	return &JSONataRunner{
		cfg:  cfg,
		slog: log,
		expr: expr,
		eval: ev,
		stop: make(chan struct{}),
	}, nil
}

// Process applies the compiled JSONata expression to the message payload.
// The message payload must be valid JSON. The result replaces the payload
// unless PreservePayload is set.
func (r *JSONataRunner) Process(msg *message.RunnerMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	defer cancel()

	metadata, data, err := msg.GetMetadataAndData()
	if err != nil {
		return fmt.Errorf("failed to get message metadata and data: %w", err)
	}

	// Enforce input size limit.
	if r.cfg.MaxInputSize > 0 && len(data) > r.cfg.MaxInputSize {
		return fmt.Errorf("input size (%d bytes) exceeds maxInputSize (%d bytes)", len(data), r.cfg.MaxInputSize)
	}

	// Deserialise the payload into a generic map so JSONata can traverse it.
	var dataVal interface{}
	if err := json.Unmarshal(data, &dataVal); err != nil {
		return fmt.Errorf("failed to unmarshal message data: %w", err)
	}

	// Build per-call bindings.
	bindings := map[string]interface{}{}
	if r.cfg.ExposeMetadata && metadata != nil {
		bindings["metadata"] = metadataToInterface(metadata)
	}

	// Execute the expression.
	result, err := r.eval.EvalWithBindings(ctx, r.expr, dataVal, bindings)
	if err != nil {
		return fmt.Errorf("jsonata evaluation error: %w", err)
	}

	// Normalise the result so that types.Null{} becomes JSON null.
	result = normaliseResult(result)

	// Optionally wrap the output together with the original payload.
	var out interface{}
	if r.cfg.PreservePayload {
		out = map[string]interface{}{
			"payload": dataVal,
			"result":  result,
		}
	} else {
		out = result
	}

	output, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("failed to marshal jsonata result: %w", err)
	}

	// Enforce output size limit.
	if r.cfg.MaxOutputSize > 0 && len(output) > r.cfg.MaxOutputSize {
		return fmt.Errorf("output size (%d bytes) exceeds maxOutputSize (%d bytes)", len(output), r.cfg.MaxOutputSize)
	}

	msg.SetData(output)

	return nil
}

// Close shuts down the runner and releases any held resources.
func (r *JSONataRunner) Close() error {
	r.slog.Info("closing jsonata runner")
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.stop:
		// Already closed.
	default:
		close(r.stop)
	}
	return nil
}

// normaliseResult recursively converts types.Null{} values to nil so that they
// serialise as JSON null rather than the zero-value struct representation.
func normaliseResult(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case types.Null:
		return nil
	case map[string]interface{}:
		for k, child := range val {
			val[k] = normaliseResult(child)
		}
		return val
	case []interface{}:
		for i, child := range val {
			val[i] = normaliseResult(child)
		}
		return val
	default:
		return val
	}
}

// metadataToInterface converts a map[string]string to map[string]interface{}
// to satisfy the EvalWithBindings bindings type.
func metadataToInterface(m map[string]string) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// describeSource returns a short human-readable label for log messages.
func describeSource(cfg *RunnerConfig) string {
	if cfg.Expression != "" {
		return "inline"
	}
	return cfg.Path
}
