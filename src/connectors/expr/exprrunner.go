package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/common/expreval"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure ExprRunner implements connectors.Runner
var _ connectors.Runner = &ExprRunner{}

type ExprRunnerConfig struct {
	Expression          string        `mapstructure:"expression" validate:"required"`
	PreservePayload     bool          `mapstructure:"preservePayload"`
	Timeout             time.Duration `mapstructure:"timeout" default:"5s" validate:"required,gt=0,lte=60s"`
	MaxExpressionLength int           `mapstructure:"maxExpressionLength" default:"10000" validate:"omitempty,gt=0,lte=100000"`
	AllowedFunctions    []string      `mapstructure:"allowedFunctions"`                                           // Empty = all allowed
	MaxInputSize        int           `mapstructure:"maxInputSize" default:"1048576" validate:"omitempty,gt=0"`   // 1MB default
	MaxOutputSize       int           `mapstructure:"maxOutputSize" default:"10485760" validate:"omitempty,gt=0"` // 10MB default
	DisableBuiltins     bool          `mapstructure:"disableBuiltins" default:"false"`                            // Disable all built-in functions
	AllowUndefined      bool          `mapstructure:"allowUndefined" default:"false"`                             // Allow undefined variables
}

type ExprRunner struct {
	cfg    *ExprRunnerConfig
	slog   *slog.Logger
	eval   *expreval.ExprEvaluator
	mu     sync.Mutex
	stopCh chan struct{}
}

func NewExprRunnerConfig() any {
	return new(ExprRunnerConfig)
}

// New creates a new instance of ExprRunner
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*ExprRunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	log := slog.Default().With("context", "Expr Runner")

	log.Info("loading expr rule",
		"expression", cfg.Expression,
		"maxLength", cfg.MaxExpressionLength,
		"allowedFunctions", cfg.AllowedFunctions,
		"disableBuiltins", cfg.DisableBuiltins)

	// Create evaluator with security validation
	eval, err := expreval.NewExprEvaluatorWithConfig(expreval.Config{
		Expression:          cfg.Expression,
		MaxExpressionLength: cfg.MaxExpressionLength,
		AllowedFunctions:    cfg.AllowedFunctions,
		DisableBuiltins:     cfg.DisableBuiltins,
		AllowUndefined:      cfg.AllowUndefined,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create expr evaluator: %w", err)
	}

	return &ExprRunner{
		cfg:    cfg,
		slog:   log,
		eval:   eval,
		stopCh: make(chan struct{}),
	}, nil
}

// Process applies the expression to the message
func (e *ExprRunner) Process(msg *message.RunnerMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.Timeout)
	defer cancel()

	metadata, data, err := msg.GetMetadataAndData()
	if err != nil {
		return fmt.Errorf("failed to get metadata and data: %w", err)
	}

	// Check input size limit
	if e.cfg.MaxInputSize > 0 && len(data) > e.cfg.MaxInputSize {
		return fmt.Errorf("input data size (%d bytes) exceeds maximum allowed (%d bytes)", len(data), e.cfg.MaxInputSize)
	}

	var dataMap map[string]interface{}
	if err := json.Unmarshal(data, &dataMap); err != nil {
		return fmt.Errorf("failed to unmarshal input data: %w", err)
	}

	input := map[string]interface{}{
		"metadata": metadata,
		"data":     dataMap,
	}

	var result bool
	done := make(chan error, 1)
	go func() {
		res, err := e.eval.Eval(input)
		if err == nil {
			result = res
		}
		done <- err
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("expr execution timeout after %v", e.cfg.Timeout)
	case err := <-done:
		if err != nil {
			return fmt.Errorf("expr execution error: %w", err)
		}
	}

	var outputStruct interface{}
	if e.cfg.PreservePayload {
		outputStruct = map[string]interface{}{
			"payload": input,
			"result":  result,
		}
	} else {
		outputStruct = result
	}

	output, err := json.Marshal(outputStruct)
	if err != nil {
		return fmt.Errorf("failed to marshal expr result: %w", err)
	}

	// Check output size limit
	if e.cfg.MaxOutputSize > 0 && len(output) > e.cfg.MaxOutputSize {
		return fmt.Errorf("output data size (%d bytes) exceeds maximum allowed (%d bytes)", len(output), e.cfg.MaxOutputSize)
	}

	msg.SetData(output)

	return nil
}

func (e *ExprRunner) Close() error {
	e.slog.Info("closing expr runner")
	e.mu.Lock()
	defer e.mu.Unlock()
	select {
	case <-e.stopCh:
		// already closed
	default:
		close(e.stopCh)
	}
	return nil
}
