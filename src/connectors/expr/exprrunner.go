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
	Expression      string        `mapstructure:"expression" validate:"required"`
	PreservePayload bool          `mapstructure:"preservePayload"`
	Timeout         time.Duration `mapstructure:"timeout" default:"5s" validate:"required"`
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
	log.Info("loading expr rule", "expression", cfg.Expression)

	eval, err := expreval.NewExprEvaluator(cfg.Expression)
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
		return fmt.Errorf("expr execution timeout")
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
