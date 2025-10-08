package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure ExprRunner implements connectors.Runner
var _ connectors.Runner = &ExprRunner{}

type ExprRunnerConfig struct {
	Expression      string                        `mapstructure:"expression" validate:"required"`
	PreservePayload bool                          `mapstructure:"preservePayload"`
	Timeout         time.Duration                 `mapstructure:"timeout" default:"5s" validate:"required"`
	Services        map[string]connectors.Service `mapstructure:"services,omitempty"`
}

type ExprRunner struct {
	cfg    *ExprRunnerConfig
	slog   *slog.Logger
	prog   *vm.Program
	mu     sync.Mutex
	stopCh chan struct{}
}

func NewRunnerConfig() any {
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

	env := map[string]any{}
	for name, service := range cfg.Services {
		env[name+"_call"] = func(name string, params ...any) (any, error) {
			return service.Call(name, params)
		}
	}

	env["log_info"] = func(msg string) bool {
		log.Info(msg)
		return true
	}
	env["log_warn"] = func(msg string) bool {
		log.Warn(msg)
		return true
	}
	env["log_error"] = func(msg string) bool {
		log.Error(msg)
		return true
	}

	prog, err := expr.Compile(cfg.Expression, expr.Env(env))
	if err != nil {
		return nil, fmt.Errorf("failed to compile expression: %w", err)
	}

	return &ExprRunner{
		cfg:    cfg,
		slog:   log,
		prog:   prog,
		stopCh: make(chan struct{}),
	}, nil
}

// Process applies the expression to the message
func (e *ExprRunner) Process(msg *message.RunnerMessage) error {

	metadata, data, err := msg.GetMetadataAndData()
	if err != nil {
		return fmt.Errorf("failed to get metadata and data: %w", err)
	}

	// TODO: decode data from config param

	input := map[string]any{
		"metadata": metadata,
		"data":     data,
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.Timeout)
	defer cancel()

	var result any
	done := make(chan error, 1)
	go func() {
		res, err := expr.Run(e.prog, input)
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
