package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure FormatRunner implements connectors.Runner
var _ connectors.Runner = &FormatRunner{}

// FormatRunner implements the format runner for message transformation
type FormatRunner struct {
	cfg      *FormatRunnerConfig
	slog     *slog.Logger
	handlers map[string]OperationHandler
	mu       sync.Mutex
	stopCh   chan struct{}
}

// NewFormatRunnerConfig creates a new FormatRunnerConfig instance
func NewRunnerConfig() any {
	return &FormatRunnerConfig{
		Timeout: 30 * time.Second,
		Verbose: false,
	}
}

// NewRunner creates a new FormatRunner instance
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*FormatRunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Validate configuration
	if len(cfg.Operations) == 0 {
		return nil, fmt.Errorf("at least one operation is required")
	}

	log := slog.Default().With("context", "Format Runner")

	// Initialize operation handlers
	handlers := make(map[string]OperationHandler)
	handlers["template"] = NewTemplateHandler(log)
	handlers["serialize"] = NewSerializeHandler(log)
	handlers["encode"] = NewEncodeHandler(log)
	handlers["compress"] = NewCompressHandler(log)
	handlers["multipart"] = NewMultipartHandler(log)
	handlers["extract"] = NewExtractHandler(log)
	handlers["merge"] = NewMergeHandler(log)
	handlers["split"] = NewSplitHandler(log)

	// Validate all operations
	for i, op := range cfg.Operations {
		if err := ValidateOperation(op); err != nil {
			return nil, fmt.Errorf("invalid operation %d: %w", i, err)
		}

		handler, exists := handlers[op.Type]
		if !exists {
			return nil, fmt.Errorf("no handler for operation type: %s", op.Type)
		}

		if err := handler.Validate(op); err != nil {
			return nil, fmt.Errorf("operation %d validation failed: %w", i, err)
		}
	}

	log.Info("format runner initialized",
		"operations", len(cfg.Operations),
		"timeout", cfg.Timeout)

	return &FormatRunner{
		cfg:      cfg,
		slog:     log,
		handlers: handlers,
		stopCh:   make(chan struct{}),
	}, nil
}

// Process executes all formatting operations on the message
func (r *FormatRunner) Process(msg *message.RunnerMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	defer cancel()

	for i, op := range r.cfg.Operations {
		select {
		case <-ctx.Done():
			return fmt.Errorf("format runner timeout after operation %d", i)
		case <-r.stopCh:
			return fmt.Errorf("format runner stopped")
		default:
		}

		if r.cfg.Verbose {
			r.slog.Debug("executing format operation",
				"index", i,
				"type", op.Type,
				"input", op.Input,
				"output", op.Output)
		}

		// Execute operation
		handler, exists := r.handlers[op.Type]
		if !exists {
			return fmt.Errorf("no handler for operation type: %s", op.Type)
		}

		if err := handler.Execute(ctx, msg, op); err != nil {
			return fmt.Errorf("operation %d (%s) failed: %w", i, op.Type, err)
		}
	}

	return nil
}

// Close closes the format runner
func (r *FormatRunner) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-r.stopCh:
		// Already closed
		return nil
	default:
		close(r.stopCh)
	}

	r.slog.Info("format runner closed")
	return nil
}
