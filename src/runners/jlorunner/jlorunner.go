package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	jsonlogic "github.com/diegoholiveira/jsonlogic"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/runners"
)

// Ensure JSONLogicRunner implements runners.Runner
var _ runners.Runner = &JSONLogicRunner{}

type JSONLogicRunner struct {
	cfg     *runners.RunnerJSONLogicConfig
	slog    *slog.Logger
	logic   map[string]interface{}
	mu      sync.Mutex
	timeout time.Duration
	stopCh  chan struct{}
}

// New creates a new instance of JSONLogicRunner
func New(cfg *runners.RunnerJSONLogicConfig) (runners.Runner, error) {
	if cfg == nil {
		return nil, fmt.Errorf("jsonlogic runner configuration cannot be nil")
	}
	if cfg.Path == "" {
		return nil, fmt.Errorf("jsonlogic rule path is required")
	}
	log := slog.Default().With("context", "JSONLOGIC")
	log.Info("loading jsonlogic rule", "path", cfg.Path)

	logicBytes, err := os.ReadFile(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read jsonlogic file: %w", err)
	}

	var logic map[string]interface{}
	if err := json.Unmarshal(logicBytes, &logic); err != nil {
		return nil, fmt.Errorf("invalid jsonlogic rule: %w", err)
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	return &JSONLogicRunner{
		cfg:     cfg,
		slog:    log,
		logic:   logic,
		timeout: timeout,
		stopCh:  make(chan struct{}),
	}, nil
}

// Process applies the JSONLogic rule to the message
func (j *JSONLogicRunner) Process(msg *message.RunnerMessage) (*message.RunnerMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), j.timeout)
	defer cancel()

	data, err := msg.GetSourceData()
	if err != nil {
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	var input map[string]interface{}
	if err := json.Unmarshal(data, &input); err != nil {
		return nil, fmt.Errorf("failed to unmarshal input data: %w", err)
	}

	var result interface{}
	done := make(chan error, 1)
	go func() {
		res, err := jsonlogic.ApplyInterface(j.logic, input)
		if err == nil {
			result = res
		}
		done <- err
	}()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("jsonlogic execution timeout")
	case err := <-done:
		if err != nil {
			return nil, fmt.Errorf("jsonlogic execution error: %w", err)
		}
	}

	// TODO: config params to obtain only the result
	outputStruct := map[string]interface{}{
		"payload": input,
		"result":  result,
	}

	output, err := json.Marshal(outputStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal jsonlogic result: %w", err)
	}

	msg.SetData(output)

	return msg, nil
}

func (j *JSONLogicRunner) Close() error {
	j.slog.Info("closing jsonlogic runner")
	j.mu.Lock()
	defer j.mu.Unlock()
	select {
	case <-j.stopCh:
		// already closed
	default:
		close(j.stopCh)
	}
	return nil
}
