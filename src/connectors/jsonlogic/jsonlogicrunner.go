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
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure JSONLogicRunner implements connectors.Runner
var _ connectors.Runner = &JSONLogicRunner{}

type RunnerConfig struct {
	Path            string        `mapstructure:"path" validate:"excluded_with=Logic|required_without=Logic"`
	Logic           string        `mapstructure:"logic" validate:"excluded_with=Path|required_without=Path"`
	PreservePayload bool          `mapstructure:"preservePayload"`
	Timeout         time.Duration `mapstructure:"timeout" default:"5s" validate:"required"`
}

type JSONLogicRunner struct {
	cfg    *RunnerConfig
	slog   *slog.Logger
	logic  map[string]interface{}
	mu     sync.Mutex
	stopCh chan struct{}
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// New creates a new instance of JSONLogicRunner
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	log := slog.Default().With("context", "JSONLogic Runner")
	log.Info("loading jsonlogic rule", "path", cfg.Path)

	var logicBytes []byte
	var err error
	if cfg.Logic != "" {
		logicBytes = []byte(cfg.Logic)
	} else {
		logicBytes, err = os.ReadFile(cfg.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to read jsonlogic file: %w", err)
		}
	}

	var logic map[string]interface{}
	if err := json.Unmarshal(logicBytes, &logic); err != nil {
		return nil, fmt.Errorf("invalid jsonlogic rule: %w", err)
	}

	return &JSONLogicRunner{
		cfg:    cfg,
		slog:   log,
		logic:  logic,
		stopCh: make(chan struct{}),
	}, nil
}

// Process applies the JSONLogic rule to the message
func (j *JSONLogicRunner) Process(msg *message.RunnerMessage) (*message.RunnerMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), j.cfg.Timeout)
	defer cancel()

	meta, err := msg.GetMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	data, err := msg.GetData()
	if err != nil {
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	var dataMap map[string]interface{}
	if err := json.Unmarshal(data, &dataMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal input data: %w", err)
	}

	input := map[string]interface{}{
		"meta": meta,
		"data": dataMap,
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

	var outputStruct interface{}
	if j.cfg.PreservePayload {
		outputStruct = map[string]interface{}{
			"payload": input,
			"result":  result,
		}
	} else {
		outputStruct = result
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
