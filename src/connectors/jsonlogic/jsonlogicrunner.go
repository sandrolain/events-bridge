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
	Path              string        `mapstructure:"path" validate:"excluded_with=Logic|required_without=Logic"`
	Logic             string        `mapstructure:"logic" validate:"excluded_with=Path|required_without=Path"`
	PreservePayload   bool          `mapstructure:"preservePayload"`
	Timeout           time.Duration `mapstructure:"timeout" default:"5s" validate:"required,gt=0,lte=60s"`
	MaxLogicSize      int           `mapstructure:"maxLogicSize" default:"100000" validate:"omitempty,gt=0,lte=1000000"` // 100KB default, 1MB max
	AllowedOperations []string      `mapstructure:"allowedOperations"`                                                   // Empty = all allowed
	MaxInputSize      int           `mapstructure:"maxInputSize" default:"1048576" validate:"omitempty,gt=0"`            // 1MB default
	MaxOutputSize     int           `mapstructure:"maxOutputSize" default:"10485760" validate:"omitempty,gt=0"`          // 10MB default
	MaxComplexity     int           `mapstructure:"maxComplexity" default:"1000" validate:"omitempty,gt=0"`              // Max operations count
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
	log.Info("loading jsonlogic rule", "path", cfg.Path, "hasLogic", cfg.Logic != "")

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

	// Validate logic JSON
	if err := validateLogicJSON(logicBytes, cfg.MaxLogicSize); err != nil {
		return nil, fmt.Errorf("logic validation failed: %w", err)
	}

	var logic map[string]interface{}
	if err := json.Unmarshal(logicBytes, &logic); err != nil {
		return nil, fmt.Errorf("invalid jsonlogic rule: %w", err)
	}

	// Validate complexity
	if err := validateComplexity(logic, cfg.MaxComplexity); err != nil {
		return nil, fmt.Errorf("logic complexity validation failed: %w", err)
	}

	// Validate allowed operations
	if len(cfg.AllowedOperations) > 0 {
		if err := validateAllowedOperations(logic, cfg.AllowedOperations); err != nil {
			return nil, fmt.Errorf("logic uses disallowed operations: %w", err)
		}
	}

	log.Info("jsonlogic rule loaded successfully",
		"size", len(logicBytes),
		"complexity", countOperations(logic),
		"allowedOperations", cfg.AllowedOperations)

	return &JSONLogicRunner{
		cfg:    cfg,
		slog:   log,
		logic:  logic,
		stopCh: make(chan struct{}),
	}, nil
}

// Process applies the JSONLogic rule to the message
func (j *JSONLogicRunner) Process(msg *message.RunnerMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), j.cfg.Timeout)
	defer cancel()

	metadata, data, err := msg.GetMetadataAndData()
	if err != nil {
		return fmt.Errorf("failed to get metadata and data: %w", err)
	}

	// Check input size limit
	if j.cfg.MaxInputSize > 0 && len(data) > j.cfg.MaxInputSize {
		return fmt.Errorf("input data size (%d bytes) exceeds maximum allowed (%d bytes)", len(data), j.cfg.MaxInputSize)
	}

	var dataMap map[string]interface{}
	if err := json.Unmarshal(data, &dataMap); err != nil {
		return fmt.Errorf("failed to unmarshal input data: %w", err)
	}

	input := map[string]interface{}{
		"metadata": metadata,
		"data":     dataMap,
	}

	var result interface{}
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("jsonlogic execution panic: %v", r)
			}
		}()

		res, err := jsonlogic.ApplyInterface(j.logic, input)
		if err == nil {
			result = res
		}
		done <- err
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("jsonlogic execution timeout after %v", j.cfg.Timeout)
	case err := <-done:
		if err != nil {
			return fmt.Errorf("jsonlogic execution error: %w", err)
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
		return fmt.Errorf("failed to marshal jsonlogic result: %w", err)
	}

	// Check output size limit
	if j.cfg.MaxOutputSize > 0 && len(output) > j.cfg.MaxOutputSize {
		return fmt.Errorf("output data size (%d bytes) exceeds maximum allowed (%d bytes)", len(output), j.cfg.MaxOutputSize)
	}

	msg.SetData(output)

	return nil
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
