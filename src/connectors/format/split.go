package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/creasty/defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/sandrolain/events-bridge/src/message"
)

// SplitHandler handles split operations
type SplitHandler struct {
	slog *slog.Logger
}

// NewSplitHandler creates a new SplitHandler
func NewSplitHandler(logger *slog.Logger) *SplitHandler {
	return &SplitHandler{
		slog: logger,
	}
}

// Validate validates the split operation configuration
func (h *SplitHandler) Validate(op FormatOperation) error {
	var opts SplitOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode split options: %w", err)
	}

	if opts.Delimiter == "" {
		return fmt.Errorf("delimiter is required")
	}

	if opts.MaxParts <= 0 {
		return fmt.Errorf("maxParts must be positive")
	}

	if op.Output != "parts" {
		return fmt.Errorf("split operation output must be 'parts'")
	}

	return nil
}

// Execute executes the split operation
func (h *SplitHandler) Execute(ctx context.Context, msg *message.RunnerMessage, op FormatOperation) error {
	var opts SplitOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode split options: %w", err)
	}

	// Get input data
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("failed to get data: %w", err)
	}

	// Split data by delimiter
	delimiter := []byte(opts.Delimiter)
	parts := bytes.Split(data, delimiter)

	// Limit number of parts
	if len(parts) > opts.MaxParts {
		h.slog.Warn("number of parts exceeds maximum, truncating",
			"parts", len(parts),
			"max", opts.MaxParts)
		parts = parts[:opts.MaxParts]
	}

	// Create message parts
	for i, partData := range parts {
		partName := fmt.Sprintf("%s%d", opts.PartNamePrefix, i)

		part := message.Part{
			Name:        partName,
			ContentType: opts.ContentType,
			Data:        partData,
		}

		msg.AddPart(part)
	}

	h.slog.Debug("split data into parts", "count", len(parts))

	return nil
}
