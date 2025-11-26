package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/creasty/defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
	"gopkg.in/yaml.v3"
)

// MergeHandler handles merge operations
type MergeHandler struct {
	slog *slog.Logger
}

// NewMergeHandler creates a new MergeHandler
func NewMergeHandler(logger *slog.Logger) *MergeHandler {
	return &MergeHandler{
		slog: logger,
	}
}

// Validate validates the merge operation configuration
func (h *MergeHandler) Validate(op FormatOperation) error {
	var opts MergeOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode merge options: %w", err)
	}

	if len(opts.Sources) == 0 {
		return fmt.Errorf("at least one source must be specified")
	}

	validFormats := map[string]bool{
		"json": true,
		"yaml": true,
	}

	if !validFormats[opts.Format] {
		return fmt.Errorf("invalid format: %s (supported: json, yaml)", opts.Format)
	}

	for i, src := range opts.Sources {
		if src.Key == "" {
			return fmt.Errorf("source %d: key is required", i)
		}

		validInputs := map[string]bool{
			sourceData:       true,
			sourceMetadata:   true,
			sourceFilesystem: true,
		}

		if !validInputs[src.Input] {
			return fmt.Errorf("source %d: invalid input: %s", i, src.Input)
		}

		if src.Input == sourceFilesystem && src.Path == "" {
			return fmt.Errorf("source %d: path is required for filesystem input", i)
		}
	}

	return nil
}

// Execute executes the merge operation
func (h *MergeHandler) Execute(ctx context.Context, msg *message.RunnerMessage, op FormatOperation) error {
	var opts MergeOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode merge options: %w", err)
	}

	// Merge all sources
	merged := make(map[string]interface{})

	for i, src := range opts.Sources {
		value, err := h.getSourceValue(msg, src)
		if err != nil {
			h.slog.Warn("failed to get source value", "index", i, "key", src.Key, "error", err)
			continue
		}

		merged[src.Key] = value
	}

	// Serialize merged data
	var output []byte
	var err error

	switch opts.Format {
	case formatJSON:
		output, err = json.Marshal(merged)
		if err != nil {
			return fmt.Errorf("failed to marshal merged data as JSON: %w", err)
		}

	case formatYAML:
		output, err = yaml.Marshal(merged)
		if err != nil {
			return fmt.Errorf("failed to marshal merged data as YAML: %w", err)
		}

	default:
		return fmt.Errorf("unsupported format: %s", opts.Format)
	}

	// Set output
	return SetOutput(msg, op.Output, output)
}

// getSourceValue gets a value from the specified source
func (h *MergeHandler) getSourceValue(msg *message.RunnerMessage, src MergeSource) (interface{}, error) {
	switch src.Input {
	case sourceData:
		data, err := msg.GetData()
		if err != nil {
			return nil, fmt.Errorf("failed to get data: %w", err)
		}

		// Try to parse as JSON
		var jsonData interface{}
		if err := json.Unmarshal(data, &jsonData); err == nil {
			return jsonData, nil
		}

		// Return as string if not JSON
		return string(data), nil

	case sourceMetadata:
		metadata, err := msg.GetMetadata()
		if err != nil {
			return nil, fmt.Errorf("failed to get metadata: %w", err)
		}
		return metadata, nil

	case sourceFilesystem:
		fs, err := msg.GetFilesystem()
		if err != nil {
			return nil, fmt.Errorf("failed to get filesystem: %w", err)
		}
		if fs == nil {
			return nil, fmt.Errorf("filesystem not available")
		}

		data, err := fsutil.ReadFile(fs, src.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to read file %s: %w", src.Path, err)
		}

		// Try to parse as JSON
		var jsonData interface{}
		if err := json.Unmarshal(data, &jsonData); err == nil {
			return jsonData, nil
		}

		// Return as string if not JSON
		return string(data), nil

	default:
		return nil, fmt.Errorf("unsupported input: %s", src.Input)
	}
}
