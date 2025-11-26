package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/creasty/defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/sandrolain/events-bridge/src/message"
	"gopkg.in/yaml.v3"
)

// ExtractHandler handles field extraction operations
type ExtractHandler struct {
	slog *slog.Logger
}

// NewExtractHandler creates a new ExtractHandler
func NewExtractHandler(logger *slog.Logger) *ExtractHandler {
	return &ExtractHandler{
		slog: logger,
	}
}

// Validate validates the extract operation configuration
func (h *ExtractHandler) Validate(op FormatOperation) error {
	var opts ExtractOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode extract options: %w", err)
	}

	validFormats := map[string]bool{
		"json": true,
		"yaml": true,
	}

	if !validFormats[opts.Format] {
		return fmt.Errorf("invalid format: %s (supported: json, yaml)", opts.Format)
	}

	if len(opts.Fields) == 0 {
		return fmt.Errorf("at least one field must be specified")
	}

	for i, field := range opts.Fields {
		if field.Path == "" {
			return fmt.Errorf("field %d: path is required", i)
		}

		if field.Destination == sourceMetadata && field.Key == "" {
			return fmt.Errorf("field %d: key is required when destination is metadata", i)
		}
	}

	return nil
}

// Execute executes the extract operation
func (h *ExtractHandler) Execute(ctx context.Context, msg *message.RunnerMessage, op FormatOperation) error {
	var opts ExtractOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode extract options: %w", err)
	}

	// Get input data
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("failed to get data: %w", err)
	}

	// Parse data based on format
	var dataMap map[string]interface{}
	switch opts.Format {
	case formatJSON:
		if err := json.Unmarshal(data, &dataMap); err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %w", err)
		}

	case formatYAML:
		if err := yaml.Unmarshal(data, &dataMap); err != nil {
			return fmt.Errorf("failed to unmarshal YAML: %w", err)
		}

	default:
		return fmt.Errorf("unsupported format: %s", opts.Format)
	}

	// Extract fields
	for _, field := range opts.Fields {
		value, err := h.extractValue(dataMap, field.Path)
		if err != nil {
			h.slog.Warn("failed to extract field", "path", field.Path, "error", err)
			continue
		}

		switch field.Destination {
		case sourceData:
			// Convert value to JSON and set as data
			jsonData, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("failed to marshal extracted value: %w", err)
			}
			msg.SetData(jsonData)

		case sourceMetadata:
			// Convert value to string and set as metadata
			strValue := fmt.Sprintf("%v", value)
			msg.AddMetadata(field.Key, strValue)

		default:
			return fmt.Errorf("unsupported destination: %s", field.Destination)
		}
	}

	return nil
}

// extractValue extracts a value from a map using a dot-separated path
func (h *ExtractHandler) extractValue(data map[string]interface{}, path string) (interface{}, error) {
	parts := strings.Split(path, ".")
	current := interface{}(data)

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			val, ok := v[part]
			if !ok {
				return nil, fmt.Errorf("path not found: %s", part)
			}
			current = val

		case map[interface{}]interface{}:
			// YAML sometimes uses interface{} keys
			val, ok := v[part]
			if !ok {
				return nil, fmt.Errorf("path not found: %s", part)
			}
			current = val

		default:
			return nil, fmt.Errorf("cannot traverse non-map type: %T", v)
		}
	}

	return current, nil
}
