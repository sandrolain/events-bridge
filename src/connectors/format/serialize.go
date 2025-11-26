package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/creasty/defaults"
	"github.com/fxamacker/cbor/v2"
	"github.com/mitchellh/mapstructure"
	"github.com/sandrolain/events-bridge/src/message"
	"gopkg.in/yaml.v3"
)

// SerializeHandler handles serialization operations
type SerializeHandler struct {
	slog *slog.Logger
}

// NewSerializeHandler creates a new SerializeHandler
func NewSerializeHandler(logger *slog.Logger) *SerializeHandler {
	return &SerializeHandler{
		slog: logger,
	}
}

// Validate validates the serialize operation configuration
func (h *SerializeHandler) Validate(op FormatOperation) error {
	var opts SerializeOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode serialize options: %w", err)
	}

	validFormats := map[string]bool{
		"json":    true,
		"yaml":    true,
		"cbor":    true,
		"msgpack": true,
	}

	if !validFormats[opts.From] {
		return fmt.Errorf("invalid source format: %s", opts.From)
	}

	if !validFormats[opts.To] {
		return fmt.Errorf("invalid target format: %s", opts.To)
	}

	return nil
}

// Execute executes the serialize operation
func (h *SerializeHandler) Execute(ctx context.Context, msg *message.RunnerMessage, op FormatOperation) error {
	var opts SerializeOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode serialize options: %w", err)
	}

	// Get input data
	data, err := GetInput(msg, op.Input)
	if err != nil {
		return fmt.Errorf("failed to get input: %w", err)
	}

	if op.Input == sourceData {
		data, err = msg.GetData()
		if err != nil {
			return fmt.Errorf("failed to get data: %w", err)
		}
	}

	// Decode from source format
	var intermediate interface{}
	switch opts.From {
	case formatJSON:
		if err := json.Unmarshal(data, &intermediate); err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %w", err)
		}

	case formatYAML:
		if err := yaml.Unmarshal(data, &intermediate); err != nil {
			return fmt.Errorf("failed to unmarshal YAML: %w", err)
		}

	case "cbor":
		if err := cbor.Unmarshal(data, &intermediate); err != nil {
			return fmt.Errorf("failed to unmarshal CBOR: %w", err)
		}

	case "msgpack":
		// msgpack support would require additional dependency
		return fmt.Errorf("msgpack format not yet implemented")

	default:
		return fmt.Errorf("unsupported source format: %s", opts.From)
	}

	// Encode to target format
	var output []byte
	switch opts.To {
	case formatJSON:
		if opts.Pretty {
			output, err = json.MarshalIndent(intermediate, "", opts.Indent)
		} else {
			output, err = json.Marshal(intermediate)
		}
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}

	case formatYAML:
		var buf bytes.Buffer
		encoder := yaml.NewEncoder(&buf)
		encoder.SetIndent(len(opts.Indent))
		if err := encoder.Encode(intermediate); err != nil {
			return fmt.Errorf("failed to marshal YAML: %w", err)
		}
		output = buf.Bytes()

	case "cbor":
		output, err = cbor.Marshal(intermediate)
		if err != nil {
			return fmt.Errorf("failed to marshal CBOR: %w", err)
		}

	case "msgpack":
		return fmt.Errorf("msgpack format not yet implemented")

	default:
		return fmt.Errorf("unsupported target format: %s", opts.To)
	}

	// Set output
	return SetOutput(msg, op.Output, output)
}
