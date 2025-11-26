package main

import (
	"context"
	"fmt"

	"github.com/sandrolain/events-bridge/src/message"
)

// Constants for common source/destination values
const (
	sourceData       = "data"
	sourceMetadata   = "metadata"
	sourceFilesystem = "filesystem"
	sourceParts      = "parts"
)

// Constants for format types
const (
	formatJSON = "json"
	formatYAML = "yaml"
	formatText = "text"
)

// Constants for MIME types
const (
	mimeTypeTextPlain = "text/plain"
)

// OperationHandler defines the interface for operation handlers
type OperationHandler interface {
	// Execute performs the operation on the message
	Execute(ctx context.Context, msg *message.RunnerMessage, op FormatOperation) error

	// Validate checks if the operation configuration is valid
	Validate(op FormatOperation) error
}

// GetInput retrieves input data based on the input source specification
func GetInput(msg *message.RunnerMessage, inputSource string) ([]byte, error) {
	switch inputSource {
	case sourceData:
		return msg.GetData()
	case sourceMetadata:
		meta, err := msg.GetMetadata()
		if err != nil {
			return nil, fmt.Errorf("failed to get metadata: %w", err)
		}
		// Return metadata as bytes (will be serialized by caller if needed)
		return []byte(fmt.Sprintf("%v", meta)), nil
	case sourceFilesystem:
		// For filesystem, return empty - handlers must access filesystem directly
		return nil, nil
	case sourceParts:
		// For parts, return empty - handlers must access parts directly
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown input source: %s", inputSource)
	}
}

// SetOutput sets output data based on the output destination specification
func SetOutput(msg *message.RunnerMessage, outputDest string, data []byte) error {
	switch outputDest {
	case sourceData:
		msg.SetData(data)
		return nil
	case sourceMetadata:
		// For metadata output, handlers must set metadata directly
		return fmt.Errorf("metadata output must be set by handler directly")
	case sourceFilesystem:
		// For filesystem output, handlers must write to filesystem directly
		return fmt.Errorf("filesystem output must be written by handler directly")
	case sourceParts:
		// For parts output, handlers must add parts directly
		return fmt.Errorf("parts output must be added by handler directly")
	default:
		return fmt.Errorf("unknown output destination: %s", outputDest)
	}
}

// ValidateOperation performs basic validation on operation configuration
func ValidateOperation(op FormatOperation) error {
	if op.Type == "" {
		return fmt.Errorf("operation type is required")
	}

	validTypes := map[string]bool{
		"template":  true,
		"serialize": true,
		"multipart": true,
		"encode":    true,
		"compress":  true,
		"extract":   true,
		"merge":     true,
		"split":     true,
	}

	if !validTypes[op.Type] {
		return fmt.Errorf("invalid operation type: %s", op.Type)
	}

	validSources := map[string]bool{
		sourceData:       true,
		sourceMetadata:   true,
		sourceFilesystem: true,
		sourceParts:      true,
	}

	if op.Input != "" && !validSources[op.Input] {
		return fmt.Errorf("invalid input source: %s", op.Input)
	}

	if op.Output != "" && !validSources[op.Output] {
		return fmt.Errorf("invalid output destination: %s", op.Output)
	}

	return nil
}
