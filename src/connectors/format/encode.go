package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/creasty/defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/sandrolain/events-bridge/src/message"
)

// EncodeHandler handles encoding/decoding operations
type EncodeHandler struct {
	slog *slog.Logger
}

// NewEncodeHandler creates a new EncodeHandler
func NewEncodeHandler(logger *slog.Logger) *EncodeHandler {
	return &EncodeHandler{
		slog: logger,
	}
}

// Validate validates the encode operation configuration
func (h *EncodeHandler) Validate(op FormatOperation) error {
	var opts EncodeOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode encode options: %w", err)
	}

	validEncodings := map[string]bool{
		"base64":    true,
		"base64url": true,
		"hex":       true,
		"url":       true,
	}

	if !validEncodings[opts.Encoding] {
		return fmt.Errorf("invalid encoding: %s", opts.Encoding)
	}

	validOperations := map[string]bool{
		"encode": true,
		"decode": true,
	}

	if opts.Operation != "" && !validOperations[opts.Operation] {
		return fmt.Errorf("invalid operation: %s", opts.Operation)
	}

	return nil
}

// Execute executes the encode operation
func (h *EncodeHandler) Execute(ctx context.Context, msg *message.RunnerMessage, op FormatOperation) error {
	var opts EncodeOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode encode options: %w", err)
	}

	// Get input data
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("failed to get data: %w", err)
	}

	var output []byte

	// Perform encoding/decoding
	switch opts.Operation {
	case "encode":
		output, err = h.encode(data, opts.Encoding)
	case "decode":
		output, err = h.decode(data, opts.Encoding)
	default:
		return fmt.Errorf("unsupported operation: %s", opts.Operation)
	}

	if err != nil {
		return err
	}

	// Set output
	return SetOutput(msg, op.Output, output)
}

// encode encodes data using the specified encoding
func (h *EncodeHandler) encode(data []byte, encoding string) ([]byte, error) {
	switch encoding {
	case "base64":
		encoded := base64.StdEncoding.EncodeToString(data)
		return []byte(encoded), nil

	case "base64url":
		encoded := base64.URLEncoding.EncodeToString(data)
		return []byte(encoded), nil

	case "hex":
		encoded := hex.EncodeToString(data)
		return []byte(encoded), nil

	case "url":
		encoded := url.QueryEscape(string(data))
		return []byte(encoded), nil

	default:
		return nil, fmt.Errorf("unsupported encoding: %s", encoding)
	}
}

// decode decodes data using the specified encoding
func (h *EncodeHandler) decode(data []byte, encoding string) ([]byte, error) {
	switch encoding {
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(string(data))
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64: %w", err)
		}
		return decoded, nil

	case "base64url":
		decoded, err := base64.URLEncoding.DecodeString(string(data))
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64url: %w", err)
		}
		return decoded, nil

	case "hex":
		decoded, err := hex.DecodeString(string(data))
		if err != nil {
			return nil, fmt.Errorf("failed to decode hex: %w", err)
		}
		return decoded, nil

	case "url":
		decoded, err := url.QueryUnescape(string(data))
		if err != nil {
			return nil, fmt.Errorf("failed to decode url: %w", err)
		}
		return []byte(decoded), nil

	default:
		return nil, fmt.Errorf("unsupported encoding: %s", encoding)
	}
}
