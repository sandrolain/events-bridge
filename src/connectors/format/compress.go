package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/creasty/defaults"
	"github.com/klauspost/compress/zstd"
	"github.com/mitchellh/mapstructure"
	"github.com/sandrolain/events-bridge/src/message"
)

// CompressHandler handles compression/decompression operations
type CompressHandler struct {
	slog *slog.Logger
}

// NewCompressHandler creates a new CompressHandler
func NewCompressHandler(logger *slog.Logger) *CompressHandler {
	return &CompressHandler{
		slog: logger,
	}
}

// Validate validates the compress operation configuration
func (h *CompressHandler) Validate(op FormatOperation) error {
	var opts CompressOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode compress options: %w", err)
	}

	validAlgorithms := map[string]bool{
		"gzip": true,
		"zstd": true,
	}

	if !validAlgorithms[opts.Algorithm] {
		return fmt.Errorf("invalid algorithm: %s (supported: gzip, zstd)", opts.Algorithm)
	}

	validOperations := map[string]bool{
		"compress":   true,
		"decompress": true,
	}

	if opts.Operation != "" && !validOperations[opts.Operation] {
		return fmt.Errorf("invalid operation: %s", opts.Operation)
	}

	if opts.Level < -1 || opts.Level > 9 {
		return fmt.Errorf("invalid compression level: %d (must be -1 to 9)", opts.Level)
	}

	return nil
}

// Execute executes the compress operation
func (h *CompressHandler) Execute(ctx context.Context, msg *message.RunnerMessage, op FormatOperation) error {
	var opts CompressOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode compress options: %w", err)
	}

	// Get input data
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("failed to get data: %w", err)
	}

	var output []byte

	// Perform compression/decompression
	switch opts.Operation {
	case "compress":
		output, err = h.compress(data, opts.Algorithm, opts.Level)
	case "decompress":
		output, err = h.decompress(data, opts.Algorithm)
	default:
		return fmt.Errorf("unsupported operation: %s", opts.Operation)
	}

	if err != nil {
		return err
	}

	// Set output
	return SetOutput(msg, op.Output, output)
}

// compress compresses data using the specified algorithm
func (h *CompressHandler) compress(data []byte, algorithm string, level int) ([]byte, error) {
	var buf bytes.Buffer

	switch algorithm {
	case "gzip":
		var writer *gzip.Writer
		var err error

		if level == -1 {
			writer = gzip.NewWriter(&buf)
		} else {
			writer, err = gzip.NewWriterLevel(&buf, level)
			if err != nil {
				return nil, fmt.Errorf("failed to create gzip writer: %w", err)
			}
		}

		if _, err := writer.Write(data); err != nil {
			_ = writer.Close() //nolint:errcheck
			return nil, fmt.Errorf("failed to write gzip data: %w", err)
		}

		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("failed to close gzip writer: %w", err)
		}

		return buf.Bytes(), nil

	case "zstd":
		var encoderLevel zstd.EncoderLevel
		switch level {
		case -1:
			encoderLevel = zstd.SpeedFastest
		case 1, 2, 3:
			encoderLevel = zstd.SpeedFastest
		case 4, 5, 6:
			encoderLevel = zstd.SpeedDefault
		case 7, 8, 9:
			encoderLevel = zstd.SpeedBetterCompression
		default:
			encoderLevel = zstd.SpeedDefault
		}

		encoder, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(encoderLevel))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
		}

		if _, err := encoder.Write(data); err != nil {
			_ = encoder.Close() //nolint:errcheck
			return nil, fmt.Errorf("failed to write zstd data: %w", err)
		}

		if err := encoder.Close(); err != nil {
			return nil, fmt.Errorf("failed to close zstd encoder: %w", err)
		}

		return buf.Bytes(), nil

	default:
		return nil, fmt.Errorf("unsupported algorithm: %s", algorithm)
	}
}

// decompress decompresses data using the specified algorithm
func (h *CompressHandler) decompress(data []byte, algorithm string) ([]byte, error) {
	reader := bytes.NewReader(data)

	switch algorithm {
	case "gzip":
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close() //nolint:errcheck

		output, err := io.ReadAll(gzReader)
		if err != nil {
			return nil, fmt.Errorf("failed to read gzip data: %w", err)
		}

		return output, nil

	case "zstd":
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
		defer decoder.Close()

		output, err := io.ReadAll(decoder)
		if err != nil {
			return nil, fmt.Errorf("failed to read zstd data: %w", err)
		}

		return output, nil

	default:
		return nil, fmt.Errorf("unsupported algorithm: %s", algorithm)
	}
}
