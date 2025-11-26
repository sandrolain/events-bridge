package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"text/template"

	"github.com/creasty/defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
	"gopkg.in/yaml.v3"
)

// MultipartHandler handles multipart composition operations
type MultipartHandler struct {
	slog *slog.Logger
}

// NewMultipartHandler creates a new MultipartHandler
func NewMultipartHandler(logger *slog.Logger) *MultipartHandler {
	return &MultipartHandler{
		slog: logger,
	}
}

// Validate validates the multipart operation configuration
func (h *MultipartHandler) Validate(op FormatOperation) error {
	var opts MultipartOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode multipart options: %w", err)
	}

	if len(opts.Parts) == 0 {
		return fmt.Errorf("at least one part must be specified")
	}

	for i, part := range opts.Parts {
		if part.Name == "" {
			return fmt.Errorf("part %d: name is required", i)
		}

		validSources := map[string]bool{
			"data":       true,
			"metadata":   true,
			"filesystem": true,
			"template":   true,
		}

		if !validSources[part.Source] {
			return fmt.Errorf("part %d: invalid source: %s", i, part.Source)
		}

		if part.Source == "filesystem" && part.Path == "" {
			return fmt.Errorf("part %d: path is required for filesystem source", i)
		}

		if part.Source == "template" && part.Template == "" {
			return fmt.Errorf("part %d: template is required for template source", i)
		}

		if part.Source == "metadata" && len(part.Keys) == 0 {
			h.slog.Warn("part has metadata source but no keys specified, will include all metadata", "part", i)
		}
	}

	return nil
}

// Execute executes the multipart operation
func (h *MultipartHandler) Execute(ctx context.Context, msg *message.RunnerMessage, op FormatOperation) error {
	var opts MultipartOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode multipart options: %w", err)
	}

	// Clear existing parts if requested
	if opts.ClearExisting {
		msg.ClearParts()
	}

	// Process each part configuration
	for i, partCfg := range opts.Parts {
		part, err := h.createPart(msg, partCfg)
		if err != nil {
			return fmt.Errorf("failed to create part %d (%s): %w", i, partCfg.Name, err)
		}

		msg.AddPart(part)
	}

	return nil
}

// createPart creates a message part from configuration
func (h *MultipartHandler) createPart(msg *message.RunnerMessage, cfg PartConfig) (message.Part, error) {
	var data []byte
	var err error

	// Get data from source
	switch cfg.Source {
	case "data":
		data, err = msg.GetData()
		if err != nil {
			return message.Part{}, fmt.Errorf("failed to get message data: %w", err)
		}

	case "metadata":
		metadata, err := msg.GetMetadata()
		if err != nil {
			return message.Part{}, fmt.Errorf("failed to get metadata: %w", err)
		}

		// Filter by keys if specified
		if len(cfg.Keys) > 0 {
			filtered := make(map[string]string)
			for _, key := range cfg.Keys {
				if val, ok := metadata[key]; ok {
					filtered[key] = val
				}
			}
			metadata = filtered
		}

		// Serialize metadata based on format
		data, err = h.serializeMetadata(metadata, cfg.Format)
		if err != nil {
			return message.Part{}, fmt.Errorf("failed to serialize metadata: %w", err)
		}

	case "filesystem":
		fs, err := msg.GetFilesystem()
		if err != nil {
			return message.Part{}, fmt.Errorf("failed to get filesystem: %w", err)
		}
		if fs == nil {
			return message.Part{}, fmt.Errorf("filesystem not available")
		}

		data, err = fsutil.ReadFile(fs, cfg.Path)
		if err != nil {
			return message.Part{}, fmt.Errorf("failed to read file %s: %w", cfg.Path, err)
		}

	case "template":
		data, err = h.executeTemplate(msg, cfg.Template)
		if err != nil {
			return message.Part{}, fmt.Errorf("failed to execute template: %w", err)
		}

	default:
		return message.Part{}, fmt.Errorf("unsupported source: %s", cfg.Source)
	}

	// Compress if requested
	if cfg.Compress {
		data, err = h.compressData(data)
		if err != nil {
			return message.Part{}, fmt.Errorf("failed to compress data: %w", err)
		}
	}

	// Create part
	part := message.Part{
		Name:        cfg.Name,
		Filename:    cfg.Filename,
		ContentType: cfg.ContentType,
		Headers:     cfg.Headers,
		Data:        data,
	}

	return part, nil
}

// serializeMetadata serializes metadata to the specified format
func (h *MultipartHandler) serializeMetadata(metadata map[string]string, format string) ([]byte, error) {
	switch format {
	case "json", "":
		return json.Marshal(metadata)

	case "yaml":
		return yaml.Marshal(metadata)

	case "text":
		var buf bytes.Buffer
		for k, v := range metadata {
			buf.WriteString(k)
			buf.WriteString("=")
			buf.WriteString(v)
			buf.WriteString("\n")
		}
		return buf.Bytes(), nil

	default:
		return nil, fmt.Errorf("unsupported metadata format: %s", format)
	}
}

// executeTemplate executes an inline template
func (h *MultipartHandler) executeTemplate(msg *message.RunnerMessage, templateContent string) ([]byte, error) {
	tmpl, err := template.New("part").Parse(templateContent)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}

	metadata, err := msg.GetMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}
	data, err := msg.GetData()
	if err != nil {
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	vars := map[string]interface{}{
		"metadata": metadata,
		"data":     string(data),
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, vars); err != nil {
		return nil, fmt.Errorf("template execution failed: %w", err)
	}

	return buf.Bytes(), nil
}

// compressData compresses data using gzip
func (h *MultipartHandler) compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	if _, err := writer.Write(data); err != nil {
		_ = writer.Close() //nolint:errcheck
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
