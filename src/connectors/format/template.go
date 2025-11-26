package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"text/template"
	"time"

	"github.com/creasty/defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
)

// TemplateHandler handles template operations
type TemplateHandler struct {
	slog *slog.Logger
}

// NewTemplateHandler creates a new TemplateHandler
func NewTemplateHandler(logger *slog.Logger) *TemplateHandler {
	return &TemplateHandler{
		slog: logger,
	}
}

// Validate validates the template operation configuration
func (h *TemplateHandler) Validate(op FormatOperation) error {
	var opts TemplateOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode template options: %w", err)
	}

	if opts.Template == "" && opts.TemplateFile == "" {
		return fmt.Errorf("either template or templateFile must be specified")
	}

	if opts.Template != "" && opts.TemplateFile != "" {
		return fmt.Errorf("cannot specify both template and templateFile")
	}

	if opts.MaxTemplateSize <= 0 {
		return fmt.Errorf("maxTemplateSize must be positive")
	}

	if len(opts.Template) > opts.MaxTemplateSize {
		return fmt.Errorf("template size (%d) exceeds maximum (%d)", len(opts.Template), opts.MaxTemplateSize)
	}

	validEngines := map[string]bool{
		"text": true,
		"html": true,
	}

	if opts.Engine != "" && !validEngines[opts.Engine] {
		return fmt.Errorf("invalid template engine: %s (supported: text, html)", opts.Engine)
	}

	return nil
}

// Execute executes the template operation
func (h *TemplateHandler) Execute(ctx context.Context, msg *message.RunnerMessage, op FormatOperation) error {
	var opts TemplateOptions
	if err := defaults.Set(&opts); err != nil {
		return fmt.Errorf("failed to set default values: %w", err)
	}
	if err := mapstructure.Decode(op.Options, &opts); err != nil {
		return fmt.Errorf("failed to decode template options: %w", err)
	}

	// Set defaults
	if opts.Engine == "" {
		opts.Engine = formatText
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.MaxTemplateSize == 0 {
		opts.MaxTemplateSize = 100000
	}

	// Get template content
	templateContent := opts.Template
	if opts.TemplateFile != "" {
		// Read from filesystem
		fs, err := msg.GetFilesystem()
		if err != nil {
			return fmt.Errorf("failed to get filesystem: %w", err)
		}
		if fs == nil {
			return fmt.Errorf("filesystem not available for templateFile")
		}

		data, err := fsutil.ReadFile(fs, opts.TemplateFile)
		if err != nil {
			return fmt.Errorf("failed to read template file %s: %w", opts.TemplateFile, err)
		}
		templateContent = string(data)
	}

	// Prepare template context
	metadata, err := msg.GetMetadata()
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}

	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("failed to get data: %w", err)
	}

	vars := map[string]interface{}{
		"metadata": metadata,
		"data":     string(data),
	}

	// Execute template with timeout
	execCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	var output bytes.Buffer
	errChan := make(chan error, 1)

	go func() {
		errChan <- h.executeTemplate(opts.Engine, templateContent, vars, &output)
	}()

	select {
	case <-execCtx.Done():
		return fmt.Errorf("template execution timeout")
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("template execution failed: %w", err)
		}
	}

	// Set output based on destination
	return SetOutput(msg, op.Output, output.Bytes())
}

// executeTemplate executes a template with the given engine
func (h *TemplateHandler) executeTemplate(engine, templateContent string, vars map[string]interface{}, output *bytes.Buffer) error {
	switch engine {
	case formatText:
		tmpl, err := template.New("format").Parse(templateContent)
		if err != nil {
			return fmt.Errorf("failed to parse text template: %w", err)
		}
		if err := tmpl.Execute(output, vars); err != nil {
			return fmt.Errorf("text template execution failed: %w", err)
		}
		return nil

	case "html":
		// For HTML templates, we still use text/template but could use html/template for auto-escaping
		tmpl, err := template.New("format").Parse(templateContent)
		if err != nil {
			return fmt.Errorf("failed to parse html template: %w", err)
		}
		if err := tmpl.Execute(output, vars); err != nil {
			return fmt.Errorf("html template execution failed: %w", err)
		}
		return nil

	default:
		return fmt.Errorf("unsupported template engine: %s", engine)
	}
}
