package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestMultipartHandler_Validate(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())

	tests := []struct {
		name    string
		op      FormatOperation
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid data source part",
			op: FormatOperation{
				Type:   "multipart",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"parts": []map[string]any{
						{
							"name":   "data_part",
							"source": "data",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid metadata source part",
			op: FormatOperation{
				Type:   "multipart",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"parts": []map[string]any{
						{
							"name":   "metadata_part",
							"source": "metadata",
							"keys":   []string{"key1", "key2"},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid filesystem source part",
			op: FormatOperation{
				Type:   "multipart",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"parts": []map[string]any{
						{
							"name":   "file_part",
							"source": "filesystem",
							"path":   "/path/to/file.txt",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid template source part",
			op: FormatOperation{
				Type:   "multipart",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"parts": []map[string]any{
						{
							"name":     "template_part",
							"source":   "template",
							"template": "Hello {{ .data }}",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "missing parts",
			op: FormatOperation{
				Type:   "multipart",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"parts": []map[string]any{},
				},
			},
			wantErr: true,
			errMsg:  "at least one part must be specified",
		},
		{
			name: "missing part name",
			op: FormatOperation{
				Type:   "multipart",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"parts": []map[string]any{
						{
							"source": "data",
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "name is required",
		},
		{
			name: "invalid part source",
			op: FormatOperation{
				Type:   "multipart",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"parts": []map[string]any{
						{
							"name":   "part1",
							"source": "invalid",
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "invalid source",
		},
		{
			name: "filesystem source without path",
			op: FormatOperation{
				Type:   "multipart",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"parts": []map[string]any{
						{
							"name":   "file_part",
							"source": "filesystem",
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "path is required for filesystem source",
		},
		{
			name: "template source without template",
			op: FormatOperation{
				Type:   "multipart",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"parts": []map[string]any{
						{
							"name":   "template_part",
							"source": "template",
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "template is required for template source",
		},
		{
			name: "metadata source without keys (warning only)",
			op: FormatOperation{
				Type:   "multipart",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"parts": []map[string]any{
						{
							"name":   "metadata_part",
							"source": "metadata",
						},
					},
				},
			},
			wantErr: false, // This is a warning, not an error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handler.Validate(tt.op)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errMsg)
				} else if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestMultipartHandler_Execute_DataSource(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "multipart",
		Input:  "data",
		Output: "parts",
		Options: map[string]any{
			"parts": []map[string]any{
				{
					"name":        "data_part",
					"source":      "data",
					"contentType": "text/plain",
				},
			},
		},
	}

	data := []byte("Hello, World!")
	msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parts := msg.GetParts()
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}

	if parts[0].Name != "data_part" {
		t.Errorf("expected part name 'data_part', got %q", parts[0].Name)
	}

	if string(parts[0].Data) != string(data) {
		t.Errorf("expected part data %q, got %q", data, parts[0].Data)
	}
}

func TestMultipartHandler_Execute_MetadataSource(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())
	ctx := context.Background()

	t.Run("metadata as JSON", func(t *testing.T) {
		op := FormatOperation{
			Type:   "multipart",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"parts": []map[string]any{
					{
						"name":   "metadata_part",
						"source": "metadata",
						"format": "json",
					},
				},
			},
		}

		metadata := map[string]string{"key1": "value1", "key2": "value2"}
		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), metadata))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if len(parts) != 1 {
			t.Fatalf("expected 1 part, got %d", len(parts))
		}

		// Verify it's valid JSON
		var result map[string]string
		if err := json.Unmarshal(parts[0].Data, &result); err != nil {
			t.Fatalf("part data is not valid JSON: %v", err)
		}

		if result["key1"] != "value1" || result["key2"] != "value2" {
			t.Errorf("unexpected metadata values: %v", result)
		}
	})

	t.Run("metadata with key filtering", func(t *testing.T) {
		op := FormatOperation{
			Type:   "multipart",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"parts": []map[string]any{
					{
						"name":   "metadata_part",
						"source": "metadata",
						"keys":   []any{"key1"},
						"format": "json",
					},
				},
			},
		}

		metadata := map[string]string{"key1": "value1", "key2": "value2"}
		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), metadata))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		var result map[string]string
		if err := json.Unmarshal(parts[0].Data, &result); err != nil {
			t.Fatalf("part data is not valid JSON: %v", err)
		}

		if _, ok := result["key1"]; !ok {
			t.Error("expected key1 in filtered metadata")
		}
		if _, ok := result["key2"]; ok {
			t.Error("did not expect key2 in filtered metadata")
		}
	})

	t.Run("metadata as YAML", func(t *testing.T) {
		op := FormatOperation{
			Type:   "multipart",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"parts": []map[string]any{
					{
						"name":   "metadata_part",
						"source": "metadata",
						"format": "yaml",
					},
				},
			},
		}

		metadata := map[string]string{"key1": "value1"}
		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), metadata))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if len(parts) != 1 {
			t.Fatalf("expected 1 part, got %d", len(parts))
		}
	})

	t.Run("metadata as text", func(t *testing.T) {
		op := FormatOperation{
			Type:   "multipart",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"parts": []map[string]any{
					{
						"name":   "metadata_part",
						"source": "metadata",
						"format": "text",
					},
				},
			},
		}

		metadata := map[string]string{"key1": "value1"}
		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), metadata))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if len(parts) != 1 {
			t.Fatalf("expected 1 part, got %d", len(parts))
		}

		// Text format should contain key=value
		if !contains(string(parts[0].Data), "key1=value1") {
			t.Errorf("expected text format with 'key1=value1', got %q", parts[0].Data)
		}
	})
}

func TestMultipartHandler_Execute_TemplateSource(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "multipart",
		Input:  "data",
		Output: "parts",
		Options: map[string]any{
			"parts": []map[string]any{
				{
					"name":     "template_part",
					"source":   "template",
					"template": "Data: {{ .data }}",
				},
			},
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("test"), nil))

	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parts := msg.GetParts()
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}

	expected := "Data: test"
	if string(parts[0].Data) != expected {
		t.Errorf("expected %q, got %q", expected, parts[0].Data)
	}
}

func TestMultipartHandler_Execute_ClearExisting(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())
	ctx := context.Background()

	// First, add some existing parts
	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), nil))
	msg.AddPart(message.Part{Name: "existing_part", Data: []byte("existing")})

	// Execute with clearExisting = true
	op := FormatOperation{
		Type:   "multipart",
		Input:  "data",
		Output: "parts",
		Options: map[string]any{
			"clearExisting": true,
			"parts": []map[string]any{
				{
					"name":   "new_part",
					"source": "data",
				},
			},
		},
	}

	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parts := msg.GetParts()
	if len(parts) != 1 {
		t.Fatalf("expected 1 part after clear, got %d", len(parts))
	}

	if parts[0].Name != "new_part" {
		t.Errorf("expected new_part, got %q", parts[0].Name)
	}
}

func TestMultipartHandler_Execute_WithCompression(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "multipart",
		Input:  "data",
		Output: "parts",
		Options: map[string]any{
			"parts": []map[string]any{
				{
					"name":     "compressed_part",
					"source":   "data",
					"compress": true,
				},
			},
		},
	}

	original := []byte("Hello, World! This is some data to compress.")
	msg := message.NewRunnerMessage(testutil.NewAdapter(original, nil))

	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parts := msg.GetParts()
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}

	// Compressed data should be different from original
	if string(parts[0].Data) == string(original) {
		t.Error("expected compressed data to be different from original")
	}
}

func TestMultipartHandler_Execute_MultipleParts(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "multipart",
		Input:  "data",
		Output: "parts",
		Options: map[string]any{
			"parts": []map[string]any{
				{
					"name":        "part1",
					"source":      "data",
					"contentType": "text/plain",
				},
				{
					"name":   "part2",
					"source": "metadata",
					"format": "json",
				},
				{
					"name":     "part3",
					"source":   "template",
					"template": "Generated content",
				},
			},
		},
	}

	metadata := map[string]string{"key": "value"}
	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), metadata))

	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parts := msg.GetParts()
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}

	// Verify part names
	names := make(map[string]bool)
	for _, p := range parts {
		names[p.Name] = true
	}

	if !names["part1"] || !names["part2"] || !names["part3"] {
		t.Error("not all expected parts were created")
	}
}

func TestMultipartHandler_Execute_WithHeaders(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "multipart",
		Input:  "data",
		Output: "parts",
		Options: map[string]any{
			"parts": []map[string]any{
				{
					"name":        "part_with_headers",
					"source":      "data",
					"filename":    "file.txt",
					"contentType": "text/plain",
					"headers": map[string]any{
						"X-Custom-Header": "custom-value",
					},
				},
			},
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), nil))

	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parts := msg.GetParts()
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}

	if parts[0].Filename != "file.txt" {
		t.Errorf("expected filename 'file.txt', got %q", parts[0].Filename)
	}

	if parts[0].ContentType != "text/plain" {
		t.Errorf("expected content type 'text/plain', got %q", parts[0].ContentType)
	}
}

func TestMultipartHandler_Execute_FilesystemSource_NoFilesystem(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "multipart",
		Input:  "data",
		Output: "parts",
		Options: map[string]any{
			"parts": []map[string]any{
				{
					"name":   "file_part",
					"source": "filesystem",
					"path":   "/path/to/file.txt",
				},
			},
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), nil))

	err := handler.Execute(ctx, msg, op)
	if err == nil {
		t.Error("expected error when filesystem is not available")
	}
}

func TestMultipartHandler_SerializeMetadata_UnsupportedFormat(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())

	metadata := map[string]string{"key": "value"}
	_, err := handler.serializeMetadata(metadata, "unsupported")
	if err == nil {
		t.Error("expected error for unsupported metadata format")
	}
}

func TestMultipartHandler_Execute_UnsupportedSource(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())

	// This would require bypassing validation, but we test the createPart directly
	partCfg := PartConfig{
		Name:   "test",
		Source: "unsupported",
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), nil))
	_, err := handler.createPart(msg, partCfg)
	if err == nil {
		t.Error("expected error for unsupported source")
	}
}

func TestMultipartHandler_Execute_TemplateError(t *testing.T) {
	handler := NewMultipartHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "multipart",
		Input:  "data",
		Output: "parts",
		Options: map[string]any{
			"parts": []map[string]any{
				{
					"name":     "template_part",
					"source":   "template",
					"template": "{{ .invalid syntax",
				},
			},
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), nil))

	err := handler.Execute(ctx, msg, op)
	if err == nil {
		t.Error("expected error for invalid template")
	}
}
