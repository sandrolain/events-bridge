package main

import (
	"context"
	"log/slog"
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestExtractHandler_Validate(t *testing.T) {
	handler := NewExtractHandler(slog.Default())

	tests := []struct {
		name    string
		op      FormatOperation
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid json extraction",
			op: FormatOperation{
				Type:   "extract",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "json",
					"fields": []map[string]any{
						{
							"path":        "name",
							"destination": "metadata",
							"key":         "extracted_name",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid yaml extraction",
			op: FormatOperation{
				Type:   "extract",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "yaml",
					"fields": []map[string]any{
						{
							"path":        "name",
							"destination": "data",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid format",
			op: FormatOperation{
				Type:   "extract",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "invalid",
					"fields": []map[string]any{
						{
							"path": "name",
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "invalid format",
		},
		{
			name: "missing fields",
			op: FormatOperation{
				Type:   "extract",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "json",
					"fields": []map[string]any{},
				},
			},
			wantErr: true,
			errMsg:  "at least one field must be specified",
		},
		{
			name: "missing path",
			op: FormatOperation{
				Type:   "extract",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "json",
					"fields": []map[string]any{
						{
							"destination": "data",
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "path is required",
		},
		{
			name: "metadata destination without key",
			op: FormatOperation{
				Type:   "extract",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "json",
					"fields": []map[string]any{
						{
							"path":        "name",
							"destination": "metadata",
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "key is required when destination is metadata",
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

func TestExtractHandler_Execute_JSON(t *testing.T) {
	handler := NewExtractHandler(slog.Default())
	ctx := context.Background()

	t.Run("extract to metadata", func(t *testing.T) {
		op := FormatOperation{
			Type:   "extract",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"format": "json",
				"fields": []map[string]any{
					{
						"path":        "name",
						"destination": "metadata",
						"key":         "extracted_name",
					},
				},
			},
		}

		data := []byte(`{"name":"John","age":30}`)
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		metadata, _ := msg.GetMetadata()
		if metadata["extracted_name"] != "John" {
			t.Errorf("expected extracted_name='John', got %q", metadata["extracted_name"])
		}
	})

	t.Run("extract to data", func(t *testing.T) {
		op := FormatOperation{
			Type:   "extract",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"format": "json",
				"fields": []map[string]any{
					{
						"path":        "nested.value",
						"destination": "data",
					},
				},
			},
		}

		data := []byte(`{"nested":{"value":"extracted"}}`)
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, _ := msg.GetData()
		if string(result) != `"extracted"` {
			t.Errorf("expected '\"extracted\"', got %q", result)
		}
	})

	t.Run("extract nested path", func(t *testing.T) {
		op := FormatOperation{
			Type:   "extract",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"format": "json",
				"fields": []map[string]any{
					{
						"path":        "level1.level2.level3",
						"destination": "metadata",
						"key":         "deep_value",
					},
				},
			},
		}

		data := []byte(`{"level1":{"level2":{"level3":"deep"}}}`)
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		metadata, _ := msg.GetMetadata()
		if metadata["deep_value"] != "deep" {
			t.Errorf("expected deep_value='deep', got %q", metadata["deep_value"])
		}
	})

	t.Run("extract multiple fields", func(t *testing.T) {
		op := FormatOperation{
			Type:   "extract",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"format": "json",
				"fields": []map[string]any{
					{
						"path":        "name",
						"destination": "metadata",
						"key":         "name",
					},
					{
						"path":        "age",
						"destination": "metadata",
						"key":         "age",
					},
				},
			},
		}

		data := []byte(`{"name":"John","age":30}`)
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		metadata, _ := msg.GetMetadata()
		if metadata["name"] != "John" {
			t.Errorf("expected name='John', got %q", metadata["name"])
		}
		if metadata["age"] != "30" {
			t.Errorf("expected age='30', got %q", metadata["age"])
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		op := FormatOperation{
			Type:   "extract",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"format": "json",
				"fields": []map[string]any{
					{
						"path":        "name",
						"destination": "metadata",
						"key":         "name",
					},
				},
			},
		}

		data := []byte(`{invalid json}`)
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err == nil {
			t.Error("expected error for invalid JSON, got nil")
		}
	})

	t.Run("path not found", func(t *testing.T) {
		op := FormatOperation{
			Type:   "extract",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"format": "json",
				"fields": []map[string]any{
					{
						"path":        "nonexistent",
						"destination": "metadata",
						"key":         "value",
					},
				},
			},
		}

		data := []byte(`{"name":"John"}`)
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		// Path not found should log warning but not fail
		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestExtractHandler_Execute_YAML(t *testing.T) {
	handler := NewExtractHandler(slog.Default())
	ctx := context.Background()

	t.Run("extract from yaml", func(t *testing.T) {
		op := FormatOperation{
			Type:   "extract",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"format": "yaml",
				"fields": []map[string]any{
					{
						"path":        "name",
						"destination": "metadata",
						"key":         "name",
					},
				},
			},
		}

		data := []byte("name: John\nage: 30\n")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		metadata, _ := msg.GetMetadata()
		if metadata["name"] != "John" {
			t.Errorf("expected name='John', got %q", metadata["name"])
		}
	})

	t.Run("extract nested yaml", func(t *testing.T) {
		op := FormatOperation{
			Type:   "extract",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"format": "yaml",
				"fields": []map[string]any{
					{
						"path":        "nested.value",
						"destination": "metadata",
						"key":         "nested_value",
					},
				},
			},
		}

		data := []byte("nested:\n  value: extracted\n")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		metadata, _ := msg.GetMetadata()
		if metadata["nested_value"] != "extracted" {
			t.Errorf("expected nested_value='extracted', got %q", metadata["nested_value"])
		}
	})

	t.Run("invalid yaml", func(t *testing.T) {
		op := FormatOperation{
			Type:   "extract",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"format": "yaml",
				"fields": []map[string]any{
					{
						"path":        "name",
						"destination": "metadata",
						"key":         "name",
					},
				},
			},
		}

		data := []byte(":\n  invalid: yaml\n    - broken")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err == nil {
			t.Error("expected error for invalid YAML, got nil")
		}
	})
}

func TestExtractHandler_Execute_UnsupportedFormat(t *testing.T) {
	handler := NewExtractHandler(slog.Default())
	ctx := context.Background()

	// Bypass validation to test execute with unsupported format
	op := FormatOperation{
		Type:   "extract",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"format": "unsupported",
			"fields": []map[string]any{
				{
					"path":        "name",
					"destination": "metadata",
					"key":         "name",
				},
			},
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte(`{"name":"test"}`), nil))

	err := handler.Execute(ctx, msg, op)
	if err == nil {
		t.Error("expected error for unsupported format, got nil")
	}
}

func TestExtractHandler_Execute_UnsupportedDestination(t *testing.T) {
	handler := NewExtractHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "extract",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"format": "json",
			"fields": []map[string]any{
				{
					"path":        "name",
					"destination": "unsupported",
				},
			},
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte(`{"name":"test"}`), nil))

	err := handler.Execute(ctx, msg, op)
	if err == nil {
		t.Error("expected error for unsupported destination, got nil")
	}
}

func TestExtractHandler_ExtractValue_NonMapType(t *testing.T) {
	handler := NewExtractHandler(slog.Default())

	data := map[string]interface{}{
		"array": []string{"a", "b", "c"},
	}

	// Try to traverse into an array (which is not a map)
	_, err := handler.extractValue(data, "array.0")
	if err == nil {
		t.Error("expected error when traversing non-map type")
	}
}

func TestExtractHandler_ExtractValue_InterfaceKeys(t *testing.T) {
	handler := NewExtractHandler(slog.Default())

	// YAML sometimes produces map[interface{}]interface{} types
	data := map[string]interface{}{
		"nested": map[interface{}]interface{}{
			"value": "test",
		},
	}

	result, err := handler.extractValue(data, "nested.value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != "test" {
		t.Errorf("expected 'test', got %v", result)
	}
}
