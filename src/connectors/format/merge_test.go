package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestMergeHandler_Validate(t *testing.T) {
	handler := NewMergeHandler(slog.Default())

	tests := []struct {
		name    string
		op      FormatOperation
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid data source",
			op: FormatOperation{
				Type:   "merge",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "json",
					"sources": []map[string]any{
						{
							"input": "data",
							"key":   "data_key",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid metadata source",
			op: FormatOperation{
				Type:   "merge",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "json",
					"sources": []map[string]any{
						{
							"input": "metadata",
							"key":   "meta_key",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid filesystem source",
			op: FormatOperation{
				Type:   "merge",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "json",
					"sources": []map[string]any{
						{
							"input": "filesystem",
							"key":   "file_key",
							"path":  "/path/to/file.json",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "missing sources",
			op: FormatOperation{
				Type:   "merge",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format":  "json",
					"sources": []map[string]any{},
				},
			},
			wantErr: true,
			errMsg:  "at least one source must be specified",
		},
		{
			name: "missing key",
			op: FormatOperation{
				Type:   "merge",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "json",
					"sources": []map[string]any{
						{
							"input": "data",
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "key is required",
		},
		{
			name: "invalid format",
			op: FormatOperation{
				Type:   "merge",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "invalid",
					"sources": []map[string]any{
						{
							"input": "data",
							"key":   "key",
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "invalid format",
		},
		{
			name: "invalid input",
			op: FormatOperation{
				Type:   "merge",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "json",
					"sources": []map[string]any{
						{
							"input": "invalid",
							"key":   "key",
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "invalid input",
		},
		{
			name: "filesystem without path",
			op: FormatOperation{
				Type:   "merge",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"format": "json",
					"sources": []map[string]any{
						{
							"input": "filesystem",
							"key":   "key",
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "path is required for filesystem input",
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

func TestMergeHandler_Execute_DataSource(t *testing.T) {
	handler := NewMergeHandler(slog.Default())
	ctx := context.Background()

	t.Run("merge json data", func(t *testing.T) {
		op := FormatOperation{
			Type:   "merge",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"format": "json",
				"sources": []map[string]any{
					{
						"input": "data",
						"key":   "original",
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

		result, _ := msg.GetData()
		var merged map[string]interface{}
		if err := json.Unmarshal(result, &merged); err != nil {
			t.Fatalf("result is not valid JSON: %v", err)
		}

		original, ok := merged["original"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected 'original' key with map value")
		}

		if original["name"] != "John" {
			t.Errorf("expected name='John', got %v", original["name"])
		}
	})

	t.Run("merge non-json data as string", func(t *testing.T) {
		op := FormatOperation{
			Type:   "merge",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"format": "json",
				"sources": []map[string]any{
					{
						"input": "data",
						"key":   "text",
					},
				},
			},
		}

		data := []byte("plain text data")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, _ := msg.GetData()
		var merged map[string]interface{}
		if err := json.Unmarshal(result, &merged); err != nil {
			t.Fatalf("result is not valid JSON: %v", err)
		}

		if merged["text"] != "plain text data" {
			t.Errorf("expected text='plain text data', got %v", merged["text"])
		}
	})
}

func TestMergeHandler_Execute_MetadataSource(t *testing.T) {
	handler := NewMergeHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "merge",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"format": "json",
			"sources": []map[string]any{
				{
					"input": "metadata",
					"key":   "meta",
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

	result, _ := msg.GetData()
	var merged map[string]interface{}
	if err := json.Unmarshal(result, &merged); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}

	meta, ok := merged["meta"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected 'meta' key with map value")
	}

	if meta["key1"] != "value1" {
		t.Errorf("expected key1='value1', got %v", meta["key1"])
	}
}

func TestMergeHandler_Execute_MultipleSources(t *testing.T) {
	handler := NewMergeHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "merge",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"format": "json",
			"sources": []map[string]any{
				{
					"input": "data",
					"key":   "payload",
				},
				{
					"input": "metadata",
					"key":   "headers",
				},
			},
		},
	}

	data := []byte(`{"value":"test"}`)
	metadata := map[string]string{"content-type": "application/json"}
	msg := message.NewRunnerMessage(testutil.NewAdapter(data, metadata))

	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, _ := msg.GetData()
	var merged map[string]interface{}
	if err := json.Unmarshal(result, &merged); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}

	if _, ok := merged["payload"]; !ok {
		t.Error("expected 'payload' key in merged result")
	}
	if _, ok := merged["headers"]; !ok {
		t.Error("expected 'headers' key in merged result")
	}
}

func TestMergeHandler_Execute_YAMLFormat(t *testing.T) {
	handler := NewMergeHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "merge",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"format": "yaml",
			"sources": []map[string]any{
				{
					"input": "data",
					"key":   "content",
				},
			},
		},
	}

	data := []byte(`{"name":"John"}`)
	msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, _ := msg.GetData()
	// YAML output should contain "content:"
	if !contains(string(result), "content:") {
		t.Errorf("expected YAML output with 'content:', got %q", result)
	}
}

func TestMergeHandler_Execute_DefaultFormat(t *testing.T) {
	handler := NewMergeHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "merge",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			// format defaults to "json"
			"sources": []map[string]any{
				{
					"input": "data",
					"key":   "content",
				},
			},
		},
	}

	data := []byte(`{"name":"John"}`)
	msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, _ := msg.GetData()
	// Should be valid JSON
	var merged map[string]interface{}
	if err := json.Unmarshal(result, &merged); err != nil {
		t.Fatalf("default format should be JSON, got: %v", err)
	}
}

func TestMergeHandler_Execute_FilesystemSource_NoFilesystem(t *testing.T) {
	handler := NewMergeHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "merge",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"format": "json",
			"sources": []map[string]any{
				{
					"input": "filesystem",
					"key":   "file",
					"path":  "/path/to/file.json",
				},
			},
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), nil))

	// Should log warning but not fail (continues without the source)
	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMergeHandler_GetSourceValue_UnsupportedInput(t *testing.T) {
	handler := NewMergeHandler(slog.Default())

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("data"), nil))

	src := MergeSource{
		Input: "unsupported",
		Key:   "key",
	}

	_, err := handler.getSourceValue(msg, src)
	if err == nil {
		t.Error("expected error for unsupported input, got nil")
	}
}

func TestMergeHandler_Execute_UnsupportedFormat(t *testing.T) {
	handler := NewMergeHandler(slog.Default())
	ctx := context.Background()

	// Bypass validation to test execute with unsupported format
	op := FormatOperation{
		Type:   "merge",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"format": "unsupported",
			"sources": []map[string]any{
				{
					"input": "data",
					"key":   "content",
				},
			},
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte(`{"name":"John"}`), nil))

	err := handler.Execute(ctx, msg, op)
	if err == nil {
		t.Error("expected error for unsupported format, got nil")
	}
}
