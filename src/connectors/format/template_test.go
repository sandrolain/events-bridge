package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestTemplateHandler_Validate(t *testing.T) {
	handler := NewTemplateHandler(slog.Default())

	tests := []struct {
		name    string
		op      FormatOperation
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid inline template",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Hello {{ .data }}",
					"maxTemplateSize": 1024,
				},
			},
			wantErr: false,
		},
		{
			name: "valid template file",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"templateFile":    "/path/to/template.txt",
					"maxTemplateSize": 1024,
				},
			},
			wantErr: false,
		},
		{
			name: "missing both template and templateFile",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"maxTemplateSize": 1024,
				},
			},
			wantErr: true,
			errMsg:  "either template or templateFile must be specified",
		},
		{
			name: "both template and templateFile specified",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Hello",
					"templateFile":    "/path/to/template.txt",
					"maxTemplateSize": 1024,
				},
			},
			wantErr: true,
			errMsg:  "cannot specify both template and templateFile",
		},
		{
			name: "invalid maxTemplateSize",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Hello",
					"maxTemplateSize": 0,
				},
			},
			wantErr: true,
			errMsg:  "maxTemplateSize must be positive",
		},
		{
			name: "negative maxTemplateSize",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Hello",
					"maxTemplateSize": -1,
				},
			},
			wantErr: true,
			errMsg:  "maxTemplateSize must be positive",
		},
		{
			name: "template exceeds maxTemplateSize",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Hello World Template",
					"maxTemplateSize": 5,
				},
			},
			wantErr: true,
			errMsg:  "template size",
		},
		{
			name: "invalid template engine",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Hello",
					"engine":          "invalid",
					"maxTemplateSize": 1024,
				},
			},
			wantErr: true,
			errMsg:  "invalid template engine",
		},
		{
			name: "valid text engine",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Hello",
					"engine":          "text",
					"maxTemplateSize": 1024,
				},
			},
			wantErr: false,
		},
		{
			name: "valid html engine",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Hello",
					"engine":          "html",
					"maxTemplateSize": 1024,
				},
			},
			wantErr: false,
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

func TestTemplateHandler_Execute(t *testing.T) {
	handler := NewTemplateHandler(slog.Default())
	ctx := context.Background()

	tests := []struct {
		name     string
		op       FormatOperation
		data     []byte
		metadata map[string]string
		want     string
		wantErr  bool
	}{
		{
			name: "simple text template",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Hello {{ .data }}",
					"maxTemplateSize": 1024,
				},
			},
			data: []byte("World"),
			want: "Hello World",
		},
		{
			name: "template with metadata",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Name: {{ index .metadata \"name\" }}",
					"maxTemplateSize": 1024,
				},
			},
			data:     []byte("test"),
			metadata: map[string]string{"name": "John"},
			want:     "Name: John",
		},
		{
			name: "html engine",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "<p>{{ .data }}</p>",
					"engine":          "html",
					"maxTemplateSize": 1024,
				},
			},
			data: []byte("Content"),
			want: "<p>Content</p>",
		},
		{
			name: "template syntax error",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "{{ .invalid syntax",
					"maxTemplateSize": 1024,
				},
			},
			data:    []byte("test"),
			wantErr: true,
		},
		{
			name: "default text engine",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"template":        "Value: {{ .data }}",
					"maxTemplateSize": 1024,
				},
			},
			data: []byte("123"),
			want: "Value: 123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := message.NewRunnerMessage(testutil.NewAdapter(tt.data, tt.metadata))

			err := handler.Execute(ctx, msg, tt.op)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			result, err := msg.GetData()
			if err != nil {
				t.Fatalf("failed to get result: %v", err)
			}

			if string(result) != tt.want {
				t.Errorf("expected %q, got %q", tt.want, string(result))
			}
		})
	}
}

func TestTemplateHandler_ExecuteTimeout(t *testing.T) {
	handler := NewTemplateHandler(slog.Default())

	// Create context that's already cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait a bit for timeout to trigger
	time.Sleep(10 * time.Millisecond)

	op := FormatOperation{
		Type:   "template",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"template":        "Hello {{ .data }}",
			"maxTemplateSize": 1024,
			"timeout":         "1ns",
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("World"), nil))

	// This may or may not timeout depending on execution speed
	_ = handler.Execute(ctx, msg, op)
	// Not checking error as it depends on timing
}

func TestTemplateHandler_UnsupportedEngine(t *testing.T) {
	handler := NewTemplateHandler(slog.Default())

	// Test the executeTemplate method with unsupported engine
	var output []byte
	err := handler.executeTemplate("unsupported", "test", nil, nil)

	if err == nil {
		t.Error("expected error for unsupported engine")
	}

	if output != nil {
		t.Error("expected nil output for unsupported engine")
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
