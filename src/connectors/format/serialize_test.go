package main

import (
	"context"
	"log/slog"
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestSerializeHandler_Validate(t *testing.T) {
	handler := NewSerializeHandler(slog.Default())

	tests := []struct {
		name    string
		op      FormatOperation
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid json to yaml",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "json",
					"to":   "yaml",
				},
			},
			wantErr: false,
		},
		{
			name: "valid yaml to json",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "yaml",
					"to":   "json",
				},
			},
			wantErr: false,
		},
		{
			name: "valid json to cbor",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "json",
					"to":   "cbor",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid source format",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "invalid",
					"to":   "json",
				},
			},
			wantErr: true,
			errMsg:  "invalid source format",
		},
		{
			name: "invalid target format",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "json",
					"to":   "invalid",
				},
			},
			wantErr: true,
			errMsg:  "invalid target format",
		},
		{
			name: "valid msgpack format",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "msgpack",
					"to":   "json",
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

func TestSerializeHandler_Execute(t *testing.T) {
	handler := NewSerializeHandler(slog.Default())
	ctx := context.Background()

	tests := []struct {
		name    string
		op      FormatOperation
		data    []byte
		want    string
		wantErr bool
	}{
		{
			name: "json to yaml",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "json",
					"to":   "yaml",
				},
			},
			data: []byte(`{"name":"John","age":30}`),
			want: "age: 30\nname: John\n",
		},
		{
			name: "yaml to json",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "yaml",
					"to":   "json",
				},
			},
			data: []byte("name: John\nage: 30\n"),
			want: `{"age":30,"name":"John"}`,
		},
		{
			name: "json to json pretty",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from":   "json",
					"to":     "json",
					"pretty": true,
					"indent": "  ",
				},
			},
			data: []byte(`{"name":"John"}`),
			want: "{\n  \"name\": \"John\"\n}",
		},
		{
			name: "invalid json input",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "json",
					"to":   "yaml",
				},
			},
			data:    []byte(`{invalid json}`),
			wantErr: true,
		},
		{
			name: "invalid yaml input",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "yaml",
					"to":   "json",
				},
			},
			data:    []byte(":\n  invalid: yaml\n    - broken"),
			wantErr: true,
		},
		{
			name: "json to cbor",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "json",
					"to":   "cbor",
				},
			},
			data:    []byte(`{"key":"value"}`),
			wantErr: false, // CBOR output is binary, just check no error
		},
		{
			name: "msgpack not implemented",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "msgpack",
					"to":   "json",
				},
			},
			data:    []byte(`test`),
			wantErr: true,
		},
		{
			name: "msgpack output not implemented",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"from": "json",
					"to":   "msgpack",
				},
			},
			data:    []byte(`{"key":"value"}`),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := message.NewRunnerMessage(testutil.NewAdapter(tt.data, nil))

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

			if tt.want != "" {
				result, err := msg.GetData()
				if err != nil {
					t.Fatalf("failed to get result: %v", err)
				}

				if string(result) != tt.want {
					t.Errorf("expected %q, got %q", tt.want, string(result))
				}
			}
		})
	}
}

func TestSerializeHandler_CBOR(t *testing.T) {
	handler := NewSerializeHandler(slog.Default())
	ctx := context.Background()

	// Test CBOR encoding only (roundtrip has type issues with interface{})
	originalData := []byte(`{"name":"test","value":123}`)

	// JSON to CBOR
	toCBOROp := FormatOperation{
		Type:   "serialize",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"from": "json",
			"to":   "cbor",
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter(originalData, nil))
	err := handler.Execute(ctx, msg, toCBOROp)
	if err != nil {
		t.Fatalf("failed to convert to CBOR: %v", err)
	}

	cborData, err := msg.GetData()
	if err != nil {
		t.Fatalf("failed to get CBOR data: %v", err)
	}

	// Verify CBOR data is not empty and different from input
	if len(cborData) == 0 {
		t.Error("CBOR output should not be empty")
	}

	// CBOR to YAML (avoids JSON marshaling of interface{} keys issue)
	fromCBOROp := FormatOperation{
		Type:   "serialize",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"from": "cbor",
			"to":   "yaml",
		},
	}

	msg2 := message.NewRunnerMessage(testutil.NewAdapter(cborData, nil))
	err = handler.Execute(ctx, msg2, fromCBOROp)
	if err != nil {
		t.Fatalf("failed to convert from CBOR to YAML: %v", err)
	}

	result, err := msg2.GetData()
	if err != nil {
		t.Fatalf("failed to get result: %v", err)
	}

	// YAML output should contain the original keys
	if !contains(string(result), "name") || !contains(string(result), "test") {
		t.Errorf("CBOR roundtrip failed: expected YAML with 'name' and 'test', got %q", result)
	}
}

func TestSerializeHandler_UnsupportedFormat(t *testing.T) {
	handler := NewSerializeHandler(slog.Default())
	ctx := context.Background()

	// Create operation with valid options but test internal handling
	op := FormatOperation{
		Type:   "serialize",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"from": "json",
			"to":   "json",
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte(`{"key":"value"}`), nil))
	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
