package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"log/slog"
	"net/url"
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestEncodeHandler_Validate(t *testing.T) {
	handler := NewEncodeHandler(slog.Default())

	tests := []struct {
		name    string
		op      FormatOperation
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid base64 encode",
			op: FormatOperation{
				Type:   "encode",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"encoding":  "base64",
					"operation": "encode",
				},
			},
			wantErr: false,
		},
		{
			name: "valid base64 decode",
			op: FormatOperation{
				Type:   "encode",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"encoding":  "base64",
					"operation": "decode",
				},
			},
			wantErr: false,
		},
		{
			name: "valid base64url",
			op: FormatOperation{
				Type:   "encode",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"encoding":  "base64url",
					"operation": "encode",
				},
			},
			wantErr: false,
		},
		{
			name: "valid hex",
			op: FormatOperation{
				Type:   "encode",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"encoding":  "hex",
					"operation": "encode",
				},
			},
			wantErr: false,
		},
		{
			name: "valid url encoding",
			op: FormatOperation{
				Type:   "encode",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"encoding":  "url",
					"operation": "encode",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid encoding",
			op: FormatOperation{
				Type:   "encode",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"encoding":  "invalid",
					"operation": "encode",
				},
			},
			wantErr: true,
			errMsg:  "invalid encoding",
		},
		{
			name: "invalid operation",
			op: FormatOperation{
				Type:   "encode",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"encoding":  "base64",
					"operation": "invalid",
				},
			},
			wantErr: true,
			errMsg:  "invalid operation",
		},
		{
			name: "default operation is encode",
			op: FormatOperation{
				Type:   "encode",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"encoding": "base64",
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

func TestEncodeHandler_Execute_Base64(t *testing.T) {
	handler := NewEncodeHandler(slog.Default())
	ctx := context.Background()

	t.Run("encode", func(t *testing.T) {
		op := FormatOperation{
			Type:   "encode",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"encoding":  "base64",
				"operation": "encode",
			},
		}

		data := []byte("Hello, World!")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, _ := msg.GetData()
		expected := base64.StdEncoding.EncodeToString(data)
		if string(result) != expected {
			t.Errorf("expected %q, got %q", expected, string(result))
		}
	})

	t.Run("decode", func(t *testing.T) {
		op := FormatOperation{
			Type:   "encode",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"encoding":  "base64",
				"operation": "decode",
			},
		}

		original := "Hello, World!"
		encoded := base64.StdEncoding.EncodeToString([]byte(original))
		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte(encoded), nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, _ := msg.GetData()
		if string(result) != original {
			t.Errorf("expected %q, got %q", original, string(result))
		}
	})

	t.Run("decode invalid", func(t *testing.T) {
		op := FormatOperation{
			Type:   "encode",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"encoding":  "base64",
				"operation": "decode",
			},
		}

		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("not valid base64!!!"), nil))

		err := handler.Execute(ctx, msg, op)
		if err == nil {
			t.Error("expected error for invalid base64, got nil")
		}
	})
}

func TestEncodeHandler_Execute_Base64URL(t *testing.T) {
	handler := NewEncodeHandler(slog.Default())
	ctx := context.Background()

	t.Run("encode", func(t *testing.T) {
		op := FormatOperation{
			Type:   "encode",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"encoding":  "base64url",
				"operation": "encode",
			},
		}

		data := []byte("Hello+World/Test=")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, _ := msg.GetData()
		expected := base64.URLEncoding.EncodeToString(data)
		if string(result) != expected {
			t.Errorf("expected %q, got %q", expected, string(result))
		}
	})

	t.Run("decode", func(t *testing.T) {
		op := FormatOperation{
			Type:   "encode",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"encoding":  "base64url",
				"operation": "decode",
			},
		}

		original := "Hello+World/Test="
		encoded := base64.URLEncoding.EncodeToString([]byte(original))
		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte(encoded), nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, _ := msg.GetData()
		if string(result) != original {
			t.Errorf("expected %q, got %q", original, string(result))
		}
	})
}

func TestEncodeHandler_Execute_Hex(t *testing.T) {
	handler := NewEncodeHandler(slog.Default())
	ctx := context.Background()

	t.Run("encode", func(t *testing.T) {
		op := FormatOperation{
			Type:   "encode",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"encoding":  "hex",
				"operation": "encode",
			},
		}

		data := []byte("Hello")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, _ := msg.GetData()
		expected := hex.EncodeToString(data)
		if string(result) != expected {
			t.Errorf("expected %q, got %q", expected, string(result))
		}
	})

	t.Run("decode", func(t *testing.T) {
		op := FormatOperation{
			Type:   "encode",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"encoding":  "hex",
				"operation": "decode",
			},
		}

		original := "Hello"
		encoded := hex.EncodeToString([]byte(original))
		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte(encoded), nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, _ := msg.GetData()
		if string(result) != original {
			t.Errorf("expected %q, got %q", original, string(result))
		}
	})

	t.Run("decode invalid", func(t *testing.T) {
		op := FormatOperation{
			Type:   "encode",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"encoding":  "hex",
				"operation": "decode",
			},
		}

		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("not valid hex!"), nil))

		err := handler.Execute(ctx, msg, op)
		if err == nil {
			t.Error("expected error for invalid hex, got nil")
		}
	})
}

func TestEncodeHandler_Execute_URL(t *testing.T) {
	handler := NewEncodeHandler(slog.Default())
	ctx := context.Background()

	t.Run("encode", func(t *testing.T) {
		op := FormatOperation{
			Type:   "encode",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"encoding":  "url",
				"operation": "encode",
			},
		}

		data := []byte("hello world&foo=bar")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, _ := msg.GetData()
		expected := url.QueryEscape(string(data))
		if string(result) != expected {
			t.Errorf("expected %q, got %q", expected, string(result))
		}
	})

	t.Run("decode", func(t *testing.T) {
		op := FormatOperation{
			Type:   "encode",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"encoding":  "url",
				"operation": "decode",
			},
		}

		original := "hello world&foo=bar"
		encoded := url.QueryEscape(original)
		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte(encoded), nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, _ := msg.GetData()
		if string(result) != original {
			t.Errorf("expected %q, got %q", original, string(result))
		}
	})

	t.Run("decode invalid", func(t *testing.T) {
		op := FormatOperation{
			Type:   "encode",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"encoding":  "url",
				"operation": "decode",
			},
		}

		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("%ZZ"), nil))

		err := handler.Execute(ctx, msg, op)
		if err == nil {
			t.Error("expected error for invalid url encoding, got nil")
		}
	})
}

func TestEncodeHandler_Execute_DefaultOperation(t *testing.T) {
	handler := NewEncodeHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "encode",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"encoding": "base64",
			// operation defaults to "encode"
		},
	}

	data := []byte("test")
	msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, _ := msg.GetData()
	expected := base64.StdEncoding.EncodeToString(data)
	if string(result) != expected {
		t.Errorf("expected %q, got %q", expected, string(result))
	}
}

func TestEncodeHandler_Execute_UnsupportedEncoding(t *testing.T) {
	handler := NewEncodeHandler(slog.Default())

	// Test internal encode method with unsupported encoding
	_, err := handler.encode([]byte("test"), "unsupported")
	if err == nil {
		t.Error("expected error for unsupported encoding")
	}

	// Test internal decode method with unsupported encoding
	_, err = handler.decode([]byte("test"), "unsupported")
	if err == nil {
		t.Error("expected error for unsupported decoding")
	}
}
