package main

import (
	"context"
	"log/slog"
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestSplitHandler_Validate(t *testing.T) {
	handler := NewSplitHandler(slog.Default())

	tests := []struct {
		name    string
		op      FormatOperation
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid split configuration",
			op: FormatOperation{
				Type:   "split",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"delimiter": "\n",
					"maxParts":  100,
				},
			},
			wantErr: false,
		},
		{
			name: "missing delimiter",
			op: FormatOperation{
				Type:   "split",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"maxParts": 100,
				},
			},
			wantErr: true,
			errMsg:  "delimiter is required",
		},
		{
			name: "empty delimiter",
			op: FormatOperation{
				Type:   "split",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"delimiter": "",
					"maxParts":  100,
				},
			},
			wantErr: true,
			errMsg:  "delimiter is required",
		},
		{
			name: "invalid maxParts zero",
			op: FormatOperation{
				Type:   "split",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"delimiter": "\n",
					"maxParts":  0,
				},
			},
			wantErr: true,
			errMsg:  "maxParts must be positive",
		},
		{
			name: "invalid maxParts negative",
			op: FormatOperation{
				Type:   "split",
				Input:  "data",
				Output: "parts",
				Options: map[string]any{
					"delimiter": "\n",
					"maxParts":  -1,
				},
			},
			wantErr: true,
			errMsg:  "maxParts must be positive",
		},
		{
			name: "invalid output (not parts)",
			op: FormatOperation{
				Type:   "split",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"delimiter": "\n",
					"maxParts":  100,
				},
			},
			wantErr: true,
			errMsg:  "split operation output must be 'parts'",
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

func TestSplitHandler_Execute(t *testing.T) {
	handler := NewSplitHandler(slog.Default())
	ctx := context.Background()

	t.Run("split by newline", func(t *testing.T) {
		op := FormatOperation{
			Type:   "split",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"delimiter": "\n",
				"maxParts":  100,
			},
		}

		data := []byte("line1\nline2\nline3")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if len(parts) != 3 {
			t.Fatalf("expected 3 parts, got %d", len(parts))
		}

		if string(parts[0].Data) != "line1" {
			t.Errorf("expected part 0 = 'line1', got %q", parts[0].Data)
		}
		if string(parts[1].Data) != "line2" {
			t.Errorf("expected part 1 = 'line2', got %q", parts[1].Data)
		}
		if string(parts[2].Data) != "line3" {
			t.Errorf("expected part 2 = 'line3', got %q", parts[2].Data)
		}
	})

	t.Run("split by comma", func(t *testing.T) {
		op := FormatOperation{
			Type:   "split",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"delimiter": ",",
				"maxParts":  100,
			},
		}

		data := []byte("a,b,c,d,e")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if len(parts) != 5 {
			t.Fatalf("expected 5 parts, got %d", len(parts))
		}
	})

	t.Run("split with custom prefix", func(t *testing.T) {
		op := FormatOperation{
			Type:   "split",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"delimiter":      "|",
				"partNamePrefix": "segment",
				"maxParts":       100,
			},
		}

		data := []byte("a|b|c")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if len(parts) != 3 {
			t.Fatalf("expected 3 parts, got %d", len(parts))
		}

		if parts[0].Name != "segment0" {
			t.Errorf("expected part name 'segment0', got %q", parts[0].Name)
		}
	})

	t.Run("split with custom content type", func(t *testing.T) {
		op := FormatOperation{
			Type:   "split",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"delimiter":   "\n",
				"contentType": "application/json",
				"maxParts":    100,
			},
		}

		data := []byte(`{"a":1}` + "\n" + `{"b":2}`)
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if parts[0].ContentType != "application/json" {
			t.Errorf("expected content type 'application/json', got %q", parts[0].ContentType)
		}
	})

	t.Run("split with max parts limit", func(t *testing.T) {
		op := FormatOperation{
			Type:   "split",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"delimiter": ",",
				"maxParts":  3,
			},
		}

		data := []byte("a,b,c,d,e,f,g,h,i,j")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if len(parts) != 3 {
			t.Fatalf("expected 3 parts (limited by maxParts), got %d", len(parts))
		}
	})

	t.Run("split with default options", func(t *testing.T) {
		op := FormatOperation{
			Type:   "split",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"delimiter": "\n",
				// defaults: partNamePrefix="part", contentType="text/plain", maxParts=100
			},
		}

		data := []byte("line1\nline2")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if len(parts) != 2 {
			t.Fatalf("expected 2 parts, got %d", len(parts))
		}

		if parts[0].Name != "part0" {
			t.Errorf("expected default part name 'part0', got %q", parts[0].Name)
		}
		if parts[0].ContentType != "text/plain" {
			t.Errorf("expected default content type 'text/plain', got %q", parts[0].ContentType)
		}
	})

	t.Run("split single item (no delimiter match)", func(t *testing.T) {
		op := FormatOperation{
			Type:   "split",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"delimiter": ",",
				"maxParts":  100,
			},
		}

		data := []byte("no commas here")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if len(parts) != 1 {
			t.Fatalf("expected 1 part, got %d", len(parts))
		}

		if string(parts[0].Data) != "no commas here" {
			t.Errorf("expected 'no commas here', got %q", parts[0].Data)
		}
	})

	t.Run("split empty data", func(t *testing.T) {
		op := FormatOperation{
			Type:   "split",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"delimiter": ",",
				"maxParts":  100,
			},
		}

		data := []byte("")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if len(parts) != 1 {
			t.Fatalf("expected 1 part for empty data, got %d", len(parts))
		}

		if len(parts[0].Data) != 0 {
			t.Errorf("expected empty part data, got %q", parts[0].Data)
		}
	})

	t.Run("split multi-character delimiter", func(t *testing.T) {
		op := FormatOperation{
			Type:   "split",
			Input:  "data",
			Output: "parts",
			Options: map[string]any{
				"delimiter": "|||",
				"maxParts":  100,
			},
		}

		data := []byte("part1|||part2|||part3")
		msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parts := msg.GetParts()
		if len(parts) != 3 {
			t.Fatalf("expected 3 parts, got %d", len(parts))
		}

		if string(parts[0].Data) != "part1" {
			t.Errorf("expected 'part1', got %q", parts[0].Data)
		}
	})
}

func TestSplitHandler_Execute_DefaultMaxParts(t *testing.T) {
	handler := NewSplitHandler(slog.Default())
	ctx := context.Background()

	// When maxParts is 0 in the options, it defaults to 100
	op := FormatOperation{
		Type:   "split",
		Input:  "data",
		Output: "parts",
		Options: map[string]any{
			"delimiter": ",",
			// maxParts will be 0 and default to 100
		},
	}

	// Create data with more than 100 parts
	var data []byte
	for i := 0; i < 150; i++ {
		if i > 0 {
			data = append(data, ',')
		}
		data = append(data, 'a')
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter(data, nil))

	// Note: The handler.Execute reads maxParts from options which would be 0
	// and then sets default to 100, so it should limit to 100 parts
	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parts := msg.GetParts()
	if len(parts) != 100 {
		t.Errorf("expected 100 parts (default max), got %d", len(parts))
	}
}
