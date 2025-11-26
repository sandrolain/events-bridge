package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"log/slog"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestCompressHandler_Validate(t *testing.T) {
	handler := NewCompressHandler(slog.Default())

	tests := []struct {
		name    string
		op      FormatOperation
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid gzip compress",
			op: FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": "gzip",
					"operation": "compress",
				},
			},
			wantErr: false,
		},
		{
			name: "valid gzip decompress",
			op: FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": "gzip",
					"operation": "decompress",
				},
			},
			wantErr: false,
		},
		{
			name: "valid zstd compress",
			op: FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": "zstd",
					"operation": "compress",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid algorithm",
			op: FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": "invalid",
					"operation": "compress",
				},
			},
			wantErr: true,
			errMsg:  "invalid algorithm",
		},
		{
			name: "invalid operation",
			op: FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": "gzip",
					"operation": "invalid",
				},
			},
			wantErr: true,
			errMsg:  "invalid operation",
		},
		{
			name: "invalid compression level too low",
			op: FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": "gzip",
					"level":     -2,
				},
			},
			wantErr: true,
			errMsg:  "invalid compression level",
		},
		{
			name: "invalid compression level too high",
			op: FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": "gzip",
					"level":     10,
				},
			},
			wantErr: true,
			errMsg:  "invalid compression level",
		},
		{
			name: "valid compression level -1 (default)",
			op: FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": "gzip",
					"level":     -1,
				},
			},
			wantErr: false,
		},
		{
			name: "valid compression level 9 (best)",
			op: FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": "gzip",
					"level":     9,
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

func TestCompressHandler_Execute_Gzip(t *testing.T) {
	handler := NewCompressHandler(slog.Default())
	ctx := context.Background()

	t.Run("compress and decompress roundtrip", func(t *testing.T) {
		original := []byte("Hello, World! This is a test message that should be compressed.")

		// Compress
		compressOp := FormatOperation{
			Type:   "compress",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"algorithm": "gzip",
				"operation": "compress",
				"level":     6,
			},
		}

		msg := message.NewRunnerMessage(testutil.NewAdapter(original, nil))
		err := handler.Execute(ctx, msg, compressOp)
		if err != nil {
			t.Fatalf("compress failed: %v", err)
		}

		compressed, _ := msg.GetData()
		if len(compressed) >= len(original) {
			t.Log("Warning: compressed data is not smaller than original (may be expected for small data)")
		}

		// Decompress
		decompressOp := FormatOperation{
			Type:   "compress",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"algorithm": "gzip",
				"operation": "decompress",
			},
		}

		msg2 := message.NewRunnerMessage(testutil.NewAdapter(compressed, nil))
		err = handler.Execute(ctx, msg2, decompressOp)
		if err != nil {
			t.Fatalf("decompress failed: %v", err)
		}

		result, _ := msg2.GetData()
		if !bytes.Equal(result, original) {
			t.Errorf("roundtrip failed: expected %q, got %q", original, result)
		}
	})

	t.Run("compress with default level", func(t *testing.T) {
		original := []byte("Test data")

		op := FormatOperation{
			Type:   "compress",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"algorithm": "gzip",
				"operation": "compress",
				// level defaults to 6
			},
		}

		msg := message.NewRunnerMessage(testutil.NewAdapter(original, nil))
		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("compress failed: %v", err)
		}
	})

	t.Run("compress with level -1", func(t *testing.T) {
		original := []byte("Test data")

		op := FormatOperation{
			Type:   "compress",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"algorithm": "gzip",
				"operation": "compress",
				"level":     -1,
			},
		}

		msg := message.NewRunnerMessage(testutil.NewAdapter(original, nil))
		err := handler.Execute(ctx, msg, op)
		if err != nil {
			t.Fatalf("compress failed: %v", err)
		}
	})

	t.Run("decompress invalid data", func(t *testing.T) {
		op := FormatOperation{
			Type:   "compress",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"algorithm": "gzip",
				"operation": "decompress",
			},
		}

		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("not gzip data"), nil))
		err := handler.Execute(ctx, msg, op)
		if err == nil {
			t.Error("expected error for invalid gzip data, got nil")
		}
	})
}

func TestCompressHandler_Execute_Zstd(t *testing.T) {
	handler := NewCompressHandler(slog.Default())
	ctx := context.Background()

	t.Run("compress and decompress roundtrip", func(t *testing.T) {
		original := []byte("Hello, World! This is a test message that should be compressed with zstd.")

		// Compress
		compressOp := FormatOperation{
			Type:   "compress",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"algorithm": "zstd",
				"operation": "compress",
			},
		}

		msg := message.NewRunnerMessage(testutil.NewAdapter(original, nil))
		err := handler.Execute(ctx, msg, compressOp)
		if err != nil {
			t.Fatalf("compress failed: %v", err)
		}

		compressed, _ := msg.GetData()

		// Decompress
		decompressOp := FormatOperation{
			Type:   "compress",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"algorithm": "zstd",
				"operation": "decompress",
			},
		}

		msg2 := message.NewRunnerMessage(testutil.NewAdapter(compressed, nil))
		err = handler.Execute(ctx, msg2, decompressOp)
		if err != nil {
			t.Fatalf("decompress failed: %v", err)
		}

		result, _ := msg2.GetData()
		if !bytes.Equal(result, original) {
			t.Errorf("roundtrip failed: expected %q, got %q", original, result)
		}
	})

	t.Run("compress with different levels", func(t *testing.T) {
		original := []byte("Test data for compression level testing")

		levels := []int{-1, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		for _, level := range levels {
			op := FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": "zstd",
					"operation": "compress",
					"level":     level,
				},
			}

			msg := message.NewRunnerMessage(testutil.NewAdapter(original, nil))
			err := handler.Execute(ctx, msg, op)
			if err != nil {
				t.Errorf("compress with level %d failed: %v", level, err)
			}
		}
	})

	t.Run("decompress invalid data", func(t *testing.T) {
		op := FormatOperation{
			Type:   "compress",
			Input:  "data",
			Output: "data",
			Options: map[string]any{
				"algorithm": "zstd",
				"operation": "decompress",
			},
		}

		msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("not zstd data"), nil))
		err := handler.Execute(ctx, msg, op)
		if err == nil {
			t.Error("expected error for invalid zstd data, got nil")
		}
	})
}

func TestCompressHandler_Execute_DefaultOperation(t *testing.T) {
	handler := NewCompressHandler(slog.Default())
	ctx := context.Background()

	op := FormatOperation{
		Type:   "compress",
		Input:  "data",
		Output: "data",
		Options: map[string]any{
			"algorithm": "gzip",
			// operation defaults to "compress"
		},
	}

	msg := message.NewRunnerMessage(testutil.NewAdapter([]byte("test data"), nil))
	err := handler.Execute(ctx, msg, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify it's valid gzip
	result, _ := msg.GetData()
	reader := bytes.NewReader(result)
	gzReader, err := gzip.NewReader(reader)
	if err != nil {
		t.Fatalf("result is not valid gzip: %v", err)
	}
	defer gzReader.Close()
}

func TestCompressHandler_UnsupportedAlgorithm(t *testing.T) {
	handler := NewCompressHandler(slog.Default())

	// Test internal compress method with unsupported algorithm
	_, err := handler.compress([]byte("test"), "unsupported", 6)
	if err == nil {
		t.Error("expected error for unsupported algorithm in compress")
	}

	// Test internal decompress method with unsupported algorithm
	_, err = handler.decompress([]byte("test"), "unsupported")
	if err == nil {
		t.Error("expected error for unsupported algorithm in decompress")
	}
}

func TestCompressHandler_Roundtrip_LargeData(t *testing.T) {
	handler := NewCompressHandler(slog.Default())
	ctx := context.Background()

	// Create large data
	original := bytes.Repeat([]byte("This is a test string that repeats. "), 1000)

	algorithms := []string{"gzip", "zstd"}
	for _, algo := range algorithms {
		t.Run(algo, func(t *testing.T) {
			// Compress
			compressOp := FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": algo,
					"operation": "compress",
				},
			}

			msg := message.NewRunnerMessage(testutil.NewAdapter(original, nil))
			err := handler.Execute(ctx, msg, compressOp)
			if err != nil {
				t.Fatalf("compress failed: %v", err)
			}

			compressed, _ := msg.GetData()

			// Verify compression actually reduced size
			if len(compressed) >= len(original) {
				t.Errorf("%s: compressed size (%d) should be smaller than original (%d)",
					algo, len(compressed), len(original))
			}

			// Decompress
			decompressOp := FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": algo,
					"operation": "decompress",
				},
			}

			msg2 := message.NewRunnerMessage(testutil.NewAdapter(compressed, nil))
			err = handler.Execute(ctx, msg2, decompressOp)
			if err != nil {
				t.Fatalf("decompress failed: %v", err)
			}

			result, _ := msg2.GetData()
			if !bytes.Equal(result, original) {
				t.Errorf("roundtrip failed for %s", algo)
			}
		})
	}
}

func TestCompressHandler_Zstd_EncoderLevels(t *testing.T) {
	handler := NewCompressHandler(slog.Default())
	ctx := context.Background()

	// Test different level mappings
	testCases := []struct {
		level       int
		description string
	}{
		{-1, "SpeedFastest via -1"},
		{1, "SpeedFastest via 1"},
		{4, "SpeedDefault via 4"},
		{7, "SpeedBetterCompression via 7"},
		{0, "default level (0 triggers default)"},
	}

	original := []byte("Test data for encoder level testing")

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			op := FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
				Options: map[string]any{
					"algorithm": "zstd",
					"operation": "compress",
					"level":     tc.level,
				},
			}

			msg := message.NewRunnerMessage(testutil.NewAdapter(original, nil))
			err := handler.Execute(ctx, msg, op)
			if err != nil {
				t.Fatalf("compress failed for level %d: %v", tc.level, err)
			}

			// Verify the result can be decompressed
			compressed, _ := msg.GetData()
			decoder, err := zstd.NewReader(bytes.NewReader(compressed))
			if err != nil {
				t.Fatalf("failed to create zstd decoder: %v", err)
			}
			defer decoder.Close()
		})
	}
}
