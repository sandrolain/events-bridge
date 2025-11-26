package main

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestValidateOperation(t *testing.T) {
	tests := []struct {
		name    string
		op      FormatOperation
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid template operation",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "data",
			},
			wantErr: false,
		},
		{
			name: "valid serialize operation",
			op: FormatOperation{
				Type:   "serialize",
				Input:  "data",
				Output: "data",
			},
			wantErr: false,
		},
		{
			name: "valid multipart operation",
			op: FormatOperation{
				Type:   "multipart",
				Input:  "data",
				Output: "parts",
			},
			wantErr: false,
		},
		{
			name: "valid encode operation",
			op: FormatOperation{
				Type:   "encode",
				Input:  "data",
				Output: "data",
			},
			wantErr: false,
		},
		{
			name: "valid compress operation",
			op: FormatOperation{
				Type:   "compress",
				Input:  "data",
				Output: "data",
			},
			wantErr: false,
		},
		{
			name: "valid extract operation",
			op: FormatOperation{
				Type:   "extract",
				Input:  "data",
				Output: "metadata",
			},
			wantErr: false,
		},
		{
			name: "valid merge operation",
			op: FormatOperation{
				Type:   "merge",
				Input:  "data",
				Output: "data",
			},
			wantErr: false,
		},
		{
			name: "valid split operation",
			op: FormatOperation{
				Type:   "split",
				Input:  "data",
				Output: "parts",
			},
			wantErr: false,
		},
		{
			name: "missing operation type",
			op: FormatOperation{
				Input:  "data",
				Output: "data",
			},
			wantErr: true,
			errMsg:  "operation type is required",
		},
		{
			name: "invalid operation type",
			op: FormatOperation{
				Type:   "invalid",
				Input:  "data",
				Output: "data",
			},
			wantErr: true,
			errMsg:  "invalid operation type",
		},
		{
			name: "invalid input source",
			op: FormatOperation{
				Type:   "template",
				Input:  "invalid",
				Output: "data",
			},
			wantErr: true,
			errMsg:  "invalid input source",
		},
		{
			name: "invalid output destination",
			op: FormatOperation{
				Type:   "template",
				Input:  "data",
				Output: "invalid",
			},
			wantErr: true,
			errMsg:  "invalid output destination",
		},
		{
			name: "valid with all sources",
			op: FormatOperation{
				Type:   "template",
				Input:  "metadata",
				Output: "filesystem",
			},
			wantErr: false,
		},
		{
			name: "valid parts input/output",
			op: FormatOperation{
				Type:   "template",
				Input:  "parts",
				Output: "parts",
			},
			wantErr: false,
		},
		{
			name: "empty input (valid - uses default)",
			op: FormatOperation{
				Type:   "template",
				Output: "data",
			},
			wantErr: false,
		},
		{
			name: "empty output (valid - uses default)",
			op: FormatOperation{
				Type:  "template",
				Input: "data",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOperation(tt.op)
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

func TestGetInput(t *testing.T) {
	tests := []struct {
		name        string
		inputSource string
		data        []byte
		metadata    map[string]string
		wantNil     bool
		wantErr     bool
	}{
		{
			name:        "get data input",
			inputSource: "data",
			data:        []byte("test data"),
			wantNil:     false,
			wantErr:     false,
		},
		{
			name:        "get metadata input",
			inputSource: "metadata",
			metadata:    map[string]string{"key": "value"},
			wantNil:     false,
			wantErr:     false,
		},
		{
			name:        "get filesystem input returns nil",
			inputSource: "filesystem",
			wantNil:     true,
			wantErr:     false,
		},
		{
			name:        "get parts input returns nil",
			inputSource: "parts",
			wantNil:     true,
			wantErr:     false,
		},
		{
			name:        "unknown input source",
			inputSource: "unknown",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := message.NewRunnerMessage(testutil.NewAdapter(tt.data, tt.metadata))

			result, err := GetInput(msg, tt.inputSource)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.wantNil && result != nil {
				t.Errorf("expected nil result, got %v", result)
			}
		})
	}
}

func TestSetOutput(t *testing.T) {
	tests := []struct {
		name       string
		outputDest string
		data       []byte
		wantErr    bool
		errMsg     string
	}{
		{
			name:       "set data output",
			outputDest: "data",
			data:       []byte("output data"),
			wantErr:    false,
		},
		{
			name:       "metadata output not supported",
			outputDest: "metadata",
			data:       []byte("output"),
			wantErr:    true,
			errMsg:     "metadata output must be set by handler directly",
		},
		{
			name:       "filesystem output not supported",
			outputDest: "filesystem",
			data:       []byte("output"),
			wantErr:    true,
			errMsg:     "filesystem output must be written by handler directly",
		},
		{
			name:       "parts output not supported",
			outputDest: "parts",
			data:       []byte("output"),
			wantErr:    true,
			errMsg:     "parts output must be added by handler directly",
		},
		{
			name:       "unknown output destination",
			outputDest: "unknown",
			data:       []byte("output"),
			wantErr:    true,
			errMsg:     "unknown output destination",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := message.NewRunnerMessage(testutil.NewAdapter(nil, nil))

			err := SetOutput(msg, tt.outputDest, tt.data)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errMsg)
				} else if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify data was set
			if tt.outputDest == "data" {
				result, _ := msg.GetData()
				if string(result) != string(tt.data) {
					t.Errorf("expected %q, got %q", tt.data, result)
				}
			}
		})
	}
}

func TestConstants(t *testing.T) {
	// Test that constants have expected values
	if sourceData != "data" {
		t.Errorf("expected sourceData='data', got %q", sourceData)
	}
	if sourceMetadata != "metadata" {
		t.Errorf("expected sourceMetadata='metadata', got %q", sourceMetadata)
	}
	if sourceFilesystem != "filesystem" {
		t.Errorf("expected sourceFilesystem='filesystem', got %q", sourceFilesystem)
	}
	if sourceParts != "parts" {
		t.Errorf("expected sourceParts='parts', got %q", sourceParts)
	}
	if formatJSON != "json" {
		t.Errorf("expected formatJSON='json', got %q", formatJSON)
	}
	if formatYAML != "yaml" {
		t.Errorf("expected formatYAML='yaml', got %q", formatYAML)
	}
	if formatText != "text" {
		t.Errorf("expected formatText='text', got %q", formatText)
	}
	if mimeTypeTextPlain != "text/plain" {
		t.Errorf("expected mimeTypeTextPlain='text/plain', got %q", mimeTypeTextPlain)
	}
}
