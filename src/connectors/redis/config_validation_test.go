package main

import "testing"

// TestSourceConfigValidation tests the validation of Redis source configuration
func TestSourceConfigValidation(t *testing.T) {
	tests := []struct {
		name          string
		sourceConfig  SourceConfig
		expectError   bool
		errorContains string
	}{
		{
			name: "valid stream config with consumer group",
			sourceConfig: SourceConfig{
				Address:       "localhost:6379",
				Stream:        "test-stream",
				ConsumerGroup: "test-group",
				ConsumerName:  "test-consumer",
				LastID:        "0",
			},
			expectError: false,
		},
		{
			name: "valid stream config without consumer group",
			sourceConfig: SourceConfig{
				Address: "localhost:6379",
				Stream:  "test-stream",
				LastID:  "$",
			},
			expectError: false,
		},
		{
			name: "valid channel config",
			sourceConfig: SourceConfig{
				Address: "localhost:6379",
				Channel: "test-channel",
			},
			expectError: false,
		},
		{
			name: "invalid - ConsumerGroup without ConsumerName",
			sourceConfig: SourceConfig{
				Address:       "localhost:6379",
				Stream:        "test-stream",
				ConsumerGroup: "test-group",
				LastID:        "0",
			},
			expectError:   true,
			errorContains: "ConsumerGroup and ConsumerName must be both set or both empty",
		},
		{
			name: "invalid - ConsumerName without ConsumerGroup",
			sourceConfig: SourceConfig{
				Address:      "localhost:6379",
				Stream:       "test-stream",
				ConsumerName: "test-consumer",
				LastID:       "0",
			},
			expectError:   true,
			errorContains: "ConsumerGroup and ConsumerName must be both set or both empty",
		},
		{
			name: "invalid - invalid LastID value",
			sourceConfig: SourceConfig{
				Address: "localhost:6379",
				Stream:  "test-stream",
				LastID:  "invalid-id",
			},
			expectError:   true,
			errorContains: "LastID must be one of",
		},
		{
			name: "valid - custom stream ID format",
			sourceConfig: SourceConfig{
				Address: "localhost:6379",
				Stream:  "test-stream",
				LastID:  "1234567890123-0",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSource(&tt.sourceConfig)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got nil")
					return
				}
				if tt.errorContains != "" && !stringContains(err.Error(), tt.errorContains) {
					t.Errorf("expected error to contain %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("expected no error but got: %v", err)
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func stringContains(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
