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

// TestValidateRedisKey tests the Redis key validation function for security
func TestValidateRedisKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		strict  bool
		wantErr bool
	}{
		{
			name:    "valid simple key",
			key:     "mykey",
			strict:  true,
			wantErr: false,
		},
		{
			name:    "valid key with colon",
			key:     "user:123",
			strict:  true,
			wantErr: false,
		},
		{
			name:    "valid key with underscore",
			key:     "user_profile",
			strict:  true,
			wantErr: false,
		},
		{
			name:    "valid key with dash",
			key:     "user-session",
			strict:  true,
			wantErr: false,
		},
		{
			name:    "valid key with dot",
			key:     "com.example.key",
			strict:  true,
			wantErr: false,
		},
		{
			name:    "valid key with forward slash",
			key:     "user/profile/123",
			strict:  true,
			wantErr: false,
		},
		{
			name:    "empty key",
			key:     "",
			strict:  true,
			wantErr: true,
		},
		{
			name:    "key with carriage return (command injection)",
			key:     "test\r\nSET malicious value\r\n",
			strict:  true,
			wantErr: true,
		},
		{
			name:    "key with newline (command injection)",
			key:     "test\nDEL key\n",
			strict:  true,
			wantErr: true,
		},
		{
			name:    "key with special chars in strict mode",
			key:     "test<>|&",
			strict:  true,
			wantErr: true,
		},
		{
			name:    "key with special chars in non-strict mode",
			key:     "test<>|&",
			strict:  false,
			wantErr: false,
		},
		{
			name:    "key with spaces in strict mode",
			key:     "my key",
			strict:  true,
			wantErr: true,
		},
		{
			name:    "key with null byte",
			key:     "test\x00key",
			strict:  true,
			wantErr: true,
		},
		{
			name:    "key with tab character",
			key:     "test\tkey",
			strict:  true,
			wantErr: true,
		},
		{
			name:    "complex command injection attempt",
			key:     "key\r\nDEL mykey\r\n",
			strict:  true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRedisKey(tt.key, tt.strict)
			if (err != nil) != tt.wantErr {
				if tt.wantErr {
					t.Errorf("expected error for %s", tt.name)
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestSourceConfigSecurityValidation tests security-related source config validation
func TestSourceConfigSecurityValidation(t *testing.T) {
	tests := []struct {
		name          string
		sourceConfig  SourceConfig
		expectError   bool
		errorContains string
	}{
		{
			name: "valid channel with strict validation",
			sourceConfig: SourceConfig{
				Address:          "localhost:6379",
				Channel:          "test:channel",
				StrictValidation: true,
			},
			expectError: false,
		},
		{
			name: "valid stream with strict validation",
			sourceConfig: SourceConfig{
				Address:          "localhost:6379",
				Stream:           "test:stream",
				StrictValidation: true,
			},
			expectError: false,
		},
		{
			name: "invalid channel name (command injection)",
			sourceConfig: SourceConfig{
				Address:          "localhost:6379",
				Channel:          "test\r\nSET malicious value\r\n",
				StrictValidation: true,
			},
			expectError:   true,
			errorContains: "invalid channel name",
		},
		{
			name: "invalid stream name (newline injection)",
			sourceConfig: SourceConfig{
				Address:          "localhost:6379",
				Stream:           "test\nDEL key\n",
				StrictValidation: true,
			},
			expectError:   true,
			errorContains: "invalid stream name",
		},
		{
			name: "invalid stream name accepted when not strict",
			sourceConfig: SourceConfig{
				Address:          "localhost:6379",
				Stream:           "test<>|&",
				StrictValidation: false,
			},
			expectError: false,
		},
		{
			name: "channel with special chars rejected in strict mode",
			sourceConfig: SourceConfig{
				Address:          "localhost:6379",
				Channel:          "test<script>alert('xss')</script>",
				StrictValidation: true,
			},
			expectError:   true,
			errorContains: "invalid channel name",
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

// TestRunnerConfigSecurityValidation tests security-related target config validation
func TestRunnerConfigSecurityValidation(t *testing.T) {
	tests := []struct {
		name          string
		targetConfig  RunnerConfig
		expectError   bool
		errorContains string
	}{
		{
			name: "invalid channel name (command injection)",
			targetConfig: RunnerConfig{
				Address:          "localhost:6379",
				Channel:          "test\r\nDEL mykey\r\n",
				StrictValidation: true,
			},
			expectError:   true,
			errorContains: "invalid channel name",
		},
		{
			name: "invalid stream name (special chars)",
			targetConfig: RunnerConfig{
				Address:          "localhost:6379",
				Stream:           "test<>|&",
				StrictValidation: true,
			},
			expectError:   true,
			errorContains: "invalid stream name",
		},
		{
			name: "stream with null byte",
			targetConfig: RunnerConfig{
				Address:          "localhost:6379",
				Stream:           "test\x00stream",
				StrictValidation: true,
			},
			expectError:   true,
			errorContains: "invalid stream name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRunner(&tt.targetConfig)

			// Note: We expect errors for validation issues before connection attempts
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got nil")
					return
				}
				if tt.errorContains != "" && !stringContains(err.Error(), tt.errorContains) {
					t.Errorf("expected error to contain %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				// For valid configs, we might get connection errors in tests
				// but the validation should pass
				if err != nil && stringContains(err.Error(), "invalid") {
					t.Errorf("expected no validation error but got: %v", err)
				}
			}
		})
	}
}

// TestCommandInjectionPrevention tests various command injection attempts
func TestCommandInjectionPrevention(t *testing.T) {
	injectionAttempts := []struct {
		name    string
		attempt string
	}{
		{"carriage return and newline", "key\r\nDEL mykey\r\n"},
		{"newline only", "key\nSET evil value\n"},
		{"null byte", "key\x00DEL key"},
		{"control character SOH", "key\x01SET key value"},
		{"carriage return only", "key\rDEL key"},
		{"tab character", "key\tvalue"},
		{"multiple control chars", "key\r\n\x00\x01"},
	}

	for _, tt := range injectionAttempts {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRedisKey(tt.attempt, true)
			if err == nil {
				t.Errorf("expected validation to reject command injection attempt: %q", tt.attempt)
			}
		})
	}
}

// TestSourceConfigInvalidType tests error handling for invalid config types
func TestSourceConfigInvalidType(t *testing.T) {
	_, err := NewSource("invalid config")
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
	if !stringContains(err.Error(), "invalid config type") {
		t.Errorf("expected type error, got: %v", err)
	}
}

// TestRunnerConfigInvalidType tests error handling for invalid config types
func TestRunnerConfigInvalidType(t *testing.T) {
	_, err := NewRunner("invalid config")
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
	if !stringContains(err.Error(), "invalid config type") {
		t.Errorf("expected type error, got: %v", err)
	}
}
