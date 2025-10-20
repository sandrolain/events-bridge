package manager

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// TestValidatePluginPath tests path validation
func TestValidatePluginPath(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()

	// Create a valid plugin executable
	validPlugin := filepath.Join(tmpDir, "test-plugin")
	// #nosec G306 - test executable needs exec permissions
	if err := os.WriteFile(validPlugin, []byte("#!/bin/sh\necho test"), 0755); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create a non-executable file
	nonExecFile := filepath.Join(tmpDir, "non-exec")
	// #nosec G306 - test file, intentionally non-executable
	if err := os.WriteFile(nonExecFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create non-executable file: %v", err)
	}

	tests := []struct {
		name        string
		pluginPath  string
		allowedDir  string
		wantErr     bool
		errContains string
	}{
		{
			name:       "Valid plugin path",
			pluginPath: validPlugin,
			allowedDir: tmpDir,
			wantErr:    false,
		},
		{
			name:        "Empty path",
			pluginPath:  "",
			allowedDir:  tmpDir,
			wantErr:     true,
			errContains: "cannot be empty",
		},
		{
			name:        "Path traversal with ..",
			pluginPath:  "../test-plugin",
			allowedDir:  tmpDir,
			wantErr:     true,
			errContains: "path traversal",
		},
		{
			name:        "Path outside allowed directory",
			pluginPath:  "/tmp/evil-plugin",
			allowedDir:  tmpDir,
			wantErr:     true,
			errContains: "outside allowed directory",
		},
		{
			name:        "Non-executable file",
			pluginPath:  nonExecFile,
			allowedDir:  tmpDir,
			wantErr:     true,
			errContains: "not executable",
		},
		{
			name:        "Non-existent file",
			pluginPath:  filepath.Join(tmpDir, "nonexistent"),
			allowedDir:  tmpDir,
			wantErr:     true,
			errContains: "does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePluginPath(tt.pluginPath, tt.allowedDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePluginPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errContains != "" {
				if err == nil || !contains(err.Error(), tt.errContains) {
					t.Errorf("ValidatePluginPath() error = %v, want error containing %q", err, tt.errContains)
				}
			}
		})
	}
}

// TestVerifyPluginHash tests hash verification
func TestVerifyPluginHash(t *testing.T) {
	// Create a temporary executable with known content
	tmpFile := filepath.Join(t.TempDir(), "test-plugin")
	content := []byte("#!/bin/sh\necho 'test plugin'")
	// #nosec G306 - test executable needs exec permissions
	if err := os.WriteFile(tmpFile, content, 0755); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Compute the actual hash
	actualHash, err := ComputePluginHash(tmpFile)
	if err != nil {
		t.Fatalf("Failed to compute hash: %v", err)
	}

	tests := []struct {
		name         string
		pluginPath   string
		expectedHash string
		wantErr      bool
		errContains  string
	}{
		{
			name:         "Valid hash",
			pluginPath:   tmpFile,
			expectedHash: actualHash,
			wantErr:      false,
		},
		{
			name:         "Empty hash",
			pluginPath:   tmpFile,
			expectedHash: "",
			wantErr:      true,
			errContains:  "cannot be empty",
		},
		{
			name:         "Invalid hash format",
			pluginPath:   tmpFile,
			expectedHash: "not-a-valid-hash",
			wantErr:      true,
			errContains:  "invalid hash format",
		},
		{
			name:         "Hash mismatch",
			pluginPath:   tmpFile,
			expectedHash: "0000000000000000000000000000000000000000000000000000000000000000",
			wantErr:      true,
			errContains:  "hash mismatch",
		},
		{
			name:         "Non-existent file",
			pluginPath:   "/nonexistent/file.so",
			expectedHash: actualHash,
			wantErr:      true,
			errContains:  "failed to open",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := VerifyPluginHash(tt.pluginPath, tt.expectedHash)
			if (err != nil) != tt.wantErr {
				t.Errorf("VerifyPluginHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errContains != "" {
				if err == nil || !contains(err.Error(), tt.errContains) {
					t.Errorf("VerifyPluginHash() error = %v, want error containing %q", err, tt.errContains)
				}
			}
		})
	}
}

// TestValidatePluginName tests plugin name validation
func TestValidatePluginName(t *testing.T) {
	tests := []struct {
		name        string
		pluginName  string
		wantErr     bool
		errContains string
	}{
		{
			name:       "Valid name with alphanumeric",
			pluginName: "myPlugin123",
			wantErr:    false,
		},
		{
			name:       "Valid name with hyphens",
			pluginName: "my-plugin-name",
			wantErr:    false,
		},
		{
			name:       "Valid name with underscores",
			pluginName: "my_plugin_name",
			wantErr:    false,
		},
		{
			name:        "Empty name",
			pluginName:  "",
			wantErr:     true,
			errContains: "cannot be empty",
		},
		{
			name:        "Name with spaces",
			pluginName:  "my plugin",
			wantErr:     true,
			errContains: "invalid characters",
		},
		{
			name:        "Name with special characters",
			pluginName:  "my@plugin",
			wantErr:     true,
			errContains: "invalid characters",
		},
		{
			name:        "Name with path separator",
			pluginName:  "my/plugin",
			wantErr:     true,
			errContains: "invalid characters",
		},
		{
			name:        "Name too long",
			pluginName:  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // 129 a's
			wantErr:     true,
			errContains: "too long",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePluginName(tt.pluginName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePluginName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errContains != "" {
				if err == nil || !contains(err.Error(), tt.errContains) {
					t.Errorf("ValidatePluginName() error = %v, want error containing %q", err, tt.errContains)
				}
			}
		})
	}
}

// TestSanitizePluginEnv tests environment variable sanitization
func TestSanitizePluginEnv(t *testing.T) {
	tests := []struct {
		name        string
		envVars     []string
		wantErr     bool
		errContains string
	}{
		{
			name:    "Valid environment variables",
			envVars: []string{"KEY1=value1", "KEY2=value2"},
			wantErr: false,
		},
		{
			name:    "Valid with underscores",
			envVars: []string{"MY_KEY=my_value"},
			wantErr: false,
		},
		{
			name:        "Invalid format - no equals",
			envVars:     []string{"INVALID"},
			wantErr:     true,
			errContains: "invalid environment variable format",
		},
		{
			name:        "Invalid key format",
			envVars:     []string{"123KEY=value"},
			wantErr:     true,
			errContains: "invalid environment variable key",
		},
		{
			name:        "Key with special characters",
			envVars:     []string{"KEY@NAME=value"},
			wantErr:     true,
			errContains: "invalid environment variable key",
		},
		{
			name:        "Value with null byte",
			envVars:     []string{"KEY=value\x00"},
			wantErr:     true,
			errContains: "dangerous characters",
		},
		{
			name:        "Value with newline",
			envVars:     []string{"KEY=value\n"},
			wantErr:     true,
			errContains: "dangerous characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SanitizePluginEnv(tt.envVars)
			if (err != nil) != tt.wantErr {
				t.Errorf("SanitizePluginEnv() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errContains != "" {
				if err == nil || !contains(err.Error(), tt.errContains) {
					t.Errorf("SanitizePluginEnv() error = %v, want error containing %q", err, tt.errContains)
				}
			}
		})
	}
}

// TestValidatePluginArgs tests plugin arguments validation
func TestValidatePluginArgs(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantErr     bool
		errContains string
	}{
		{
			name:    "Valid arguments",
			args:    []string{"--config", "/path/to/config", "--verbose"},
			wantErr: false,
		},
		{
			name:    "Empty arguments",
			args:    []string{},
			wantErr: false,
		},
		{
			name:        "Argument with null byte",
			args:        []string{"--config\x00"},
			wantErr:     true,
			errContains: "null byte",
		},
		{
			name:        "Argument with semicolon (command injection)",
			args:        []string{"--config; rm -rf /"},
			wantErr:     true,
			errContains: "dangerous character",
		},
		{
			name:        "Argument with ampersand (command injection)",
			args:        []string{"--config & malicious"},
			wantErr:     true,
			errContains: "dangerous character",
		},
		{
			name:        "Argument with pipe (command injection)",
			args:        []string{"--config | malicious"},
			wantErr:     true,
			errContains: "dangerous character",
		},
		{
			name:        "Argument with dollar (variable expansion)",
			args:        []string{"--config $EVIL"},
			wantErr:     true,
			errContains: "dangerous character",
		},
		{
			name:        "Argument with backtick (command substitution)",
			args:        []string{"--config `malicious`"},
			wantErr:     true,
			errContains: "dangerous character",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePluginArgs(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePluginArgs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errContains != "" {
				if err == nil || !contains(err.Error(), tt.errContains) {
					t.Errorf("ValidatePluginArgs() error = %v, want error containing %q", err, tt.errContains)
				}
			}
		})
	}
}

// TestComputePluginHash tests hash computation
func TestComputePluginHash(t *testing.T) {
	// Create a temporary executable with known content
	tmpFile := filepath.Join(t.TempDir(), "test-plugin")
	content := []byte("#!/bin/sh\necho test")
	// #nosec G306 - test executable needs exec permissions
	if err := os.WriteFile(tmpFile, content, 0755); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	hash, err := ComputePluginHash(tmpFile)
	if err != nil {
		t.Fatalf("ComputePluginHash() error = %v", err)
	}

	// Verify hash format
	if len(hash) != 64 {
		t.Errorf("ComputePluginHash() returned hash with length %d, want 64", len(hash))
	}

	// Verify hash is consistent
	hash2, err := ComputePluginHash(tmpFile)
	if err != nil {
		t.Fatalf("ComputePluginHash() second call error = %v", err)
	}

	if hash != hash2 {
		t.Errorf("ComputePluginHash() returned different hashes: %s vs %s", hash, hash2)
	}

	// Test with non-existent file
	_, err = ComputePluginHash("/nonexistent/file.so")
	if err == nil {
		t.Error("ComputePluginHash() expected error for non-existent file")
	}
}

// TestPluginSecurityValidation tests the complete security validation in Plugin
func TestPluginSecurityValidation(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a valid plugin executable
	validPlugin := filepath.Join(tmpDir, "test-plugin-exec")
	// #nosec G306 - test executable needs exec permissions
	if err := os.WriteFile(validPlugin, []byte("#!/bin/sh\necho 'plugin'"), 0755); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	hash, err := ComputePluginHash(validPlugin)
	if err != nil {
		t.Fatalf("Failed to compute hash: %v", err)
	}

	tests := []struct {
		name        string
		config      PluginConfig
		wantErr     bool
		errContains string
	}{
		{
			name: "Valid configuration",
			config: PluginConfig{
				Name:              "test-plugin",
				Exec:              validPlugin,
				Protocol:          "unix",
				StrictValidation:  true,
				AllowedPluginsDir: tmpDir,
				VerifyHash:        false,
			},
			wantErr: false,
		},
		{
			name: "Valid with hash verification",
			config: PluginConfig{
				Name:              "test-plugin",
				Exec:              validPlugin,
				Protocol:          "unix",
				StrictValidation:  true,
				AllowedPluginsDir: tmpDir,
				VerifyHash:        true,
				ExpectedSHA256:    hash,
			},
			wantErr: false,
		},
		{
			name: "Invalid plugin name",
			config: PluginConfig{
				Name:             "test@plugin",
				Exec:             validPlugin,
				Protocol:         "unix",
				StrictValidation: true,
			},
			wantErr:     true,
			errContains: "invalid plugin name",
		},
		{
			name: "Path outside allowed directory",
			config: PluginConfig{
				Name:              "test-plugin",
				Exec:              "/tmp/evil-plugin",
				Protocol:          "unix",
				StrictValidation:  true,
				AllowedPluginsDir: tmpDir,
			},
			wantErr:     true,
			errContains: "path validation failed",
		},
		{
			name: "Hash verification enabled without hash",
			config: PluginConfig{
				Name:             "test-plugin",
				Exec:             validPlugin,
				Protocol:         "unix",
				StrictValidation: false,
				VerifyHash:       true,
				ExpectedSHA256:   "",
			},
			wantErr:     true,
			errContains: "expectedSHA256 not provided",
		},
		{
			name: "Invalid environment variables",
			config: PluginConfig{
				Name:             "test-plugin",
				Exec:             validPlugin,
				Protocol:         "unix",
				StrictValidation: false,
				Env:              []string{"INVALID"},
			},
			wantErr:     true,
			errContains: "invalid environment variables",
		},
		{
			name: "Invalid arguments",
			config: PluginConfig{
				Name:             "test-plugin",
				Exec:             validPlugin,
				Protocol:         "unix",
				StrictValidation: false,
				Args:             []string{"--config; rm -rf /"},
			},
			wantErr:     true,
			errContains: "invalid plugin arguments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Plugin{
				Config: tt.config,
				slog:   testLogger(t),
			}

			err := p.validateSecurity()
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSecurity() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errContains != "" {
				if err == nil || !contains(err.Error(), tt.errContains) {
					t.Errorf("validateSecurity() error = %v, want error containing %q", err, tt.errContains)
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// testLogger creates a logger for testing
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.Default()
}
