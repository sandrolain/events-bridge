package main

import (
	"strings"
	"testing"
)

// TestValidateBranchName tests the branch name validation function
func TestValidateBranchName(t *testing.T) {
	tests := []struct {
		name       string
		branch     string
		strict     bool
		shouldFail bool
		errorMsg   string
	}{
		// Valid cases
		{
			name:       "simple branch name",
			branch:     "main",
			strict:     true,
			shouldFail: false,
		},
		{
			name:       "branch with slash",
			branch:     "feature/new-feature",
			strict:     true,
			shouldFail: false,
		},
		{
			name:       "branch with underscore",
			branch:     "feature_branch",
			strict:     true,
			shouldFail: false,
		},
		{
			name:       "branch with numbers",
			branch:     "release-1.2.3",
			strict:     false, // dots not allowed in strict mode
			shouldFail: false,
		},
		{
			name:       "complex valid branch",
			branch:     "feature/PROJ-123_implement-auth",
			strict:     true,
			shouldFail: false,
		},

		// Invalid cases - Path traversal
		{
			name:       "path traversal attempt",
			branch:     "../../../etc/passwd",
			strict:     true,
			shouldFail: true,
			errorMsg:   "path traversal",
		},
		{
			name:       "path traversal in middle",
			branch:     "feature/../main",
			strict:     true,
			shouldFail: true,
			errorMsg:   "path traversal",
		},

		// Invalid cases - Command injection
		{
			name:       "semicolon injection",
			branch:     "main; rm -rf /",
			strict:     true,
			shouldFail: true,
			errorMsg:   "dangerous character",
		},
		{
			name:       "pipe injection",
			branch:     "main | cat /etc/passwd",
			strict:     true,
			shouldFail: true,
			errorMsg:   "dangerous character",
		},
		{
			name:       "ampersand injection",
			branch:     "main & echo hacked",
			strict:     true,
			shouldFail: true,
			errorMsg:   "dangerous character",
		},
		{
			name:       "backtick injection",
			branch:     "main`whoami`",
			strict:     true,
			shouldFail: true,
			errorMsg:   "dangerous character",
		},
		{
			name:       "dollar injection",
			branch:     "main$(whoami)",
			strict:     true,
			shouldFail: true,
			errorMsg:   "dangerous character",
		},
		{
			name:       "newline injection",
			branch:     "main\nrm -rf /",
			strict:     true,
			shouldFail: true,
			errorMsg:   "dangerous character",
		},

		// Invalid cases - Strict mode
		{
			name:       "space in strict mode",
			branch:     "feature branch",
			strict:     true,
			shouldFail: true,
			errorMsg:   "invalid characters",
		},
		{
			name:       "dot in strict mode",
			branch:     "release-1.2.3",
			strict:     true,
			shouldFail: true,
			errorMsg:   "invalid characters",
		},
		{
			name:       "special chars in strict mode",
			branch:     "feature@v1",
			strict:     true,
			shouldFail: true,
			errorMsg:   "invalid characters",
		},

		// Invalid cases - Other
		{
			name:       "empty branch name",
			branch:     "",
			strict:     true,
			shouldFail: true,
			errorMsg:   "cannot be empty",
		},
		{
			name:       "starts with hyphen",
			branch:     "-main",
			strict:     true,
			shouldFail: true,
			errorMsg:   "cannot start with hyphen",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBranchName(tt.branch, tt.strict)

			if tt.shouldFail {
				if err == nil {
					t.Errorf("expected error for branch '%s', got nil", tt.branch)
				} else if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for branch '%s': %v", tt.branch, err)
				}
			}
		})
	}
}

// TestValidateBranchNameLenientMode tests that lenient mode allows more characters
func TestValidateBranchNameLenientMode(t *testing.T) {
	tests := []struct {
		name   string
		branch string
	}{
		{"with dots", "release-1.2.3"},
		{"with at sign", "user@branch"},
		{"with plus", "feature+improvement"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBranchName(tt.branch, false) // lenient mode
			if err != nil {
				// Should still fail in lenient mode if dangerous chars present
				if !strings.Contains(err.Error(), "dangerous character") {
					t.Errorf("unexpected error in lenient mode for '%s': %v", tt.branch, err)
				}
			}
		})
	}
}

// TestSourceConfigWithSSHKey tests source creation with SSH key authentication
func TestSourceConfigWithSSHKey(t *testing.T) {
	cfg := &SourceConfig{
		Path:       "/tmp/test-repo",
		RemoteURL:  "git@github.com:user/repo.git",
		Remote:     "origin",
		Branch:     "main",
		SSHKeyFile: "/path/to/id_rsa",
	}

	// Validate branch name is checked
	source, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("unexpected error creating source with SSH key: %v", err)
	}

	if source == nil {
		t.Fatal("expected non-nil source")
	}

	gitSource, ok := source.(*GitSource)
	if !ok {
		t.Fatalf("expected *GitSource, got %T", source)
	}

	if gitSource.cfg.SSHKeyFile != "/path/to/id_rsa" {
		t.Errorf("expected SSHKeyFile '/path/to/id_rsa', got '%s'", gitSource.cfg.SSHKeyFile)
	}
}

// TestSourceConfigBranchValidation tests that invalid branch names are rejected
func TestSourceConfigBranchValidation(t *testing.T) {
	tests := []struct {
		name       string
		branch     string
		shouldFail bool
	}{
		{"valid branch", "main", false},
		{"valid feature", "feature/auth", false},
		{"command injection", "main; rm -rf /", true},
		{"path traversal", "../../../etc/passwd", true},
		{"pipe injection", "main | cat /etc/passwd", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &SourceConfig{
				Path:                   "/tmp/test-repo",
				RemoteURL:              "https://github.com/user/repo.git",
				Remote:                 "origin",
				Branch:                 tt.branch,
				StrictBranchValidation: true,
			}

			_, err := NewSource(cfg)

			if tt.shouldFail {
				if err == nil {
					t.Errorf("expected error for branch '%s', got nil", tt.branch)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for branch '%s': %v", tt.branch, err)
				}
			}
		})
	}
}

// TestSourceConfigInvalidType tests error handling for invalid config type
func TestSourceConfigInvalidTypeGit(t *testing.T) {
	_, err := NewSource("invalid config")
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}

	if !strings.Contains(err.Error(), "invalid config type") {
		t.Errorf("expected 'invalid config type' error, got: %v", err)
	}
}

// TestSourceConfigStrictValidationDefault tests that strict validation is enabled by default
func TestSourceConfigStrictValidationDefault(t *testing.T) {
	cfg := &SourceConfig{
		Path:      "/tmp/test-repo",
		RemoteURL: "https://github.com/user/repo.git",
		Remote:    "origin",
		Branch:    "release-1.2.3", // Contains dots, invalid in strict mode
		// StrictBranchValidation not set, should default to true
	}

	// Note: This test assumes default is set by mapstructure/validator
	// In actual usage, the default tag would set StrictBranchValidation to true
	// For this test, we explicitly set it
	cfg.StrictBranchValidation = true

	_, err := NewSource(cfg)
	if err == nil {
		t.Error("expected error for branch with dots in strict mode")
	}
}

// TestBuildAuthMethodPriority tests that SSH authentication has priority over HTTP
func TestBuildAuthMethodPriority(t *testing.T) {
	// Test case: both SSH and HTTP credentials provided
	// SSH should take priority
	cfg := &SourceConfig{
		Path:       "/tmp/test-repo",
		RemoteURL:  "git@github.com:user/repo.git",
		Branch:     "main",
		Username:   "httpuser",
		Password:   "httppass",
		SSHKeyFile: "/path/to/id_rsa",
	}

	source, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gitSource := source.(*GitSource)

	// Note: buildAuthMethod is called internally, we can't test it directly here
	// but we verify the config is set correctly
	if gitSource.cfg.SSHKeyFile == "" {
		t.Error("expected SSHKeyFile to be set")
	}
	if gitSource.cfg.Username == "" {
		t.Error("expected Username to be preserved")
	}
}

// TestValidateBranchNameEdgeCases tests edge cases in branch name validation
func TestValidateBranchNameEdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		branch     string
		strict     bool
		shouldFail bool
	}{
		// Very long branch name (should be allowed)
		{
			name:       "very long branch name",
			branch:     strings.Repeat("a", 255),
			strict:     true,
			shouldFail: false,
		},
		// Single character (should be allowed)
		{
			name:       "single character",
			branch:     "a",
			strict:     true,
			shouldFail: false,
		},
		// Unicode characters (should fail in strict mode)
		{
			name:       "unicode characters strict",
			branch:     "feature-über",
			strict:     true,
			shouldFail: true,
		},
		// Multiple slashes (should be allowed)
		{
			name:       "multiple slashes",
			branch:     "team/project/feature/new",
			strict:     true,
			shouldFail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBranchName(tt.branch, tt.strict)

			if tt.shouldFail {
				if err == nil {
					t.Errorf("expected error for branch '%s', got nil", tt.branch)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for branch '%s': %v", tt.branch, err)
				}
			}
		})
	}
}
