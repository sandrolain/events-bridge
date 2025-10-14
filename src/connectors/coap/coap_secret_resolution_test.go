package main

import (
	"os"
	"path/filepath"
	"testing"
)

const (
	errMsgUnexpected = "unexpected error: %v"
)

// TestResolveSecretPlainText ensures plain text is returned as-is
func TestResolveSecretPlainText(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "simple string",
			input: "my-secret-key",
			want:  "my-secret-key",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "string with spaces",
			input: "  secret-with-spaces  ",
			want:  "secret-with-spaces",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveSecret(tt.input)
			if err != nil {
				t.Fatalf(errMsgUnexpected, err)
			}
			if got != tt.want {
				t.Errorf("resolveSecret(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestResolveSecretFromEnv tests environment variable resolution
func TestResolveSecretFromEnv(t *testing.T) {
	envVar := "TEST_COAP_PSK_SECRET"
	expectedSecret := "super-secret-psk-value"

	// Set environment variable
	err := os.Setenv(envVar, expectedSecret)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}
	defer os.Unsetenv(envVar)

	got, err := resolveSecret("env:" + envVar)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}
	if got != expectedSecret {
		t.Errorf("resolveSecret(env:%s) = %q, want %q", envVar, got, expectedSecret)
	}
}

// TestResolveSecretFromEnvMissing tests that missing env vars return empty string
func TestResolveSecretFromEnvMissing(t *testing.T) {
	envVar := "NONEXISTENT_COAP_VAR"

	got, err := resolveSecret("env:" + envVar)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}
	if got != "" {
		t.Errorf("resolveSecret(env:%s) = %q, want empty string", envVar, got)
	}
}

// TestResolveSecretFromFile tests reading secret from file
func TestResolveSecretFromFile(t *testing.T) {
	// Create temporary file with secret
	tmpDir := t.TempDir()
	secretFile := filepath.Join(tmpDir, "coap-secret.txt")
	secretContent := "file-based-psk-secret"

	err := os.WriteFile(secretFile, []byte(secretContent), 0600)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}
	got, err := resolveSecret("file:" + secretFile)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}
	if got != secretContent {
		t.Errorf("resolveSecret(file:%s) = %q, want %q", secretFile, got, secretContent)
	}
}

// TestResolveSecretFromFileWithWhitespace tests trimming of file content
func TestResolveSecretFromFileWithWhitespace(t *testing.T) {
	tmpDir := t.TempDir()
	secretFile := filepath.Join(tmpDir, "coap-secret-whitespace.txt")
	secretContent := "  \n  secret-with-newlines  \n  "
	expectedSecret := "secret-with-newlines"

	err := os.WriteFile(secretFile, []byte(secretContent), 0600)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}

	got, err := resolveSecret("file:" + secretFile)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}
	if got != expectedSecret {
		t.Errorf("resolveSecret(file:%s) = %q, want %q", secretFile, got, expectedSecret)
	}
}

// TestResolveSecretFileRequiresAbsolute ensures relative paths are rejected
func TestResolveSecretFileRequiresAbsolute(t *testing.T) {
	_, err := resolveSecret("file:relative/path/secret.txt")
	if err == nil {
		t.Fatal("expected error for relative path, got nil")
	}
	expectedMsg := "file secret path must be absolute"
	if err.Error() != expectedMsg {
		t.Errorf("error = %q, want %q", err.Error(), expectedMsg)
	}
}

// TestResolveSecretFromFileMissing tests error handling for missing files
func TestResolveSecretFromFileMissing(t *testing.T) {
	nonExistentPath := "/tmp/nonexistent-coap-secret-file-12345.txt"

	_, err := resolveSecret("file:" + nonExistentPath)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	// Error should contain file path
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}
