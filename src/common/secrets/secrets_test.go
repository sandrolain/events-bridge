package secrets

import (
	"os"
	"path/filepath"
	"testing"
)

const (
	errMsgUnexpected = "unexpected error: %v"
)

// TestResolvePlainText ensures plain text is returned as-is
func TestResolvePlainText(t *testing.T) {
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
			got, err := Resolve(tt.input)
			if err != nil {
				t.Fatalf(errMsgUnexpected, err)
			}
			if got != tt.want {
				t.Errorf("Resolve(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestResolveFromEnv tests environment variable resolution
func TestResolveFromEnv(t *testing.T) {
	envVar := "TEST_SECRET_VAR"
	expectedSecret := "super-secret-value-from-env"

	// Set environment variable
	err := os.Setenv(envVar, expectedSecret)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}
	defer os.Unsetenv(envVar)

	got, err := Resolve("env:" + envVar)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}
	if got != expectedSecret {
		t.Errorf("Resolve(env:%s) = %q, want %q", envVar, got, expectedSecret)
	}
}

// TestResolveFromEnvMissing tests that missing env vars return empty string
func TestResolveFromEnvMissing(t *testing.T) {
	envVar := "NONEXISTENT_SECRET_VAR"

	got, err := Resolve("env:" + envVar)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}
	if got != "" {
		t.Errorf("Resolve(env:%s) = %q, want empty string", envVar, got)
	}
}

// TestResolveFromFile tests reading secret from file
func TestResolveFromFile(t *testing.T) {
	// Create temporary file with secret
	tmpDir := t.TempDir()
	secretFile := filepath.Join(tmpDir, "secret.txt")
	const secretContent = "file-based-secret-content" // #nosec G101 - test constant, not a credential

	err := os.WriteFile(secretFile, []byte(secretContent), 0600)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}

	got, err := Resolve("file:" + secretFile)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}
	if got != secretContent {
		t.Errorf("Resolve(file:%s) = %q, want %q", secretFile, got, secretContent)
	}
}

// TestResolveFromFileWithWhitespace tests trimming of file content
func TestResolveFromFileWithWhitespace(t *testing.T) {
	tmpDir := t.TempDir()
	secretFile := filepath.Join(tmpDir, "secret-whitespace.txt")
	secretContent := "  \n  secret-with-newlines  \n  "
	expectedSecret := "secret-with-newlines"

	err := os.WriteFile(secretFile, []byte(secretContent), 0600)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}

	got, err := Resolve("file:" + secretFile)
	if err != nil {
		t.Fatalf(errMsgUnexpected, err)
	}
	if got != expectedSecret {
		t.Errorf("Resolve(file:%s) = %q, want %q", secretFile, got, expectedSecret)
	}
}

// TestResolveFileRequiresAbsolute ensures relative paths are rejected
func TestResolveFileRequiresAbsolute(t *testing.T) {
	_, err := Resolve("file:relative/path/secret.txt")
	if err == nil {
		t.Fatal("expected error for relative path, got nil")
	}
	expectedMsgPart := "file secret path must be absolute"
	if !containsString(err.Error(), expectedMsgPart) {
		t.Errorf("error = %q, want to contain %q", err.Error(), expectedMsgPart)
	}
}

// TestResolveFromFileMissing tests error handling for missing files
func TestResolveFromFileMissing(t *testing.T) {
	nonExistentPath := "/tmp/nonexistent-secret-file-12345.txt"

	_, err := Resolve("file:" + nonExistentPath)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	// Error should contain file path
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

// Helper function to check if a string contains a substring
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			len(s) > len(substr)+1 && anyContains(s, substr)))
}

func anyContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
