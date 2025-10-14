// Package secrets provides utilities for resolving secret values from various sources.
// It supports reading secrets from environment variables, files, or using plain text values.
package secrets

import (
	"fmt"
	"os"
	"strings"
)

// Resolve resolves a secret value supporting multiple formats:
// - "env:NAME" reads from environment variable NAME
// - "file:/absolute/path" reads the contents of a file (requires absolute path for security)
// - Any other value is returned as-is (plain text)
//
// Empty or whitespace-only values return empty string without error.
//
// Examples:
//
//	secret, err := secrets.Resolve("env:MY_API_KEY")
//	secret, err := secrets.Resolve("file:/etc/secrets/api-key.txt")
//	secret, err := secrets.Resolve("plain-text-secret")
func Resolve(value string) (string, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return "", nil
	}

	// Environment variable: env:VARIABLE_NAME
	if strings.HasPrefix(v, "env:") {
		name := strings.TrimPrefix(v, "env:")
		return os.Getenv(name), nil
	}

	// File-based secret: file:/absolute/path
	if strings.HasPrefix(v, "file:") {
		path := strings.TrimPrefix(v, "file:")
		// Security: require absolute path to avoid traversal of relative locations
		if !strings.HasPrefix(path, "/") {
			return "", fmt.Errorf("file secret path must be absolute, got: %s", path)
		}
		// #nosec G304 - path is user-provided by configuration and required for file-based secrets; we enforce absolute path above
		content, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("failed to read secret file %s: %w", path, err)
		}
		return strings.TrimSpace(string(content)), nil
	}

	// Plain text (not recommended for production)
	return v, nil
}
