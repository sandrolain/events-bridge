package manager

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ValidatePluginPath validates that a plugin executable path is safe and within allowed directory
// Prevents path traversal attacks and ensures plugins are loaded from trusted locations
// Supports CLI executables (not .so files) that communicate via gRPC
func ValidatePluginPath(pluginPath string, allowedDir string) error {
	if pluginPath == "" {
		return fmt.Errorf("plugin path cannot be empty")
	}

	// Check for path traversal attempts in the raw path
	if strings.Contains(pluginPath, "..") {
		return fmt.Errorf("plugin path contains path traversal sequence '..'")
	}

	// Resolve to absolute path
	absPluginPath, err := filepath.Abs(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute plugin path: %w", err)
	}

	// Clean the path to resolve any remaining .. or .
	absPluginPath = filepath.Clean(absPluginPath)

	// Check if within allowed directory if specified
	if allowedDir != "" {
		absAllowedDir, err := filepath.Abs(allowedDir)
		if err != nil {
			return fmt.Errorf("failed to resolve absolute allowed directory: %w", err)
		}

		absAllowedDir = filepath.Clean(absAllowedDir)

		// Ensure plugin path is within allowed directory
		if !strings.HasPrefix(absPluginPath, absAllowedDir+string(filepath.Separator)) &&
			absPluginPath != absAllowedDir {
			return fmt.Errorf("plugin path '%s' is outside allowed directory '%s'", absPluginPath, absAllowedDir)
		}
	}

	// Check if file exists
	info, err := os.Stat(pluginPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("plugin executable does not exist: %s", pluginPath)
		}
		return fmt.Errorf("failed to stat plugin executable: %w", err)
	}

	// Verify it's a regular file
	if !info.Mode().IsRegular() {
		return fmt.Errorf("plugin path is not a regular file")
	}

	// Check if file is executable (on Unix-like systems)
	if info.Mode().Perm()&0111 == 0 {
		return fmt.Errorf("plugin file is not executable: %s", pluginPath)
	}

	return nil
}

// VerifyPluginHash computes and verifies the SHA256 hash of a plugin executable
func VerifyPluginHash(pluginPath string, expectedHash string) error {
	if expectedHash == "" {
		return fmt.Errorf("expected hash cannot be empty")
	}

	// Validate hash format (must be 64 hex characters for SHA256)
	if !regexp.MustCompile(`^[a-fA-F0-9]{64}$`).MatchString(expectedHash) {
		return fmt.Errorf("invalid hash format, expected 64 hex characters")
	}

	f, err := os.Open(pluginPath) // #nosec G304 - pluginPath is validated by ValidatePluginPath before this function is called
	if err != nil {
		return fmt.Errorf("failed to open plugin executable: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("failed to compute hash: %w", err)
	}

	actualHash := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(actualHash, expectedHash) {
		return fmt.Errorf("hash mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	return nil
}

// ValidatePluginName validates that a plugin name is safe for use
func ValidatePluginName(name string) error {
	if name == "" {
		return fmt.Errorf("plugin name cannot be empty")
	}

	// Only allow alphanumeric characters, hyphens, and underscores
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]+$`).MatchString(name) {
		return fmt.Errorf("plugin name contains invalid characters (only alphanumeric, hyphens, and underscores allowed)")
	}

	// Prevent excessively long names
	if len(name) > 128 {
		return fmt.Errorf("plugin name too long (max 128 characters)")
	}

	return nil
}

// SanitizePluginEnv sanitizes environment variables to prevent injection
func SanitizePluginEnv(envVars []string) error {
	for _, env := range envVars {
		// Check format KEY=VALUE
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid environment variable format: %s", env)
		}

		key := parts[0]
		value := parts[1]

		// Validate key format
		if !regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`).MatchString(key) {
			return fmt.Errorf("invalid environment variable key: %s", key)
		}

		// Check for dangerous characters in value
		if strings.ContainsAny(value, "\x00\n\r") {
			return fmt.Errorf("environment variable value contains dangerous characters: %s", key)
		}
	}
	return nil
}

// ValidatePluginArgs validates plugin arguments to prevent command injection
func ValidatePluginArgs(args []string) error {
	for i, arg := range args {
		// Check for null bytes
		if strings.Contains(arg, "\x00") {
			return fmt.Errorf("argument %d contains null byte", i)
		}

		// Check for command injection patterns
		dangerousPatterns := []string{";", "&", "|", "$", "`", "\n", "\r"}
		for _, pattern := range dangerousPatterns {
			if strings.Contains(arg, pattern) {
				return fmt.Errorf("argument %d contains dangerous character: %s", i, pattern)
			}
		}
	}
	return nil
}

// ComputePluginHash computes the SHA256 hash of a plugin executable
// Useful for generating expected hashes for verification
func ComputePluginHash(pluginPath string) (string, error) {
	f, err := os.Open(pluginPath) // #nosec G304 - pluginPath is provided by administrator for hash computation
	if err != nil {
		return "", fmt.Errorf("failed to open plugin executable: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("failed to compute hash: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
