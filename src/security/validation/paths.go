package validation

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ValidatePluginPath validates that a plugin path is safe to load
// It checks for:
// - Path traversal attempts
// - File existence and type
// - Correct file extension (.so)
// - Optional directory restriction
func ValidatePluginPath(pluginPath string, allowedDir string) error {
	// Clean the path to resolve any . or .. components
	cleanPath := filepath.Clean(pluginPath)

	// Check for path traversal attempts
	if strings.Contains(cleanPath, "..") {
		return fmt.Errorf("path traversal detected in plugin path: %s", pluginPath)
	}

	// If allowedDir is specified, ensure the plugin is within it
	if allowedDir != "" {
		absAllowedDir, err := filepath.Abs(allowedDir)
		if err != nil {
			return fmt.Errorf("failed to resolve allowed directory: %w", err)
		}

		absPluginPath, err := filepath.Abs(cleanPath)
		if err != nil {
			return fmt.Errorf("failed to resolve plugin path: %w", err)
		}

		if !strings.HasPrefix(absPluginPath, absAllowedDir+string(filepath.Separator)) &&
			absPluginPath != absAllowedDir {
			return fmt.Errorf("plugin path outside allowed directory: %s", pluginPath)
		}
	}

	// Check file exists
	info, err := os.Stat(cleanPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("plugin file does not exist: %s", cleanPath)
		}
		return fmt.Errorf("failed to stat plugin file: %w", err)
	}

	// Ensure it's a regular file, not a directory or symlink
	if !info.Mode().IsRegular() {
		return fmt.Errorf("plugin path is not a regular file: %s", cleanPath)
	}

	// Ensure it has .so extension
	if filepath.Ext(cleanPath) != ".so" {
		return fmt.Errorf("invalid plugin extension: must be .so, got %s", filepath.Ext(cleanPath))
	}

	return nil
}

// ValidateConfigPath validates that a configuration file path is safe to load
// It checks for:
// - Path traversal attempts
// - File is within allowed directories
// - No symlink attacks
func ValidateConfigPath(configPath string, allowedDirs []string) error {
	// Clean the path
	cleanPath := filepath.Clean(configPath)

	// Check for path traversal
	if strings.Contains(cleanPath, "..") {
		return fmt.Errorf("path traversal detected in config path: %s", configPath)
	}

	// Resolve to absolute path
	absPath, err := filepath.Abs(cleanPath)
	if err != nil {
		return fmt.Errorf("failed to resolve config path: %w", err)
	}

	// Check if path is in allowed directories
	allowed := false
	for _, dir := range allowedDirs {
		absDir, err := filepath.Abs(dir)
		if err != nil {
			continue
		}
		if strings.HasPrefix(absPath, absDir+string(filepath.Separator)) ||
			absPath == absDir {
			allowed = true
			break
		}
	}

	if !allowed && len(allowedDirs) > 0 {
		return fmt.Errorf("config path outside allowed directories: %s", configPath)
	}

	// Check for symlink attacks using Lstat
	info, err := os.Lstat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("config file does not exist: %s", absPath)
		}
		return fmt.Errorf("failed to stat config file: %w", err)
	}

	// Reject symlinks
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("config path cannot be a symlink: %s", absPath)
	}

	// Ensure it's a regular file
	if !info.Mode().IsRegular() {
		return fmt.Errorf("config path is not a regular file: %s", absPath)
	}

	return nil
}

// SanitizePath cleans a file path and ensures it's safe
func SanitizePath(path string) (string, error) {
	// Clean the path
	cleanPath := filepath.Clean(path)

	// Check for path traversal in cleaned path (.. as directory component)
	parts := strings.Split(cleanPath, string(filepath.Separator))
	for _, part := range parts {
		if part == ".." {
			return "", fmt.Errorf("path traversal detected: %s", path)
		}
	}

	return cleanPath, nil
}
