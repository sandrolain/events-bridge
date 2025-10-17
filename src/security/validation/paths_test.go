package validation_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sandrolain/events-bridge/src/security/validation"
)

func TestValidatePluginPath(t *testing.T) {
	// Create temp directory for tests
	tmpDir := t.TempDir()

	// Create a valid plugin file
	validPlugin := filepath.Join(tmpDir, "test.so")
	if err := os.WriteFile(validPlugin, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test plugin: %v", err)
	}

	// Create a file with wrong extension
	wrongExt := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(wrongExt, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	tests := []struct {
		name       string
		pluginPath string
		allowedDir string
		wantErr    bool
		errMsg     string
	}{
		{
			name:       "valid plugin path",
			pluginPath: validPlugin,
			allowedDir: tmpDir,
			wantErr:    false,
		},
		{
			name:       "path traversal attempt",
			pluginPath: filepath.Join(tmpDir, "..", "test.so"),
			allowedDir: tmpDir,
			wantErr:    true,
			errMsg:     "outside allowed directory", // filepath.Clean normalizes .. so it becomes outside dir
		},
		{
			name:       "path outside allowed directory",
			pluginPath: "/tmp/other/test.so",
			allowedDir: tmpDir,
			wantErr:    true,
			errMsg:     "outside allowed directory",
		},
		{
			name:       "non-existent file",
			pluginPath: filepath.Join(tmpDir, "nonexistent.so"),
			allowedDir: tmpDir,
			wantErr:    true,
			errMsg:     "does not exist",
		},
		{
			name:       "invalid extension",
			pluginPath: wrongExt,
			allowedDir: tmpDir,
			wantErr:    true,
			errMsg:     "extension",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validation.ValidatePluginPath(tt.pluginPath, tt.allowedDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePluginPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errMsg != "" && err != nil {
				if !contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidatePluginPath() error = %v, want error containing %q", err, tt.errMsg)
				}
			}
		})
	}
}

func TestValidateConfigPath(t *testing.T) {
	// Create temp directories
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()
	allowedDirs := []string{tmpDir1, tmpDir2}

	// Create valid config file
	validConfig := filepath.Join(tmpDir1, "config.yaml")
	if err := os.WriteFile(validConfig, []byte("test: value"), 0644); err != nil {
		t.Fatalf("Failed to create test config: %v", err)
	}

	// Create symlink
	symlinkPath := filepath.Join(tmpDir1, "link.yaml")
	_ = os.Symlink(validConfig, symlinkPath) // Ignore error if symlinks not supported

	tests := []struct {
		name        string
		configPath  string
		allowedDirs []string
		wantErr     bool
		errMsg      string
	}{
		{
			name:        "valid config path",
			configPath:  validConfig,
			allowedDirs: allowedDirs,
			wantErr:     false,
		},
		{
			name:        "path traversal attempt",
			configPath:  filepath.Join(tmpDir1, "..", "config.yaml"),
			allowedDirs: allowedDirs,
			wantErr:     true,
			errMsg:      "outside allowed directories", // filepath.Clean normalizes ..
		},
		{
			name:        "path outside allowed directories",
			configPath:  "/tmp/other/config.yaml",
			allowedDirs: allowedDirs,
			wantErr:     true,
			errMsg:      "outside allowed directories",
		},
		{
			name:        "non-existent file",
			configPath:  filepath.Join(tmpDir1, "nonexistent.yaml"),
			allowedDirs: allowedDirs,
			wantErr:     true,
			errMsg:      "does not exist",
		},
		{
			name:        "symlink",
			configPath:  symlinkPath,
			allowedDirs: allowedDirs,
			wantErr:     true,
			errMsg:      "symlink",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validation.ValidateConfigPath(tt.configPath, tt.allowedDirs)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateConfigPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errMsg != "" && err != nil {
				if !contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateConfigPath() error = %v, want error containing %q", err, tt.errMsg)
				}
			}
		})
	}
}

func TestSanitizePath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    string
		wantErr bool
	}{
		{
			name:    "clean path",
			path:    "/var/lib/test.so",
			want:    "/var/lib/test.so",
			wantErr: false,
		},
		{
			name:    "path with dots in name",
			path:    "/var/lib/test..txt", // Contains .. but not as directory traversal
			want:    "/var/lib/test..txt",
			wantErr: false,
		},
		{
			name:    "relative path",
			path:    "./config/test.yaml",
			want:    "config/test.yaml",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validation.SanitizePath(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("SanitizePath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("SanitizePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			len(s) > len(substr)+1 && findSubstr(s, substr)))
}

func findSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
