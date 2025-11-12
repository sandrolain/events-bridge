package utils_test

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/utils"
)

func getTestPluginPath() string {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	return filepath.Join(dir, "testdata", "testplugin", "testplugin.so")
}

func skipIfPluginNotAvailable(t *testing.T) {
	t.Helper()
	pluginPath := getTestPluginPath()
	if _, err := filepath.Abs(pluginPath); err != nil {
		t.Skip("plugin path resolution failed, skipping test")
	}
}

func TestLoadPluginMissingFile(t *testing.T) {
	value, err := utils.LoadPluginAndConfig[int]("/non/existent/plugin.so", "Constructor", "NewConfig", nil)
	if err == nil {
		t.Fatal("expected error when plugin file is missing")
	}
	if !strings.Contains(err.Error(), "failed to open plugin") {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != 0 {
		t.Fatalf("expected zero value for generic type, got %d", value)
	}
}

func TestLoadPluginAndConfigMissingConfigMethod(t *testing.T) {
	skipIfPluginNotAvailable(t)

	type Runner interface {
		GetName() string
		GetValue() int
	}

	pluginPath := getTestPluginPath()
	_, err := utils.LoadPluginAndConfig[Runner](pluginPath, "NewRunner", "NonExistentConfig", nil)
	if err == nil {
		t.Fatal("expected error when config method is missing")
	}
	if !strings.Contains(err.Error(), "failed to find config constructor") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestLoadPluginAndConfigInvalidConfigSignature(t *testing.T) {
	skipIfPluginNotAvailable(t)

	type Runner interface {
		GetName() string
		GetValue() int
	}

	pluginPath := getTestPluginPath()
	_, err := utils.LoadPluginAndConfig[Runner](pluginPath, "NewRunner", "InvalidConfig", nil)
	if err == nil {
		t.Fatal("expected error when config has invalid signature")
	}
	if !strings.Contains(err.Error(), "plugin has invalid signature") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestLoadPluginAndConfigMissingMethod(t *testing.T) {
	skipIfPluginNotAvailable(t)

	type Runner interface {
		GetName() string
		GetValue() int
	}

	pluginPath := getTestPluginPath()
	_, err := utils.LoadPluginAndConfig[Runner](pluginPath, "NonExistentMethod", "NewConfig", nil)
	if err == nil {
		t.Fatal("expected error when method is missing")
	}
	if !strings.Contains(err.Error(), "failed to find constructor") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestLoadPluginAndConfigInvalidMethodSignature(t *testing.T) {
	skipIfPluginNotAvailable(t)

	type Runner interface {
		GetName() string
		GetValue() int
	}

	pluginPath := getTestPluginPath()
	_, err := utils.LoadPluginAndConfig[Runner](pluginPath, "NewInvalidRunner", "NewConfig", nil)
	if err == nil {
		t.Fatal("expected error when method has invalid signature")
	}
	if !strings.Contains(err.Error(), "plugin has invalid signature") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestLoadPluginAndConfigParseConfigError(t *testing.T) {
	skipIfPluginNotAvailable(t)

	type Runner interface {
		GetName() string
		GetValue() int
	}

	pluginPath := getTestPluginPath()
	opts := map[string]any{
		"value": "not-a-number",
	}

	_, err := utils.LoadPluginAndConfig[Runner](pluginPath, "NewRunner", "NewConfig", opts)
	if err == nil {
		t.Fatal("expected error when config parsing fails")
	}
	if !strings.Contains(err.Error(), "failed to parse config") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestParseConfig(t *testing.T) {
	t.Run("success with defaults", func(t *testing.T) {
		type TestConfig struct {
			Name    string        `mapstructure:"name" default:"test"`
			Timeout time.Duration `mapstructure:"timeout" default:"5s"`
			Count   int           `mapstructure:"count" default:"10" validate:"required,min=1"`
		}

		config := &TestConfig{}
		opts := map[string]any{
			"name":  "custom",
			"count": 20,
		}

		err := utils.ParseConfig(opts, config)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if config.Name != "custom" {
			t.Errorf("expected Name to be 'custom', got %s", config.Name)
		}
		if config.Count != 20 {
			t.Errorf("expected Count to be 20, got %d", config.Count)
		}
		if config.Timeout != 5*time.Second {
			t.Errorf("expected Timeout to be 5s, got %v", config.Timeout)
		}
	})

	t.Run("success with duration parsing", func(t *testing.T) {
		type TestConfig struct {
			Timeout time.Duration `mapstructure:"timeout"`
		}

		config := &TestConfig{}
		opts := map[string]any{
			"timeout": "30s",
		}

		err := utils.ParseConfig(opts, config)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if config.Timeout != 30*time.Second {
			t.Errorf("expected Timeout to be 30s, got %v", config.Timeout)
		}
	})

	t.Run("validation error", func(t *testing.T) {
		type TestConfig struct {
			Count int `mapstructure:"count" validate:"required,min=10"`
		}

		config := &TestConfig{}
		opts := map[string]any{
			"count": 5,
		}

		err := utils.ParseConfig(opts, config)
		if err == nil {
			t.Fatal("expected validation error")
		}
		if !strings.Contains(err.Error(), "failed to validate config") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("decode error with incompatible types", func(t *testing.T) {
		type TestConfig struct {
			Count int `mapstructure:"count"`
		}

		config := &TestConfig{}
		opts := map[string]any{
			"count": "not-a-number",
		}

		err := utils.ParseConfig(opts, config)
		if err == nil {
			t.Fatal("expected decode error")
		}
		if !strings.Contains(err.Error(), "failed to decode options") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("empty options", func(t *testing.T) {
		type TestConfig struct {
			Name string `mapstructure:"name" default:"default"`
		}

		config := &TestConfig{}
		err := utils.ParseConfig(nil, config)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if config.Name != "default" {
			t.Errorf("expected Name to be 'default', got %s", config.Name)
		}
	})
}
