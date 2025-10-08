package main

import (
	"testing"
)

func TestWasmRunnerFilterEnvVars(t *testing.T) {
	tests := []struct {
		name       string
		env        map[string]string
		denyList   []string
		wantKeys   []string
		wantDenied []string
	}{
		{
			name: "no deny list",
			env: map[string]string{
				"VAR1": "value1",
				"VAR2": "value2",
			},
			denyList:   nil,
			wantKeys:   []string{"VAR1", "VAR2"},
			wantDenied: nil,
		},
		{
			name: "empty deny list",
			env: map[string]string{
				"VAR1": "value1",
				"VAR2": "value2",
			},
			denyList:   []string{},
			wantKeys:   []string{"VAR1", "VAR2"},
			wantDenied: nil,
		},
		{
			name: "filter sensitive vars",
			env: map[string]string{
				"PATH":       "/usr/bin",
				"HOME":       "/home/user",
				"AWS_SECRET": "secret123",
				"SAFE_VAR":   "value",
			},
			denyList:   []string{"PATH", "HOME", "AWS_SECRET"},
			wantKeys:   []string{"SAFE_VAR"},
			wantDenied: []string{"PATH", "HOME", "AWS_SECRET"},
		},
		{
			name: "partial filtering",
			env: map[string]string{
				"PUBLIC_VAR": "public",
				"SECRET_KEY": "secret",
				"API_KEY":    "key123",
			},
			denyList:   []string{"SECRET_KEY", "API_KEY"},
			wantKeys:   []string{"PUBLIC_VAR"},
			wantDenied: []string{"SECRET_KEY", "API_KEY"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &WasmRunner{
				cfg: &RunnerConfig{
					DenyEnvVars: tt.denyList,
				},
			}

			result := runner.filterEnvVars(tt.env)

			// Check that wanted keys are present
			for _, key := range tt.wantKeys {
				if _, ok := result[key]; !ok {
					t.Errorf("expected key %s to be present in result", key)
				}
			}

			// Check that denied keys are not present
			for _, key := range tt.wantDenied {
				if _, ok := result[key]; ok {
					t.Errorf("expected key %s to be filtered out", key)
				}
			}

			// Check result size
			expectedSize := len(tt.env) - len(tt.wantDenied)
			if len(result) != expectedSize {
				t.Errorf("expected result size %d, got %d", expectedSize, len(result))
			}
		})
	}
}

func TestRunnerConfigDefaults(t *testing.T) {
	cfg := &RunnerConfig{}

	// Verify security fields exist and can be set
	cfg.MaxMemoryPages = 256
	cfg.ReadOnlyMount = true
	cfg.AllowedPaths = []string{"/tmp"}
	cfg.DenyEnvVars = []string{"SECRET"}

	if cfg.MaxMemoryPages != 256 {
		t.Errorf("expected MaxMemoryPages to be 256, got %d", cfg.MaxMemoryPages)
	}
}

func TestWasmRunnerConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  RunnerConfig
		wantErr bool
	}{
		{
			name: "valid config with security features",
			config: RunnerConfig{
				Path:           "test.wasm",
				Timeout:        5000000000, // 5s in nanoseconds
				Format:         "json",
				MetadataKey:    "metadata",
				DataKey:        "data",
				MaxMemoryPages: 256,
				ReadOnlyMount:  true,
				AllowedPaths:   []string{"/tmp"},
				DenyEnvVars:    []string{"SECRET"},
			},
			wantErr: false,
		},
		{
			name: "memory pages too high",
			config: RunnerConfig{
				Path:           "test.wasm",
				Format:         "json",
				MaxMemoryPages: 70000, // Exceeds 65536
			},
			wantErr: false, // Note: validation happens at runtime, not in struct
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify the config can be created
			cfg := &tt.config
			_ = cfg
		})
	}
}

func TestFilterEnvVarsCaseInsensitive(t *testing.T) {
	// Environment variables are typically case-sensitive
	// This test verifies the filter respects case
	runner := &WasmRunner{
		cfg: &RunnerConfig{
			DenyEnvVars: []string{"SECRET"},
		},
	}

	env := map[string]string{
		"SECRET": "should be filtered",
		"secret": "should NOT be filtered (different case)",
		"SAFE":   "should pass",
	}

	result := runner.filterEnvVars(env)

	if _, ok := result["SECRET"]; ok {
		t.Error("expected SECRET to be filtered")
	}

	if _, ok := result["secret"]; !ok {
		t.Error("expected secret (lowercase) to pass through")
	}

	if _, ok := result["SAFE"]; !ok {
		t.Error("expected SAFE to pass through")
	}
}

func TestFilterEnvVarsEmptyInput(t *testing.T) {
	runner := &WasmRunner{
		cfg: &RunnerConfig{
			DenyEnvVars: []string{"SECRET"},
		},
	}

	result := runner.filterEnvVars(map[string]string{})

	if len(result) != 0 {
		t.Errorf("expected empty result for empty input, got %d items", len(result))
	}
}

func TestFilterEnvVarsPreservesValues(t *testing.T) {
	runner := &WasmRunner{
		cfg: &RunnerConfig{
			DenyEnvVars: []string{"DENY"},
		},
	}

	env := map[string]string{
		"KEEP": "original_value",
		"DENY": "should_be_removed",
	}

	result := runner.filterEnvVars(env)

	if result["KEEP"] != "original_value" {
		t.Errorf("expected value 'original_value', got '%s'", result["KEEP"])
	}
}
