package main

import (
	"io"
	"testing"
	"time"
)

func TestCommandAllowlist(t *testing.T) {
	tests := []struct {
		name            string
		command         string
		allowedCommands []string
		wantErr         bool
	}{
		{
			name:            "command in allowlist",
			command:         "/usr/bin/jq",
			allowedCommands: []string{"/usr/bin/jq", "/usr/bin/cat"},
			wantErr:         false,
		},
		{
			name:            "command not in allowlist",
			command:         "/usr/bin/rm",
			allowedCommands: []string{"/usr/bin/jq", "/usr/bin/cat"},
			wantErr:         true,
		},
		{
			name:            "empty allowlist allows all",
			command:         "/usr/bin/anything",
			allowedCommands: []string{},
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &RunnerConfig{
				Command:         tt.command,
				Args:            []string{},
				AllowedCommands: tt.allowedCommands,
				Timeout:         5 * time.Second,
				Format:          "json",
				MetadataKey:     "metadata",
				DataKey:         "data",
			}

			_, err := NewRunner(cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRunner() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestShellCommandValidation(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		useShell bool
		wantErr  bool
	}{
		{
			name:     "shell command with useShell=true",
			command:  "sh",
			useShell: true,
			wantErr:  false,
		},
		{
			name:     "shell command with useShell=false",
			command:  "sh",
			useShell: false,
			wantErr:  true,
		},
		{
			name:     "bash command with useShell=false",
			command:  "bash",
			useShell: false,
			wantErr:  true,
		},
		{
			name:     "normal command with useShell=false",
			command:  "/usr/bin/cat",
			useShell: false,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &RunnerConfig{
				Command:     tt.command,
				Args:        []string{},
				UseShell:    tt.useShell,
				Timeout:     5 * time.Second,
				Format:      "json",
				MetadataKey: "metadata",
				DataKey:     "data",
			}

			_, err := NewRunner(cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRunner() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDangerousCharactersValidation(t *testing.T) {
	tests := []struct {
		name    string
		command string
		args    []string
		wantErr bool
	}{
		{
			name:    "clean command",
			command: "/usr/bin/cat",
			args:    []string{"file.txt"},
			wantErr: false,
		},
		{
			name:    "command with semicolon",
			command: "/usr/bin/cat;/bin/rm",
			args:    []string{},
			wantErr: true,
		},
		{
			name:    "argument with pipe",
			command: "/usr/bin/cat",
			args:    []string{"file.txt|grep"},
			wantErr: true,
		},
		{
			name:    "argument with command substitution",
			command: "/usr/bin/echo",
			args:    []string{"$(whoami)"},
			wantErr: true,
		},
		{
			name:    "argument with backtick",
			command: "/usr/bin/echo",
			args:    []string{"`whoami`"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &RunnerConfig{
				Command:     tt.command,
				Args:        tt.args,
				Timeout:     5 * time.Second,
				Format:      "json",
				MetadataKey: "metadata",
				DataKey:     "data",
			}

			_, err := NewRunner(cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRunner() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEnvVarDenyList(t *testing.T) {
	tests := []struct {
		name        string
		envs        map[string]string
		denyEnvVars []string
		wantErr     bool
	}{
		{
			name:        "allowed env var",
			envs:        map[string]string{"MY_VAR": "value"},
			denyEnvVars: []string{"SECRET"},
			wantErr:     false,
		},
		{
			name:        "denied env var",
			envs:        map[string]string{"SECRET": "password"},
			denyEnvVars: []string{"SECRET"},
			wantErr:     true,
		},
		{
			name:        "empty deny list",
			envs:        map[string]string{"SECRET": "password"},
			denyEnvVars: []string{},
			wantErr:     false,
		},
		{
			name:        "multiple env vars with one denied",
			envs:        map[string]string{"MY_VAR": "value", "PASSWORD": "secret"},
			denyEnvVars: []string{"PASSWORD"},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &RunnerConfig{
				Command:     "/usr/bin/cat",
				Args:        []string{},
				Envs:        tt.envs,
				DenyEnvVars: tt.denyEnvVars,
				Timeout:     5 * time.Second,
				Format:      "json",
				MetadataKey: "metadata",
				DataKey:     "data",
			}

			_, err := NewRunner(cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRunner() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorkDirValidation(t *testing.T) {
	tests := []struct {
		name    string
		workDir string
		wantErr bool
	}{
		{
			name:    "valid work dir",
			workDir: "/opt/myapp",
			wantErr: false,
		},
		{
			name:    "work dir with path traversal",
			workDir: "/opt/../etc",
			wantErr: true,
		},
		{
			name:    "empty work dir",
			workDir: "",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &RunnerConfig{
				Command:     "/usr/bin/cat",
				Args:        []string{},
				WorkDir:     tt.workDir,
				Timeout:     5 * time.Second,
				Format:      "json",
				MetadataKey: "metadata",
				DataKey:     "data",
			}

			_, err := NewRunner(cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRunner() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMaxOutputSizeDefault(t *testing.T) {
	cfg := &RunnerConfig{
		Command:     "/usr/bin/cat",
		Args:        []string{},
		Timeout:     5 * time.Second,
		Format:      "json",
		MetadataKey: "metadata",
		DataKey:     "data",
		// MaxOutputSize not set, should default to 1MB
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	cliRunner, ok := runner.(*CLIRunner)
	if !ok {
		t.Fatal("failed to cast runner to CLIRunner")
	}

	// Check that default was applied to the executor
	expectedDefault := int64(1048576) // 1MB
	if cliRunner.executor.MaxOutputSize != expectedDefault {
		t.Errorf("MaxOutputSize = %d, want %d", cliRunner.executor.MaxOutputSize, expectedDefault)
	}
}

func TestInvalidEnvVarKey(t *testing.T) {
	cfg := &RunnerConfig{
		Command: "/usr/bin/cat",
		Args:    []string{},
		Envs: map[string]string{
			"INVALID-KEY": "value", // Hyphen is not allowed
		},
		Timeout:     5 * time.Second,
		Format:      "json",
		MetadataKey: "metadata",
		DataKey:     "data",
	}

	_, err := NewRunner(cfg)
	if err == nil {
		t.Error("NewRunner() should fail with invalid env var key")
	}
}

func TestLimitedReader(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		limit     int64
		wantBytes int
		wantErr   bool
	}{
		{
			name:      "read within limit",
			input:     "hello",
			limit:     10,
			wantBytes: 5,
			wantErr:   false,
		},
		{
			name:      "read exactly at limit",
			input:     "hello",
			limit:     5,
			wantBytes: 5,
			wantErr:   false,
		},
		{
			name:      "read exceeds limit",
			input:     "hello world",
			limit:     5,
			wantBytes: 5,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewLimitedReader(
				&stringReader{s: tt.input, pos: 0},
				tt.limit,
			)

			buf := make([]byte, len(tt.input))
			n, err := reader.Read(buf)
			if err != nil && err != io.EOF {
				t.Fatalf("Read() error = %v", err)
			}

			if n != tt.wantBytes {
				t.Errorf("Read() bytes = %d, want %d", n, tt.wantBytes)
			}
			if tt.wantErr {
				// Try to read more
				_, err := reader.Read(buf)
				if err == nil {
					t.Error("Read() should return error when limit exceeded")
				}
			}
		})
	}
}

// stringReader is a simple io.Reader implementation for testing
type stringReader struct {
	s   string
	pos int
}

func (r *stringReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.s) {
		return 0, nil
	}
	n = copy(p, r.s[r.pos:])
	r.pos += n
	return n, nil
}

func TestSourceWithAllowlist(t *testing.T) {
	cfg := &SourceConfig{
		Command:         "cat",
		Args:            []string{},
		AllowedCommands: []string{"cat"},
		Timeout:         5 * time.Second,
		Format:          "json",
		MetadataKey:     "metadata",
		DataKey:         "data",
	}

	_, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("NewSource() error = %v", err)
	}
}

func TestRunnerWithWorkDir(t *testing.T) {
	cfg := &RunnerConfig{
		Command:     "cat",
		Args:        []string{},
		WorkDir:     "/tmp",
		Timeout:     5 * time.Second,
		Format:      "json",
		MetadataKey: "metadata",
		DataKey:     "data",
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}
	defer runner.Close()
}

func TestCommandExecutorClose(t *testing.T) {
	baseConfig := &BaseConfig{
		Command: "/usr/bin/cat",
		Args:    []string{},
		Timeout: 5 * time.Second,
	}

	executor, err := NewCommandExecutor(baseConfig, nil)
	if err != nil {
		t.Fatalf("NewCommandExecutor() error = %v", err)
	}

	err = executor.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestEnvVarWithDangerousPattern(t *testing.T) {
	cfg := &RunnerConfig{
		Command: "/usr/bin/cat",
		Args:    []string{},
		Envs: map[string]string{
			"DANGER": "$(whoami)",
		},
		Timeout:     5 * time.Second,
		Format:      "json",
		MetadataKey: "metadata",
		DataKey:     "data",
	}

	_, err := NewRunner(cfg)
	if err == nil {
		t.Error("NewRunner() should fail with dangerous env var pattern")
	}
}
