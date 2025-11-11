package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

// LimitedReader wraps an io.Reader and limits the total bytes read
type LimitedReader struct {
	R         io.Reader
	N         int64 // max bytes remaining
	TotalRead int64 // total bytes read so far
}

// Read implements io.Reader with size limiting
func (l *LimitedReader) Read(p []byte) (n int, err error) {
	if l.N <= 0 {
		return 0, fmt.Errorf("output size limit exceeded (%d bytes)", l.TotalRead)
	}
	if int64(len(p)) > l.N {
		p = p[0:l.N]
	}
	n, err = l.R.Read(p)
	l.N -= int64(n)
	l.TotalRead += int64(n)
	return
}

// NewLimitedReader creates a new LimitedReader with the specified limit
func NewLimitedReader(r io.Reader, limit int64) *LimitedReader {
	return &LimitedReader{R: r, N: limit}
}

// BaseConfig contains common configuration fields for internal use
type BaseConfig struct {
	Command         string            `mapstructure:"command" validate:"required"`
	Timeout         time.Duration     `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	Args            []string          `mapstructure:"args"`
	Envs            map[string]string `mapstructure:"envs"`
	AllowedCommands []string          `mapstructure:"allowedCommands"`                 // Whitelist of allowed commands (required for security)
	WorkDir         string            `mapstructure:"workDir"`                         // Working directory for command execution
	MaxOutputSize   int64             `mapstructure:"maxOutputSize" default:"1048576"` // Max output size in bytes (default 1MB)
	DenyEnvVars     []string          `mapstructure:"denyEnvVars"`                     // Blacklist of environment variables to filter
	UseShell        bool              `mapstructure:"useShell" default:"false"`        // Allow shell interpretation (dangerous, disabled by default)
}

// validateCommand validates the command and arguments to prevent command injection
func validateCommand(command string, args []string, allowedCommands []string, useShell bool) error {
	// Check if command is in the allowlist (if allowlist is provided)
	if err := validateCommandAllowlist(command, allowedCommands); err != nil {
		return err
	}

	// Check for dangerous characters in command
	if err := validateCommandCharacters(command); err != nil {
		return err
	}

	// Validate command is not empty
	command = strings.TrimSpace(command)
	if command == "" {
		return fmt.Errorf("command cannot be empty")
	}

	// Check if this is a shell command and validate accordingly
	isShell := isShellCommand(command)
	if err := validateShellUsage(command, isShell, useShell); err != nil {
		return err
	}

	// Validate arguments based on shell usage
	if err := validateCommandArgs(args, isShell, useShell); err != nil {
		return err
	}

	return nil
}

// validateCommandAllowlist checks if command is in the allowlist
func validateCommandAllowlist(command string, allowedCommands []string) error {
	if len(allowedCommands) > 0 {
		found := false
		for _, allowed := range allowedCommands {
			if command == allowed {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("command not in allowlist: %s (allowed: %v)", command, allowedCommands)
		}
	}
	return nil
}

// validateCommandCharacters checks for dangerous characters in command
func validateCommandCharacters(command string) error {
	dangerousChars := []string{";", "&", "|", "$", "`", ">", "<"}
	for _, char := range dangerousChars {
		if strings.Contains(command, char) {
			return fmt.Errorf("command contains potentially dangerous character '%s': %s", char, command)
		}
	}
	return nil
}

// isShellCommand checks if the command is a shell interpreter
func isShellCommand(command string) bool {
	return strings.HasSuffix(command, "/sh") ||
		strings.HasSuffix(command, "/bash") ||
		strings.HasSuffix(command, "/zsh") ||
		command == "sh" || command == "bash" || command == "zsh"
}

// validateShellUsage validates if shell usage is allowed
func validateShellUsage(command string, isShell, useShell bool) error {
	if isShell && !useShell {
		return fmt.Errorf("shell commands are disabled for security (command: %s). Set useShell=true to allow", command)
	}
	return nil
}

// validateCommandArgs validates command arguments
func validateCommandArgs(args []string, isShell, useShell bool) error {
	if isShell && useShell {
		// Allow shell syntax when explicitly enabled
		return nil
	}

	// For non-shell commands or when shell is disabled, be restrictive
	dangerousChars := regexp.MustCompile(`[;&|$\x60<>]`)
	for i, arg := range args {
		if dangerousChars.MatchString(arg) {
			return fmt.Errorf("argument %d contains potentially dangerous characters: %s", i, arg)
		}
		// Additional check for command substitution patterns
		if strings.Contains(arg, "$(") || strings.Contains(arg, "`") {
			return fmt.Errorf("argument %d contains command substitution pattern: %s", i, arg)
		}
	}

	return nil
}

// sanitizeEnvVars validates environment variable keys and values
func sanitizeEnvVars(envs map[string]string, denyList []string) error {
	validKeyPattern := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

	for k, v := range envs {
		// Check if env var is in deny list
		for _, denied := range denyList {
			if k == denied {
				return fmt.Errorf("environment variable '%s' is not allowed (in deny list)", k)
			}
		}

		// Validate environment variable key
		if !validKeyPattern.MatchString(k) {
			return fmt.Errorf("invalid environment variable key: %s", k)
		}

		// Check for dangerous patterns in values
		if strings.Contains(v, "$") && strings.Contains(v, "(") {
			return fmt.Errorf("environment variable value contains potentially dangerous pattern: %s", k)
		}
	}

	return nil
}

// validateBaseConfig validates the common configuration fields
func validateBaseConfig(cfg *BaseConfig) error {
	// Set default for MaxOutputSize if not set
	if cfg.MaxOutputSize == 0 {
		cfg.MaxOutputSize = 1048576 // 1MB default
	}

	if err := validateCommand(cfg.Command, cfg.Args, cfg.AllowedCommands, cfg.UseShell); err != nil {
		return fmt.Errorf("command validation failed: %w", err)
	}

	if err := sanitizeEnvVars(cfg.Envs, cfg.DenyEnvVars); err != nil {
		return fmt.Errorf("environment variable validation failed: %w", err)
	}

	// Validate MaxOutputSize
	if cfg.MaxOutputSize <= 0 {
		return fmt.Errorf("maxOutputSize must be greater than 0, got: %d", cfg.MaxOutputSize)
	}

	// Validate WorkDir if specified
	if cfg.WorkDir != "" {
		// Basic path validation to prevent path traversal
		if strings.Contains(cfg.WorkDir, "..") {
			return fmt.Errorf("workDir contains path traversal: %s", cfg.WorkDir)
		}
	}

	return nil
}

// CommandExecutor provides common functionality for executing CLI commands
type CommandExecutor struct {
	BaseConfig
	ctx    context.Context
	cancel context.CancelFunc
	slog   *slog.Logger
}

// NewCommandExecutor creates a new command executor with validation
func NewCommandExecutor(cfg *BaseConfig, logger *slog.Logger) (*CommandExecutor, error) {
	if err := validateBaseConfig(cfg); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &CommandExecutor{
		BaseConfig: *cfg,
		ctx:        ctx,
		cancel:     cancel,
		slog:       logger,
	}, nil
}

// CreateCommand creates and configures an exec.Cmd with the given context
func (ce *CommandExecutor) CreateCommand(ctx context.Context) *exec.Cmd {
	cmd := exec.CommandContext(ctx, ce.Command, ce.Args...) // #nosec G204 - CLI connector requires external command execution

	// Set working directory if specified
	if ce.WorkDir != "" {
		cmd.Dir = ce.WorkDir
	}

	if len(ce.Envs) > 0 {
		env := make([]string, 0, len(ce.Envs))
		for k, v := range ce.Envs {
			env = append(env, k+"="+v)
		}
		cmd.Env = append(cmd.Env, env...)
	}

	return cmd
}

// Close cancels the command context
func (ce *CommandExecutor) Close() error {
	if ce.cancel != nil {
		ce.cancel()
	}
	return nil
}

// PipeLogger is a utility function to log output from pipes
func PipeLogger(logger *slog.Logger, reader io.Reader, logType string, ctx context.Context) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Text()
		logger.Debug("CLI output", "type", logType, "line", line)
	}

	if err := scanner.Err(); err != nil {
		logger.Error("Error reading CLI output", "type", logType, "error", err)
	}
}

// Helper functions to convert config structs to BaseConfig for validation
func sourceToBaseConfig(cfg *SourceConfig) *BaseConfig {
	return &BaseConfig{
		Command:         cfg.Command,
		Timeout:         cfg.Timeout,
		Args:            cfg.Args,
		Envs:            cfg.Envs,
		AllowedCommands: cfg.AllowedCommands,
		WorkDir:         cfg.WorkDir,
		MaxOutputSize:   cfg.MaxOutputSize,
		DenyEnvVars:     cfg.DenyEnvVars,
		UseShell:        cfg.UseShell,
	}
}

func runnerToBaseConfig(cfg *RunnerConfig) *BaseConfig {
	return &BaseConfig{
		Command:         cfg.Command,
		Timeout:         cfg.Timeout,
		Args:            cfg.Args,
		Envs:            cfg.Envs,
		AllowedCommands: cfg.AllowedCommands,
		WorkDir:         cfg.WorkDir,
		MaxOutputSize:   cfg.MaxOutputSize,
		DenyEnvVars:     cfg.DenyEnvVars,
		UseShell:        cfg.UseShell,
	}
}
