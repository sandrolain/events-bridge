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

// CLIFormat represents the data format for CLI operations
type CLIFormat string

const (
	FormatJSON CLIFormat = "JSON"
	FormatCBOR CLIFormat = "CBOR"
)

// BaseConfig contains common configuration fields for internal use
type BaseConfig struct {
	Command string            `mapstructure:"command" validate:"required"`
	Timeout time.Duration     `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	Args    []string          `mapstructure:"args"`
	Envs    map[string]string `mapstructure:"envs"`
}

// parseFormat validates and parses the format string
func parseFormat(value string) (CLIFormat, error) {
	v := CLIFormat(strings.ToUpper(strings.TrimSpace(value)))
	switch v {
	case FormatJSON, FormatCBOR:
		return v, nil
	default:
		return "", fmt.Errorf("unsupported format %q", value)
	}
}

// validateCommand validates the command and arguments to prevent command injection
func validateCommand(command string, args []string) error {
	// Check if command is a valid executable name or absolute path
	if strings.Contains(command, ";") || strings.Contains(command, "&") ||
		strings.Contains(command, "|") || strings.Contains(command, "$") ||
		strings.Contains(command, "`") || strings.Contains(command, ">") ||
		strings.Contains(command, "<") {
		return fmt.Errorf("command contains potentially dangerous characters: %s", command)
	}

	// Validate command is not empty and doesn't start with suspicious patterns
	command = strings.TrimSpace(command)
	if command == "" {
		return fmt.Errorf("command cannot be empty")
	}

	// For shell commands (/bin/sh, bash, etc.), allow more flexibility in arguments
	// since they are expected to contain shell syntax
	isShell := strings.HasSuffix(command, "/sh") ||
		strings.HasSuffix(command, "/bash") ||
		strings.HasSuffix(command, "/zsh") ||
		command == "sh" || command == "bash" || command == "zsh"

	if !isShell {
		// For non-shell commands, be more restrictive with arguments
		dangerousChars := regexp.MustCompile(`[;&|$\x60<>]`)
		for _, arg := range args {
			if dangerousChars.MatchString(arg) {
				return fmt.Errorf("argument contains potentially dangerous characters: %s", arg)
			}
		}
	}

	return nil
}

// sanitizeEnvVars validates environment variable keys and values
func sanitizeEnvVars(envs map[string]string) error {
	validKeyPattern := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

	for k, v := range envs {
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
	if err := validateCommand(cfg.Command, cfg.Args); err != nil {
		return fmt.Errorf("command validation failed: %w", err)
	}

	if err := sanitizeEnvVars(cfg.Envs); err != nil {
		return fmt.Errorf("environment variable validation failed: %w", err)
	}

	return nil
}

// CommandExecutor provides common functionality for executing CLI commands
type CommandExecutor struct {
	BaseConfig
	ctx    context.Context
	cancel context.CancelFunc
	cmd    *exec.Cmd
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
	cmd := exec.CommandContext(ctx, ce.Command, ce.Args...)

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
		Command: cfg.Command,
		Timeout: cfg.Timeout,
		Args:    cfg.Args,
		Envs:    cfg.Envs,
	}
}

func targetToBaseConfig(cfg *TargetConfig) *BaseConfig {
	return &BaseConfig{
		Command: cfg.Command,
		Timeout: cfg.Timeout,
		Args:    cfg.Args,
		Envs:    cfg.Envs,
	}
}

func runnerToBaseConfig(cfg *RunnerConfig) *BaseConfig {
	return &BaseConfig{
		Command: cfg.Command,
		Timeout: cfg.Timeout,
		Args:    cfg.Args,
		Envs:    cfg.Envs,
	}
}
