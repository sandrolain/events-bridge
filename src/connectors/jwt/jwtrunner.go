package main

import (
	"fmt"
	"log/slog"

	"github.com/sandrolain/events-bridge/src/common/jwtauth"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ connectors.Runner = (*JWTRunner)(nil)

// RunnerConfig is an alias for the shared jwtauth.Config.
// The JWT runner uses the same configuration as the authenticator.
type RunnerConfig = jwtauth.Config

// JWTRunner validates JWT tokens and extracts claims to message metadata.
type JWTRunner struct {
	cfg           *RunnerConfig
	slog          *slog.Logger
	authenticator *jwtauth.Authenticator
}

// NewRunnerConfig creates a new RunnerConfig instance with default values.
func NewRunnerConfig() any {
	return &jwtauth.Config{
		// Override defaults for runner: fail on error by default
		FailOnError: true,
	}
}

// NewRunner creates a new JWT runner instance from the provided configuration.
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Enable JWT authentication for the runner
	cfg.Enabled = true

	logger := slog.Default().With("context", "JWT Runner")
	logger.Info("initializing JWT runner",
		"jwksUrl", cfg.JWKsURL,
		"issuer", cfg.Issuer,
		"audience", cfg.Audience,
		"allowedAlgorithms", cfg.AllowedAlgorithms)

	authenticator, err := jwtauth.NewAuthenticator(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create authenticator: %w", err)
	}

	return &JWTRunner{
		cfg:           cfg,
		slog:          logger,
		authenticator: authenticator,
	}, nil
}

// Process validates the JWT token in the message metadata and extracts claims.
func (r *JWTRunner) Process(msg *message.RunnerMessage) error {
	// Get message metadata
	metadata, err := msg.GetMetadata()
	if err != nil {
		return r.handleError(msg, fmt.Errorf("failed to get metadata: %w", err))
	}

	// Authenticate using the shared authenticator
	result := r.authenticator.Authenticate(metadata)

	if !result.Verified {
		return r.handleError(msg, result.Error)
	}

	// Replace metadata with enriched version
	for key, value := range result.Metadata {
		msg.AddMetadata(key, value)
	}

	// Log successful validation
	sub, ok := result.Claims["sub"].(string)
	if !ok {
		sub = ""
	}
	iss, ok := result.Claims["iss"].(string)
	if !ok {
		iss = ""
	}
	r.slog.Debug("JWT validated successfully",
		"sub", sub,
		"iss", iss,
		"claims", len(result.Claims))

	return nil
}

// handleError processes validation errors according to the FailOnError configuration.
func (r *JWTRunner) handleError(msg *message.RunnerMessage, err error) error {
	r.slog.Warn("JWT validation failed", "error", err)

	if r.cfg.FailOnError {
		// Return error, which will cause the bridge to Nak the message
		return err
	}

	// Continue processing but mark as unverified
	msg.AddMetadata(r.cfg.ClaimPrefix+"verified", "false")
	if err != nil {
		msg.AddMetadata(r.cfg.ClaimPrefix+"error", err.Error())
	}

	return nil
}

// Close releases resources used by the runner.
func (r *JWTRunner) Close() error {
	if r.authenticator != nil {
		return r.authenticator.Close()
	}
	return nil
}
