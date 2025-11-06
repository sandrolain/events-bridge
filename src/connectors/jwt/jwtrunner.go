package jwt

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ connectors.Runner = (*JWTRunner)(nil)

// RunnerConfig defines the configuration for the JWT runner.
// It validates JWT tokens using JWKs and extracts claims to message metadata.
type RunnerConfig struct {
	// TokenMetadataKey specifies the metadata key containing the JWT token.
	// Default: "authorization" (compatible with HTTP Authorization header)
	TokenMetadataKey string `mapstructure:"tokenMetadataKey" default:"authorization"`

	// TokenPrefix is the prefix before the token (e.g., "Bearer ").
	// If empty, no prefix is expected or removed.
	// Default: "Bearer "
	TokenPrefix string `mapstructure:"tokenPrefix" default:"Bearer "`

	// JWKsURL is the URL of the JSON Web Key Set (JWKS) endpoint.
	// This is typically found at the issuer's .well-known/jwks.json endpoint.
	// Example: "https://auth.example.com/.well-known/jwks.json"
	JWKsURL string `mapstructure:"jwksUrl" validate:"required,url"`

	// JWKsRefreshInterval defines how often to refresh the JWKS from the URL.
	// Default: 1 hour
	JWKsRefreshInterval time.Duration `mapstructure:"jwksRefreshInterval" default:"1h" validate:"gt=0"`

	// Issuer is the expected value of the "iss" claim in the JWT.
	// Tokens with a different issuer will be rejected.
	Issuer string `mapstructure:"issuer" validate:"required"`

	// Audience is the expected value of the "aud" claim in the JWT.
	// Tokens without this audience will be rejected.
	Audience string `mapstructure:"audience" validate:"required"`

	// RequiredClaims is a list of claim names that must be present in the token.
	// If any required claim is missing, the token is rejected.
	RequiredClaims []string `mapstructure:"requiredClaims"`

	// ClaimPrefix is prepended to claim names when adding them to metadata.
	// Example: with prefix "jwt_", the "sub" claim becomes "jwt_sub" in metadata.
	// Default: "jwt_"
	ClaimPrefix string `mapstructure:"claimPrefix" default:"jwt_"`

	// FailOnError determines the behavior when token validation fails.
	// If true, the runner returns an error and the message is Nak'd.
	// If false, the runner continues but sets jwt_verified=false in metadata.
	// Default: true
	FailOnError bool `mapstructure:"failOnError" default:"true"`

	// AllowedAlgorithms is a list of permitted signing algorithms.
	// Only asymmetric algorithms should be used for security.
	// Default: ["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"]
	AllowedAlgorithms []string `mapstructure:"allowedAlgorithms" default:"[RS256,RS384,RS512,ES256,ES384,ES512]"`

	// ClockSkew is the tolerance for time-based claims (exp, nbf, iat).
	// This accounts for clock drift between systems.
	// Default: 60 seconds
	ClockSkew time.Duration `mapstructure:"clockSkew" default:"60s" validate:"gte=0"`
}

// JWTRunner validates JWT tokens and extracts claims to message metadata.
type JWTRunner struct {
	cfg       *RunnerConfig
	slog      *slog.Logger
	validator *Validator
}

// NewRunnerConfig creates a new RunnerConfig instance with default values.
func NewRunnerConfig() any {
	return &RunnerConfig{}
}

// NewRunner creates a new JWT runner instance from the provided configuration.
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	logger := slog.Default().With("context", "JWT Runner")
	logger.Info("initializing JWT runner",
		"jwksUrl", cfg.JWKsURL,
		"issuer", cfg.Issuer,
		"audience", cfg.Audience,
		"allowedAlgorithms", cfg.AllowedAlgorithms)

	validator, err := NewValidator(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create validator: %w", err)
	}

	return &JWTRunner{
		cfg:       cfg,
		slog:      logger,
		validator: validator,
	}, nil
}

// Process validates the JWT token in the message metadata and extracts claims.
func (r *JWTRunner) Process(msg *message.RunnerMessage) error {
	// Get message metadata
	metadata, err := msg.GetMetadata()
	if err != nil {
		return r.handleError(msg, fmt.Errorf("failed to get metadata: %w", err))
	}

	// Extract token from metadata
	tokenString, exists := metadata[r.cfg.TokenMetadataKey]
	if !exists {
		return r.handleError(msg, fmt.Errorf("token not found in metadata key: %s", r.cfg.TokenMetadataKey))
	}

	if tokenString == "" {
		return r.handleError(msg, fmt.Errorf("token is empty in metadata key: %s", r.cfg.TokenMetadataKey))
	}

	// Validate token
	claims, err := r.validator.ValidateToken(tokenString)
	if err != nil {
		return r.handleError(msg, fmt.Errorf("token validation failed: %w", err))
	}

	// Add claims to metadata
	r.addClaimsToMetadata(msg, claims)

	// Set verification flag
	msg.AddMetadata(r.cfg.ClaimPrefix+"verified", "true")

	// Log successful validation
	sub, ok := claims["sub"].(string)
	if !ok {
		sub = ""
	}
	iss, ok := claims["iss"].(string)
	if !ok {
		iss = ""
	}
	r.slog.Debug("JWT validated successfully",
		"sub", sub,
		"iss", iss,
		"claims", len(claims))

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
	msg.AddMetadata(r.cfg.ClaimPrefix+"error", err.Error())

	return nil
}

// addClaimsToMetadata extracts claims from the JWT and adds them to message metadata.
func (r *JWTRunner) addClaimsToMetadata(msg *message.RunnerMessage, claims jwt.MapClaims) {
	for key, value := range claims {
		// Convert claim value to string
		strValue := r.claimToString(value)
		if strValue == "" {
			continue
		}

		// Add to metadata with prefix
		metaKey := r.cfg.ClaimPrefix + key
		msg.AddMetadata(metaKey, strValue)
	}
}

// claimToString converts a claim value to a string representation.
func (r *JWTRunner) claimToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		// JWT numeric claims are typically float64
		return fmt.Sprintf("%v", v)
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case []interface{}:
		// For arrays (e.g., roles, scopes), join as comma-separated string
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if str := r.claimToString(item); str != "" {
				parts = append(parts, str)
			}
		}
		return strings.Join(parts, ",")
	case map[string]interface{}:
		// Skip complex nested objects
		r.slog.Debug("skipping complex claim object", "type", "map")
		return ""
	default:
		// For other types, use default formatting
		return fmt.Sprintf("%v", v)
	}
}

// Close releases resources used by the runner.
func (r *JWTRunner) Close() error {
	if r.validator != nil {
		return r.validator.Close()
	}
	return nil
}
