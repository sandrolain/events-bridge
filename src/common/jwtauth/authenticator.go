package jwtauth

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

const (
	// Metadata values for verified status
	metadataTrue  = "true"
	metadataFalse = "false"
)

// Authenticator handles JWT authentication for sources.
// It validates tokens and enriches metadata with claims.
type Authenticator struct {
	cfg       *Config
	slog      *slog.Logger
	validator *Validator
}

// NewAuthenticator creates a new JWT authenticator instance.
// Returns nil if JWT is not enabled in the configuration.
func NewAuthenticator(cfg *Config, logger *slog.Logger) (*Authenticator, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}

	logger = logger.With("component", "JWT Authenticator")

	logger.Info("initializing JWT authenticator",
		"jwksUrl", cfg.JWKsURL,
		"issuer", cfg.Issuer,
		"audience", cfg.Audience,
		"allowedAlgorithms", cfg.AllowedAlgorithms)

	validator, err := NewValidator(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create validator: %w", err)
	}

	return &Authenticator{
		cfg:       cfg,
		slog:      logger,
		validator: validator,
	}, nil
}

// AuthResult contains the result of JWT authentication.
type AuthResult struct {
	// Verified indicates if the token was successfully validated.
	Verified bool

	// Claims contains the validated JWT claims.
	Claims jwt.MapClaims

	// Error contains the validation error if verification failed.
	Error error

	// Metadata contains the enriched metadata with JWT claims.
	Metadata map[string]string
}

// Authenticate validates a JWT token from metadata and returns enriched metadata.
// This method is designed to be used by source connectors.
func (a *Authenticator) Authenticate(metadata map[string]string) *AuthResult {
	result := &AuthResult{
		Verified: false,
		Metadata: make(map[string]string),
	}

	// Extract token from metadata
	tokenString, exists := metadata[a.cfg.TokenMetadataKey]
	if !exists {
		result.Error = fmt.Errorf("token not found in metadata key: %s", a.cfg.TokenMetadataKey)
		a.enrichMetadataWithError(result)
		return result
	}

	if tokenString == "" {
		result.Error = fmt.Errorf("token is empty in metadata key: %s", a.cfg.TokenMetadataKey)
		a.enrichMetadataWithError(result)
		return result
	}

	// Validate token
	claims, err := a.validator.ValidateToken(tokenString)
	if err != nil {
		result.Error = fmt.Errorf("token validation failed: %w", err)
		a.enrichMetadataWithError(result)
		return result
	}

	// Token validated successfully
	result.Verified = true
	result.Claims = claims
	result.Metadata[a.cfg.ClaimPrefix+"verified"] = metadataTrue

	// Add claims to metadata
	a.addClaimsToMetadata(result.Metadata, claims)

	// Log successful validation
	sub, ok := claims["sub"].(string)
	if !ok {
		sub = ""
	}
	iss, ok := claims["iss"].(string)
	if !ok {
		iss = ""
	}
	a.slog.Debug("JWT validated successfully",
		"sub", sub,
		"iss", iss,
		"claims", len(claims))

	return result
}

// enrichMetadataWithError adds error information to metadata when validation fails.
func (a *Authenticator) enrichMetadataWithError(result *AuthResult) {
	a.slog.Warn("JWT validation failed", "error", result.Error)

	result.Metadata[a.cfg.ClaimPrefix+"verified"] = metadataFalse
	if result.Error != nil {
		result.Metadata[a.cfg.ClaimPrefix+"error"] = result.Error.Error()
	}
}

// addClaimsToMetadata extracts claims from the JWT and adds them to metadata.
func (a *Authenticator) addClaimsToMetadata(metadata map[string]string, claims jwt.MapClaims) {
	for key, value := range claims {
		// Convert claim value to string
		strValue := a.claimToString(value)
		if strValue == "" {
			continue
		}

		// Add to metadata with prefix
		metaKey := a.cfg.ClaimPrefix + key
		metadata[metaKey] = strValue
	}
}

// claimToString converts a claim value to a string representation.
func (a *Authenticator) claimToString(value interface{}) string {
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
			if str := a.claimToString(item); str != "" {
				parts = append(parts, str)
			}
		}
		return strings.Join(parts, ",")
	case map[string]interface{}:
		// Skip complex nested objects
		a.slog.Debug("skipping complex claim object", "type", "map")
		return ""
	default:
		// For other types, use default formatting
		return fmt.Sprintf("%v", v)
	}
}

// ShouldFailOnError returns true if authentication errors should fail the message.
func (a *Authenticator) ShouldFailOnError() bool {
	return a.cfg.FailOnError
}

// Close releases resources used by the authenticator.
func (a *Authenticator) Close() error {
	if a.validator != nil {
		return a.validator.Close()
	}
	return nil
}
