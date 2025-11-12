package jwtauth

import (
	"time"
)

// Config defines the configuration for JWT authentication and validation.
// It validates JWT tokens using JWKs and extracts claims.
type Config struct {
	// Enabled determines if JWT authentication is active.
	// Default: false
	Enabled bool `mapstructure:"enabled" default:"false"`

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
	JWKsURL string `mapstructure:"jwksUrl" validate:"required_if=Enabled true,omitempty,url"`

	// JWKsRefreshInterval defines how often to refresh the JWKS from the URL.
	// Default: 1 hour
	JWKsRefreshInterval time.Duration `mapstructure:"jwksRefreshInterval" default:"1h" validate:"gt=0"`

	// Issuer is the expected value of the "iss" claim in the JWT.
	// Tokens with a different issuer will be rejected.
	Issuer string `mapstructure:"issuer" validate:"required_if=Enabled true,omitempty"`

	// Audience is the expected value of the "aud" claim in the JWT.
	// Tokens without this audience will be rejected.
	Audience string `mapstructure:"audience" validate:"required_if=Enabled true,omitempty"`

	// RequiredClaims is a list of claim names that must be present in the token.
	// If any required claim is missing, the token is rejected.
	RequiredClaims []string `mapstructure:"requiredClaims"`

	// ClaimPrefix is prepended to claim names when adding them to metadata.
	// Example: with prefix "jwt_", the "sub" claim becomes "jwt_sub" in metadata.
	// Default: "jwt_"
	ClaimPrefix string `mapstructure:"claimPrefix" default:"jwt_"`

	// FailOnError determines the behavior when token validation fails.
	// If true, the authenticator returns an error.
	// If false, the authenticator continues but sets jwt_verified=false in metadata.
	// Default: false (for source authentication, we typically want to continue)
	FailOnError bool `mapstructure:"failOnError" default:"false"`

	// AllowedAlgorithms is a list of permitted signing algorithms.
	// Only asymmetric algorithms should be used for security.
	// Default: ["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"]
	AllowedAlgorithms []string `mapstructure:"allowedAlgorithms" default:"[RS256,RS384,RS512,ES256,ES384,ES512]"`

	// ClockSkew is the tolerance for time-based claims (exp, nbf, iat).
	// This accounts for clock drift between systems.
	// Default: 60 seconds
	ClockSkew time.Duration `mapstructure:"clockSkew" default:"60s" validate:"gte=0"`
}

// IsEnabled checks if JWT authentication is enabled in the configuration.
func (c *Config) IsEnabled() bool {
	return c != nil && c.Enabled
}
