package main

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Validator handles JWT token validation using JWKS.
type Validator struct {
	cfg        *RunnerConfig
	slog       *slog.Logger
	jwksClient *JWKSClient
}

// NewValidator creates a new JWT validator instance.
func NewValidator(cfg *RunnerConfig, logger *slog.Logger) (*Validator, error) {
	jwksClient, err := NewJWKSClient(cfg.JWKsURL, cfg.JWKsRefreshInterval, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWKS client: %w", err)
	}

	return &Validator{
		cfg:        cfg,
		slog:       logger,
		jwksClient: jwksClient,
	}, nil
}

// ValidateToken parses and validates a JWT token string.
// It verifies the signature, standard claims, and required custom claims.
func (v *Validator) ValidateToken(tokenString string) (jwt.MapClaims, error) {
	// Remove token prefix if configured
	if v.cfg.TokenPrefix != "" {
		tokenString = strings.TrimPrefix(tokenString, v.cfg.TokenPrefix)
	}

	tokenString = strings.TrimSpace(tokenString)
	if tokenString == "" {
		return nil, errors.New("token is empty after removing prefix")
	}

	// Parse and validate token
	token, err := jwt.Parse(tokenString, v.keyFunc, jwt.WithLeeway(v.cfg.ClockSkew))
	if err != nil {
		return nil, fmt.Errorf("token parsing failed: %w", err)
	}

	if !token.Valid {
		return nil, errors.New("token is invalid")
	}

	// Extract claims
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("invalid claims format")
	}

	// Validate standard claims
	if err := v.validateStandardClaims(claims); err != nil {
		return nil, err
	}

	// Validate required claims
	if err := v.validateRequiredClaims(claims); err != nil {
		return nil, err
	}

	return claims, nil
}

// keyFunc is called by jwt.Parse to get the key for signature verification.
func (v *Validator) keyFunc(token *jwt.Token) (interface{}, error) {
	// Validate algorithm
	alg := token.Method.Alg()
	if !v.isAllowedAlgorithm(alg) {
		return nil, fmt.Errorf("unexpected signing method: %v", alg)
	}

	// Get kid from token header
	kid, ok := token.Header["kid"].(string)
	if !ok {
		return nil, errors.New("missing kid in token header")
	}

	if kid == "" {
		return nil, errors.New("kid is empty in token header")
	}

	// Fetch public key from JWKS
	key, err := v.jwksClient.GetKey(kid)
	if err != nil {
		// Try to refresh JWKS once in case of key rotation
		v.slog.Debug("key not found, refreshing JWKS", "kid", kid)
		if refreshErr := v.jwksClient.Refresh(); refreshErr != nil {
			v.slog.Warn("JWKS refresh failed", "error", refreshErr)
		} else {
			// Retry after refresh
			key, err = v.jwksClient.GetKey(kid)
			if err == nil {
				return key, nil
			}
		}
	}

	return key, err
}

// validateStandardClaims validates the standard JWT claims.
func (v *Validator) validateStandardClaims(claims jwt.MapClaims) error {
	// Validate issuer
	iss, err := claims.GetIssuer()
	if err != nil {
		return fmt.Errorf("invalid iss claim: %w", err)
	}
	if iss != v.cfg.Issuer {
		return fmt.Errorf("invalid issuer: expected %s, got %s", v.cfg.Issuer, iss)
	}

	// Validate audience
	aud, err := claims.GetAudience()
	if err != nil {
		return fmt.Errorf("invalid aud claim: %w", err)
	}

	audienceValid := false
	for _, a := range aud {
		if a == v.cfg.Audience {
			audienceValid = true
			break
		}
	}
	if !audienceValid {
		return fmt.Errorf("invalid audience: expected %s, got %v", v.cfg.Audience, aud)
	}

	// Validate expiration
	exp, err := claims.GetExpirationTime()
	if err != nil {
		return fmt.Errorf("invalid exp claim: %w", err)
	}
	if exp == nil {
		return errors.New("missing exp claim")
	}
	// Note: jwt.Parse already validates exp with leeway, but we double-check
	if time.Now().After(exp.Add(v.cfg.ClockSkew)) {
		return fmt.Errorf("token has expired at %v", exp.Time)
	}

	// Validate not-before if present
	nbf, err := claims.GetNotBefore()
	if err == nil && nbf != nil {
		if time.Now().Before(nbf.Add(-v.cfg.ClockSkew)) {
			return fmt.Errorf("token not yet valid, valid from %v", nbf.Time)
		}
	}

	// Validate issued-at if present
	iat, err := claims.GetIssuedAt()
	if err == nil && iat != nil {
		// Token shouldn't be issued in the future
		if time.Now().Before(iat.Add(-v.cfg.ClockSkew)) {
			return fmt.Errorf("token issued in the future: %v", iat.Time)
		}
	}

	return nil
}

// validateRequiredClaims checks that all required claims are present.
func (v *Validator) validateRequiredClaims(claims jwt.MapClaims) error {
	for _, required := range v.cfg.RequiredClaims {
		if _, exists := claims[required]; !exists {
			return fmt.Errorf("missing required claim: %s", required)
		}
	}
	return nil
}

// isAllowedAlgorithm checks if the algorithm is in the allowed list.
func (v *Validator) isAllowedAlgorithm(alg string) bool {
	for _, allowed := range v.cfg.AllowedAlgorithms {
		if alg == allowed {
			return true
		}
	}
	return false
}

// Close releases resources used by the validator.
func (v *Validator) Close() error {
	if v.jwksClient != nil {
		v.jwksClient.Close()
	}
	return nil
}
