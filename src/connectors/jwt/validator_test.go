package main

import (
	"log/slog"
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
)

func TestValidatorValidToken(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		TokenPrefix:         "Bearer ",
	}

	validator, err := NewValidator(cfg, slog.Default())
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}
	defer validator.Close()

	// Create valid token
	claims := jwtgo.MapClaims{
		"iss": testIssuer,
		"aud": testAudience,
		"sub": "user123",
		"exp": time.Now().Add(1 * time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		t.Fatalf("failed to create token: %v", err)
	}

	// Validate
	resultClaims, err := validator.ValidateToken("Bearer " + tokenString)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resultClaims["sub"] != "user123" {
		t.Errorf("expected sub=user123, got %v", resultClaims["sub"])
	}
}

func TestValidatorExpiredToken(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           1 * time.Second,
		TokenPrefix:         "",
	}

	validator, err := NewValidator(cfg, slog.Default())
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}
	defer validator.Close()

	// Create expired token
	claims := jwtgo.MapClaims{
		"iss": testIssuer,
		"aud": testAudience,
		"sub": "user123",
		"exp": time.Now().Add(-1 * time.Hour).Unix(),
		"iat": time.Now().Add(-2 * time.Hour).Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		t.Fatalf("failed to create token: %v", err)
	}

	// Validate should fail
	_, err = validator.ValidateToken(tokenString)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestValidatorInvalidAlgorithm(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"ES256"}, // Only allow ES256
		ClockSkew:           60 * time.Second,
		TokenPrefix:         "",
	}

	validator, err := NewValidator(cfg, slog.Default())
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}
	defer validator.Close()

	// Create token with RS256 (not allowed)
	claims := jwtgo.MapClaims{
		"iss": testIssuer,
		"aud": testAudience,
		"sub": "user123",
		"exp": time.Now().Add(1 * time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		t.Fatalf("failed to create token: %v", err)
	}

	// Validate should fail due to algorithm mismatch
	_, err = validator.ValidateToken(tokenString)
	if err == nil {
		t.Fatal("expected error for invalid algorithm")
	}
}

func TestValidatorMissingKid(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		TokenPrefix:         "",
	}

	validator, err := NewValidator(cfg, slog.Default())
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}
	defer validator.Close()

	// Create token without kid header
	claims := jwtgo.MapClaims{
		"iss": testIssuer,
		"aud": testAudience,
		"sub": "user123",
		"exp": time.Now().Add(1 * time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}

	token := jwtgo.NewWithClaims(jwtgo.SigningMethodRS256, claims)
	// Don't set kid header
	tokenString, err := token.SignedString(server.privateKey)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	// Validate should fail
	_, err = validator.ValidateToken(tokenString)
	if err == nil {
		t.Fatal("expected error for missing kid")
	}
}

func TestValidatorInvalidIssuer(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		TokenPrefix:         "",
	}

	validator, err := NewValidator(cfg, slog.Default())
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}
	defer validator.Close()

	// Create token with wrong issuer
	claims := jwtgo.MapClaims{
		"iss": "https://wrong-issuer.com",
		"aud": testAudience,
		"sub": "user123",
		"exp": time.Now().Add(1 * time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		t.Fatalf("failed to create token: %v", err)
	}

	// Validate should fail
	_, err = validator.ValidateToken(tokenString)
	if err == nil {
		t.Fatal("expected error for invalid issuer")
	}
}

func TestValidatorInvalidAudience(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		TokenPrefix:         "",
	}

	validator, err := NewValidator(cfg, slog.Default())
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}
	defer validator.Close()

	// Create token with wrong audience
	claims := jwtgo.MapClaims{
		"iss": testIssuer,
		"aud": "wrong-audience",
		"sub": "user123",
		"exp": time.Now().Add(1 * time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		t.Fatalf("failed to create token: %v", err)
	}

	// Validate should fail
	_, err = validator.ValidateToken(tokenString)
	if err == nil {
		t.Fatal("expected error for invalid audience")
	}
}

func TestValidatorRequiredClaims(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		RequiredClaims:      []string{"sub", "email", "roles"},
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		TokenPrefix:         "",
	}

	validator, err := NewValidator(cfg, slog.Default())
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}
	defer validator.Close()

	// Create token missing "roles"
	claims := jwtgo.MapClaims{
		"iss":   testIssuer,
		"aud":   testAudience,
		"sub":   "user123",
		"email": "user@example.com",
		"exp":   time.Now().Add(1 * time.Hour).Unix(),
		"iat":   time.Now().Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		t.Fatalf("failed to create token: %v", err)
	}

	// Validate should fail
	_, err = validator.ValidateToken(tokenString)
	if err == nil {
		t.Fatal("expected error for missing required claim")
	}
}

func TestValidatorClockSkew(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           120 * time.Second, // 2 minutes tolerance
		TokenPrefix:         "",
	}

	validator, err := NewValidator(cfg, slog.Default())
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}
	defer validator.Close()

	// Create token that expired 90 seconds ago (within clock skew)
	claims := jwtgo.MapClaims{
		"iss": testIssuer,
		"aud": testAudience,
		"sub": "user123",
		"exp": time.Now().Add(-90 * time.Second).Unix(),
		"iat": time.Now().Add(-2 * time.Hour).Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		t.Fatalf("failed to create token: %v", err)
	}

	// Should be valid due to clock skew
	_, err = validator.ValidateToken(tokenString)
	if err != nil {
		t.Fatalf("unexpected error with clock skew: %v", err)
	}
}
