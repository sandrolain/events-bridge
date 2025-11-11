package main

import (
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRunner(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}

	if runner == nil {
		t.Fatal(errExpectedNonNil)
	}

	if err := runner.Close(); err != nil {
		t.Fatalf("failed to close runner: %v", err)
	}
}

func TestProcessValidToken(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		RequiredClaims:      []string{"sub", "email"},
		ClaimPrefix:         "jwt_",
		FailOnError:         true,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}
	defer runner.Close()

	// Create valid token
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

	// Create message with token
	stub := testutil.NewAdapter(
		[]byte("test data"),
		map[string]string{
			"authorization": "Bearer " + tokenString,
		},
	)
	msg := message.NewRunnerMessage(stub)

	// Process
	err = runner.(*JWTRunner).Process(msg)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}

	// Verify metadata
	metadata, _ := msg.GetMetadata()

	if metadata["jwt_verified"] != "true" {
		t.Errorf("expected jwt_verified=true, got %s", metadata["jwt_verified"])
	}

	if metadata["jwt_sub"] != "user123" {
		t.Errorf("expected jwt_sub=user123, got %s", metadata["jwt_sub"])
	}

	if metadata["jwt_email"] != "user@example.com" {
		t.Errorf("expected jwt_email=user@example.com, got %s", metadata["jwt_email"])
	}

	if metadata["jwt_iss"] != testIssuer {
		t.Errorf("expected jwt_iss=%s, got %s", testIssuer, metadata["jwt_iss"])
	}
}

func TestProcessExpiredToken(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		FailOnError:         true,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           1 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}
	defer runner.Close()

	// Create expired token
	claims := jwtgo.MapClaims{
		"iss": testIssuer,
		"aud": testAudience,
		"sub": "user123",
		"exp": time.Now().Add(-1 * time.Hour).Unix(), // Expired 1 hour ago
		"iat": time.Now().Add(-2 * time.Hour).Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		t.Fatalf("failed to create token: %v", err)
	}

	// Create message
	stub := testutil.NewAdapter(
		[]byte("test data"),
		map[string]string{
			"authorization": "Bearer " + tokenString,
		},
	)
	msg := message.NewRunnerMessage(stub)

	// Process should fail
	err = runner.(*JWTRunner).Process(msg)
	if err == nil {
		t.Fatal(errExpectedErr)
	}
}

func TestProcessInvalidIssuer(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		FailOnError:         true,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}
	defer runner.Close()

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

	// Create message
	stub := testutil.NewAdapter(
		[]byte("test data"),
		map[string]string{
			"authorization": "Bearer " + tokenString,
		},
	)
	msg := message.NewRunnerMessage(stub)

	// Process should fail
	err = runner.(*JWTRunner).Process(msg)
	if err == nil {
		t.Fatal(errExpectedErr)
	}
}

func TestProcessMissingToken(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		FailOnError:         true,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}
	defer runner.Close()

	// Create message without token
	stub := testutil.NewAdapter(
		[]byte("test data"),
		map[string]string{},
	)
	msg := message.NewRunnerMessage(stub)

	// Process should fail
	err = runner.(*JWTRunner).Process(msg)
	if err == nil {
		t.Fatal(errExpectedErr)
	}
}

func TestProcessFailOnErrorFalse(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		ClaimPrefix:         "jwt_",
		FailOnError:         false, // Don't fail on error
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           1 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}
	defer runner.Close()

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

	// Create message
	stub := testutil.NewAdapter(
		[]byte("test data"),
		map[string]string{
			"authorization": "Bearer " + tokenString,
		},
	)
	msg := message.NewRunnerMessage(stub)

	// Process should NOT fail
	err = runner.(*JWTRunner).Process(msg)
	require.NoError(t, err, "expected no error with FailOnError=false")

	// Verify metadata indicates failure
	metadata, _ := msg.GetMetadata()

	assert.Equal(t, "false", metadata["jwt_verified"], "expected jwt_verified=false")
	assert.NotEmpty(t, metadata["jwt_error"], "expected jwt_error to be set")
}

func TestProcessMissingRequiredClaim(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		RequiredClaims:      []string{"sub", "email", "roles"}, // roles is missing
		FailOnError:         true,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}
	defer runner.Close()

	// Create token without "roles" claim
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

	// Create message
	stub := testutil.NewAdapter(
		[]byte("test data"),
		map[string]string{
			"authorization": "Bearer " + tokenString,
		},
	)
	msg := message.NewRunnerMessage(stub)

	// Process should fail
	err = runner.(*JWTRunner).Process(msg)
	if err == nil {
		t.Fatal(errExpectedErr)
	}
}

func TestProcessArrayClaim(t *testing.T) {
	t.Parallel()

	server := setupMockJWKSServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		ClaimPrefix:         "jwt_",
		FailOnError:         true,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}
	defer runner.Close()

	// Create token with array claim
	claims := jwtgo.MapClaims{
		"iss":   testIssuer,
		"aud":   testAudience,
		"sub":   "user123",
		"roles": []interface{}{"admin", "operator", "user"},
		"exp":   time.Now().Add(1 * time.Hour).Unix(),
		"iat":   time.Now().Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		t.Fatalf("failed to create token: %v", err)
	}

	// Create message
	stub := testutil.NewAdapter(
		[]byte("test data"),
		map[string]string{
			"authorization": "Bearer " + tokenString,
		},
	)
	msg := message.NewRunnerMessage(stub)

	// Process
	err = runner.(*JWTRunner).Process(msg)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}

	// Verify array claim is converted to comma-separated string
	metadata, _ := msg.GetMetadata()

	roles := metadata["jwt_roles"]
	if roles != "admin,operator,user" {
		t.Errorf("expected jwt_roles=admin,operator,user, got %s", roles)
	}
}
