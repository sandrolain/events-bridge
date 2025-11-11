package main

import (
	"log/slog"
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
	"github.com/stretchr/testify/assert"
)

// TestNewRunnerConfig tests the NewRunnerConfig function
func TestNewRunnerConfig(t *testing.T) {
	cfg := NewRunnerConfig()
	assert.NotNil(t, cfg)
	assert.IsType(t, &RunnerConfig{}, cfg)
}

// TestClaimToStringVariousTypes tests the claimToString method with various types
func TestClaimToStringVariousTypes(t *testing.T) {
	runner := &JWTRunner{
		cfg:  &RunnerConfig{},
		slog: slog.Default(),
	}

	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"string", "test-value", "test-value"},
		{"int", 42, "42"},
		{"int64", int64(123456789), "123456789"},
		{"float64", 3.14159, "3.14159"},
		{"bool-true", true, "true"},
		{"bool-false", false, "false"},
		{"slice-strings", []interface{}{"admin", "user"}, "admin,user"},
		{"slice-ints", []interface{}{1, 2, 3}, "1,2,3"},
		{"slice-mixed", []interface{}{"a", 1, true}, "a,1,true"},
		{"empty-slice", []interface{}{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runner.claimToString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestRunnerClose tests the Close method
func TestRunnerClose(t *testing.T) {
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
	assert.NoError(t, err)
	assert.NotNil(t, runner)

	// Close should not return error
	err = runner.Close()
	assert.NoError(t, err)
}

// TestAddClaimsToMetadataWithComplexTypes tests claim extraction with complex types
func TestAddClaimsToMetadataWithComplexTypes(t *testing.T) {
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
		FailOnError:         false,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
	}

	runner, err := NewRunner(cfg)
	assert.NoError(t, err)
	defer runner.Close()

	// Create token with various claim types
	claims := jwtgo.MapClaims{
		"iss":      testIssuer,
		"aud":      testAudience,
		"sub":      "user123",
		"email":    "user@example.com",
		"age":      30,
		"balance":  1234.56,
		"active":   true,
		"roles":    []interface{}{"admin", "operator"},
		"metadata": map[string]interface{}{"department": "engineering"},
		"exp":      time.Now().Add(1 * time.Hour).Unix(),
		"iat":      time.Now().Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	assert.NoError(t, err)

	// Create message
	stub := testutil.NewAdapter(
		[]byte("test data"),
		map[string]string{
			"authorization": "Bearer " + tokenString,
		},
	)
	msg := message.NewRunnerMessage(stub)

	// Process should succeed
	err = runner.(*JWTRunner).Process(msg)
	assert.NoError(t, err)

	// Verify all claim types were added to metadata
	metadata, _ := msg.GetMetadata()

	assert.Equal(t, "user123", metadata["jwt_sub"])
	assert.Equal(t, "user@example.com", metadata["jwt_email"])
	assert.Equal(t, "30", metadata["jwt_age"])
	assert.Equal(t, "1234.56", metadata["jwt_balance"])
	assert.Equal(t, "true", metadata["jwt_active"])
	assert.Equal(t, "admin,operator", metadata["jwt_roles"])
	// Map types are logged but not added to metadata as simple strings
	assert.Equal(t, "true", metadata["jwt_verified"])
}

// TestNewRunnerWithInvalidConfig tests NewRunner with invalid config
func TestNewRunnerWithInvalidConfig(t *testing.T) {
	// Test with wrong config type
	runner, err := NewRunner("invalid-config")
	assert.Error(t, err)
	assert.Nil(t, runner)
	assert.Contains(t, err.Error(), "invalid config type")

	// Test with nil config
	runner, err = NewRunner(nil)
	assert.Error(t, err)
	assert.Nil(t, runner)

	// Test with empty config (should fail when creating validator)
	runner, err = NewRunner(&RunnerConfig{})
	assert.Error(t, err)
	assert.Nil(t, runner)
}

// TestValidatorClose tests the Validator Close method
func TestValidatorClose(t *testing.T) {
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
	assert.NoError(t, err)
	assert.NotNil(t, validator)

	// Close should not return error
	err = validator.Close()
	assert.NoError(t, err)
}
