package jwtauth

import (
	"log/slog"
	"os"
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
)

func TestNewAuthenticator(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	t.Run("nil config", func(t *testing.T) {
		auth, err := NewAuthenticator(nil, logger)
		if err != nil {
			t.Errorf(errUnexpectedErr, err)
		}
		if auth != nil {
			t.Error("expected nil authenticator for nil config")
		}
	})

	t.Run("disabled config", func(t *testing.T) {
		cfg := &Config{Enabled: false}
		auth, err := NewAuthenticator(cfg, logger)
		if err != nil {
			t.Errorf(errUnexpectedErr, err)
		}
		if auth != nil {
			t.Error("expected nil authenticator for disabled config")
		}
	})

	t.Run("enabled config without JWKS URL", func(t *testing.T) {
		cfg := &Config{
			Enabled: true,
			// Missing JWKsURL
		}
		_, err := NewAuthenticator(cfg, logger)
		if err == nil {
			t.Error(errExpectedErr)
		}
	})
}

func TestAuthenticator_Authenticate(t *testing.T) {
	mockServer := SetupMockJWKSServer(t)
	defer mockServer.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	cfg := &Config{
		Enabled:             true,
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             mockServer.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		ClaimPrefix:         "jwt_",
		FailOnError:         false,
	}

	auth, err := NewAuthenticator(cfg, logger)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}
	defer func() {
		if err := auth.Close(); err != nil {
			t.Logf("failed to close authenticator: %v", err)
		}
	}()

	t.Run("valid token in metadata", func(t *testing.T) {
		claims := jwtgo.MapClaims{
			"iss":   testIssuer,
			"aud":   testAudience,
			"sub":   "test-user",
			"email": "test@example.com",
			"role":  "admin",
			"exp":   time.Now().Add(1 * time.Hour).Unix(),
			"iat":   time.Now().Unix(),
		}

		token, err := mockServer.CreateValidToken(claims)
		if err != nil {
			t.Fatalf(errUnexpectedErr, err)
		}

		metadata := map[string]string{
			"authorization": "Bearer " + token,
			"other":         "value",
		}

		result := auth.Authenticate(metadata)

		if !result.Verified {
			t.Errorf("expected token to be verified, got error: %v", result.Error)
		}

		if result.Metadata["jwt_verified"] != "true" {
			t.Error("expected jwt_verified to be true")
		}

		if result.Metadata["jwt_sub"] != "test-user" {
			t.Errorf("expected jwt_sub to be 'test-user', got %v", result.Metadata["jwt_sub"])
		}

		if result.Metadata["jwt_email"] != "test@example.com" {
			t.Errorf("expected jwt_email to be 'test@example.com', got %v", result.Metadata["jwt_email"])
		}

		if result.Metadata["other"] != "value" {
			t.Error("expected original metadata to be preserved")
		}
	})

	t.Run("missing token in metadata", func(t *testing.T) {
		metadata := map[string]string{
			"other": "value",
		}

		result := auth.Authenticate(metadata)

		if result.Verified {
			t.Error("expected token to fail verification")
		}

		if result.Error == nil {
			t.Error(errExpectedErr)
		}

		if result.Metadata["jwt_verified"] != "false" {
			t.Error("expected jwt_verified to be false")
		}
	})

	t.Run("empty token in metadata", func(t *testing.T) {
		metadata := map[string]string{
			"authorization": "",
		}

		result := auth.Authenticate(metadata)

		if result.Verified {
			t.Error("expected token to fail verification")
		}

		if result.Metadata["jwt_verified"] != "false" {
			t.Error("expected jwt_verified to be false")
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		metadata := map[string]string{
			"authorization": "Bearer invalid-token",
		}

		result := auth.Authenticate(metadata)

		if result.Verified {
			t.Error("expected token to fail verification")
		}

		if result.Metadata["jwt_verified"] != "false" {
			t.Error("expected jwt_verified to be false")
		}

		if result.Metadata["jwt_error"] == "" {
			t.Error("expected jwt_error to be set")
		}
	})

	t.Run("array claim conversion", func(t *testing.T) {
		claims := jwtgo.MapClaims{
			"iss":   testIssuer,
			"aud":   testAudience,
			"sub":   "test-user",
			"roles": []interface{}{"admin", "user", "developer"},
			"exp":   time.Now().Add(1 * time.Hour).Unix(),
			"iat":   time.Now().Unix(),
		}

		token, err := mockServer.CreateValidToken(claims)
		if err != nil {
			t.Fatalf(errUnexpectedErr, err)
		}

		metadata := map[string]string{
			"authorization": "Bearer " + token,
		}

		result := auth.Authenticate(metadata)

		if !result.Verified {
			t.Errorf("expected token to be verified, got error: %v", result.Error)
		}

		roles := result.Metadata["jwt_roles"]
		if roles != "admin,user,developer" {
			t.Errorf("expected roles to be 'admin,user,developer', got %v", roles)
		}
	})
}

func TestAuthenticator_FailOnError(t *testing.T) {
	mockServer := SetupMockJWKSServer(t)
	defer mockServer.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	cfg := &Config{
		Enabled:             true,
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             mockServer.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		ClaimPrefix:         "jwt_",
		FailOnError:         true,
	}

	auth, err := NewAuthenticator(cfg, logger)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}
	defer func() {
		if err := auth.Close(); err != nil {
			t.Logf("failed to close authenticator: %v", err)
		}
	}()

	if !auth.ShouldFailOnError() {
		t.Error("expected ShouldFailOnError to be true")
	}
}
