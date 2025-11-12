package jwtauth

import (
	"log/slog"
	"os"
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
)

func TestConfig_IsEnabled(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		expected bool
	}{
		{
			name:     "nil config",
			config:   nil,
			expected: false,
		},
		{
			name: "enabled config",
			config: &Config{
				Enabled: true,
			},
			expected: true,
		},
		{
			name: "disabled config",
			config: &Config{
				Enabled: false,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.IsEnabled()
			if result != tt.expected {
				t.Errorf("IsEnabled() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestValidator_ValidateToken(t *testing.T) {
	mockServer := SetupMockJWKSServer(t)
	defer mockServer.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	cfg := &Config{
		Enabled:             true,
		JWKsURL:             mockServer.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		TokenPrefix:         "Bearer ",
	}

	validator, err := NewValidator(cfg, logger)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}
	defer func() {
		if err := validator.Close(); err != nil {
			t.Logf("failed to close validator: %v", err)
		}
	}()

	t.Run("valid token", func(t *testing.T) {
		claims := jwtgo.MapClaims{
			"iss": testIssuer,
			"aud": testAudience,
			"sub": "test-user",
			"exp": time.Now().Add(1 * time.Hour).Unix(),
			"iat": time.Now().Unix(),
		}

		token, err := mockServer.CreateValidToken(claims)
		if err != nil {
			t.Fatalf(errUnexpectedErr, err)
		}

		validatedClaims, err := validator.ValidateToken(token)
		if err != nil {
			t.Fatalf(errUnexpectedErr, err)
		}

		if validatedClaims["sub"] != "test-user" {
			t.Errorf("expected sub claim to be 'test-user', got %v", validatedClaims["sub"])
		}
	})

	t.Run("token with prefix", func(t *testing.T) {
		claims := jwtgo.MapClaims{
			"iss": testIssuer,
			"aud": testAudience,
			"sub": "test-user",
			"exp": time.Now().Add(1 * time.Hour).Unix(),
			"iat": time.Now().Unix(),
		}

		token, err := mockServer.CreateValidToken(claims)
		if err != nil {
			t.Fatalf(errUnexpectedErr, err)
		}

		// Add Bearer prefix
		tokenWithPrefix := "Bearer " + token

		validatedClaims, err := validator.ValidateToken(tokenWithPrefix)
		if err != nil {
			t.Fatalf(errUnexpectedErr, err)
		}

		if validatedClaims["sub"] != "test-user" {
			t.Errorf("expected sub claim to be 'test-user', got %v", validatedClaims["sub"])
		}
	})

	t.Run("expired token", func(t *testing.T) {
		claims := jwtgo.MapClaims{
			"iss": testIssuer,
			"aud": testAudience,
			"sub": "test-user",
			"exp": time.Now().Add(-1 * time.Hour).Unix(), // Expired
			"iat": time.Now().Add(-2 * time.Hour).Unix(),
		}

		token, err := mockServer.CreateValidToken(claims)
		if err != nil {
			t.Fatalf(errUnexpectedErr, err)
		}

		_, err = validator.ValidateToken(token)
		if err == nil {
			t.Error(errExpectedErr)
		}
	})

	t.Run("wrong issuer", func(t *testing.T) {
		claims := jwtgo.MapClaims{
			"iss": "https://wrong-issuer.com",
			"aud": testAudience,
			"sub": "test-user",
			"exp": time.Now().Add(1 * time.Hour).Unix(),
			"iat": time.Now().Unix(),
		}

		token, err := mockServer.CreateValidToken(claims)
		if err != nil {
			t.Fatalf(errUnexpectedErr, err)
		}

		_, err = validator.ValidateToken(token)
		if err == nil {
			t.Error(errExpectedErr)
		}
	})

	t.Run("wrong audience", func(t *testing.T) {
		claims := jwtgo.MapClaims{
			"iss": testIssuer,
			"aud": "wrong-audience",
			"sub": "test-user",
			"exp": time.Now().Add(1 * time.Hour).Unix(),
			"iat": time.Now().Unix(),
		}

		token, err := mockServer.CreateValidToken(claims)
		if err != nil {
			t.Fatalf(errUnexpectedErr, err)
		}

		_, err = validator.ValidateToken(token)
		if err == nil {
			t.Error(errExpectedErr)
		}
	})

	t.Run("empty token", func(t *testing.T) {
		_, err := validator.ValidateToken("")
		if err == nil {
			t.Error(errExpectedErr)
		}
	})

	t.Run("invalid token format", func(t *testing.T) {
		_, err := validator.ValidateToken("not-a-valid-token")
		if err == nil {
			t.Error(errExpectedErr)
		}
	})
}

func TestValidator_RequiredClaims(t *testing.T) {
	mockServer := SetupMockJWKSServer(t)
	defer mockServer.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	cfg := &Config{
		Enabled:             true,
		JWKsURL:             mockServer.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              testIssuer,
		Audience:            testAudience,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		RequiredClaims:      []string{"email", "role"},
	}

	validator, err := NewValidator(cfg, logger)
	if err != nil {
		t.Fatalf(errUnexpectedErr, err)
	}
	defer func() {
		if err := validator.Close(); err != nil {
			t.Logf("failed to close validator: %v", err)
		}
	}()

	t.Run("all required claims present", func(t *testing.T) {
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

		_, err = validator.ValidateToken(token)
		if err != nil {
			t.Fatalf(errUnexpectedErr, err)
		}
	})

	t.Run("missing required claim", func(t *testing.T) {
		claims := jwtgo.MapClaims{
			"iss":   testIssuer,
			"aud":   testAudience,
			"sub":   "test-user",
			"email": "test@example.com",
			// Missing "role" claim
			"exp": time.Now().Add(1 * time.Hour).Unix(),
			"iat": time.Now().Unix(),
		}

		token, err := mockServer.CreateValidToken(claims)
		if err != nil {
			t.Fatalf(errUnexpectedErr, err)
		}

		_, err = validator.ValidateToken(token)
		if err == nil {
			t.Error(errExpectedErr)
		}
	})
}
