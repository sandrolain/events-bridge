package main

import (
	"log/slog"
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
	"github.com/sandrolain/events-bridge/src/common/jwtauth"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func BenchmarkJWTRunnerProcess(b *testing.B) {
	server := jwtauth.SetupMockJWKSServer(&testing.T{})
	defer server.Close()

	cfg := &RunnerConfig{
		Enabled:             true,
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              "https://test.example.com",
		Audience:            "test-audience",
		RequiredClaims:      []string{"sub", "email"},
		ClaimPrefix:         "eb-jwt-",
		FailOnError:         true,
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		b.Fatalf("failed to create runner: %v", err)
	}
	defer runner.Close()

	// Create valid token
	claims := jwtgo.MapClaims{
		"iss":   "https://test.example.com",
		"aud":   "test-audience",
		"sub":   "user123",
		"email": "user@example.com",
		"exp":   time.Now().Add(1 * time.Hour).Unix(),
		"iat":   time.Now().Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		b.Fatalf("failed to create token: %v", err)
	}

	// Create message
	stub := testutil.NewAdapter(
		[]byte("test data"),
		map[string]string{
			"authorization": "Bearer " + tokenString,
		},
	)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := message.NewRunnerMessage(stub)
		if err := runner.(*JWTRunner).Process(msg); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkJWKSClientGetKey(b *testing.B) {
	server := jwtauth.SetupMockJWKSServer(&testing.T{})
	defer server.Close()

	client, err := jwtauth.NewJWKSClient(server.URL(), 1*time.Hour, slog.Default())
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	kid := server.KID()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.GetKey(kid)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkValidatorValidateToken(b *testing.B) {
	server := jwtauth.SetupMockJWKSServer(&testing.T{})
	defer server.Close()

	cfg := &jwtauth.Config{
		Enabled:             true,
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              "https://test.example.com",
		Audience:            "test-audience",
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		TokenPrefix:         "Bearer ",
	}

	validator, err := jwtauth.NewValidator(cfg, slog.Default())
	if err != nil {
		b.Fatalf("failed to create validator: %v", err)
	}
	defer validator.Close()

	// Create valid token
	claims := jwtgo.MapClaims{
		"iss": "https://test.example.com",
		"aud": "test-audience",
		"sub": "user123",
		"exp": time.Now().Add(1 * time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		b.Fatalf("failed to create token: %v", err)
	}

	fullToken := "Bearer " + tokenString

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := validator.ValidateToken(fullToken)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkAuthenticatorAuthenticate tests the performance of the authenticator
func BenchmarkAuthenticatorAuthenticate(b *testing.B) {
	server := jwtauth.SetupMockJWKSServer(&testing.T{})
	defer server.Close()

	cfg := &jwtauth.Config{
		Enabled:             true,
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             server.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              "https://test.example.com",
		Audience:            "test-audience",
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		ClaimPrefix:         "eb-jwt-",
		FailOnError:         false,
	}

	auth, err := jwtauth.NewAuthenticator(cfg, slog.Default())
	if err != nil {
		b.Fatalf("failed to create authenticator: %v", err)
	}
	defer auth.Close()

	// Create valid token
	claims := jwtgo.MapClaims{
		"iss":   "https://test.example.com",
		"aud":   "test-audience",
		"sub":   "user123",
		"email": "user@example.com",
		"exp":   time.Now().Add(1 * time.Hour).Unix(),
		"iat":   time.Now().Unix(),
	}

	tokenString, err := server.CreateValidToken(claims)
	if err != nil {
		b.Fatalf("failed to create token: %v", err)
	}

	metadata := map[string]string{
		"authorization": "Bearer " + tokenString,
		"other":         "value",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result := auth.Authenticate(metadata)
		if !result.Verified {
			b.Fatalf("unexpected error: %v", result.Error)
		}
	}
}
