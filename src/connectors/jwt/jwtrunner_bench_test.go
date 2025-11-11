package main

import (
	"log/slog"
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func BenchmarkJWTRunnerProcess(b *testing.B) {
	server := setupMockJWKSServer(&testing.T{})
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
		b.Fatalf("failed to create runner: %v", err)
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
	server := setupMockJWKSServer(&testing.T{})
	defer server.Close()

	client, err := NewJWKSClient(server.URL(), 1*time.Hour, slog.Default())
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	kid := server.kid

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
	server := setupMockJWKSServer(&testing.T{})
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
		b.Fatalf("failed to create validator: %v", err)
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

func BenchmarkClaimToString(b *testing.B) {
	runner := &JWTRunner{
		cfg: &RunnerConfig{},
	}

	testCases := []struct {
		name  string
		value interface{}
	}{
		{"string", "test-value"},
		{"int", 123},
		{"float", 123.456},
		{"bool", true},
		{"array", []interface{}{"admin", "operator", "user"}},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = runner.claimToString(tc.value)
			}
		})
	}
}
