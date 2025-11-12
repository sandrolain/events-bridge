package main

import (
	"log/slog"
	"os"
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
	"github.com/sandrolain/events-bridge/src/common/jwtauth"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func TestJWTRunner_Process(t *testing.T) {
	mockServer := jwtauth.SetupMockJWKSServer(t)
	defer mockServer.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	cfg := &RunnerConfig{
		Enabled:             true,
		TokenMetadataKey:    "authorization",
		TokenPrefix:         "Bearer ",
		JWKsURL:             mockServer.URL(),
		JWKsRefreshInterval: 1 * time.Hour,
		Issuer:              "https://test.example.com",
		Audience:            "test-audience",
		AllowedAlgorithms:   []string{"RS256"},
		ClockSkew:           60 * time.Second,
		ClaimPrefix:         "jwt_",
		FailOnError:         true,
	}

	authenticator, err := jwtauth.NewAuthenticator(cfg, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() {
		if err := authenticator.Close(); err != nil {
			t.Logf("failed to close authenticator: %v", err)
		}
	}()

	runner := &JWTRunner{
		cfg:           cfg,
		slog:          logger,
		authenticator: authenticator,
	}

	t.Run("valid token", func(t *testing.T) {
		claims := jwtgo.MapClaims{
			"iss":   "https://test.example.com",
			"aud":   "test-audience",
			"sub":   "test-user",
			"email": "test@example.com",
			"exp":   time.Now().Add(1 * time.Hour).Unix(),
			"iat":   time.Now().Unix(),
		}

		token, err := mockServer.CreateValidToken(claims)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		stub := testutil.NewAdapter(
			[]byte("test data"),
			map[string]string{
				"authorization": "Bearer " + token,
			},
		)
		msg := message.NewRunnerMessage(stub)

		err = runner.Process(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		metadata, _ := msg.GetMetadata()
		if metadata["jwt_verified"] != "true" {
			t.Error("expected jwt_verified to be true")
		}

		if metadata["jwt_sub"] != "test-user" {
			t.Errorf("expected jwt_sub to be 'test-user', got %v", metadata["jwt_sub"])
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		stub := testutil.NewAdapter(
			[]byte("test data"),
			map[string]string{
				"authorization": "Bearer invalid-token",
			},
		)
		msg := message.NewRunnerMessage(stub)

		err := runner.Process(msg)
		if err == nil {
			t.Error("expected error but got none")
		}
	})

	t.Run("missing token", func(t *testing.T) {
		stub := testutil.NewAdapter(
			[]byte("test data"),
			map[string]string{},
		)
		msg := message.NewRunnerMessage(stub)

		err := runner.Process(msg)
		if err == nil {
			t.Error("expected error but got none")
		}
	})
}

func TestNewRunnerConfig(t *testing.T) {
	cfg := NewRunnerConfig()
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}

	runnerCfg, ok := cfg.(*RunnerConfig)
	if !ok {
		t.Fatalf("expected *RunnerConfig, got %T", cfg)
	}

	if !runnerCfg.FailOnError {
		t.Error("expected FailOnError to be true by default for runner")
	}
}
