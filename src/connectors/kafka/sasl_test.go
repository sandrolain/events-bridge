package main

import (
	"testing"

	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	errUnexpected     = "unexpected error: %v"
	errExpectedNonNil = "expected non-nil mechanism"
)

func TestSASLConfigDisabled(t *testing.T) {
	cfg := &SASLConfig{
		Enabled: false,
	}

	mechanism, err := cfg.BuildSASLMechanism()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if mechanism != nil {
		t.Fatal("expected nil mechanism when SASL is disabled")
	}
}

func TestSASLConfigPlain(t *testing.T) {
	cfg := &SASLConfig{
		Enabled:   true,
		Mechanism: "PLAIN",
		Username:  "testuser",
		Password:  "testpass",
	}

	mechanism, err := cfg.BuildSASLMechanism()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if mechanism == nil {
		t.Fatal(errExpectedNonNil)
	}

	plainMech, ok := mechanism.(plain.Mechanism)
	if !ok {
		t.Fatalf("expected plain.Mechanism, got %T", mechanism)
	}

	if plainMech.Username != "testuser" {
		t.Errorf("expected username 'testuser', got '%s'", plainMech.Username)
	}

	if plainMech.Password != "testpass" {
		t.Errorf("expected password 'testpass', got '%s'", plainMech.Password)
	}
}

func TestSASLConfigSCRAMSHA256(t *testing.T) {
	cfg := &SASLConfig{
		Enabled:   true,
		Mechanism: "SCRAM-SHA-256",
		Username:  "testuser",
		Password:  "testpass",
	}

	mechanism, err := cfg.BuildSASLMechanism()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if mechanism == nil {
		t.Fatal(errExpectedNonNil)
	}
}

func TestSASLConfigSCRAMSHA512(t *testing.T) {
	cfg := &SASLConfig{
		Enabled:   true,
		Mechanism: "SCRAM-SHA-512",
		Username:  "testuser",
		Password:  "testpass",
	}

	mechanism, err := cfg.BuildSASLMechanism()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if mechanism == nil {
		t.Fatal(errExpectedNonNil)
	}
}

func TestSASLConfigUnsupportedMechanism(t *testing.T) {
	cfg := &SASLConfig{
		Enabled:   true,
		Mechanism: "INVALID",
		Username:  "testuser",
		Password:  "testpass",
	}

	_, err := cfg.BuildSASLMechanism()
	if err == nil {
		t.Fatal("expected error for unsupported mechanism")
	}
}

func TestSASLConfigMissingCredentials(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
	}{
		{"no username", "", "testpass"},
		{"no password", "testuser", ""},
		{"no credentials", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &SASLConfig{
				Enabled:   true,
				Mechanism: "PLAIN",
				Username:  tt.username,
				Password:  tt.password,
			}

			_, err := cfg.BuildSASLMechanism()
			if err == nil {
				t.Fatal("expected error for missing credentials")
			}
		})
	}
}
