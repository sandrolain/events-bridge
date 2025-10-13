package main

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
)

const (
	testAddress                = "nats://localhost:4222"
	testSubject                = "test.subject"
	errExpectedNATSSource      = "expected *NATSSource, got %T"
	errExpectedAuthTrue        = "expected hasAuthentication to return true"
	errExpectedAuthFalse       = "expected hasAuthentication to return false"
	errExpectedTargetAuthTrue  = "expected hasTargetAuthentication to return true"
	errExpectedTargetAuthFalse = "expected hasTargetAuthentication to return false"
	errUnexpectedError         = "unexpected error: %v"
)

func TestSourceConfigWithUsername(t *testing.T) {
	cfg := &SourceConfig{
		Address:  testAddress,
		Subject:  testSubject,
		Username: "testuser",
		Password: "testpass",
	}

	source, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedError, err)
	}

	natsSource, ok := source.(*NATSSource)
	if !ok {
		t.Fatalf(errExpectedNATSSource, source)
	}

	if !natsSource.hasAuthentication() {
		t.Error(errExpectedAuthTrue)
	}
}

func TestSourceConfigWithToken(t *testing.T) {
	cfg := &SourceConfig{
		Address: testAddress,
		Subject: testSubject,
		Token:   "test-token-123",
	}

	source, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedError, err)
	}

	natsSource, ok := source.(*NATSSource)
	if !ok {
		t.Fatalf(errExpectedNATSSource, source)
	}

	if !natsSource.hasAuthentication() {
		t.Error(errExpectedAuthTrue)
	}
}

func TestSourceConfigWithNKey(t *testing.T) {
	cfg := &SourceConfig{
		Address:  testAddress,
		Subject:  testSubject,
		NKeyFile: "/path/to/nkey.seed",
	}

	source, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedError, err)
	}

	natsSource, ok := source.(*NATSSource)
	if !ok {
		t.Fatalf(errExpectedNATSSource, source)
	}

	if !natsSource.hasAuthentication() {
		t.Error(errExpectedAuthTrue)
	}
}

func TestSourceConfigWithCredentials(t *testing.T) {
	cfg := &SourceConfig{
		Address:         testAddress,
		Subject:         testSubject,
		CredentialsFile: "/path/to/creds.creds",
	}

	source, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedError, err)
	}

	natsSource, ok := source.(*NATSSource)
	if !ok {
		t.Fatalf(errExpectedNATSSource, source)
	}

	if !natsSource.hasAuthentication() {
		t.Error(errExpectedAuthTrue)
	}
}

func TestSourceConfigNoAuthentication(t *testing.T) {
	cfg := &SourceConfig{
		Address: testAddress,
		Subject: testSubject,
	}

	source, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedError, err)
	}

	natsSource, ok := source.(*NATSSource)
	if !ok {
		t.Fatalf(errExpectedNATSSource, source)
	}

	if natsSource.hasAuthentication() {
		t.Error(errExpectedAuthFalse)
	}
}

func TestSourceConfigWithTLS(t *testing.T) {
	cfg := &SourceConfig{
		Address: "tls://localhost:4222",
		Subject: testSubject,
		TLS: &tlsconfig.Config{
			Enabled:    true,
			MinVersion: "1.2",
		},
	}

	source, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedError, err)
	}

	natsSource, ok := source.(*NATSSource)
	if !ok {
		t.Fatalf(errExpectedNATSSource, source)
	}

	if natsSource.cfg.TLS == nil || !natsSource.cfg.TLS.Enabled {
		t.Error("expected TLS to be enabled")
	}
}

func TestSourceConfigReconnection(t *testing.T) {
	cfg := &SourceConfig{
		Address:       testAddress,
		Subject:       testSubject,
		MaxReconnects: 10,
		ReconnectWait: 5,
	}

	source, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedError, err)
	}

	natsSource, ok := source.(*NATSSource)
	if !ok {
		t.Fatalf(errExpectedNATSSource, source)
	}

	if natsSource.cfg.MaxReconnects != 10 {
		t.Errorf("expected MaxReconnects 10, got %d", natsSource.cfg.MaxReconnects)
	}

	if natsSource.cfg.ReconnectWait != 5 {
		t.Errorf("expected ReconnectWait 5, got %d", natsSource.cfg.ReconnectWait)
	}
}

func TestTargetConfigWithAuthentication(t *testing.T) {
	cfg := &TargetConfig{
		Address:  testAddress,
		Subject:  testSubject,
		Username: "testuser",
		Password: "testpass",
	}

	if !hasTargetAuthentication(cfg) {
		t.Error(errExpectedTargetAuthTrue)
	}
}

func TestTargetConfigNoAuthentication(t *testing.T) {
	cfg := &TargetConfig{
		Address: testAddress,
		Subject: testSubject,
	}

	if hasTargetAuthentication(cfg) {
		t.Error(errExpectedTargetAuthFalse)
	}
}

func TestTargetConfigWithTLS(t *testing.T) {
	cfg := &TargetConfig{
		Address: "tls://localhost:4222",
		Subject: testSubject,
		TLS: &tlsconfig.Config{
			Enabled:    true,
			MinVersion: "1.3",
		},
	}

	if cfg.TLS == nil || !cfg.TLS.Enabled {
		t.Error("expected TLS to be enabled")
	}

	if cfg.TLS.MinVersion != "1.3" {
		t.Errorf("expected TLS MinVersion 1.3, got %s", cfg.TLS.MinVersion)
	}
}

func TestNewSourceConfigInvalidType(t *testing.T) {
	_, err := NewSource("invalid config")
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
}

func TestNewTargetConfigInvalidType(t *testing.T) {
	_, err := NewTarget("invalid config")
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
}
