package main

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
)

const (
	testAddress                = "nats://localhost:4222"
	testSubject                = "test.subject"
	errExpectedNATSSource      = "expected *NATSSource, got %T"
	errExpectedAuthTrue        = "expected hasAuthentication to return true"
	errExpectedAuthFalse       = "expected hasAuthentication to return false"
	errExpectedTargetAuthTrue  = "expected hasRunnerAuthentication to return true"
	errExpectedTargetAuthFalse = "expected hasRunnerAuthentication to return false"
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
	cfg := &SourceConfig{ //nolint:gosec // test placeholder credentials path
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

func TestRunnerConfigWithAuthentication(t *testing.T) {
	cfg := &RunnerConfig{
		Address:  testAddress,
		Subject:  testSubject,
		Username: "testuser",
		Password: "testpass",
	}

	if hasRunnerAuthentication(cfg) == "" {
		t.Error(errExpectedTargetAuthTrue)
	}
}

func TestRunnerConfigNoAuthentication(t *testing.T) {
	cfg := &RunnerConfig{
		Address: testAddress,
		Subject: testSubject,
	}

	if hasRunnerAuthentication(cfg) != "none" {
		t.Error(errExpectedTargetAuthFalse)
	}
}

func TestRunnerConfigWithTLS(t *testing.T) {
	cfg := &RunnerConfig{
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

func TestNewRunnerConfigInvalidType(t *testing.T) {
	_, err := NewRunner("invalid config")
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
}

func TestRunnerConfigJetStreamMode(t *testing.T) {
	cfg := &RunnerConfig{
		Address: testAddress,
		Subject: testSubject,
		Mode:    "jetstream",
		Stream:  "TEST_STREAM",
	}

	// Only test configuration parsing
	if cfg.Mode != "jetstream" {
		t.Errorf("expected Mode jetstream, got %s", cfg.Mode)
	}

	if cfg.Stream != "TEST_STREAM" {
		t.Errorf("expected Stream TEST_STREAM, got %s", cfg.Stream)
	}
}

func TestRunnerConfigKVSetMode(t *testing.T) {
	cfg := &RunnerConfig{
		Address:  testAddress,
		Subject:  testSubject,
		Mode:     "kv-set",
		KVBucket: "test_bucket",
		KVKey:    "test_key",
	}

	// Only test configuration parsing, not actual connection
	if cfg.Mode != "kv-set" {
		t.Errorf("expected Mode kv-set, got %s", cfg.Mode)
	}

	if cfg.KVBucket != "test_bucket" {
		t.Errorf("expected KVBucket test_bucket, got %s", cfg.KVBucket)
	}

	if cfg.KVKey != "test_key" {
		t.Errorf("expected KVKey test_key, got %s", cfg.KVKey)
	}
}

func TestRunnerConfigPublishMode(t *testing.T) {
	addr, cleanup := startNATSServer(t)
	defer cleanup()

	cfg := &RunnerConfig{
		Address: "nats://" + addr,
		Subject: testSubject,
		Mode:    "publish",
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedError, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Logf("failed to close runner: %v", err)
		}
	}()

	natsRunner, ok := runner.(*NATSRunner)
	if !ok {
		t.Fatalf("expected *NATSRunner, got %T", runner)
	}

	if natsRunner.cfg.Mode != "publish" {
		t.Errorf("expected Mode publish, got %s", natsRunner.cfg.Mode)
	}

	if natsRunner.cfg.Timeout != 5*time.Second {
		t.Errorf("expected Timeout 5s, got %v", natsRunner.cfg.Timeout)
	}
}

func TestSourceConfigRequestMode(t *testing.T) {
	cfg := &SourceConfig{
		Address:        testAddress,
		Subject:        testSubject,
		Mode:           "request",
		RequestTimeout: 10 * time.Second,
	}

	// Only test configuration parsing
	if cfg.Mode != "request" {
		t.Errorf("expected Mode request, got %s", cfg.Mode)
	}

	if cfg.RequestTimeout != 10*time.Second {
		t.Errorf("expected RequestTimeout 10s, got %v", cfg.RequestTimeout)
	}
}

func TestSourceConfigKVWatchMode(t *testing.T) {
	cfg := &SourceConfig{
		Address:  testAddress,
		Subject:  testSubject,
		Mode:     "kv-watch",
		KVBucket: "test_bucket",
		KVKeys:   []string{"key1", "key2"},
	}

	// Only test configuration parsing
	if cfg.Mode != "kv-watch" {
		t.Errorf("expected Mode kv-watch, got %s", cfg.Mode)
	}

	if cfg.KVBucket != "test_bucket" {
		t.Errorf("expected KVBucket test_bucket, got %s", cfg.KVBucket)
	}

	if len(cfg.KVKeys) != 2 {
		t.Errorf("expected 2 KVKeys, got %d", len(cfg.KVKeys))
	}
}

func TestSourceConfigSubscribeWithJetStream(t *testing.T) {
	cfg := &SourceConfig{
		Address:    testAddress,
		Subject:    testSubject,
		Mode:       "subscribe",
		Stream:     "TEST_STREAM",
		Consumer:   "TEST_CONSUMER",
		QueueGroup: "test_queue",
	}

	// Only test configuration parsing
	if cfg.Stream != "TEST_STREAM" {
		t.Errorf("expected Stream TEST_STREAM, got %s", cfg.Stream)
	}

	if cfg.Consumer != "TEST_CONSUMER" {
		t.Errorf("expected Consumer TEST_CONSUMER, got %s", cfg.Consumer)
	}

	if cfg.QueueGroup != "test_queue" {
		t.Errorf("expected QueueGroup test_queue, got %s", cfg.QueueGroup)
	}
}
