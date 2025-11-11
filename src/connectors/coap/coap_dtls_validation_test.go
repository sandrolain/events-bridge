package main

import "testing"

const (
	testDTLSAddr     = "127.0.0.1:5683"
	testCertFile     = "cert.pem"
	testKeyFile      = "key.pem"
	errMsgKeyMissing = "expected error when key missing"
	testPSKSecret    = "secret"
)

// Test DTLS configuration validation for source, target, runner.
func TestDTLSValidationSource(t *testing.T) {
	base := SourceConfig{Protocol: CoAPProtocolDTLS, Address: testDTLSAddr, Path: "/x", Method: "GET"}

	// Missing both
	if err := validateSourceSecurity(&base); err == nil {
		t.Fatalf("expected error when neither PSK nor cert provided")
	}

	// Both modes provided
	both := base
	both.PSK = "abc"
	both.PSKIdentity = "id"
	both.TLSCertFile = testCertFile
	if err := validateSourceSecurity(&both); err == nil {
		t.Fatalf("expected error when both PSK and cert fields set")
	}

	// PSK missing identity
	pskMissingID := base
	pskMissingID.PSK = testPSKSecret
	if err := validateSourceSecurity(&pskMissingID); err == nil {
		t.Fatalf("expected error when pskIdentity missing")
	}

	// Valid PSK
	pskOK := base
	pskOK.PSK = testPSKSecret
	pskOK.PSKIdentity = "device1"
	if err := validateSourceSecurity(&pskOK); err != nil {
		t.Fatalf("unexpected error for valid PSK mode: %v", err)
	}

	// Cert missing key
	certMissingKey := base
	certMissingKey.TLSCertFile = testCertFile
	if err := validateSourceSecurity(&certMissingKey); err == nil {
		t.Fatal(errMsgKeyMissing)
	}

	// Valid cert
	certOK := base
	certOK.TLSCertFile = testCertFile
	certOK.TLSKeyFile = testKeyFile
	if err := validateSourceSecurity(&certOK); err != nil {
		t.Fatalf("unexpected error for valid cert mode: %v", err)
	}
}

func TestDTLSValidationRunner(t *testing.T) {
	base := CoAPRunnerConfig{Protocol: CoAPProtocolDTLS, Address: testDTLSAddr, Path: "/x", Method: "GET"}

	if err := validateRunnerSecurity(&base); err == nil {
		t.Fatalf("expected error when neither mode provided")
	}

	both := base
	both.PSK = "x"
	both.PSKIdentity = "id"
	both.TLSCertFile = testCertFile
	if err := validateRunnerSecurity(&both); err == nil {
		t.Fatalf("expected error when both modes set")
	}

	pskMissingID := base
	pskMissingID.PSK = testPSKSecret
	if err := validateRunnerSecurity(&pskMissingID); err == nil {
		t.Fatalf("expected error when identity missing")
	}

	pskOK := base
	pskOK.PSK = testPSKSecret
	pskOK.PSKIdentity = "id"
	if err := validateRunnerSecurity(&pskOK); err != nil {
		t.Fatalf("unexpected error for valid PSK: %v", err)
	}

	certMissingKey := base
	certMissingKey.TLSCertFile = testCertFile
	if err := validateRunnerSecurity(&certMissingKey); err == nil {
		t.Fatal(errMsgKeyMissing)
	}

	certOK := base
	certOK.TLSCertFile = testCertFile
	certOK.TLSKeyFile = testKeyFile
	if err := validateRunnerSecurity(&certOK); err != nil {
		t.Fatalf("unexpected error for valid cert: %v", err)
	}
}

func TestDTLSValidationRunnerAlt(t *testing.T) {
	// Test the same validation logic used by runner (already tested above as "Runner")
	// but keeping this separate test for symmetry with source/target pattern
	base := CoAPRunnerConfig{Protocol: CoAPProtocolDTLS, Address: testDTLSAddr, Path: "/x", Method: "GET"}

	if err := validateRunnerSecurity(&base); err == nil {
		t.Fatalf("expected error when neither mode provided")
	}

	both := base
	both.PSK = "x"
	both.PSKIdentity = "id"
	both.TLSCertFile = testCertFile
	if err := validateRunnerSecurity(&both); err == nil {
		t.Fatalf("expected error when both modes set")
	}

	pskMissingID := base
	pskMissingID.PSK = testPSKSecret
	if err := validateRunnerSecurity(&pskMissingID); err == nil {
		t.Fatalf("expected error when identity missing")
	}

	pskOK := base
	pskOK.PSK = testPSKSecret
	pskOK.PSKIdentity = "id"
	if err := validateRunnerSecurity(&pskOK); err != nil {
		t.Fatalf("unexpected error for valid PSK: %v", err)
	}

	certMissingKey := base
	certMissingKey.TLSCertFile = testCertFile
	if err := validateRunnerSecurity(&certMissingKey); err == nil {
		t.Fatal(errMsgKeyMissing)
	}

	certOK := base
	certOK.TLSCertFile = testCertFile
	certOK.TLSKeyFile = testKeyFile
	if err := validateRunnerSecurity(&certOK); err != nil {
		t.Fatalf("unexpected error for valid cert: %v", err)
	}
}
