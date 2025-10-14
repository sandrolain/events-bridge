package main

import "testing"

const (
	testDTLSAddr     = "127.0.0.1:5683"
	testCertFile     = "cert.pem"
	testKeyFile      = "key.pem"
	errMsgKeyMissing = "expected error when key missing"
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
	pskMissingID.PSK = "secret"
	if err := validateSourceSecurity(&pskMissingID); err == nil {
		t.Fatalf("expected error when pskIdentity missing")
	}

	// Valid PSK
	pskOK := base
	pskOK.PSK = "secret"
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

func TestDTLSValidationTarget(t *testing.T) {
	base := TargetConfig{Protocol: CoAPProtocolDTLS, Address: testDTLSAddr, Path: "/x", Method: "GET"}

	if err := validateTargetSecurity(&base); err == nil {
		t.Fatalf("expected error when neither PSK nor cert provided")
	}

	both := base
	both.PSK = "x"
	both.PSKIdentity = "id"
	both.TLSCertFile = testCertFile
	if err := validateTargetSecurity(&both); err == nil {
		t.Fatalf("expected error when both PSK and cert fields set")
	}

	pskMissingID := base
	pskMissingID.PSK = "secret"
	if err := validateTargetSecurity(&pskMissingID); err == nil {
		t.Fatalf("expected error when identity missing")
	}

	pskOK := base
	pskOK.PSK = "secret"
	pskOK.PSKIdentity = "id"
	if err := validateTargetSecurity(&pskOK); err != nil {
		t.Fatalf("unexpected error for valid PSK mode: %v", err)
	}

	certMissingKey := base
	certMissingKey.TLSCertFile = testCertFile
	if err := validateTargetSecurity(&certMissingKey); err == nil {
		t.Fatal(errMsgKeyMissing)
	}

	certOK := base
	certOK.TLSCertFile = testCertFile
	certOK.TLSKeyFile = testKeyFile
	if err := validateTargetSecurity(&certOK); err != nil {
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
	pskMissingID.PSK = "secret"
	if err := validateRunnerSecurity(&pskMissingID); err == nil {
		t.Fatalf("expected error when identity missing")
	}

	pskOK := base
	pskOK.PSK = "secret"
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
