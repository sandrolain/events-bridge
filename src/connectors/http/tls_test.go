package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	errUnexpected = "unexpected error: %v"
)

// generateTestCertificate creates a self-signed certificate for testing using Go's crypto library.
func generateTestCertificate(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	// Generate ECDSA private key (more efficient than RSA for tests)
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	// Create certificate template
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("failed to generate serial number: %v", err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(24 * time.Hour) // Valid for 24 hours

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Events Bridge Test"},
			CommonName:   "localhost",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:              []string{"localhost"},
	}

	// Create self-signed certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	// Encode certificate to PEM
	certPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	// Encode private key to PEM
	privateKeyBytes, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		t.Fatalf("failed to marshal private key: %v", err)
	}

	keyPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: privateKeyBytes,
	})

	return certPEM, keyPEM
}

// createTestCertFiles generates valid self-signed certificates for testing using Go's crypto library.
func createTestCertFiles(t *testing.T) (certFile, keyFile string, cleanup func()) {
	t.Helper()

	certPEM, keyPEM := generateTestCertificate(t)

	tmpDir := t.TempDir()

	certFile = filepath.Join(tmpDir, "cert.pem")
	keyFile = filepath.Join(tmpDir, "key.pem")

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("failed to write cert file: %v", err)
	}

	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("failed to write key file: %v", err)
	}

	cleanup = func() {
		os.RemoveAll(tmpDir)
	}

	return certFile, keyFile, cleanup
}

func TestTLSConfigDisabled(t *testing.T) {
	cfg := &TLSConfig{Enabled: false}

	serverConfig, err := cfg.BuildServerTLSConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}
	if serverConfig != nil {
		t.Fatal("expected nil config when TLS is disabled")
	}

	clientConfig, err := cfg.BuildClientTLSConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}
	if clientConfig != nil {
		t.Fatal("expected nil config when TLS is disabled")
	}
}

func TestTLSConfigBuildServerTLSConfig(t *testing.T) {
	certFile, keyFile, cleanup := createTestCertFiles(t)
	defer cleanup()

	cfg := &TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		MinVersion: "1.2",
	}

	tlsConfig, err := cfg.BuildServerTLSConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if tlsConfig == nil {
		t.Fatal("expected non-nil TLS config")
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(tlsConfig.Certificates))
	}

	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected TLS 1.2, got %d", tlsConfig.MinVersion)
	}

	if len(tlsConfig.CipherSuites) == 0 {
		t.Fatal("expected non-empty cipher suites")
	}
}

func TestTLSConfigBuildServerTLSConfigWithClientAuth(t *testing.T) {
	certFile, keyFile, cleanup := createTestCertFiles(t)
	defer cleanup()

	caCertFile := certFile // Using same cert as CA for testing

	cfg := &TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		CACertFile: caCertFile,
		ClientAuth: "RequireAndVerifyClientCert",
		MinVersion: "1.2",
	}

	tlsConfig, err := cfg.BuildServerTLSConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected RequireAndVerifyClientCert, got %d", tlsConfig.ClientAuth)
	}

	if tlsConfig.ClientCAs == nil {
		t.Fatal("expected non-nil ClientCAs")
	}
}

func TestTLSConfigBuildClientTLSConfig(t *testing.T) {
	certFile, keyFile, cleanup := createTestCertFiles(t)
	defer cleanup()

	cfg := &TLSConfig{
		Enabled:    true,
		MinVersion: "1.3",
	}

	tlsConfig, err := cfg.BuildClientTLSConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if tlsConfig == nil {
		t.Fatal("expected non-nil TLS config")
	}

	if tlsConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("expected TLS 1.3, got %d", tlsConfig.MinVersion)
	}

	if tlsConfig.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify to be false")
	}

	// Test with client certificate
	cfg.CertFile = certFile
	cfg.KeyFile = keyFile

	tlsConfig, err = cfg.BuildClientTLSConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Fatalf("expected 1 client certificate, got %d", len(tlsConfig.Certificates))
	}
}

func TestTLSConfigBuildClientTLSConfigWithCA(t *testing.T) {
	certFile, _, cleanup := createTestCertFiles(t)
	defer cleanup()

	cfg := &TLSConfig{
		Enabled:    true,
		CACertFile: certFile, // Using cert as CA for testing
		MinVersion: "1.2",
	}

	tlsConfig, err := cfg.BuildClientTLSConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if tlsConfig.RootCAs == nil {
		t.Fatal("expected non-nil RootCAs")
	}
}

func TestTLSConfigBuildServerTLSConfigInvalidCert(t *testing.T) {
	cfg := &TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	}

	_, err := cfg.BuildServerTLSConfig()
	if err == nil {
		t.Fatal("expected error for invalid certificate files")
	}
}

func TestTLSConfigBuildServerTLSConfigInvalidCA(t *testing.T) {
	certFile, keyFile, cleanup := createTestCertFiles(t)
	defer cleanup()

	cfg := &TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		CACertFile: "/nonexistent/ca.pem",
	}

	_, err := cfg.BuildServerTLSConfig()
	if err == nil {
		t.Fatal("expected error for invalid CA certificate file")
	}
}

func TestTLSConfigInsecureSkipVerify(t *testing.T) {
	cfg := &TLSConfig{
		Enabled:            true,
		InsecureSkipVerify: true,
	}

	tlsConfig, err := cfg.BuildClientTLSConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if !tlsConfig.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify to be true")
	}
}

func TestTLSConfigMinVersionDefault(t *testing.T) {
	cfg := &TLSConfig{MinVersion: ""}

	version := cfg.getMinTLSVersion()
	if version != tls.VersionTLS12 {
		t.Fatalf("expected default TLS 1.2, got %d", version)
	}
}

func TestTLSConfigMinVersionAll(t *testing.T) {
	tests := []struct {
		version  string
		expected uint16
	}{
		{"1.0", tls.VersionTLS10},
		{"1.1", tls.VersionTLS11},
		{"1.2", tls.VersionTLS12},
		{"1.3", tls.VersionTLS13},
		{"invalid", tls.VersionTLS12}, // Default
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			cfg := &TLSConfig{MinVersion: tt.version}
			version := cfg.getMinTLSVersion()
			if version != tt.expected {
				t.Fatalf("expected %d, got %d", tt.expected, version)
			}
		})
	}
}

func TestTLSConfigClientAuthTypes(t *testing.T) {
	tests := []struct {
		authType string
		expected tls.ClientAuthType
	}{
		{"NoClientCert", tls.NoClientCert},
		{"RequestClientCert", tls.RequestClientCert},
		{"RequireAnyClientCert", tls.RequireAnyClientCert},
		{"VerifyClientCertIfGiven", tls.VerifyClientCertIfGiven},
		{"RequireAndVerifyClientCert", tls.RequireAndVerifyClientCert},
		{"invalid", tls.NoClientCert}, // Default
	}

	for _, tt := range tests {
		t.Run(tt.authType, func(t *testing.T) {
			cfg := &TLSConfig{ClientAuth: tt.authType}
			authType := cfg.getClientAuthType()
			if authType != tt.expected {
				t.Fatalf("expected %d, got %d", tt.expected, authType)
			}
		})
	}
}

func TestGetSecureCipherSuites(t *testing.T) {
	suites := getSecureCipherSuites()
	if len(suites) == 0 {
		t.Fatal("expected non-empty cipher suites")
	}

	// Verify all returned suites are secure
	for _, suite := range suites {
		// All our cipher suites should use either GCM or ChaCha20-Poly1305
		switch suite {
		case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305:
			// Valid secure cipher suite
		default:
			t.Fatalf("unexpected cipher suite: %d", suite)
		}
	}
}
