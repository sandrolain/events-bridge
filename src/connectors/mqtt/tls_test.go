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

// generateTestCertificate creates a self-signed certificate for testing.
// Returns PEM-encoded certificate and private key.
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

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Events Bridge Test"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
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

	// PEM encode certificate
	certPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	// PEM encode private key
	keyBytes, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		t.Fatalf("failed to marshal private key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyBytes,
	})

	return certPEM, keyPEM
}

// createTestCertFiles creates temporary certificate and key files for testing.
// Returns paths to cert and key files, and a cleanup function.
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
		// t.TempDir() handles cleanup automatically
	}

	return certFile, keyFile, cleanup
}

func TestTLSConfigDisabled(t *testing.T) {
	cfg := &TLSConfig{
		Enabled: false,
	}

	tlsConfig, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig() failed: %v", err)
	}

	if tlsConfig != nil {
		t.Error("expected nil TLS config when disabled")
	}
}

func TestTLSConfigBasic(t *testing.T) {
	cfg := &TLSConfig{
		Enabled: true,
	}

	tlsConfig, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig() failed: %v", err)
	}

	if tlsConfig == nil {
		t.Fatal("expected non-nil TLS config")
	}

	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("expected MinVersion TLS 1.2, got %d", tlsConfig.MinVersion)
	}

	if tlsConfig.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be false by default")
	}
}

func TestTLSConfigWithCA(t *testing.T) {
	certFile, _, cleanup := createTestCertFiles(t)
	defer cleanup()

	cfg := &TLSConfig{
		Enabled:    true,
		CACertFile: certFile,
	}

	tlsConfig, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig() failed: %v", err)
	}

	if tlsConfig.RootCAs == nil {
		t.Error("expected RootCAs to be set")
	}
}

func TestTLSConfigWithClientCert(t *testing.T) {
	certFile, keyFile, cleanup := createTestCertFiles(t)
	defer cleanup()

	cfg := &TLSConfig{
		Enabled:        true,
		ClientCertFile: certFile,
		ClientKeyFile:  keyFile,
	}

	tlsConfig, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig() failed: %v", err)
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(tlsConfig.Certificates))
	}
}

func TestTLSConfigInvalidCAFile(t *testing.T) {
	cfg := &TLSConfig{
		Enabled:    true,
		CACertFile: "/nonexistent/ca.pem",
	}

	_, err := cfg.BuildTLSConfig()
	if err == nil {
		t.Error("expected error for invalid CA file")
	}
}

func TestTLSConfigInvalidClientCert(t *testing.T) {
	cfg := &TLSConfig{
		Enabled:        true,
		ClientCertFile: "/nonexistent/cert.pem",
		ClientKeyFile:  "/nonexistent/key.pem",
	}

	_, err := cfg.BuildTLSConfig()
	if err == nil {
		t.Error("expected error for invalid client cert files")
	}
}

func TestTLSConfigIncompleteClientCert(t *testing.T) {
	certFile, _, cleanup := createTestCertFiles(t)
	defer cleanup()

	tests := []struct {
		name     string
		certFile string
		keyFile  string
	}{
		{
			name:     "cert without key",
			certFile: certFile,
			keyFile:  "",
		},
		{
			name:     "key without cert",
			certFile: "",
			keyFile:  certFile,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &TLSConfig{
				Enabled:        true,
				ClientCertFile: tt.certFile,
				ClientKeyFile:  tt.keyFile,
			}

			_, err := cfg.BuildTLSConfig()
			if err == nil {
				t.Error("expected error for incomplete client cert configuration")
			}
		})
	}
}

func TestTLSConfigInsecureSkipVerify(t *testing.T) {
	cfg := &TLSConfig{
		Enabled:            true,
		InsecureSkipVerify: true,
	}

	tlsConfig, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig() failed: %v", err)
	}

	if !tlsConfig.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be true")
	}
}

func TestTLSConfigServerName(t *testing.T) {
	cfg := &TLSConfig{
		Enabled:    true,
		ServerName: "mqtt.example.com",
	}

	tlsConfig, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig() failed: %v", err)
	}

	if tlsConfig.ServerName != "mqtt.example.com" {
		t.Errorf("expected ServerName 'mqtt.example.com', got '%s'", tlsConfig.ServerName)
	}
}

func TestTLSConfigMinVersionDefault(t *testing.T) {
	cfg := &TLSConfig{
		Enabled: true,
	}

	tlsConfig, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig() failed: %v", err)
	}

	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("expected default MinVersion TLS 1.2, got %d", tlsConfig.MinVersion)
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
		{"invalid", tls.VersionTLS12}, // Should default to 1.2
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			cfg := &TLSConfig{
				Enabled:    true,
				MinVersion: tt.version,
			}

			tlsConfig, err := cfg.BuildTLSConfig()
			if err != nil {
				t.Fatalf("BuildTLSConfig() failed: %v", err)
			}

			if tlsConfig.MinVersion != tt.expected {
				t.Errorf("expected MinVersion %d for version '%s', got %d", tt.expected, tt.version, tlsConfig.MinVersion)
			}
		})
	}
}

func TestTLSConfigFullConfiguration(t *testing.T) {
	certFile, keyFile, cleanup := createTestCertFiles(t)
	defer cleanup()

	cfg := &TLSConfig{
		Enabled:        true,
		CACertFile:     certFile,
		ClientCertFile: certFile,
		ClientKeyFile:  keyFile,
		MinVersion:     "1.3",
		ServerName:     "mqtt.secure.example.com",
	}

	tlsConfig, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig() failed: %v", err)
	}

	if tlsConfig.RootCAs == nil {
		t.Error("expected RootCAs to be set")
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(tlsConfig.Certificates))
	}

	if tlsConfig.MinVersion != tls.VersionTLS13 {
		t.Errorf("expected MinVersion TLS 1.3, got %d", tlsConfig.MinVersion)
	}

	if tlsConfig.ServerName != "mqtt.secure.example.com" {
		t.Errorf("expected ServerName 'mqtt.secure.example.com', got '%s'", tlsConfig.ServerName)
	}
}
