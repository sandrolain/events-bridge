package tlsconfig

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

// createTestCertFiles generates valid self-signed certificates for testing.
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

func TestConfigDisabled(t *testing.T) {
	cfg := &Config{Enabled: false}

	serverConfig, err := cfg.BuildServerConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}
	if serverConfig != nil {
		t.Fatal("expected nil config when TLS is disabled")
	}

	clientConfig, err := cfg.BuildClientConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}
	if clientConfig != nil {
		t.Fatal("expected nil config when TLS is disabled")
	}
}

func TestBuildServerConfig(t *testing.T) {
	certFile, keyFile, cleanup := createTestCertFiles(t)
	defer cleanup()

	cfg := &Config{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		MinVersion: "1.2",
	}

	tlsConfig, err := cfg.BuildServerConfig()
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
		t.Errorf("expected MinVersion TLS 1.2, got %d", tlsConfig.MinVersion)
	}
}

func TestBuildClientConfig(t *testing.T) {
	cfg := &Config{
		Enabled:    true,
		MinVersion: "1.2",
	}

	tlsConfig, err := cfg.BuildClientConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if tlsConfig == nil {
		t.Fatal("expected non-nil TLS config")
	}

	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("expected MinVersion TLS 1.2, got %d", tlsConfig.MinVersion)
	}
}

func TestBuildClientConfigWithCA(t *testing.T) {
	certFile, _, cleanup := createTestCertFiles(t)
	defer cleanup()

	cfg := &Config{
		Enabled:    true,
		CACertFile: certFile,
		MinVersion: "1.2",
	}

	tlsConfig, err := cfg.BuildClientConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if tlsConfig == nil {
		t.Fatal("expected non-nil TLS config")
	}

	if tlsConfig.RootCAs == nil {
		t.Fatal("expected CA pool to be configured")
	}
}

func TestBuildClientConfigWithClientCert(t *testing.T) {
	certFile, keyFile, cleanup := createTestCertFiles(t)
	defer cleanup()

	cfg := &Config{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		MinVersion: "1.2",
	}

	tlsConfig, err := cfg.BuildClientConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if tlsConfig == nil {
		t.Fatal("expected non-nil TLS config")
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Fatalf("expected 1 client certificate, got %d", len(tlsConfig.Certificates))
	}
}

func TestMinTLSVersions(t *testing.T) {
	tests := []struct {
		version  string
		expected uint16
	}{
		{"1.0", tls.VersionTLS10},
		{"1.1", tls.VersionTLS11},
		{"1.2", tls.VersionTLS12},
		{"1.3", tls.VersionTLS13},
		{"invalid", tls.VersionTLS12}, // default
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			cfg := &Config{MinVersion: tt.version}
			got := cfg.getMinTLSVersion()
			if got != tt.expected {
				t.Errorf("getMinTLSVersion(%s) = %d, want %d", tt.version, got, tt.expected)
			}
		})
	}
}

func TestInsecureSkipVerify(t *testing.T) {
	cfg := &Config{
		Enabled:            true,
		InsecureSkipVerify: true,
		MinVersion:         "1.2",
	}

	tlsConfig, err := cfg.BuildClientConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if !tlsConfig.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be true")
	}
}

func TestServerName(t *testing.T) {
	cfg := &Config{
		Enabled:    true,
		ServerName: "example.com",
		MinVersion: "1.2",
	}

	tlsConfig, err := cfg.BuildClientConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if tlsConfig.ServerName != "example.com" {
		t.Errorf("expected ServerName 'example.com', got '%s'", tlsConfig.ServerName)
	}
}

func TestBuildServerConfigWithClientAuth(t *testing.T) {
	certFile, keyFile, cleanup := createTestCertFiles(t)
	defer cleanup()

	cfg := &Config{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		CACertFile: certFile,
		ClientAuth: "RequireAndVerifyClientCert",
		MinVersion: "1.2",
	}

	tlsConfig, err := cfg.BuildServerConfig()
	if err != nil {
		t.Fatalf(errUnexpected, err)
	}

	if tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("expected ClientAuth RequireAndVerifyClientCert, got %d", tlsConfig.ClientAuth)
	}

	if tlsConfig.ClientCAs == nil {
		t.Fatal("expected ClientCAs to be configured")
	}
}

func TestClientAuthTypes(t *testing.T) {
	tests := []struct {
		authType string
		expected tls.ClientAuthType
	}{
		{"NoClientCert", tls.NoClientCert},
		{"RequestClientCert", tls.RequestClientCert},
		{"RequireAnyClientCert", tls.RequireAnyClientCert},
		{"VerifyClientCertIfGiven", tls.VerifyClientCertIfGiven},
		{"RequireAndVerifyClientCert", tls.RequireAndVerifyClientCert},
		{"invalid", tls.NoClientCert}, // default
	}

	for _, tt := range tests {
		t.Run(tt.authType, func(t *testing.T) {
			cfg := &Config{ClientAuth: tt.authType}
			got := cfg.getClientAuthType()
			if got != tt.expected {
				t.Errorf("getClientAuthType(%s) = %d, want %d", tt.authType, got, tt.expected)
			}
		})
	}
}

func TestBuildServerConfigNoCertFile(t *testing.T) {
	cfg := &Config{
		Enabled: true,
	}

	_, err := cfg.BuildServerConfig()
	if err == nil {
		t.Fatal("expected error when cert/key files are missing for server config")
	}
}

func TestBuildClientConfigMismatchedCertKey(t *testing.T) {
	certFile, _, cleanup := createTestCertFiles(t)
	defer cleanup()

	// Only certFile without keyFile
	cfg := &Config{
		Enabled:  true,
		CertFile: certFile,
	}

	_, err := cfg.BuildClientConfig()
	if err == nil {
		t.Fatal("expected error when only cert file is provided")
	}
}

// TestBuildClientConfigIfEnabled tests the BuildClientConfigIfEnabled helper function.
func TestBuildClientConfigIfEnabled(t *testing.T) {
	t.Run("nil config returns nil", func(t *testing.T) {
		config, err := BuildClientConfigIfEnabled(nil)
		if err != nil {
			t.Fatalf(errUnexpected, err)
		}
		if config != nil {
			t.Fatal("expected nil config for nil input")
		}
	})

	t.Run("disabled config returns nil", func(t *testing.T) {
		cfg := &Config{Enabled: false}
		config, err := BuildClientConfigIfEnabled(cfg)
		if err != nil {
			t.Fatalf(errUnexpected, err)
		}
		if config != nil {
			t.Fatal("expected nil config for disabled TLS")
		}
	})

	t.Run("enabled config builds successfully", func(t *testing.T) {
		cfg := &Config{
			Enabled:            true,
			InsecureSkipVerify: true, // For test simplicity
		}
		config, err := BuildClientConfigIfEnabled(cfg)
		if err != nil {
			t.Fatalf(errUnexpected, err)
		}
		if config == nil {
			t.Fatal("expected non-nil config for enabled TLS")
		}
		if !config.InsecureSkipVerify {
			t.Fatal("expected InsecureSkipVerify to be true")
		}
	})

	t.Run("enabled config with invalid cert fails", func(t *testing.T) {
		cfg := &Config{
			Enabled:  true,
			CertFile: "/nonexistent/cert.pem",
			KeyFile:  "/nonexistent/key.pem",
		}
		_, err := BuildClientConfigIfEnabled(cfg)
		if err == nil {
			t.Fatal("expected error for invalid cert files")
		}
	})
}

// TestBuildServerConfigIfEnabled tests the BuildServerConfigIfEnabled helper function.
func TestBuildServerConfigIfEnabled(t *testing.T) {
	certPEM, keyPEM := generateTestCertificate(t)
	dir := t.TempDir()

	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("failed to write key file: %v", err)
	}

	t.Run("nil config returns nil", func(t *testing.T) {
		config, err := BuildServerConfigIfEnabled(nil)
		if err != nil {
			t.Fatalf(errUnexpected, err)
		}
		if config != nil {
			t.Fatal("expected nil config for nil input")
		}
	})

	t.Run("disabled config returns nil", func(t *testing.T) {
		cfg := &Config{Enabled: false}
		config, err := BuildServerConfigIfEnabled(cfg)
		if err != nil {
			t.Fatalf(errUnexpected, err)
		}
		if config != nil {
			t.Fatal("expected nil config for disabled TLS")
		}
	})

	t.Run("enabled config builds successfully", func(t *testing.T) {
		cfg := &Config{
			Enabled:  true,
			CertFile: certFile,
			KeyFile:  keyFile,
		}
		config, err := BuildServerConfigIfEnabled(cfg)
		if err != nil {
			t.Fatalf(errUnexpected, err)
		}
		if config == nil {
			t.Fatal("expected non-nil config for enabled TLS")
		}
		if len(config.Certificates) != 1 {
			t.Fatal("expected one certificate")
		}
	})

	t.Run("enabled config without cert fails", func(t *testing.T) {
		cfg := &Config{
			Enabled: true,
		}
		_, err := BuildServerConfigIfEnabled(cfg)
		if err == nil {
			t.Fatal("expected error for server config without cert")
		}
	})
}

// TestIsEnabled tests the IsEnabled helper function.
func TestIsEnabled(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *Config
		expected bool
	}{
		{
			name:     "nil config",
			cfg:      nil,
			expected: false,
		},
		{
			name:     "disabled config",
			cfg:      &Config{Enabled: false},
			expected: false,
		},
		{
			name:     "enabled config",
			cfg:      &Config{Enabled: true},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsEnabled(tt.cfg)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
