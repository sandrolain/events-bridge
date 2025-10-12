package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Config holds TLS configuration for secure connections.
// It provides secure defaults and supports both server and client TLS configurations.
type Config struct {
	// Enabled determines if TLS should be used
	Enabled bool `mapstructure:"enabled" default:"false"`

	// CertFile is the path to the TLS certificate file (PEM format)
	// For servers: server certificate
	// For clients: client certificate for mutual TLS (optional)
	CertFile string `mapstructure:"certFile"`

	// KeyFile is the path to the TLS private key file (PEM format)
	// For servers: server private key
	// For clients: client private key for mutual TLS (optional)
	KeyFile string `mapstructure:"keyFile"`

	// CACertFile is the path to the CA certificate file
	// For servers: CA cert for client verification (optional)
	// For clients: CA cert for server verification (optional, system CAs used if empty)
	CACertFile string `mapstructure:"caCertFile"`

	// ClientAuth determines the client authentication mode (server-side only)
	// Valid values: NoClientCert, RequestClientCert, RequireAnyClientCert, VerifyClientCertIfGiven, RequireAndVerifyClientCert
	ClientAuth string `mapstructure:"clientAuth" default:"NoClientCert" validate:"omitempty,oneof=NoClientCert RequestClientCert RequireAnyClientCert VerifyClientCertIfGiven RequireAndVerifyClientCert"`

	// MinVersion specifies the minimum TLS version
	// Supported values: "1.0", "1.1", "1.2", "1.3"
	// Default: "1.2" (TLS 1.2)
	// Recommended: "1.2" or "1.3"
	MinVersion string `mapstructure:"minVersion" default:"1.2" validate:"omitempty,oneof=1.0 1.1 1.2 1.3"`

	// InsecureSkipVerify controls whether to verify the peer's certificate chain (client-side)
	// WARNING: Should only be true for testing purposes
	// Default: false
	InsecureSkipVerify bool `mapstructure:"insecureSkipVerify" default:"false"`

	// ServerName is used to verify the hostname on the returned certificates (client-side)
	// If empty, the hostname from the server address will be used.
	ServerName string `mapstructure:"serverName"`
}

// BuildServerConfig creates a tls.Config for server use.
// It loads the certificate and key files, optionally configures client authentication,
// and sets secure defaults for TLS version and cipher suites.
func (c *Config) BuildServerConfig() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}

	if c.CertFile == "" || c.KeyFile == "" {
		return nil, fmt.Errorf("certFile and keyFile are required for server TLS")
	}

	// Load server certificate and key
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate and key: %w", err)
	}

	// #nosec G402 - MinVersion is configurable by user, not hardcoded to a low value
	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   c.getMinTLSVersion(),
		CipherSuites: getSecureCipherSuites(),
	}

	// Configure client authentication if CA cert is provided
	if c.CACertFile != "" {
		caCert, err := os.ReadFile(c.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		config.ClientCAs = caCertPool
		config.ClientAuth = c.getClientAuthType()
	}

	return config, nil
}

// BuildClientConfig creates a tls.Config for client use.
// It optionally loads CA certificates for server verification and client certificates for mutual TLS.
func (c *Config) BuildClientConfig() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}

	// #nosec G402 - MinVersion is configurable by user, not hardcoded to a low value
	config := &tls.Config{
		MinVersion:         c.getMinTLSVersion(),
		CipherSuites:       getSecureCipherSuites(),
		InsecureSkipVerify: c.InsecureSkipVerify, // #nosec G402 - InsecureSkipVerify is configurable with default=false, used only for testing
		ServerName:         c.ServerName,
	}

	// Load client certificate if provided (for mutual TLS)
	if c.CertFile != "" && c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate and key: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
	} else if c.CertFile != "" || c.KeyFile != "" {
		return nil, fmt.Errorf("both certFile and keyFile must be provided for client authentication")
	}

	// Load CA certificate for server verification if provided
	if c.CACertFile != "" {
		caCert, err := os.ReadFile(c.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		config.RootCAs = caCertPool
	}

	return config, nil
}

// getMinTLSVersion converts the string version to tls constant.
// Defaults to TLS 1.2 for secure connections.
func (c *Config) getMinTLSVersion() uint16 {
	switch c.MinVersion {
	case "1.0":
		return tls.VersionTLS10
	case "1.1":
		return tls.VersionTLS11
	case "1.2":
		return tls.VersionTLS12
	case "1.3":
		return tls.VersionTLS13
	default:
		return tls.VersionTLS12 // Secure default
	}
}

// getClientAuthType converts the string client auth type to tls constant.
func (c *Config) getClientAuthType() tls.ClientAuthType {
	switch c.ClientAuth {
	case "RequestClientCert":
		return tls.RequestClientCert
	case "RequireAnyClientCert":
		return tls.RequireAnyClientCert
	case "VerifyClientCertIfGiven":
		return tls.VerifyClientCertIfGiven
	case "RequireAndVerifyClientCert":
		return tls.RequireAndVerifyClientCert
	default:
		return tls.NoClientCert
	}
}

// getSecureCipherSuites returns a list of secure cipher suites.
// These are recommended cipher suites that provide forward secrecy and strong encryption.
func getSecureCipherSuites() []uint16 {
	return []uint16{
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	}
}
