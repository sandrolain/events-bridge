package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig represents TLS configuration options for HTTP connections.
// It provides secure defaults and supports both server and client TLS configurations.
type TLSConfig struct {
	// Enabled determines if TLS should be used
	Enabled bool `mapstructure:"tlsEnabled" default:"false"`

	// CertFile is the path to the TLS certificate file (PEM format)
	CertFile string `mapstructure:"tlsCertFile" validate:"required_if=Enabled true"`

	// KeyFile is the path to the TLS private key file (PEM format)
	KeyFile string `mapstructure:"tlsKeyFile" validate:"required_if=Enabled true"`

	// CACertFile is the path to the CA certificate file for client verification (optional)
	CACertFile string `mapstructure:"tlsCACertFile"`

	// ClientAuth determines the client authentication mode (server-side only)
	// Valid values: NoClientCert, RequestClientCert, RequireAnyClientCert, VerifyClientCertIfGiven, RequireAndVerifyClientCert
	ClientAuth string `mapstructure:"tlsClientAuth" default:"NoClientCert" validate:"omitempty,oneof=NoClientCert RequestClientCert RequireAnyClientCert VerifyClientCertIfGiven RequireAndVerifyClientCert"`

	// MinVersion specifies the minimum TLS version (default: 1.2)
	MinVersion string `mapstructure:"tlsMinVersion" default:"1.2" validate:"omitempty,oneof=1.0 1.1 1.2 1.3"`

	// InsecureSkipVerify controls whether to verify the server's certificate chain (client-side only)
	// WARNING: Should only be true for testing purposes
	InsecureSkipVerify bool `mapstructure:"tlsInsecureSkipVerify" default:"false"`
}

// BuildServerTLSConfig creates a tls.Config for server use.
// It loads the certificate and key files, optionally configures client authentication,
// and sets secure defaults for TLS version and cipher suites.
func (c *TLSConfig) BuildServerTLSConfig() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
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

// BuildClientTLSConfig creates a tls.Config for client use.
// It optionally loads a CA certificate for server verification and sets secure defaults.
func (c *TLSConfig) BuildClientTLSConfig() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}

	// #nosec G402 - InsecureSkipVerify is configurable with default=false, used only for testing
	config := &tls.Config{
		MinVersion:         c.getMinTLSVersion(),
		CipherSuites:       getSecureCipherSuites(),
		InsecureSkipVerify: c.InsecureSkipVerify,
	}

	// Load client certificate if provided
	if c.CertFile != "" && c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate and key: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
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
func (c *TLSConfig) getMinTLSVersion() uint16 {
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
func (c *TLSConfig) getClientAuthType() tls.ClientAuthType {
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
