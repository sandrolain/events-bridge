package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig holds TLS configuration for MQTT connections.
// It provides secure defaults and supports both server verification and client certificate authentication.
type TLSConfig struct {
	// Enabled enables TLS/SSL for MQTT connections.
	// When true, the broker URL must use ssl:// or tls:// scheme.
	Enabled bool `mapstructure:"enabled" default:"false"`

	// CACertFile is the path to the CA certificate file for server verification.
	// If empty and Enabled is true, system CA certificates will be used.
	CACertFile string `mapstructure:"caCertFile"`

	// ClientCertFile is the path to the client certificate file for mutual TLS authentication.
	// Must be provided together with ClientKeyFile if client authentication is required.
	ClientCertFile string `mapstructure:"clientCertFile"`

	// ClientKeyFile is the path to the client private key file for mutual TLS authentication.
	// Must be provided together with ClientCertFile if client authentication is required.
	ClientKeyFile string `mapstructure:"clientKeyFile"`

	// InsecureSkipVerify skips server certificate verification.
	// WARNING: This is insecure and should only be used for testing.
	// Default: false (verify server certificates)
	InsecureSkipVerify bool `mapstructure:"insecureSkipVerify" default:"false"`

	// MinVersion sets the minimum TLS version to accept.
	// Supported values: "1.0", "1.1", "1.2", "1.3"
	// Default: "1.2" (TLS 1.2)
	// Recommended: "1.2" or "1.3"
	MinVersion string `mapstructure:"minVersion" default:"1.2"`

	// ServerName is used to verify the hostname on the returned certificates.
	// If empty, the hostname from the broker address will be used.
	ServerName string `mapstructure:"serverName"`
}

// BuildTLSConfig creates a tls.Config for MQTT client use.
// It loads CA certificates, client certificates, and sets secure defaults.
func (c *TLSConfig) BuildTLSConfig() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}

	// #nosec G402 - MinVersion is configurable by user, not hardcoded to a low value
	config := &tls.Config{
		MinVersion:         c.getMinTLSVersion(),
		InsecureSkipVerify: c.InsecureSkipVerify, // #nosec G402 - InsecureSkipVerify is configurable with default=false, used only for testing
		ServerName:         c.ServerName,
	}

	// Load CA certificate if provided
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

	// Load client certificate and key if provided
	if c.ClientCertFile != "" && c.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.ClientCertFile, c.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate and key: %w", err)
		}

		config.Certificates = []tls.Certificate{cert}
	} else if c.ClientCertFile != "" || c.ClientKeyFile != "" {
		return nil, fmt.Errorf("both clientCertFile and clientKeyFile must be provided for client authentication")
	}

	return config, nil
}

// getMinTLSVersion converts the MinVersion string to a tls constant.
// Defaults to TLS 1.2 for secure connections.
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
