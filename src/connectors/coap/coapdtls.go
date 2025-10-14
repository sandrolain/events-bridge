package main

// DTLS helpers - provide DTLS server/client creation using go-coap/dtls and pion/dtls.
// Keep DTLS-specific dependencies isolated inside the coap connector package.

import (
	"crypto/tls"
	"fmt"
	"os"
	"strings"

	piondtls "github.com/pion/dtls/v3"
	"github.com/plgd-dev/go-coap/v3"
	coapdtls "github.com/plgd-dev/go-coap/v3/dtls"
	coapmux "github.com/plgd-dev/go-coap/v3/mux"
	coapudpclient "github.com/plgd-dev/go-coap/v3/udp/client"
)

// buildDTLSServer constructs a DTLS server (PSK or cert mode) and starts listening.
func buildDTLSServer(cfg *SourceConfig, router *coapmux.Router) error {
	// Resolve PSK if using PSK mode
	psk := cfg.PSK
	if psk != "" {
		resolved, err := resolveSecret(psk)
		if err != nil {
			return fmt.Errorf("failed to resolve PSK: %w", err)
		}
		psk = resolved
	}

	dtlsConfig, err := buildDTLSConfig(cfg.PSKIdentity, psk, cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return fmt.Errorf("failed to build DTLS config: %w", err)
	}

	go func() {
		// ListenAndServeDTLS blocks; errors are logged by caller if needed
		_ = coap.ListenAndServeDTLS("udp", cfg.Address, dtlsConfig, router)
	}()
	return nil
}

// buildDTLSClientPSK creates a DTLS client using PSK authentication.
func buildDTLSClientPSK(identity, psk, address string) (*coapudpclient.Conn, error) {
	// Resolve PSK secret
	resolvedPSK, err := resolveSecret(psk)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve PSK: %w", err)
	}

	dtlsConfig := &piondtls.Config{
		PSK: func(hint []byte) ([]byte, error) {
			return []byte(resolvedPSK), nil
		},
		PSKIdentityHint: []byte(identity),
		CipherSuites:    []piondtls.CipherSuiteID{piondtls.TLS_PSK_WITH_AES_128_GCM_SHA256},
	}

	conn, err := coapdtls.Dial(address, dtlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to dial DTLS PSK: %w", err)
	}
	return conn, nil
}

// buildDTLSClientCert creates a DTLS client using certificate authentication.
func buildDTLSClientCert(certFile, keyFile, address string) (*coapudpclient.Conn, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load certificate: %w", err)
	}

	dtlsConfig := &piondtls.Config{
		Certificates: []tls.Certificate{cert},
	}

	conn, err := coapdtls.Dial(address, dtlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to dial DTLS cert: %w", err)
	}
	return conn, nil
}

// buildDTLSConfig creates a pion/dtls Config for server use (PSK or cert mode).
func buildDTLSConfig(pskIdentity, psk, certFile, keyFile string) (*piondtls.Config, error) {
	hasPSK := psk != "" && pskIdentity != ""
	hasCert := certFile != "" && keyFile != ""

	if hasPSK {
		return &piondtls.Config{
			PSK: func(hint []byte) ([]byte, error) {
				return []byte(psk), nil
			},
			PSKIdentityHint: []byte(pskIdentity),
			CipherSuites:    []piondtls.CipherSuiteID{piondtls.TLS_PSK_WITH_AES_128_GCM_SHA256},
		}, nil
	}

	if hasCert {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load server certificate: %w", err)
		}
		return &piondtls.Config{
			Certificates: []tls.Certificate{cert},
		}, nil
	}

	return nil, fmt.Errorf("neither PSK nor certificate provided for DTLS config")
}

// resolveSecret supports prefixes:
// - "env:NAME" to read from environment variable NAME
// - "file:/path" to read the contents of a file
// Any other value is returned as-is
func resolveSecret(value string) (string, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return "", nil
	}
	if strings.HasPrefix(v, "env:") {
		name := strings.TrimPrefix(v, "env:")
		return os.Getenv(name), nil
	}
	if strings.HasPrefix(v, "file:") {
		path := strings.TrimPrefix(v, "file:")
		// Basic hardening: require absolute path to avoid traversal of relative locations
		if !strings.HasPrefix(path, "/") {
			return "", fmt.Errorf("file secret path must be absolute")
		}
		b, err := os.ReadFile(path) // #nosec G304 - path is user-provided by configuration and required for file-based secrets; we enforce absolute path above.
		if err != nil {
			return "", fmt.Errorf("read file %s: %w", path, err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	return v, nil
}
