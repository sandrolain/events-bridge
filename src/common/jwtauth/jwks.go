package jwtauth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// JWKSClient manages JSON Web Key Sets with caching and automatic refresh.
type JWKSClient struct {
	url             string
	refreshInterval time.Duration
	slog            *slog.Logger
	keys            map[string]interface{} // kid -> public key
	mu              sync.RWMutex
	stopCh          chan struct{}
	httpClient      *http.Client
}

// JWKS represents a JSON Web Key Set.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// JWK represents a JSON Web Key.
type JWK struct {
	Kid string `json:"kid"` // Key ID
	Kty string `json:"kty"` // Key Type (RSA, EC, etc.)
	Alg string `json:"alg"` // Algorithm
	Use string `json:"use"` // Public Key Use (sig, enc)
	N   string `json:"n"`   // RSA modulus
	E   string `json:"e"`   // RSA exponent
	X   string `json:"x"`   // EC x coordinate
	Y   string `json:"y"`   // EC y coordinate
	Crv string `json:"crv"` // EC curve name
}

// NewJWKSClient creates a new JWKS client with automatic refresh.
func NewJWKSClient(url string, refreshInterval time.Duration, logger *slog.Logger) (*JWKSClient, error) {
	client := &JWKSClient{
		url:             url,
		refreshInterval: refreshInterval,
		slog:            logger.With("component", "JWKS Client"),
		keys:            make(map[string]interface{}),
		stopCh:          make(chan struct{}),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	// Initial fetch
	if err := client.refresh(); err != nil {
		return nil, fmt.Errorf("initial JWKS fetch failed: %w", err)
	}

	client.slog.Info("JWKS client initialized",
		"url", url,
		"refreshInterval", refreshInterval,
		"keysLoaded", len(client.keys))

	// Start background refresh
	go client.refreshLoop()

	return client, nil
}

// GetKey retrieves a public key by its key ID.
func (c *JWKSClient) GetKey(kid string) (interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key, exists := c.keys[kid]
	if !exists {
		return nil, fmt.Errorf("key with kid '%s' not found in JWKS", kid)
	}

	return key, nil
}

// Refresh manually triggers a JWKS refresh.
func (c *JWKSClient) Refresh() error {
	return c.refresh()
}

// refresh fetches and updates the JWKS from the configured URL.
func (c *JWKSClient) refresh() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c.slog.Debug("fetching JWKS", "url", c.url)

	req, err := http.NewRequestWithContext(ctx, "GET", c.url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers for better compatibility
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "events-bridge-jwtauth/1.0")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch JWKS: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.slog.Warn("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var jwks JWKS
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return fmt.Errorf("failed to decode JWKS: %w", err)
	}

	// Convert JWKs to public keys
	newKeys := make(map[string]interface{})
	for _, jwk := range jwks.Keys {
		// Skip keys not intended for signature verification
		if jwk.Use != "" && jwk.Use != "sig" {
			c.slog.Debug("skipping non-signature key", "kid", jwk.Kid, "use", jwk.Use)
			continue
		}

		pubKey, err := jwk.toPublicKey()
		if err != nil {
			c.slog.Warn("failed to convert JWK to public key",
				"kid", jwk.Kid,
				"kty", jwk.Kty,
				"error", err)
			continue
		}

		newKeys[jwk.Kid] = pubKey
	}

	if len(newKeys) == 0 {
		return fmt.Errorf("no valid keys found in JWKS")
	}

	// Update keys atomically
	c.mu.Lock()
	c.keys = newKeys
	c.mu.Unlock()

	c.slog.Info("JWKS refreshed successfully", "keysCount", len(newKeys))

	return nil
}

// refreshLoop periodically refreshes the JWKS in the background.
func (c *JWKSClient) refreshLoop() {
	ticker := time.NewTicker(c.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.refresh(); err != nil {
				c.slog.Error("JWKS refresh failed", "error", err)
			}
		case <-c.stopCh:
			c.slog.Debug("JWKS refresh loop stopped")
			return
		}
	}
}

// Close stops the background refresh loop.
func (c *JWKSClient) Close() {
	close(c.stopCh)
}

// toPublicKey converts a JWK to a public key.
func (j *JWK) toPublicKey() (interface{}, error) {
	switch j.Kty {
	case "RSA":
		return j.toRSAPublicKey()
	case "EC":
		return j.toECDSAPublicKey()
	default:
		return nil, fmt.Errorf("unsupported key type: %s", j.Kty)
	}
}

// toRSAPublicKey converts a JWK to an RSA public key.
func (j *JWK) toRSAPublicKey() (*rsa.PublicKey, error) {
	// Decode modulus (n)
	nBytes, err := base64.RawURLEncoding.DecodeString(j.N)
	if err != nil {
		return nil, fmt.Errorf("failed to decode modulus: %w", err)
	}

	// Decode exponent (e)
	eBytes, err := base64.RawURLEncoding.DecodeString(j.E)
	if err != nil {
		return nil, fmt.Errorf("failed to decode exponent: %w", err)
	}

	// Convert exponent bytes to int
	var eInt int
	for _, b := range eBytes {
		eInt = eInt<<8 + int(b)
	}

	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: eInt,
	}, nil
}

// toECDSAPublicKey converts a JWK to an ECDSA public key.
func (j *JWK) toECDSAPublicKey() (*ecdsa.PublicKey, error) {
	// Determine curve
	var curve elliptic.Curve
	switch j.Crv {
	case "P-256":
		curve = elliptic.P256()
	case "P-384":
		curve = elliptic.P384()
	case "P-521":
		curve = elliptic.P521()
	default:
		return nil, fmt.Errorf("unsupported curve: %s", j.Crv)
	}

	// Decode x coordinate
	xBytes, err := base64.RawURLEncoding.DecodeString(j.X)
	if err != nil {
		return nil, fmt.Errorf("failed to decode x coordinate: %w", err)
	}

	// Decode y coordinate
	yBytes, err := base64.RawURLEncoding.DecodeString(j.Y)
	if err != nil {
		return nil, fmt.Errorf("failed to decode y coordinate: %w", err)
	}

	return &ecdsa.PublicKey{
		Curve: curve,
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}, nil
}
