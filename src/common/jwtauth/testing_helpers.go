package jwtauth

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	jwtgo "github.com/golang-jwt/jwt/v5"
)

const (
	testIssuer       = "https://test.example.com"
	testAudience     = "test-audience"
	errUnexpectedErr = "unexpected error: %v"
	errExpectedErr   = "expected error but got none"
)

// MockJWKSServer provides a mock JWKS server for testing.
type MockJWKSServer struct {
	server     *httptest.Server
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
	kid        string
}

// SetupMockJWKSServer creates a new mock JWKS server for testing.
func SetupMockJWKSServer(t *testing.T) *MockJWKSServer {
	t.Helper()

	// Generate RSA key pair
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	kid := "test-key-1"

	// Create JWKS response
	pubKey := &privateKey.PublicKey
	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kid": kid,
				"kty": "RSA",
				"alg": "RS256",
				"use": "sig",
				"n":   encodeRSAModulus(pubKey.N),
				"e":   encodeRSAExponent(pubKey.E),
			},
		},
	}

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(jwks); err != nil {
			t.Logf("failed to encode JWKS: %v", err)
		}
	}))

	return &MockJWKSServer{
		server:     server,
		privateKey: privateKey,
		publicKey:  &privateKey.PublicKey,
		kid:        kid,
	}
}

// Close stops the mock JWKS server.
func (m *MockJWKSServer) Close() {
	m.server.Close()
}

// URL returns the JWKS endpoint URL.
func (m *MockJWKSServer) URL() string {
	return m.server.URL + "/jwks.json"
}

// KID returns the key ID used by the mock server.
func (m *MockJWKSServer) KID() string {
	return m.kid
}

// CreateValidToken creates a valid JWT token with the provided claims.
func (m *MockJWKSServer) CreateValidToken(claims jwtgo.MapClaims) (string, error) {
	token := jwtgo.NewWithClaims(jwtgo.SigningMethodRS256, claims)
	token.Header["kid"] = m.kid
	return token.SignedString(m.privateKey)
}

func encodeRSAModulus(n *big.Int) string {
	return base64.RawURLEncoding.EncodeToString(n.Bytes())
}

func encodeRSAExponent(e int) string {
	u := uint32(e) //nolint:gosec // RSA public exponent is always positive and fits in uint32
	buf := make([]byte, 4)
	buf[0] = byte(u >> 24)
	buf[1] = byte(u >> 16) //nolint:gosec // intentional bit-shift truncation to byte
	buf[2] = byte(u >> 8)  //nolint:gosec // intentional bit-shift truncation to byte
	buf[3] = byte(u)       //nolint:gosec // intentional bit-shift truncation to byte
	// Remove leading zeros
	for len(buf) > 1 && buf[0] == 0 {
		buf = buf[1:]
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}
