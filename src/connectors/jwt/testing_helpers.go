package main

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
	testIssuer        = "https://test.example.com"
	testAudience      = "test-audience"
	errUnexpectedErr  = "unexpected error: %v"
	errExpectedErr    = "expected error but got none"
	errExpectedNonNil = "expected non-nil value"
)

// Mock JWKS server for testing
type mockJWKSServer struct {
	server     *httptest.Server
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
	kid        string
}

func setupMockJWKSServer(t *testing.T) *mockJWKSServer {
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

	return &mockJWKSServer{
		server:     server,
		privateKey: privateKey,
		publicKey:  &privateKey.PublicKey,
		kid:        kid,
	}
}

func (m *mockJWKSServer) Close() {
	m.server.Close()
}

func (m *mockJWKSServer) URL() string {
	return m.server.URL + "/jwks.json"
}

func (m *mockJWKSServer) CreateValidToken(claims jwtgo.MapClaims) (string, error) {
	token := jwtgo.NewWithClaims(jwtgo.SigningMethodRS256, claims)
	token.Header["kid"] = m.kid
	return token.SignedString(m.privateKey)
}

func encodeRSAModulus(n *big.Int) string {
	return base64.RawURLEncoding.EncodeToString(n.Bytes())
}

func encodeRSAExponent(e int) string {
	buf := make([]byte, 4)
	buf[0] = byte(e >> 24)
	buf[1] = byte(e >> 16)
	buf[2] = byte(e >> 8)
	buf[3] = byte(e)
	// Remove leading zeros
	for len(buf) > 1 && buf[0] == 0 {
		buf = buf[1:]
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}
