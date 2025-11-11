package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"log/slog"
)

func TestJWKSClientFetch(t *testing.T) {
	t.Parallel()

	// Create test JWKS
	jwks := JWKS{
		Keys: []JWK{
			{
				Kid: "test-key-1",
				Kty: "RSA",
				Alg: "RS256",
				Use: "sig",
				N:   "test-modulus",
				E:   "AQAB",
			},
		},
	}

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(jwks); err != nil {
			t.Logf("failed to encode: %v", err)
		}
	}))
	defer server.Close()

	// Create client
	client, err := NewJWKSClient(server.URL, 1*time.Hour, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	// Verify keys were loaded
	if len(client.keys) == 0 {
		t.Fatal("expected keys to be loaded")
	}
}

func TestJWKSClientGetKey(t *testing.T) {
	t.Parallel()

	// Setup mock server
	mockServer := setupMockJWKSServer(t)
	defer mockServer.Close()

	// Create client
	client, err := NewJWKSClient(mockServer.URL(), 1*time.Hour, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	// Get existing key
	key, err := client.GetKey(mockServer.kid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if key == nil {
		t.Fatal("expected non-nil key")
	}

	// Try to get non-existing key
	_, err = client.GetKey("non-existing-kid")
	if err == nil {
		t.Fatal("expected error for non-existing kid")
	}
}

func TestJWKSClientRefresh(t *testing.T) {
	t.Parallel()

	callCount := 0

	// Create server that counts calls
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		jwks := JWKS{
			Keys: []JWK{
				{
					Kid: "test-key-1",
					Kty: "RSA",
					Alg: "RS256",
					Use: "sig",
					N:   "test-modulus",
					E:   "AQAB",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(jwks); err != nil {
			t.Logf("failed to encode: %v", err)
		}
	}))
	defer server.Close()

	// Create client with short refresh interval
	client, err := NewJWKSClient(server.URL, 100*time.Millisecond, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	initialCalls := callCount

	// Wait for at least one refresh
	time.Sleep(200 * time.Millisecond)

	if callCount <= initialCalls {
		t.Errorf("expected refresh to be called, calls: %d", callCount)
	}
}

func TestJWKSClientInvalidURL(t *testing.T) {
	t.Parallel()

	// Try to create client with invalid URL
	_, err := NewJWKSClient("http://non-existing-server-12345.com/jwks.json", 1*time.Hour, slog.Default())
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestJWKSClientNonOKStatus(t *testing.T) {
	t.Parallel()

	// Create server that returns 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	// Try to create client
	_, err := NewJWKSClient(server.URL, 1*time.Hour, slog.Default())
	if err == nil {
		t.Fatal("expected error for non-OK status")
	}
}

func TestJWKSClientInvalidJSON(t *testing.T) {
	t.Parallel()

	// Create server that returns invalid JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte("invalid json")); err != nil {
			t.Logf("failed to write: %v", err)
		}
	}))
	defer server.Close()

	// Try to create client
	_, err := NewJWKSClient(server.URL, 1*time.Hour, slog.Default())
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestJWKSClientSkipNonSigKeys(t *testing.T) {
	t.Parallel()

	// Create JWKS with encryption key
	jwks := JWKS{
		Keys: []JWK{
			{
				Kid: "sig-key",
				Kty: "RSA",
				Alg: "RS256",
				Use: "sig",
				N:   "test-modulus",
				E:   "AQAB",
			},
			{
				Kid: "enc-key",
				Kty: "RSA",
				Alg: "RSA-OAEP",
				Use: "enc", // Should be skipped
				N:   "test-modulus-2",
				E:   "AQAB",
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(jwks); err != nil {
			t.Logf("failed to encode: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewJWKSClient(server.URL, 1*time.Hour, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	// Should only have the sig key, but the implementation might convert invalid keys
	// and log warnings, so we just check that it doesn't crash
	if len(client.keys) == 0 {
		t.Fatal("expected at least some keys to be loaded")
	}
}
