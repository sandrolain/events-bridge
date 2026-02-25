package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type JWKS struct {
	Keys []JWK `json:"keys"`
}

type JWK struct {
	Kty string `json:"kty"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

var privateKey *rsa.PrivateKey
var publicKey *rsa.PublicKey

func processClaimValue(value interface{}) interface{} {
	if str, ok := value.(string); ok {
		switch str {
		case "{{now}}":
			return time.Now().Unix()
		case "{{now+1h}}":
			return time.Now().Add(time.Hour).Unix()
		case "{{now-1h}}":
			return time.Now().Add(-time.Hour).Unix()
		}
		if strings.HasPrefix(str, "{{random}}") {
			// Simple random number
			randomBytes := make([]byte, 4)
			rand.Read(randomBytes)
			return int(randomBytes[0]) + int(randomBytes[1])*256 + int(randomBytes[2])*65536 + int(randomBytes[3])*16777216
		}
	}
	return value
}

func main() { //nolint:gocyclo
	cmd := flag.String("cmd", "serve", "Command to run: serve or gen (default: serve)")
	pkPath := flag.String("pk", "", "Path to private key file (PEM format). If not specified, generates a volatile key in memory (default: empty)")
	issuer := flag.String("iss", "jwktool", "Default issuer for JWT (default: jwktool)")
	subject := flag.String("sub", "test", "Default subject for JWT (default: test)")
	audience := flag.String("aud", "test", "Default audience for JWT (default: test)")
	expiry := flag.Duration("exp", 24*time.Hour, "Default expiry duration for JWT (default: 24h)")
	port := flag.String("port", "8080", "Port to run the server on (default: 8080)")
	claimsJSON := flag.String("claims", "{}", "JSON string for additional claims (default: {})")
	invalidJSON := flag.String("invalid", "{}", "JSON string for invalid options (default: {})")
	kid := flag.String("kid", "", "Key ID for JWT header (default: empty)")
	silent := flag.Bool("silent", false, "Silent mode (no logs) (default: false)")
	flag.Parse()

	// Setup logger
	var logger *slog.Logger
	if *silent {
		devNull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		logger = slog.New(slog.NewTextHandler(devNull, &slog.HandlerOptions{Level: slog.LevelError}))
	} else {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	slog.SetDefault(logger)

	// Load or generate key
	if *pkPath != "" {
		if _, err := os.Stat(*pkPath); os.IsNotExist(err) {
			// Generate new key
			key, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				slog.Error("Failed to generate private key", "error", err)
				os.Exit(1)
			}
			privateKey = key
			publicKey = &key.PublicKey
			// Save to file
			pemData := pem.EncodeToMemory(&pem.Block{
				Type:  "RSA PRIVATE KEY",
				Bytes: x509.MarshalPKCS1PrivateKey(key),
			})
			if err := os.WriteFile(*pkPath, pemData, 0600); err != nil {
				slog.Error("Failed to save private key", "error", err)
				os.Exit(1)
			}
			slog.Info("Generated and saved private key", "path", *pkPath)
		} else {
			// Load from file
			pemData, err := os.ReadFile(*pkPath)
			if err != nil {
				slog.Error("Failed to read private key file", "error", err)
				os.Exit(1)
			}
			block, _ := pem.Decode(pemData)
			if block == nil || block.Type != "RSA PRIVATE KEY" {
				slog.Error("Invalid PEM block in private key file")
				os.Exit(1)
			}
			key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
			if err != nil {
				slog.Error("Failed to parse private key", "error", err)
				os.Exit(1)
			}
			privateKey = key
			publicKey = &key.PublicKey
			slog.Info("Loaded private key", "path", *pkPath)
		}
	} else {
		// Generate volatile key
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			slog.Error("Failed to generate private key", "error", err)
			os.Exit(1)
		}
		privateKey = key
		publicKey = &key.PublicKey
		slog.Info("Generated volatile private key in memory")
	}

	kidValue := *kid
	if kidValue == "" {
		kidValue = "1"
	}

	if *cmd == "gen" {
		// Generate token
		var claims map[string]interface{}
		if err := json.Unmarshal([]byte(*claimsJSON), &claims); err != nil {
			slog.Error("Invalid claims JSON", "error", err)
			os.Exit(1)
		}
		var invalid map[string]interface{}
		if err := json.Unmarshal([]byte(*invalidJSON), &invalid); err != nil {
			slog.Error("Invalid invalid JSON", "error", err)
			os.Exit(1)
		}
		now := time.Now()
		tokenClaims := jwt.MapClaims{
			"iss": *issuer,
			"sub": *subject,
			"aud": *audience,
			"exp": now.Add(*expiry).Unix(),
			"iat": now.Unix(),
			"nbf": now.Unix(),
		}
		for k, v := range claims {
			tokenClaims[k] = processClaimValue(v)
		}
		// Apply invalid options
		if expired, ok := invalid["expired"].(bool); ok && expired {
			tokenClaims["exp"] = now.Add(-time.Hour).Unix()
		}
		if missingClaim, ok := invalid["missing_claim"].(string); ok {
			delete(tokenClaims, missingClaim)
		}
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, tokenClaims)
		token.Header["kid"] = kidValue
		var tokenString string
		var err error
		if wrongKey, ok := invalid["wrong_key"].(bool); ok && wrongKey {
			// Use a different key for signing
			wrongPrivateKey, _ := rsa.GenerateKey(rand.Reader, 2048)
			tokenString, err = token.SignedString(wrongPrivateKey)
		} else {
			tokenString, err = token.SignedString(privateKey)
		}
		if err != nil {
			slog.Error("Failed to sign token", "error", err)
			os.Exit(1)
		}
		pkInfo := "volatile"
		if *pkPath != "" {
			pkInfo = *pkPath
		}
		slog.Info("Generated JWT", "claims", tokenClaims, "pk", pkInfo)
		fmt.Println(tokenString)
		return
	}

	// Serve mode (default)

	http.HandleFunc("/generate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload struct {
			Claims  map[string]interface{} `json:"claims"`
			Invalid map[string]interface{} `json:"invalid"`
			Kid     string                 `json:"kid"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil && r.Body != http.NoBody {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if payload.Claims == nil {
			payload.Claims = make(map[string]interface{})
		}
		if payload.Kid == "" {
			payload.Kid = strconv.FormatInt(time.Now().Unix(), 10)
		}
		now := time.Now()
		claims := jwt.MapClaims{
			"iss": *issuer,
			"sub": *subject,
			"aud": *audience,
			"exp": now.Add(*expiry).Unix(),
			"iat": now.Unix(),
			"nbf": now.Unix(),
		}
		// Override from query params
		kidValue := payload.Kid
		query := r.URL.Query()
		for k, v := range query {
			if len(v) > 0 {
				switch k {
				case "kid":
					kidValue = v[0]
				case "exp", "iat", "nbf":

					if ts, err := strconv.ParseInt(v[0], 10, 64); err == nil {
						claims[k] = ts
					} else {
						claims[k] = processClaimValue(v[0])
					}
				default:
					claims[k] = processClaimValue(v[0])
				}
			}
		}
		for k, v := range payload.Claims {
			claims[k] = processClaimValue(v)
		}
		// Apply invalid options
		if payload.Invalid != nil {
			if expired, ok := payload.Invalid["expired"].(bool); ok && expired {
				claims["exp"] = now.Add(-time.Hour).Unix()
			}
			if missingClaim, ok := payload.Invalid["missing_claim"].(string); ok {
				delete(claims, missingClaim)
			}
		}
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		token.Header["kid"] = kidValue
		var tokenString string
		var err error
		if payload.Invalid != nil {
			if wrongKey, ok := payload.Invalid["wrong_key"].(bool); ok && wrongKey {
				// Use a different key for signing
				wrongPrivateKey, _ := rsa.GenerateKey(rand.Reader, 2048)
				tokenString, err = token.SignedString(wrongPrivateKey)
			} else {
				tokenString, err = token.SignedString(privateKey)
			}
		} else {
			tokenString, err = token.SignedString(privateKey)
		}
		if err != nil {
			http.Error(w, "Failed to sign token", http.StatusInternalServerError)
			return
		}
		pkInfo := "volatile"
		if *pkPath != "" {
			pkInfo = *pkPath
		}
		slog.Info("Generated JWT via API", "claims", claims, "pk", pkInfo)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"token": tokenString})
	})

	http.HandleFunc("/validate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || len(authHeader) < 7 || authHeader[:7] != "Bearer " {
			http.Error(w, "Missing or invalid Authorization header", http.StatusUnauthorized)
			return
		}
		tokenString := authHeader[7:]
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return publicKey, nil
		})
		if err != nil {
			http.Error(w, "Invalid token: "+err.Error(), http.StatusUnauthorized)
			return
		}
		if !token.Valid {
			http.Error(w, "Token not valid", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(token.Claims)
	})

	http.HandleFunc("/decode", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		tokenString, ok := payload["token"]
		if !ok {
			http.Error(w, "Missing token in payload", http.StatusBadRequest)
			return
		}
		token, _, err := jwt.NewParser().ParseUnverified(tokenString, jwt.MapClaims{})
		if err != nil {
			http.Error(w, "Failed to decode token: "+err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(token.Claims)
	})

	http.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// For simplicity, use kid "1"
		nBytes := publicKey.N.Bytes()
		eBytes := make([]byte, 4)
		for i := 0; i < 4; i++ {
			eBytes[3-i] = byte(uint32(publicKey.E) >> (i * 8)) //nolint:gosec // RSA public exponent fits in uint32
		}
		jwk := JWK{
			Kty: "RSA",
			Use: "sig",
			Kid: kidValue,
			N:   base64.RawURLEncoding.EncodeToString(nBytes),
			E:   base64.RawURLEncoding.EncodeToString(eBytes),
		}
		jwks := JWKS{Keys: []JWK{jwk}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jwks)
	})

	slog.Info("Starting server", "port", *port)
	if err := http.ListenAndServe(":"+*port, nil); err != nil { //nolint:gosec
		slog.Error("Server failed", "error", err)
		os.Exit(1)
	}
}
