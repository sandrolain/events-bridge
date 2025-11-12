package main

import (
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/common/jwtauth"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/valyala/fasthttp"
	"golang.org/x/time/rate"
)

// SourceConfig defines the configuration for the HTTP source connector.
// It supports TLS, authentication, rate limiting, and request size limits.
type SourceConfig struct {
	// Address is the TCP address to listen on (e.g., "0.0.0.0:8080")
	Address string `mapstructure:"address" validate:"required"`

	// Method restricts accepted HTTP methods (optional, e.g., "POST")
	Method string `mapstructure:"method"`

	// Path restricts accepted URL paths (optional, e.g., "/webhook")
	Path string `mapstructure:"path"`

	// Timeout is the maximum duration for request processing
	Timeout time.Duration `mapstructure:"timeout" default:"5s" validate:"required"`

	// TLS configuration
	TLS tlsconfig.Config `mapstructure:"tls"`

	// MaxBodySize limits the maximum request body size in bytes (default: 10MB)
	MaxBodySize int64 `mapstructure:"maxBodySize" default:"10485760" validate:"gt=0"`

	// Authentication configuration
	Auth AuthConfig `mapstructure:"auth"`

	// RateLimit configuration
	RateLimit RateLimitConfig `mapstructure:"rateLimit"`

	// JWT authentication configuration (optional)
	JWT *jwtauth.Config `mapstructure:"jwt"`
}

// AuthConfig defines authentication settings for the HTTP source.
type AuthConfig struct {
	// Enabled determines if authentication is required
	Enabled bool `mapstructure:"enabled" default:"false"`

	// Type specifies the authentication method: "basic", "bearer", or "apikey"
	Type string `mapstructure:"type" validate:"required_if=Enabled true,omitempty,oneof=basic bearer apikey"`

	// Tokens is a list of valid tokens (for bearer or apikey auth)
	Tokens []string `mapstructure:"tokens" validate:"required_if=Type bearer,required_if=Type apikey,omitempty,min=1"`

	// Username for basic authentication
	Username string `mapstructure:"username" validate:"required_if=Type basic,omitempty"`

	// Password for basic authentication
	Password string `mapstructure:"password" validate:"required_if=Type basic,omitempty"`

	// HeaderName is the custom header name for apikey authentication (default: "X-API-Key")
	HeaderName string `mapstructure:"headerName" default:"X-API-Key"`
}

// RateLimitConfig defines rate limiting settings.
type RateLimitConfig struct {
	// Enabled determines if rate limiting is active
	Enabled bool `mapstructure:"enabled" default:"true"`

	// RequestsPerSecond limits the rate of requests (default: 100)
	RequestsPerSecond float64 `mapstructure:"requestsPerSecond" default:"100" validate:"gt=0"`

	// Burst allows bursts of requests up to this size (default: 10)
	Burst int `mapstructure:"burst" default:"10" validate:"gt=0"`
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

// NewSource creates a new HTTP source connector from the provided configuration.
func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	var limiter *rate.Limiter
	if cfg.RateLimit.Enabled {
		limiter = rate.NewLimiter(rate.Limit(cfg.RateLimit.RequestsPerSecond), cfg.RateLimit.Burst)
	}

	logger := slog.Default().With("context", "HTTP Source")

	// Initialize JWT authenticator if enabled
	jwtAuth, err := jwtauth.NewAuthenticator(cfg.JWT, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWT authenticator: %w", err)
	}

	return &HTTPSource{
		cfg:     cfg,
		slog:    logger,
		limiter: limiter,
		jwtAuth: jwtAuth,
	}, nil
}

// HTTPSource implements the HTTP source connector.
type HTTPSource struct {
	cfg      *SourceConfig
	slog     *slog.Logger
	c        chan *message.RunnerMessage
	listener net.Listener
	limiter  *rate.Limiter
	authMu   sync.RWMutex
	jwtAuth  *jwtauth.Authenticator
}

// Produce starts the HTTP server and returns a channel for incoming messages.
func (s *HTTPSource) Produce(buffer int) (res <-chan *message.RunnerMessage, err error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting HTTP server", "addr", s.cfg.Address, "method", s.cfg.Method, "path", s.cfg.Path, "tls", s.cfg.TLS.Enabled)

	// Create TLS config if enabled
	tlsConfig, err := s.cfg.TLS.BuildServerConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	// Listen on the configured address
	listener, e := net.Listen("tcp", s.cfg.Address)
	if e != nil {
		err = fmt.Errorf("failed to listen: %w", e)
		return
	}

	// Wrap with TLS if enabled
	if tlsConfig != nil {
		s.listener = tls.NewListener(listener, tlsConfig)
	} else {
		s.listener = listener
	}

	go func() {
		e = fasthttp.Serve(s.listener, s.handleRequest)
		if e != nil {
			s.slog.Error("HTTP server error", "error", e)
		}
	}()

	res = s.c
	return
}

// handleRequest processes individual HTTP requests with authentication, rate limiting, and size checks.
func (s *HTTPSource) handleRequest(ctx *fasthttp.RequestCtx) {
	method := string(ctx.Method())
	path := string(ctx.Path())

	s.slog.Debug("received HTTP request", "method", method, "path", path)

	// Check rate limit
	if s.limiter != nil && !s.limiter.Allow() {
		ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
		ctx.SetBodyString("Rate limit exceeded")
		return
	}

	// Check method
	if s.cfg.Method != "" && method != s.cfg.Method {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		return
	}

	// Check path
	if s.cfg.Path != "" && path != s.cfg.Path {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return
	}

	// Check body size
	if int64(ctx.Request.Header.ContentLength()) > s.cfg.MaxBodySize {
		ctx.SetStatusCode(fasthttp.StatusRequestEntityTooLarge)
		ctx.SetBodyString("Request body too large")
		return
	}

	// Authenticate if enabled
	if s.cfg.Auth.Enabled && !s.authenticate(ctx) {
		ctx.SetStatusCode(fasthttp.StatusUnauthorized)
		ctx.Response.Header.Set("WWW-Authenticate", s.getAuthChallenge())
		ctx.SetBodyString("Unauthorized")
		return
	}

	// Prepare initial metadata
	metadata := s.extractMetadata(ctx)

	// Validate JWT if configured
	if s.jwtAuth != nil {
		authResult := s.jwtAuth.Authenticate(metadata)
		if !authResult.Verified {
			s.slog.Warn("JWT validation failed", "error", authResult.Error)
			ctx.SetStatusCode(fasthttp.StatusUnauthorized)
			ctx.Response.Header.Set("WWW-Authenticate", "Bearer realm=\"Events Bridge\"")
			ctx.SetBodyString("Invalid or expired JWT token")
			return
		}
		// Use enriched metadata with JWT claims
		metadata = authResult.Metadata
	}

	done := make(chan message.ResponseStatus, 1)
	reply := make(chan *message.ReplyData, 1)

	msg := &HTTPMessage{
		httpCtx:  ctx,
		done:     done,
		reply:    reply,
		metadata: metadata,
	}

	s.c <- message.NewRunnerMessage(msg)

	// Wait for Ack/Nak or reply
	s.processResponse(ctx, done, reply)
}

// extractMetadata extracts HTTP headers and request info as metadata.
func (s *HTTPSource) extractMetadata(ctx *fasthttp.RequestCtx) map[string]string {
	metadata := make(map[string]string)
	header := &ctx.Request.Header
	keys := header.PeekKeys()
	for _, k := range keys {
		key := string(k)
		v := header.PeekAll(key)
		values := make([]string, len(v))
		for i, val := range v {
			values[i] = string(val)
		}
		metadata[key] = strings.Join(values, ",")
	}
	metadata["method"] = string(ctx.Method())
	metadata["path"] = string(ctx.Path())
	return metadata
}

// authenticate checks request authentication based on the configured auth type.
func (s *HTTPSource) authenticate(ctx *fasthttp.RequestCtx) bool {
	s.authMu.RLock()
	defer s.authMu.RUnlock()

	switch s.cfg.Auth.Type {
	case "basic":
		return s.authenticateBasic(ctx)
	case "bearer":
		return s.authenticateBearer(ctx)
	case "apikey":
		return s.authenticateAPIKey(ctx)
	default:
		return false
	}
}

// authenticateBasic performs HTTP Basic authentication.
func (s *HTTPSource) authenticateBasic(ctx *fasthttp.RequestCtx) bool {
	authHeader := string(ctx.Request.Header.Peek("Authorization"))
	if !strings.HasPrefix(authHeader, "Basic ") {
		return false
	}

	// Decode base64
	payload := strings.TrimPrefix(authHeader, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return false
	}

	// Split username:password
	pair := string(decoded)
	idx := strings.IndexByte(pair, ':')
	if idx == -1 {
		return false
	}

	username := pair[:idx]
	password := pair[idx+1:]

	// Use constant-time comparison to prevent timing attacks
	usernameMatch := subtle.ConstantTimeCompare([]byte(username), []byte(s.cfg.Auth.Username)) == 1
	passwordMatch := subtle.ConstantTimeCompare([]byte(password), []byte(s.cfg.Auth.Password)) == 1

	return usernameMatch && passwordMatch
}

// authenticateBearer performs Bearer token authentication.
func (s *HTTPSource) authenticateBearer(ctx *fasthttp.RequestCtx) bool {
	authHeader := string(ctx.Request.Header.Peek("Authorization"))
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return false
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	return s.isValidToken(token)
}

// authenticateAPIKey performs API key authentication via custom header.
func (s *HTTPSource) authenticateAPIKey(ctx *fasthttp.RequestCtx) bool {
	apiKey := string(ctx.Request.Header.Peek(s.cfg.Auth.HeaderName))
	return s.isValidToken(apiKey)
}

// isValidToken checks if a token is in the list of valid tokens using constant-time comparison.
func (s *HTTPSource) isValidToken(token string) bool {
	for _, validToken := range s.cfg.Auth.Tokens {
		if subtle.ConstantTimeCompare([]byte(token), []byte(validToken)) == 1 {
			return true
		}
	}
	return false
}

// getAuthChallenge returns the appropriate WWW-Authenticate header value.
func (s *HTTPSource) getAuthChallenge() string {
	switch s.cfg.Auth.Type {
	case "basic":
		return "Basic realm=\"Events Bridge\""
	case "bearer":
		return "Bearer realm=\"Events Bridge\""
	default:
		return ""
	}
}

// processResponse handles the response from the message processing pipeline.
func (s *HTTPSource) processResponse(ctx *fasthttp.RequestCtx, done chan message.ResponseStatus, reply chan *message.ReplyData) {
	r, status, timeout := message.AwaitReplyOrStatus(s.cfg.Timeout, done, reply)
	if timeout {
		ctx.SetStatusCode(fasthttp.StatusGatewayTimeout)
		return
	}

	if r != nil {
		var statusCode int

		// Set response headers from metadata, skipping eb- headers
		for k, v := range r.Metadata {
			lk := strings.ToLower(k)
			if strings.HasPrefix(lk, "eb-") {
				switch lk {
				case "eb-status":
					vi, err := strconv.Atoi(v)
					if err != nil {
						s.slog.Warn("invalid eb-status metadata value, must be an integer", "value", v)
					} else if vi < 100 || vi > 599 {
						s.slog.Warn("invalid eb-status metadata value, must be a valid HTTP status code (100-599)", "value", vi)
					} else {
						s.slog.Debug("setting response status from eb-status metadata", "status", vi)
						statusCode = vi
					}
				default:
					s.slog.Warn("skipping metadata key starting with eb- in HTTP response", "key", k)
				}
				continue
			}
			ctx.Response.Header.Add(k, v)
		}

		if statusCode == 0 {
			statusCode = fasthttp.StatusOK
			if len(r.Data) == 0 {
				statusCode = fasthttp.StatusNoContent
			}
		}

		ctx.SetStatusCode(statusCode)
		ctx.SetBody(r.Data)
		return
	}

	if status != nil {
		switch *status {
		case message.ResponseStatusAck:
			ctx.SetStatusCode(fasthttp.StatusAccepted)
		case message.ResponseStatusNak:
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		default:
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		}
	}
}

// Close stops the HTTP server and releases resources.
func (s *HTTPSource) Close() (err error) {
	if s.jwtAuth != nil {
		if closeErr := s.jwtAuth.Close(); closeErr != nil {
			s.slog.Warn("failed to close JWT authenticator", "error", closeErr)
		}
	}
	if s.listener != nil {
		err = s.listener.Close()
	}
	return
}
