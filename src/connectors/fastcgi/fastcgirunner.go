// Package main implements the FastCGI connector for the Events Bridge.
//
// The FastCGI connector enables execution of FastCGI applications (such as PHP-FPM)
// as message processors, allowing seamless integration with existing FastCGI
// infrastructure.
//
// Key features:
//   - TCP and Unix socket connection support
//   - Connection pooling for improved performance
//   - Configurable timeouts and connection limits
//   - Support for custom environment variables and parameters
//   - PHP-FPM compatible parameter handling
//
// Example configuration:
//
//	runners:
//	  - type: fastcgi
//	    options:
//	      network: tcp
//	      address: localhost:9000
//	      documentRoot: /var/www/html
//	      scriptFilename: /var/www/html/index.php
//	      timeout: 30s
//	      poolSize: 10
//	      poolExpiry: 60s
//
// For Unix socket connections:
//
//	runners:
//	  - type: fastcgi
//	    options:
//	      network: unix
//	      address: /run/php/php-fpm.sock
//	      documentRoot: /var/www/html
//	      scriptFilename: /var/www/html/process.php
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/yookoala/gofast"
)

// Ensure FastCGIRunner implements connectors.Runner
var _ connectors.Runner = (*FastCGIRunner)(nil)

// FastCGIRunnerConfig holds configuration for the FastCGI runner.
type FastCGIRunnerConfig struct {
	// Network is the network protocol: "tcp", "tcp4", "tcp6", or "unix"
	Network string `mapstructure:"network" default:"tcp" validate:"required,oneof=tcp tcp4 tcp6 unix"`

	// Address is the FastCGI server address (e.g., "localhost:9000" or "/run/php/php-fpm.sock")
	Address string `mapstructure:"address" validate:"required"`

	// DocumentRoot is the document root path for the FastCGI application
	DocumentRoot string `mapstructure:"documentRoot" default:"/var/www/html" validate:"required"`

	// ScriptFilename is the path to the PHP script to execute
	ScriptFilename string `mapstructure:"scriptFilename" validate:"required"`

	// ScriptName is the script name (usually the path after document root)
	// If empty, it will be derived from ScriptFilename and DocumentRoot
	ScriptName string `mapstructure:"scriptName"`

	// RequestMethod is the HTTP method to use for FastCGI requests
	RequestMethod string `mapstructure:"requestMethod" default:"POST" validate:"omitempty,oneof=GET POST PUT DELETE PATCH"`

	// RequestURI is the request URI to use for FastCGI requests
	// If empty, defaults to ScriptName
	RequestURI string `mapstructure:"requestUri"`

	// QueryString is an optional query string to append to requests
	QueryString string `mapstructure:"queryString"`

	// ContentType is the Content-Type header for the request body
	ContentType string `mapstructure:"contentType" default:"application/octet-stream"`

	// Timeout is the maximum execution time for a single request
	Timeout time.Duration `mapstructure:"timeout" default:"30s" validate:"required,gt=0"`

	// PoolSize is the number of connections to maintain in the pool
	PoolSize uint `mapstructure:"poolSize" default:"10" validate:"min=1,max=1000"`

	// PoolExpiry is the time after which idle connections are closed
	PoolExpiry time.Duration `mapstructure:"poolExpiry" default:"60s" validate:"gt=0"`

	// Env contains additional environment variables to pass to the FastCGI application
	Env map[string]string `mapstructure:"env"`

	// PassMetadataAsEnv when true, passes message metadata as environment variables
	// with the prefix "HTTP_" (standard CGI convention for headers)
	PassMetadataAsEnv bool `mapstructure:"passMetadataAsEnv" default:"true"`

	// ServerSoftware is the SERVER_SOFTWARE environment variable value
	ServerSoftware string `mapstructure:"serverSoftware" default:"events-bridge"`

	// ServerName is the SERVER_NAME environment variable value
	ServerName string `mapstructure:"serverName" default:"localhost"`

	// ServerPort is the SERVER_PORT environment variable value
	ServerPort string `mapstructure:"serverPort" default:"80"`
}

// NewRunnerConfig returns a new FastCGIRunnerConfig instance.
func NewRunnerConfig() any { //nolint:revive
	return new(FastCGIRunnerConfig)
}

// NewRunner creates a new FastCGI runner.
func NewRunner(anyCfg any) (connectors.Runner, error) { //nolint:revive
	cfg, ok := anyCfg.(*FastCGIRunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Apply defaults
	if cfg.Network == "" {
		cfg.Network = "tcp"
	}
	if cfg.RequestMethod == "" {
		cfg.RequestMethod = "POST"
	}
	if cfg.ContentType == "" {
		cfg.ContentType = "application/octet-stream"
	}
	if cfg.ServerSoftware == "" {
		cfg.ServerSoftware = "events-bridge"
	}
	if cfg.ServerName == "" {
		cfg.ServerName = "localhost"
	}
	if cfg.ServerPort == "" {
		cfg.ServerPort = "80"
	}
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 10
	}
	if cfg.PoolExpiry == 0 {
		cfg.PoolExpiry = 60 * time.Second
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}

	// Derive script name from filename if not provided
	if cfg.ScriptName == "" && cfg.ScriptFilename != "" && cfg.DocumentRoot != "" {
		if strings.HasPrefix(cfg.ScriptFilename, cfg.DocumentRoot) {
			cfg.ScriptName = strings.TrimPrefix(cfg.ScriptFilename, cfg.DocumentRoot)
		} else {
			cfg.ScriptName = cfg.ScriptFilename
		}
	}

	// Default request URI to script name
	if cfg.RequestURI == "" {
		cfg.RequestURI = cfg.ScriptName
	}

	// Create connection factory
	connFactory := gofast.SimpleConnFactory(cfg.Network, cfg.Address)

	// Create client pool for connection reuse
	pool := gofast.NewClientPool(
		gofast.SimpleClientFactory(connFactory),
		cfg.PoolSize,
		cfg.PoolExpiry,
	)

	logger := slog.Default().With("context", "FastCGI Runner")
	logger.Info("created FastCGI runner",
		"network", cfg.Network,
		"address", cfg.Address,
		"scriptFilename", cfg.ScriptFilename,
		"poolSize", cfg.PoolSize,
	)

	return &FastCGIRunner{
		cfg:    cfg,
		pool:   pool,
		logger: logger,
	}, nil
}

// FastCGIRunner implements a FastCGI client runner.
// It sends message data to a FastCGI application and processes the response.
type FastCGIRunner struct {
	cfg    *FastCGIRunnerConfig
	pool   *gofast.ClientPool
	logger *slog.Logger
}

// Process executes the FastCGI request with the message data.
func (r *FastCGIRunner) Process(msg *message.RunnerMessage) error {
	metadata, data, err := msg.GetMetadataAndData()
	if err != nil {
		return fmt.Errorf("error getting metadata and data: %w", err)
	}

	r.logger.Debug("processing FastCGI request",
		"scriptFilename", r.cfg.ScriptFilename,
		"dataSize", len(data),
	)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	defer cancel()

	// Get client from pool
	client, err := r.pool.CreateClient()
	if err != nil {
		return fmt.Errorf("failed to get FastCGI client: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			slog.Warn("failed to close FastCGI client", "error", closeErr)
		}
	}()

	// Create FastCGI request
	req := gofast.NewRequest(nil)
	req.Role = gofast.RoleResponder

	// Set standard CGI/FastCGI parameters
	req.Params["GATEWAY_INTERFACE"] = "CGI/1.1"
	req.Params["SERVER_PROTOCOL"] = "HTTP/1.1"
	req.Params["SERVER_SOFTWARE"] = r.cfg.ServerSoftware
	req.Params["SERVER_NAME"] = r.cfg.ServerName
	req.Params["SERVER_PORT"] = r.cfg.ServerPort
	req.Params["REQUEST_METHOD"] = r.cfg.RequestMethod
	req.Params["REQUEST_URI"] = r.cfg.RequestURI
	req.Params["SCRIPT_NAME"] = r.cfg.ScriptName
	req.Params["SCRIPT_FILENAME"] = r.cfg.ScriptFilename
	req.Params["DOCUMENT_ROOT"] = r.cfg.DocumentRoot
	req.Params["QUERY_STRING"] = r.cfg.QueryString
	req.Params["REDIRECT_STATUS"] = "200"

	// Set content info
	req.Params["CONTENT_TYPE"] = r.cfg.ContentType
	req.Params["CONTENT_LENGTH"] = fmt.Sprintf("%d", len(data))

	// Add custom environment variables
	for k, v := range r.cfg.Env {
		req.Params[k] = v
	}

	// Pass metadata as HTTP headers (CGI convention)
	if r.cfg.PassMetadataAsEnv {
		for k, v := range metadata {
			// Convert header name to CGI format: X-Custom-Header -> HTTP_X_CUSTOM_HEADER
			envName := "HTTP_" + strings.ToUpper(strings.ReplaceAll(k, "-", "_"))
			req.Params[envName] = v
		}
	}

	// Set request body via Stdin
	if len(data) > 0 {
		req.Stdin = io.NopCloser(strings.NewReader(string(data)))
	}

	// Execute the request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("FastCGI request failed: %w", err)
	}

	// Handle context cancellation
	select {
	case <-ctx.Done():
		return fmt.Errorf("FastCGI request timed out: %w", ctx.Err())
	default:
	}

	// Read and parse response
	responseData, responseHeaders, statusCode, err := r.parseResponse(resp)
	if err != nil {
		return fmt.Errorf("failed to parse FastCGI response: %w", err)
	}

	r.logger.Debug("FastCGI request completed",
		"statusCode", statusCode,
		"responseSize", len(responseData),
	)

	// Check for error status
	if statusCode >= 400 {
		return fmt.Errorf("FastCGI returned error status: %d", statusCode)
	}

	// Update message with response
	msg.SetData(responseData)

	// Merge response headers into metadata
	responseHeaders["eb-status"] = fmt.Sprintf("%d", statusCode)
	msg.MergeMetadata(responseHeaders)

	return nil
}

// parseResponse reads the FastCGI response using gofast's WriteTo method.
func (r *FastCGIRunner) parseResponse(resp *gofast.ResponsePipe) ([]byte, map[string]string, int, error) {
	// Use httptest.ResponseRecorder to capture the response
	recorder := httptest.NewRecorder()
	errBuffer := new(bytes.Buffer)

	// WriteTo parses the FastCGI response and writes to the recorder
	if err := resp.WriteTo(recorder, errBuffer); err != nil {
		return nil, nil, 0, fmt.Errorf("failed to write FastCGI response: %w", err)
	}

	// Log any stderr content
	if errBuffer.Len() > 0 {
		r.logger.Warn("FastCGI stderr output", "content", errBuffer.String())
	}

	// Extract response body
	body := recorder.Body.Bytes()

	// Extract headers
	headers := make(map[string]string)
	for k, v := range recorder.Header() {
		if len(v) > 0 {
			headers[strings.ToLower(k)] = v[0]
		}
	}

	return body, headers, recorder.Code, nil
}

// Close releases resources used by the runner.
func (r *FastCGIRunner) Close() error {
	r.logger.Info("closing FastCGI runner")
	// The pool doesn't have a Close method, connections will expire naturally
	return nil
}
