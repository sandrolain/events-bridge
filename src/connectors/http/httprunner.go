package main

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/valyala/fasthttp"
)

// Ensure HTTPRunner implements connectors.Runner
var _ connectors.Runner = (*HTTPRunner)(nil)

// HTTPRunnerConfig holds configuration for the HTTP runner.
// It mirrors the style of the HTTP source/runner configs.
type HTTPRunnerConfig struct {
	Method  string            `mapstructure:"method" default:"POST" validate:"required,oneof=GET POST PUT DELETE PATCH HEAD OPTIONS"`
	URL     string            `mapstructure:"url" validate:"required,url"`
	Headers map[string]string `mapstructure:"headers"`
	Timeout time.Duration     `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	TLS     tlsconfig.Config  `mapstructure:"tls"`
}

// NewRunnerConfig returns a new HTTPRunnerConfig instance (exported for plugin loading conventions).
func NewRunnerConfig() any { //nolint:revive
	return new(HTTPRunnerConfig)
}

// NewRunner creates a new HTTP runner.
func NewRunner(anyCfg any) (connectors.Runner, error) { //nolint:revive
	cfg, ok := anyCfg.(*HTTPRunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	if cfg.Method == "" {
		cfg.Method = "POST"
	}

	// Build TLS config if enabled
	tlsConfig, err := tlsconfig.BuildClientConfigIfEnabled(&cfg.TLS)
	if err != nil {
		return nil, err
	}

	client := &fasthttp.Client{
		ReadTimeout:                   cfg.Timeout,
		WriteTimeout:                  cfg.Timeout,
		NoDefaultUserAgentHeader:      true,
		DisableHeaderNamesNormalizing: true,
		DisablePathNormalizing:        true,
		TLSConfig:                     tlsConfig,
		MaxConnsPerHost:               100,
		Dial: (&fasthttp.TCPDialer{
			Concurrency: 4096,
		}).Dial,
	}

	return &HTTPRunner{
		cfg:    cfg,
		slog:   slog.Default().With("context", "HTTP Runner"),
		client: client,
	}, nil
}

// HTTPRunner implements a simple HTTP request transformation step.
// It sends the current message payload as the request body (for all methods) and, if successful,
// optionally overwrites the payload with the response body.
type HTTPRunner struct {
	cfg    *HTTPRunnerConfig
	slog   *slog.Logger
	client *fasthttp.Client
}

// Process executes the configured HTTP request.
func (r *HTTPRunner) Process(msg *message.RunnerMessage) error {
	metadata, data, err := msg.GetMetadataAndData()
	if err != nil {
		return fmt.Errorf("error getting metadata and data: %w", err)
	}

	method := strings.ToUpper(r.cfg.Method)
	url := r.cfg.URL

	r.slog.Debug("executing HTTP runner request", "method", method, "url", url, "metadata", metadata, "bodysize", len(data))

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(method)
	req.SetRequestURI(url)

	// Add custom headers from config first
	for k, v := range r.cfg.Headers {
		req.Header.Set(k, v)
	}

	// Forward message metadata as headers (like runner implementation)
	for k, v := range metadata {
		req.Header.Add(k, v)
	}

	// Set body (even for GET etc., mirroring runner behavior; caller controls method semantics)
	if len(data) > 0 {
		req.SetBody(data)
	}

	if r.cfg.Timeout > 0 {
		err = r.client.DoTimeout(req, res, r.cfg.Timeout)
	} else {
		err = r.client.Do(req, res)
	}
	if err != nil {
		return fmt.Errorf("error performing HTTP request: %w", err)
	}

	status := res.StatusCode()
	if status > 299 { // treat non 2xx as failure (similar to runner)
		return fmt.Errorf("non-2XX status code: %d", status)
	}

	r.slog.Debug("HTTP runner request completed", "status", status, "resbodysize", len(res.Body()))

	// Merge response headers into metadata with a prefix to avoid overriding original metadata keys.
	// Also expose status code as eb-status (aligning with HTTP source reply handling semantics).
	respHeaders := make(map[string]string)
	for k, v := range res.Header.All() {
		// lower-case keys for consistency
		key := strings.ToLower(string(k))
		respHeaders[key] = string(v)
	}
	respHeaders["eb-status"] = fmt.Sprintf("%d", status)
	msg.MergeMetadata(respHeaders)

	bodyCopy := append([]byte(nil), res.Body()...) // copy to detach from fasthttp buffer
	msg.SetData(bodyCopy)

	return nil
}

// Close releases underlying resources (currently no-op besides future extensibility).
func (r *HTTPRunner) Close() error { //nolint:revive
	r.slog.Info("closing http runner")
	return nil
}
