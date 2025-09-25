package main

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
	"github.com/sandrolain/events-bridge/src/utils"
	"github.com/valyala/fasthttp"
)

type TargetConfig struct {
	Method  string            `yaml:"method" json:"method"`
	URL     string            `yaml:"url" json:"url"`
	Headers map[string]string `yaml:"headers" json:"headers"`
	Timeout time.Duration     `yaml:"timeout" json:"timeout"`
}

// parseTargetOptions decodes a generic options map into the connector-specific config.
// Expected keys: url, method, headers (map[string]string), timeout.
func parseTargetOptions(opts map[string]any) (*TargetConfig, error) {
	cfg := &TargetConfig{}
	op := &utils.OptsParser{}
	cfg.URL = op.OptString(opts, "url", "")
	cfg.Method = op.OptString(opts, "method", "POST", utils.StringOneOf("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"))
	cfg.Headers = op.OptStringMap(opts, "headers", map[string]string{})
	cfg.Timeout = op.OptDuration(opts, "timeout", targets.DefaultTimeout)
	if op.Error() != nil {
		return nil, op.Error()
	}
	return cfg, nil
}

// NewTarget creates an HTTP target from options map.
func NewTarget(opts map[string]any) (targets.Target, error) {
	cfg, err := parseTargetOptions(opts)
	if err != nil {
		return nil, err
	}

	client := &fasthttp.Client{
		ReadTimeout:                   cfg.Timeout,
		WriteTimeout:                  cfg.Timeout,
		NoDefaultUserAgentHeader:      true,
		DisableHeaderNamesNormalizing: true,
		DisablePathNormalizing:        true,
		Dial: (&fasthttp.TCPDialer{
			Concurrency: 4096,
			//DNSCacheDuration: time.Hour, // increase DNS cache time to an hour instead of default minute
		}).Dial,
	}

	return &HTTPTarget{
		config: cfg,
		slog:   slog.Default().With("context", "HTTP"),
		client: client,
		stopCh: make(chan struct{}),
	}, nil
}

type HTTPTarget struct {
	slog    *slog.Logger
	config  *TargetConfig
	stopped bool
	stopCh  chan struct{}
	client  *fasthttp.Client
}

func (s *HTTPTarget) Consume(result *message.RunnerMessage) (err error) {
	metadata, err := result.GetTargetMetadata()
	if err != nil {
		err = fmt.Errorf("error getting metadata: %w", err)
		return
	}

	data, err := result.GetTargetData()
	if err != nil {
		err = fmt.Errorf("error getting data: %w", err)
		return
	}

	method := strings.ToUpper(s.config.Method)
	url := s.config.URL

	s.slog.Debug("publishing", "method", method, "url", url, "metadata", metadata, "bodysize", len(data))

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethod(method)
	for k, v := range s.config.Headers {
		req.Header.Set(k, v)
	}
	req.SetRequestURI(url)
	req.SetBody(data)

	for k, v := range metadata {
		req.Header.Add(k, v)
	}

	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	err = s.client.Do(req, res)
	if err != nil {
		err = fmt.Errorf("error sending request: %w", err)
		return
	}

	if res.StatusCode() > 299 {
		err = fmt.Errorf("non-2XX status code: %d", res.StatusCode())
		return
	}

	s.slog.Debug("published", "status", res.StatusCode())

	return
}

func (s *HTTPTarget) Close() (err error) {
	s.stopped = true
	if s.stopCh != nil {
		close(s.stopCh)
	}
	return
}
