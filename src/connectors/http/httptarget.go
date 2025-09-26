package main

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/common"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/valyala/fasthttp"
)

type TargetConfig struct {
	Method  string            `mapstructure:"method" default:"POST" validate:"required,oneof=GET POST PUT DELETE PATCH HEAD OPTIONS"`
	URL     string            `mapstructure:"url" validate:"required,url"`
	Headers map[string]string `mapstructure:"headers"`
	Timeout time.Duration     `mapstructure:"timeout" default:"5s" validate:"gt=0"`
}

// NewTarget creates an HTTP target from options map.
func NewTarget(opts map[string]any) (connectors.Target, error) {
	cfg, err := common.ParseConfig[TargetConfig](opts)
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
		cfg:    cfg,
		slog:   slog.Default().With("context", "HTTP Target"),
		client: client,
		stopCh: make(chan struct{}),
	}, nil
}

type HTTPTarget struct {
	cfg    *TargetConfig
	slog   *slog.Logger
	client *fasthttp.Client
	stopCh chan struct{}
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

	method := strings.ToUpper(s.cfg.Method)
	url := s.cfg.URL

	s.slog.Debug("publishing", "method", method, "url", url, "metadata", metadata, "bodysize", len(data))

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethod(method)
	for k, v := range s.cfg.Headers {
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
	if s.stopCh != nil {
		close(s.stopCh)
	}
	return
}
