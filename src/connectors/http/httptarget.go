package main

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
	"github.com/valyala/fasthttp"
)

func NewTarget(cfg *targets.TargetHTTPConfig) (res targets.Target, err error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = targets.DefaultTimeout
	}

	client := &fasthttp.Client{
		ReadTimeout:                   timeout,
		WriteTimeout:                  timeout,
		NoDefaultUserAgentHeader:      true,
		DisableHeaderNamesNormalizing: true,
		DisablePathNormalizing:        true,
		Dial: (&fasthttp.TCPDialer{
			Concurrency: 4096,
			//DNSCacheDuration: time.Hour, // increase DNS cache time to an hour instead of default minute
		}).Dial,
	}

	res = &HTTPTarget{
		config: cfg,
		slog:   slog.Default().With("context", "HTTP"),
		client: client,
		stopCh: make(chan struct{}),
	}

	return
}

type HTTPTarget struct {
	slog    *slog.Logger
	config  *targets.TargetHTTPConfig
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
