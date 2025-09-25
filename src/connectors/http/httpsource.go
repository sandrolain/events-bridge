package main

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/sandrolain/events-bridge/src/utils"
	"github.com/valyala/fasthttp"
)

type SourceConfig struct {
	Address string        `yaml:"address" json:"address"`
	Method  string        `yaml:"method" json:"method"`
	Path    string        `yaml:"path" json:"path"`
	Timeout time.Duration `yaml:"timeout" json:"timeout"`
}

// parseSourceOptions decodes a generic options map into the connector-specific config.
// Expected keys: address, method, path, timeout.
func parseSourceOptions(opts map[string]any) (*SourceConfig, error) {
	op := &utils.OptsParser{}
	cfg := &SourceConfig{}
	cfg.Address = op.OptString(opts, "address", "", utils.StringNonEmpty())
	cfg.Method = op.OptString(opts, "method", "", utils.StringNonEmpty())
	cfg.Path = op.OptString(opts, "path", "", utils.StringNonEmpty())
	cfg.Timeout = op.OptDuration(opts, "timeout", sources.DefaultTimeout)
	if err := op.Error(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// NewSource creates an HTTP source from options map.
func NewSource(opts map[string]any) (sources.Source, error) {
	cfg, err := parseSourceOptions(opts)
	if err != nil {
		return nil, err
	}
	return &HTTPSource{
		config: cfg,
		slog:   slog.Default().With("context", "HTTP"),
	}, nil
}

type HTTPSource struct {
	config   *SourceConfig
	slog     *slog.Logger
	listener net.Listener
	c        chan *message.RunnerMessage
	started  bool
}

func (s *HTTPSource) Produce(buffer int) (res <-chan *message.RunnerMessage, err error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting HTTP server", "addr", s.config.Address, "method", s.config.Method, "path", s.config.Path)

	// TODO: manage TLS?
	var e error
	s.listener, e = net.Listen("tcp", s.config.Address)
	if e != nil {
		err = fmt.Errorf("failed to listen: %w", e)
		return
	}

	go func() {

		reqMethod := s.config.Method
		reqPath := s.config.Path

		e = fasthttp.Serve(s.listener, func(ctx *fasthttp.RequestCtx) {
			method := string(ctx.Method())
			path := string(ctx.Path())

			s.slog.Debug("received HTTP request", "method", method, "path", path)

			if method != reqMethod {
				ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
				return
			}

			if path != reqPath {
				ctx.SetStatusCode(fasthttp.StatusNotFound)
				return
			}

			done := make(chan message.ResponseStatus, 1)
			reply := make(chan *message.ReplyData, 1)

			msg := &HTTPMessage{
				httpCtx: ctx,
				done:    done,
				reply:   reply,
			}

			s.c <- message.NewRunnerMessage(msg)

			// Wait for Ack/Nak or reply using helper
			r, status, timeout := utils.AwaitReplyOrStatus(s.config.Timeout, done, reply)
			if timeout {
				ctx.SetStatusCode(fasthttp.StatusGatewayTimeout)
				return
			}
			if r != nil {
				for k, v := range r.Metadata {
					ctx.Response.Header.Add(k, v)
				}
				ctx.SetStatusCode(fasthttp.StatusOK)
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
				return
			}
		})
		if e != nil {
			err = fmt.Errorf("failed to start HTTP server: %w", e)
		}
	}()

	s.started = true

	res = s.c

	return
}

func (s *HTTPSource) Close() (err error) {
	if s.listener != nil {
		err = s.listener.Close()
	}
	return
}
