package main

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/valyala/fasthttp"
)

func NewSource(cfg *sources.SourceHTTPConfig) (sources.Source, error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = sources.DefaultTimeout
	}

	return &HTTPSource{
		config:  cfg,
		slog:    slog.Default().With("context", "HTTP"),
		timeout: timeout,
	}, nil
}

type HTTPSource struct {
	config   *sources.SourceHTTPConfig
	slog     *slog.Logger
	listener net.Listener
	c        chan *message.RunnerMessage
	started  bool
	timeout  time.Duration
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

			// Wait for Ack or Nak
			select {
			case status := <-done:
				switch status {
				case message.ResponseStatusAck:
					ctx.SetStatusCode(fasthttp.StatusAccepted)
				case message.ResponseStatusNak:
					ctx.SetStatusCode(fasthttp.StatusInternalServerError)
				default:
					ctx.SetStatusCode(fasthttp.StatusInternalServerError)
				}
			case r := <-reply:
				for k, v := range r.Metadata {
					ctx.Response.Header.Add(k, v)
				}
				ctx.SetStatusCode(fasthttp.StatusOK)
				ctx.SetBody(r.Data)
			case <-time.After(s.timeout):
				ctx.SetStatusCode(fasthttp.StatusGatewayTimeout)
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
	if s.c != nil {
		close(s.c)
	}
	err = s.listener.Close()
	return
}
