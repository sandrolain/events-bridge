package main

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/common"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/valyala/fasthttp"
)

type SourceConfig struct {
	Address string        `mapstructure:"address" validate:"required"`
	Method  string        `mapstructure:"method" validate:"required"`
	Path    string        `mapstructure:"path" validate:"required"`
	Timeout time.Duration `mapstructure:"timeout" default:"5s" validate:"required"`
}

// NewSource creates an HTTP source from options map.
func NewSource(opts map[string]any) (connectors.Source, error) {
	cfg, err := common.ParseConfig[SourceConfig](opts)
	if err != nil {
		return nil, err
	}
	return &HTTPSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "HTTP Source"),
	}, nil
}

type HTTPSource struct {
	cfg      *SourceConfig
	slog     *slog.Logger
	c        chan *message.RunnerMessage
	listener net.Listener
}

func (s *HTTPSource) Produce(buffer int) (res <-chan *message.RunnerMessage, err error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting HTTP server", "addr", s.cfg.Address, "method", s.cfg.Method, "path", s.cfg.Path)

	// TODO: manage TLS?
	var e error
	s.listener, e = net.Listen("tcp", s.cfg.Address)
	if e != nil {
		err = fmt.Errorf("failed to listen: %w", e)
		return
	}

	go func() {

		reqMethod := s.cfg.Method
		reqPath := s.cfg.Path

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
			r, status, timeout := common.AwaitReplyOrStatus(s.cfg.Timeout, done, reply)
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

	res = s.c

	return
}

func (s *HTTPSource) Close() (err error) {
	if s.listener != nil {
		err = s.listener.Close()
	}
	return
}
