package httpsource

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/valyala/fasthttp"
)

func New(cfg *sources.SourceHTTPConfig) (res sources.Source, err error) {
	conn := &HTTPSource{
		config: cfg,
		slog:   slog.Default().With("context", "HTTP"),
	}

	res = conn

	return
}

type HTTPSource struct {
	config   *sources.SourceHTTPConfig
	slog     *slog.Logger
	listener net.Listener
	c        chan message.Message
	started  bool
}

func (s *HTTPSource) Produce(buffer int) (res <-chan message.Message, err error) {
	s.c = make(chan message.Message, buffer)

	s.slog.Info("starting HTTP server", "addr", s.config.Address, "method", s.config.Method, "path", s.config.Path)

	// TODO: manage TLS?
	var e error
	s.listener, e = net.Listen("tcp", s.config.Address)
	if e != nil {
		err = fmt.Errorf("failed to listen: %w", e)
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

			done := make(chan responseStatus)
			msg := &HTTPMessage{
				httpCtx: ctx,
				done:    done,
			}

			s.c <- msg

			// Attendi Ack o Nak
			select {
			case status := <-done:
				switch status {
				case statusAck:
					ctx.SetStatusCode(fasthttp.StatusAccepted)
				case statusNak:
					ctx.SetStatusCode(fasthttp.StatusInternalServerError)
				default:
					ctx.SetStatusCode(fasthttp.StatusInternalServerError)
				}
			case <-time.After(10 * time.Second): // timeout di esempio
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
