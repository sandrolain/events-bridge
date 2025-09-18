package main

import (
	"bytes"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/plgd-dev/go-coap/v3"
	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
	coapmux "github.com/plgd-dev/go-coap/v3/mux"
	coapnet "github.com/plgd-dev/go-coap/v3/net"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
)

func NewSource(cfg *sources.SourceCoAPConfig) (sources.Source, error) {
	if cfg.Protocol != sources.CoAPProtocolUDP && cfg.Protocol != sources.CoAPProtocolTCP {
		return nil, fmt.Errorf("invalid CoAP protocol: %q (must be 'udp' or 'tcp')", cfg.Protocol)
	}
	return &CoAPSource{
		config: cfg,
		slog:   slog.Default().With("context", "CoAP"),
	}, nil
}

type CoAPSource struct {
	config  *sources.SourceCoAPConfig
	slog    *slog.Logger
	c       chan *message.RunnerMessage
	started bool
	conn    *coapnet.UDPConn
}

func (s *CoAPSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting CoAP server", "protocol", s.config.Protocol, "addr", s.config.Address, "method", s.config.Method, "path", s.config.Path)

	router := coapmux.NewRouter()
	router.Use(loggingMiddleware)
	e := router.Handle(s.config.Path, coapmux.HandlerFunc(s.handleCoAP))
	if e != nil {
		err := fmt.Errorf("failed to handle CoAP path %q: %w", s.config.Path, e)
		return nil, err
	}

	go func() {
		err := coap.ListenAndServe(string(s.config.Protocol), s.config.Address, router)
		if err != nil {
			s.slog.Error("failed to start CoAP server", "err", err)
		}
	}()

	s.started = true
	return s.c, nil
}

const failResponseError = "failed to send CoAP response"

func (s *CoAPSource) handleCoAP(w coapmux.ResponseWriter, req *coapmux.Message) {
	method := req.Code().String()
	path, _ := req.Options().Path()

	s.slog.Debug("received CoAP request", "method", method, "path", path)

	if s.config.Method != "" && method != s.config.Method {
		err := w.SetResponse(coapcodes.MethodNotAllowed, coapmessage.TextPlain, nil)
		if err != nil {
			s.slog.Error(failResponseError, "err", err)
		}
		return
	}

	if path != s.config.Path {
		err := w.SetResponse(coapcodes.NotFound, coapmessage.TextPlain, nil)
		if err != nil {
			s.slog.Error(failResponseError, "err", err)
		}
		return
	}

	done := make(chan message.ResponseStatus)
	reply := make(chan *message.ReplyData)
	msg := &CoAPMessage{
		req:   req,
		w:     w,
		done:  done,
		reply: reply,
	}

	s.c <- message.NewRunnerMessage(msg)

	var err error
	select {
	case status := <-done:
		switch status {
		case message.ResponseStatusAck:
			err = w.SetResponse(coapcodes.Changed, coapmessage.TextPlain, nil)
		case message.ResponseStatusNak:
			err = w.SetResponse(coapcodes.InternalServerError, coapmessage.TextPlain, nil)

		default:
			err = w.SetResponse(coapcodes.InternalServerError, coapmessage.TextPlain, nil)
		}
	case r := <-reply:

		// TODO: type from metadata
		err = w.SetResponse(coapcodes.Content, coapmessage.TextPlain, bytes.NewReader(r.Data))
	case <-time.After(10 * time.Second): // TODO: duration from config
		err = w.SetResponse(coapcodes.GatewayTimeout, coapmessage.TextPlain, nil)
	}
	if err != nil {
		s.slog.Error("failed to set CoAP response", "err", err)
	}
}

func (s *CoAPSource) Close() error {
	if s.c != nil {
		close(s.c)
	}
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

type responseStatus int

const (
	statusAck responseStatus = iota
	statusNak
)

// Middleware function, which will be called for each request.
func loggingMiddleware(next coapmux.Handler) coapmux.Handler {
	return coapmux.HandlerFunc(func(w coapmux.ResponseWriter, r *coapmux.Message) {
		log.Printf("ClientAddress %v, %v\n", w.Conn().RemoteAddr(), r.String())
		next.ServeCOAP(w, r)
	})
}

func coapTypeFromContentType(ct string) coapmessage.MediaType {
	b := []byte(ct)
	b = bytes.ToLower(bytes.TrimSpace(b))
	if len(b) == 0 {
		return coapmessage.TextPlain
	}
	if i := bytes.IndexByte(b, ';'); i >= 0 {
		b = bytes.TrimSpace(b[:i])
	}

	switch string(b) {
	case "text/plain":
		return coapmessage.TextPlain
	case "application/json", "text/json", "application/ld+json", "application/problem+json", "application/senml+json":
		return coapmessage.AppJSON
	case "application/cbor", "application/senml+cbor":
		return coapmessage.AppCBOR
	case "application/xml", "text/xml":
		return coapmessage.AppXML
	case "application/link-format":
		return coapmessage.AppLinkFormat
	case "application/octet-stream":
		return coapmessage.AppOctets
	default:
		return coapmessage.TextPlain
	}
}
