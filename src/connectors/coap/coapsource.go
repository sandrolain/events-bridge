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

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/common"
	"github.com/sandrolain/events-bridge/src/message"
)

type SourceConfig struct {
	Protocol CoAPProtocol  `mapstructure:"protocol" default:"udp" validate:"oneof=udp tcp"`
	Address  string        `mapstructure:"address" validate:"required"`
	Path     string        `mapstructure:"path" validate:"required"`
	Method   string        `mapstructure:"method" validate:"required"`
	Timeout  time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`
}

// NewSource creates a CoAP source from options map.
func NewSource(opts map[string]any) (connectors.Source, error) {
	cfg, err := common.ParseConfig[SourceConfig](opts)
	if err != nil {
		return nil, err
	}
	return &CoAPSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "CoAP Source"),
	}, nil
}

type CoAPSource struct {
	cfg  *SourceConfig
	slog *slog.Logger
	c    chan *message.RunnerMessage
	conn *coapnet.UDPConn
}

func (s *CoAPSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting CoAP server", "protocol", s.cfg.Protocol, "addr", s.cfg.Address, "method", s.cfg.Method, "path", s.cfg.Path)

	router := coapmux.NewRouter()
	router.Use(loggingMiddleware)
	e := router.Handle(s.cfg.Path, coapmux.HandlerFunc(s.handleCoAP))
	if e != nil {
		err := fmt.Errorf("failed to handle CoAP path %q: %w", s.cfg.Path, e)
		return nil, err
	}

	go func() {
		err := coap.ListenAndServe(string(s.cfg.Protocol), s.cfg.Address, router)
		if err != nil {
			s.slog.Error("failed to start CoAP server", "err", err)
		}
	}()

	return s.c, nil
}

const failResponseError = "failed to send CoAP response"

func (s *CoAPSource) handleCoAP(w coapmux.ResponseWriter, req *coapmux.Message) {
	method := req.Code().String()
	path, _ := req.Options().Path()

	s.slog.Debug("received CoAP request", "method", method, "path", path)

	if s.cfg.Method != "" && method != s.cfg.Method {
		err := w.SetResponse(coapcodes.MethodNotAllowed, coapmessage.TextPlain, nil)
		if err != nil {
			s.slog.Error(failResponseError, "err", err)
		}
		return
	}

	if path != s.cfg.Path {
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

	r, status, timeout := common.AwaitReplyOrStatus(s.cfg.Timeout, done, reply)
	var err error
	if timeout {
		err = w.SetResponse(coapcodes.GatewayTimeout, coapmessage.TextPlain, nil)
	} else if r != nil {
		contentFormat := coapTypeFromMetadata(r.Metadata)
		err = w.SetResponse(coapcodes.Content, contentFormat, bytes.NewReader(r.Data))
	} else if status != nil {
		switch *status {
		case message.ResponseStatusAck:
			err = w.SetResponse(coapcodes.Changed, coapmessage.TextPlain, nil)
		case message.ResponseStatusNak:
			err = w.SetResponse(coapcodes.InternalServerError, coapmessage.TextPlain, nil)
		default:
			err = w.SetResponse(coapcodes.InternalServerError, coapmessage.TextPlain, nil)
		}
	}
	if err != nil {
		s.slog.Error("failed to set CoAP response", "err", err)
	}
}

func (s *CoAPSource) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

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

func coapTypeFromMetadata(md message.MessageMetadata) coapmessage.MediaType {
	for k, v := range md {
		if bytes.EqualFold([]byte(k), []byte("Content-Type")) {
			return coapTypeFromContentType(v)
		}
	}
	return coapmessage.TextPlain
}
