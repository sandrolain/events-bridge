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
	"github.com/sandrolain/events-bridge/src/utils"
)

type SourceConfig struct {
	Protocol CoAPProtocol  `yaml:"protocol" json:"protocol"`
	Address  string        `yaml:"address" json:"address"`
	Path     string        `yaml:"path" json:"path"`
	Method   string        `yaml:"method" json:"method"`
	Timeout  time.Duration `yaml:"timeout" json:"timeout"`
}

// NewSourceOptions builds a CoAP config from options map.
// Expected keys: protocol ("udp"|"tcp"), address, path, method, timeout (ns).
func NewSourceOptions(opts map[string]any) (sources.Source, error) {
	cfg := &SourceConfig{}
	if v, ok := opts["protocol"].(string); ok {
		if v == string(CoAPProtocolTCP) {
			cfg.Protocol = CoAPProtocolTCP
		} else {
			cfg.Protocol = CoAPProtocolUDP
		}
	}
	if v, ok := opts["address"].(string); ok {
		cfg.Address = v
	}
	if v, ok := opts["path"].(string); ok {
		cfg.Path = v
	}
	if v, ok := opts["method"].(string); ok {
		cfg.Method = v
	}
	if v, ok := opts["timeout"].(int); ok {
		cfg.Timeout = time.Duration(v)
	}
	if v, ok := opts["timeout"].(int64); ok {
		cfg.Timeout = time.Duration(v)
	}
	if v, ok := opts["timeout"].(float64); ok {
		cfg.Timeout = time.Duration(int64(v))
	}
	return NewSource(cfg)
}

func NewSource(cfg *SourceConfig) (sources.Source, error) {
	if cfg.Protocol != CoAPProtocolUDP && cfg.Protocol != CoAPProtocolTCP {
		return nil, fmt.Errorf("invalid CoAP protocol: %q (must be 'udp' or 'tcp')", cfg.Protocol)
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = sources.DefaultTimeout
	}

	return &CoAPSource{
		config:  cfg,
		slog:    slog.Default().With("context", "CoAP"),
		timeout: timeout,
	}, nil
}

type CoAPSource struct {
	config  *SourceConfig
	slog    *slog.Logger
	c       chan *message.RunnerMessage
	started bool
	conn    *coapnet.UDPConn
	timeout time.Duration
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

	r, status, timeout := utils.AwaitReplyOrStatus(s.timeout, done, reply)
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
