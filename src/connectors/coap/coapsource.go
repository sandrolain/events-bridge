package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"log/slog"
	"time"

	"github.com/plgd-dev/go-coap/v3"
	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
	coapmux "github.com/plgd-dev/go-coap/v3/mux"
	coapnet "github.com/plgd-dev/go-coap/v3/net"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

type SourceConfig struct {
	Protocol CoAPProtocol  `mapstructure:"protocol" default:"udp" validate:"oneof=udp tcp dtls"`
	Address  string        `mapstructure:"address" validate:"required"`
	Path     string        `mapstructure:"path" validate:"required"`
	Method   string        `mapstructure:"method" validate:"required"`
	Timeout  time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	// MaxPayloadSize limits the request body size in bytes (CoAP default MTU-based typical 1152)
	MaxPayloadSize int `mapstructure:"maxPayloadSize" default:"1152" validate:"gte=0"`
	// DTLS Pre-Shared Key identity
	PSKIdentity string `mapstructure:"pskIdentity"`
	// DTLS Pre-Shared Key secret (supports plain, env:VAR, or file:/abs/path)
	PSK string `mapstructure:"psk"`
	// Use certificate-based DTLS (mutually exclusive with PSK)
	TLSCertFile string `mapstructure:"tlsCertFile"`
	TLSKeyFile  string `mapstructure:"tlsKeyFile"`
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

// NewSource creates a CoAP source from options map.
func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}
	if err := validateSourceSecurity(cfg); err != nil {
		return nil, err
	}
	return &CoAPSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "CoAP Source"),
	}, nil
}

// validateSourceSecurity validates DTLS-related security configuration.
// Rules:
// - If protocol != dtls: ignore DTLS fields.
// - If protocol == dtls: either PSK+PSKIdentity OR (TLSCertFile & TLSKeyFile) must be provided.
// - PSK and cert mode are mutually exclusive.
func validateSourceSecurity(cfg *SourceConfig) error {
	if cfg.Protocol != CoAPProtocolDTLS {
		return nil
	}
	hasPSK := cfg.PSK != "" || cfg.PSKIdentity != ""
	hasCert := cfg.TLSCertFile != "" || cfg.TLSKeyFile != ""
	if hasPSK && hasCert {
		return fmt.Errorf("psk/pskIdentity and tlsCertFile/tlsKeyFile are mutually exclusive")
	}
	if !hasPSK && !hasCert {
		return fmt.Errorf("dtls requires either psk+pskIdentity or tlsCertFile+tlsKeyFile")
	}
	if hasPSK {
		if cfg.PSK == "" || cfg.PSKIdentity == "" {
			return fmt.Errorf("both psk and pskIdentity must be provided for DTLS PSK mode")
		}
	} else {
		if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
			return fmt.Errorf("both tlsCertFile and tlsKeyFile must be provided for DTLS certificate mode")
		}
	}
	return nil
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

	if s.cfg.Protocol == CoAPProtocolDTLS {
		if err := buildDTLSServer(s.cfg, router, s.slog); err != nil {
			return nil, fmt.Errorf("failed to start DTLS server: %w", err)
		}
	} else {
		go func() {
			err := coap.ListenAndServe(string(s.cfg.Protocol), s.cfg.Address, router)
			if err != nil {
				s.slog.Error("failed to start CoAP server", "err", err)
			}
		}()
	}

	return s.c, nil
}

const failResponseError = "failed to send CoAP response"

func (s *CoAPSource) handleCoAP(w coapmux.ResponseWriter, req *coapmux.Message) {
	method := req.Code().String()
	path, err := req.Options().Path()
	if err != nil {
		s.slog.Warn("failed to get request path", "error", err)
	}

	s.slog.Debug("received CoAP request", "method", method, "path", path)

	// Validate method and path
	if err := s.validateRequest(w, method, path); err != nil {
		return
	}

	// Read and validate payload
	preloaded, err := s.readRequestBody(w, req)
	if err != nil {
		return
	}

	// Create message and send to channel
	msg := s.createCoAPMessage(req, w, preloaded)
	s.c <- message.NewRunnerMessage(msg)

	// Wait for response and send CoAP reply
	s.sendCoAPResponse(w, msg)
}

// validateRequest checks if method and path match configuration
func (s *CoAPSource) validateRequest(w coapmux.ResponseWriter, method, path string) error {
	if s.cfg.Method != "" && method != s.cfg.Method {
		err := w.SetResponse(coapcodes.MethodNotAllowed, coapmessage.TextPlain, nil)
		if err != nil {
			s.slog.Error(failResponseError, "err", err)
		}
		return fmt.Errorf("method not allowed: %s", method)
	}

	if path != s.cfg.Path {
		err := w.SetResponse(coapcodes.NotFound, coapmessage.TextPlain, nil)
		if err != nil {
			s.slog.Error(failResponseError, "err", err)
		}
		return fmt.Errorf("path not found: %s", path)
	}

	return nil
}

// readRequestBody reads and validates the request body size
func (s *CoAPSource) readRequestBody(w coapmux.ResponseWriter, req *coapmux.Message) ([]byte, error) {
	body := req.Body()
	if body == nil {
		return nil, nil
	}

	var buf bytes.Buffer
	if s.cfg.MaxPayloadSize > 0 {
		lr := &io.LimitedReader{R: body, N: int64(s.cfg.MaxPayloadSize) + 1}
		n, err := buf.ReadFrom(lr)
		if err != nil {
			s.slog.Warn("failed to read from limited reader", "error", err)
		}
		if n > int64(s.cfg.MaxPayloadSize) {
			if err := w.SetResponse(coapcodes.RequestEntityTooLarge, coapmessage.TextPlain, nil); err != nil {
				s.slog.Error(failResponseError, "err", err)
			}
			return nil, fmt.Errorf("payload too large: %d bytes", n)
		}
	} else {
		if _, err := buf.ReadFrom(body); err != nil {
			s.slog.Warn("failed to read from body", "error", err)
		}
	}

	return buf.Bytes(), nil
}

// createCoAPMessage creates a CoAPMessage with channels
func (s *CoAPSource) createCoAPMessage(req *coapmux.Message, w coapmux.ResponseWriter, data []byte) *CoAPMessage {
	done := make(chan message.ResponseStatus)
	reply := make(chan *message.ReplyData)
	return &CoAPMessage{
		req:   req,
		w:     w,
		done:  done,
		reply: reply,
		data:  data,
	}
}

// sendCoAPResponse waits for response and sends CoAP reply
func (s *CoAPSource) sendCoAPResponse(w coapmux.ResponseWriter, msg *CoAPMessage) {
	r, status, timeout := message.AwaitReplyOrStatus(s.cfg.Timeout, msg.done, msg.reply)

	var err error
	if timeout {
		err = w.SetResponse(coapcodes.GatewayTimeout, coapmessage.TextPlain, nil)
	} else if r != nil {
		contentFormat := coapTypeFromMetadata(r.Metadata)
		err = w.SetResponse(coapcodes.Content, contentFormat, bytes.NewReader(r.Data))
	} else if status != nil {
		err = s.setStatusResponse(w, *status)
	}

	if err != nil {
		s.slog.Error("failed to set CoAP response", "err", err)
	}
}

// setStatusResponse converts message status to CoAP response code
func (s *CoAPSource) setStatusResponse(w coapmux.ResponseWriter, status message.ResponseStatus) error {
	switch status {
	case message.ResponseStatusAck:
		return w.SetResponse(coapcodes.Changed, coapmessage.TextPlain, nil)
	case message.ResponseStatusNak:
		return w.SetResponse(coapcodes.InternalServerError, coapmessage.TextPlain, nil)
	default:
		return w.SetResponse(coapcodes.InternalServerError, coapmessage.TextPlain, nil)
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
		// Avoid logging full message payloads to prevent sensitive data leakage
		p, err := r.Options().Path()
		if err != nil {
			log.Printf("Failed to get path: %v", err)
		}
		log.Printf("ClientAddress %v, Code %v, Path %v\n", w.Conn().RemoteAddr(), r.Code(), p)
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

func coapTypeFromMetadata(md map[string]string) coapmessage.MediaType {
	for k, v := range md {
		if bytes.EqualFold([]byte(k), []byte("Content-Type")) {
			return coapTypeFromContentType(v)
		}
	}
	return coapmessage.TextPlain
}
