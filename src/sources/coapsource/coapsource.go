package coapsource

import (
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/plgd-dev/go-coap/v3"
	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/mux"
	coapmux "github.com/plgd-dev/go-coap/v3/mux"
	coapnet "github.com/plgd-dev/go-coap/v3/net"

	msg "github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources/source"
)

type CoAPProtocol string

const (
	CoAPProtocolUDP CoAPProtocol = "udp"
	CoAPProtocolTCP CoAPProtocol = "tcp"
)

type SourceCoAPConfig struct {
	Protocol CoAPProtocol `yaml:"protocol" json:"protocol" validate:"required,oneof=udp tcp"`
	Address  string       `yaml:"address" json:"address" validate:"required,hostname_port"`
	Path     string       `yaml:"path" json:"path" validate:"required"`
	Method   string       `yaml:"method" json:"method" validate:"omitempty,oneof=POST PUT GET"`
}

func New(cfg *SourceCoAPConfig) (source.Source, error) {
	if cfg.Protocol != CoAPProtocolUDP && cfg.Protocol != CoAPProtocolTCP {
		return nil, fmt.Errorf("invalid CoAP protocol: %q (must be 'udp' or 'tcp')", cfg.Protocol)
	}
	return &CoAPSource{
		config: cfg,
		slog:   slog.Default().With("context", "CoAP"),
	}, nil
}

type CoAPSource struct {
	config   *SourceCoAPConfig
	slog     *slog.Logger
	c        chan msg.Message
	started  bool
	shutdown func()
	conn     *coapnet.UDPConn
}

func (s *CoAPSource) Produce(buffer int) (<-chan msg.Message, error) {
	s.c = make(chan msg.Message, buffer)

	s.slog.Info("starting CoAP server", "protocol", s.config.Protocol, "addr", s.config.Address, "method", s.config.Method, "path", s.config.Path)

	router := coapmux.NewRouter()
	router.Use(loggingMiddleware)
	router.Handle(s.config.Path, coapmux.HandlerFunc(s.handleCoAP))

	go func() {
		err := coap.ListenAndServe(string(s.config.Protocol), s.config.Address, router)
		if err != nil {
			s.slog.Error("failed to start CoAP server", "err", err)
		}
	}()

	s.started = true
	return s.c, nil
}

func (s *CoAPSource) handleCoAP(w coapmux.ResponseWriter, req *coapmux.Message) {
	method := req.Code().String()
	path, _ := req.Options().Path()

	s.slog.Debug("received CoAP request", "method", method, "path", path)

	if s.config.Method != "" && method != s.config.Method {
		w.SetResponse(coapcodes.MethodNotAllowed, coapmessage.TextPlain, nil)
		return
	}
	if path != s.config.Path {
		w.SetResponse(coapcodes.NotFound, coapmessage.TextPlain, nil)
		return
	}
	done := make(chan responseStatus)
	msg := &CoAPMessage{
		req:  req,
		w:    w,
		done: done,
	}

	s.c <- msg

	select {
	case status := <-done:
		switch status {
		case statusAck:
			w.SetResponse(coapcodes.Changed, coapmessage.TextPlain, nil)
		case statusNak:
			w.SetResponse(coapcodes.InternalServerError, coapmessage.TextPlain, nil)
		default:
			w.SetResponse(coapcodes.InternalServerError, coapmessage.TextPlain, nil)
		}
	case <-time.After(10 * time.Second):
		w.SetResponse(coapcodes.GatewayTimeout, coapmessage.TextPlain, nil)
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
func loggingMiddleware(next mux.Handler) mux.Handler {
	return mux.HandlerFunc(func(w mux.ResponseWriter, r *mux.Message) {
		log.Printf("ClientAddress %v, %v\n", w.Conn().RemoteAddr(), r.String())
		next.ServeCOAP(w, r)
	})
}
