// Implementazione di un target CoAP configurabile (endpoint, path, protocol)
package coaptarget

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coaptcp "github.com/plgd-dev/go-coap/v3/tcp"
	coapudp "github.com/plgd-dev/go-coap/v3/udp"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets/target"
)

type CoAPProtocol string

const (
	CoAPProtocolUDP CoAPProtocol = "udp"
	CoAPProtocolTCP CoAPProtocol = "tcp"
)

type TargetCoAPConfig struct {
	Protocol CoAPProtocol  `yaml:"protocol" json:"protocol" validate:"required,oneof=udp tcp"`
	Address  string        `yaml:"address" json:"address" validate:"required,hostname_port"`
	Path     string        `yaml:"path" json:"path" validate:"required"`
	Method   string        `yaml:"method" json:"method" validate:"omitempty,oneof=POST PUT GET"`
	Timeout  time.Duration `yaml:"timeout" json:"timeout" validate:"required"`
}

func New(cfg *TargetCoAPConfig) (target.Target, error) {
	t := &CoAPTarget{
		config:  cfg,
		slog:    slog.Default().With("context", "COAP"),
		stopped: false,
		stopCh:  make(chan struct{}),
	}
	return t, nil
}

type CoAPTarget struct {
	slog    *slog.Logger
	config  *TargetCoAPConfig
	stopped bool
	stopCh  chan struct{}
}

func (t *CoAPTarget) Consume(c <-chan message.Message) error {
	go func() {
		for {
			select {
			case <-t.stopCh:
				return
			case msg := <-c:
				err := t.send(msg)
				if err != nil {
					msg.Nak()
					t.slog.Error("error sending coap message", "err", err)
				} else {
					msg.Ack()
				}
			}
		}
	}()
	return nil
}

// Send è esportato solo per i test di integrazione
func (t *CoAPTarget) Send(msg message.Message) error {
	return t.send(msg)
}

func (t *CoAPTarget) send(msg message.Message) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	method := strings.ToUpper(t.config.Method)
	path := t.config.Path
	address := t.config.Address
	protocol := string(t.config.Protocol)
	t.slog.Debug("sending coap message", "protocol", protocol, "address", address, "path", path, "method", method)

	ctx := context.Background()

	switch protocol {
	case "udp":
		client, e := coapudp.Dial(address)
		if e != nil {
			return fmt.Errorf("failed to dial coap server: %w", e)
		}

		defer client.Close()
		switch method {
		case "POST":
			_, err = client.Post(ctx, path, coapmessage.AppCBOR, strings.NewReader(string(data)))
		case "PUT":
			_, err = client.Put(ctx, path, coapmessage.AppCBOR, strings.NewReader(string(data)))
		case "GET":
			_, err = client.Get(ctx, path)
		default:
			return fmt.Errorf(errUnsupportedCoapMethod, method)
		}
	case "tcp":
		client, e := coaptcp.Dial(address)
		if e != nil {
			return fmt.Errorf("failed to dial coap server: %w", e)
		}

		defer client.Close()
		switch method {
		case "POST":
			_, err = client.Post(ctx, path, coapmessage.AppCBOR, strings.NewReader(string(data)))
		case "PUT":
			_, err = client.Put(ctx, path, coapmessage.AppCBOR, strings.NewReader(string(data)))
		case "GET":
			_, err = client.Get(ctx, path)
		default:
			return fmt.Errorf(errUnsupportedCoapMethod, method)
		}
	default:
		return fmt.Errorf("unsupported coap protocol: %s", protocol)
	}

	if err != nil {
		return fmt.Errorf("error sending coap request: %w", err)
	}

	t.slog.Debug("coap message sent")
	return nil
}

func (t *CoAPTarget) Close() error {
	t.stopped = true
	if t.stopCh != nil {
		close(t.stopCh)
	}
	return nil
}

const errUnsupportedCoapMethod = "unsupported coap method: %s"
