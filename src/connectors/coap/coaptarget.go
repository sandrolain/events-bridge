// Implementation of a configurable CoAP target (endpoint, path, protocol)
package main

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
	"github.com/sandrolain/events-bridge/src/targets"
)

func NewTarget(cfg *targets.TargetCoAPConfig) (targets.Target, error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = targets.DefaultTimeout
	}
	t := &CoAPTarget{
		config:  cfg,
		timeout: timeout,
		slog:    slog.Default().With("context", "COAP"),
		stopped: false,
		stopCh:  make(chan struct{}),
	}
	return t, nil
}

type CoAPTarget struct {
	slog    *slog.Logger
	config  *targets.TargetCoAPConfig
	timeout time.Duration
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
					t.slog.Error("error publishing message", "err", err)
					if err := msg.Nak(); err != nil {
						t.slog.Error("error naking message", "err", err)
					}
				} else {
					if err := msg.Ack(); err != nil {
						t.slog.Error("error acking message", "err", err)
					}
				}
			}
		}
	}()
	return nil
}

// Send is exported only for integration tests
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

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	switch protocol {
	case "udp":
		client, e := coapudp.Dial(address)
		if e != nil {
			return fmt.Errorf("failed to dial coap server: %w", e)
		}

		defer func ()  {
			err = client.Close()
			if err != nil {
				t.slog.Error("error closing coap client", "err", err)
			}
		}()
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

		defer func() {
			err = client.Close()
			if err != nil {
				t.slog.Error("error closing coap client", "err", err)
			}
		}()
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
