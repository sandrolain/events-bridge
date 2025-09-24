// Implementation of a configurable CoAP target (endpoint, path, protocol)
package main

import (
	"bytes"
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

type TargetConfig struct {
	Protocol CoAPProtocol  `yaml:"protocol" json:"protocol"`
	Address  string        `yaml:"address" json:"address"`
	Path     string        `yaml:"path" json:"path"`
	Method   string        `yaml:"method" json:"method"`
	Timeout  time.Duration `yaml:"timeout" json:"timeout"`
}

// NewTargetOptions builds a CoAP target config from options map.
// Expected keys: protocol, address, path, method, timeout (ns).
func NewTargetOptions(opts map[string]any) (targets.Target, error) {
	cfg := &TargetConfig{}
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
	return NewTarget(cfg)
}

func NewTarget(cfg *TargetConfig) (targets.Target, error) {
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
	config  *TargetConfig
	timeout time.Duration
	stopped bool
	stopCh  chan struct{}
}

func (t *CoAPTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetTargetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}
	meta, _ := msg.GetTargetMetadata()
	contentFormat := coapmessage.TextPlain
	if meta != nil {
		contentFormat = coapTypeFromMetadata(meta)
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

		defer func() {
			err = client.Close()
			if err != nil {
				t.slog.Error("error closing coap client", "err", err)
			}
		}()
		switch method {
		case "POST":
			_, err = client.Post(ctx, path, contentFormat, bytes.NewReader(data))
		case "PUT":
			_, err = client.Put(ctx, path, contentFormat, bytes.NewReader(data))
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
			_, err = client.Post(ctx, path, contentFormat, bytes.NewReader(data))
		case "PUT":
			_, err = client.Put(ctx, path, contentFormat, bytes.NewReader(data))
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
