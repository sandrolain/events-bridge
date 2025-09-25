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
	"github.com/sandrolain/events-bridge/src/utils"
)

type TargetConfig struct {
	Protocol CoAPProtocol  `yaml:"protocol" json:"protocol"`
	Address  string        `yaml:"address" json:"address"`
	Path     string        `yaml:"path" json:"path"`
	Method   string        `yaml:"method" json:"method"`
	Timeout  time.Duration `yaml:"timeout" json:"timeout"`
}

// parseTargetOptions builds a CoAP target config from options map with validation.
// Expected keys: protocol (udp|tcp), address, path, method (GET|POST|PUT), timeout.
func parseTargetOptions(opts map[string]any) (*TargetConfig, error) {
	// Local minimal parser to avoid extra deps; integrates simple validations.
	cfg := &TargetConfig{}
	op := &utils.OptsParser{}
	cfg.Protocol = CoAPProtocol(op.OptString(opts, "protocol", "udp", utils.StringOneOf("udp", "tcp")))
	cfg.Address = op.OptString(opts, "address", "", utils.StringNonEmpty())
	cfg.Path = op.OptString(opts, "path", "", utils.StringNonEmpty())
	cfg.Method = op.OptString(opts, "method", "", utils.StringOneOf("GET", "POST", "PUT"))
	cfg.Timeout = op.OptDuration(opts, "timeout", targets.DefaultTimeout)
	if err := op.Error(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// NewTarget creates a CoAP target from options map.
func NewTarget(opts map[string]any) (targets.Target, error) {
	cfg, err := parseTargetOptions(opts)
	if err != nil {
		return nil, err
	}
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
	config  *TargetConfig
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

	ctx, cancel := context.WithTimeout(context.Background(), t.config.Timeout)
	defer cancel()

	switch protocol {
	case "udp":
		err = t.sendUDP(ctx, method, path, address, contentFormat, data)
	case "tcp":
		err = t.sendTCP(ctx, method, path, address, contentFormat, data)
	default:
		return fmt.Errorf("unsupported coap protocol: %s", protocol)
	}

	if err != nil {
		return fmt.Errorf("error sending coap request: %w", err)
	}

	t.slog.Debug("coap message sent")
	return nil
}

func (t *CoAPTarget) sendUDP(ctx context.Context, method, path, address string, contentFormat coapmessage.MediaType, data []byte) error {
	client, err := coapudp.Dial(address)
	if err != nil {
		return fmt.Errorf("failed to dial coap server: %w", err)
	}
	defer func() {
		if e := client.Close(); e != nil {
			t.slog.Error("error closing coap client", "err", e)
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
	return err
}

func (t *CoAPTarget) sendTCP(ctx context.Context, method, path, address string, contentFormat coapmessage.MediaType, data []byte) error {
	client, err := coaptcp.Dial(address)
	if err != nil {
		return fmt.Errorf("failed to dial coap server: %w", err)
	}
	defer func() {
		if e := client.Close(); e != nil {
			t.slog.Error("error closing coap client", "err", e)
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
	return err
}

func (t *CoAPTarget) Close() error {
	t.stopped = true
	if t.stopCh != nil {
		close(t.stopCh)
	}
	return nil
}

const errUnsupportedCoapMethod = "unsupported coap method: %s"
