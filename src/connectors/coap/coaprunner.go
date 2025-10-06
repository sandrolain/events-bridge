package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
	coaptcp "github.com/plgd-dev/go-coap/v3/tcp"
	coapudp "github.com/plgd-dev/go-coap/v3/udp"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure CoAPRunner implements connectors.Runner
var _ connectors.Runner = (*CoAPRunner)(nil)

type CoAPRunnerConfig struct {
	Protocol CoAPProtocol  `mapstructure:"protocol" default:"udp" validate:"oneof=udp tcp"`
	Address  string        `mapstructure:"address" validate:"required"`
	Path     string        `mapstructure:"path" validate:"required"`
	Method   string        `mapstructure:"method" validate:"required,oneof=GET POST PUT"`
	Timeout  time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`
}

func NewRunnerConfig() any { //nolint:revive
	return new(CoAPRunnerConfig)
}

func NewRunner(anyCfg any) (connectors.Runner, error) { //nolint:revive
	cfg, ok := anyCfg.(*CoAPRunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}
	return &CoAPRunner{
		cfg:  cfg,
		slog: slog.Default().With("context", "CoAP Runner"),
	}, nil
}

type CoAPRunner struct {
	cfg  *CoAPRunnerConfig
	slog *slog.Logger
}

func (r *CoAPRunner) Process(msg *message.RunnerMessage) error {
	metadata, data, err := msg.GetMetadataAndData()
	if err != nil {
		return fmt.Errorf("error getting metadata and data: %w", err)
	}

	method := strings.ToUpper(r.cfg.Method)
	path := r.cfg.Path
	address := r.cfg.Address
	protocol := string(r.cfg.Protocol)

	r.slog.Debug("executing CoAP runner request", "protocol", protocol, "address", address, "path", path, "method", method, "bodysize", len(data))

	contentFormat := coapTypeFromMetadata(metadata)

	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	defer cancel()

	var code coapcodes.Code
	var payload []byte
	switch protocol {
	case "udp":
		code, payload, err = r.sendUDP(ctx, method, path, address, contentFormat, data)
	case "tcp":
		code, payload, err = r.sendTCP(ctx, method, path, address, contentFormat, data)
	default:
		return fmt.Errorf("unsupported coap protocol: %s", protocol)
	}
	if err != nil {
		return fmt.Errorf("error performing coap request: %w", err)
	}

	// Accept 2.xx success codes only
	if code/32 != 2 { // CoAP class 2 = success
		return fmt.Errorf("non-2.xx CoAP code: %v", code)
	}

	// Overwrite payload with response body (if any)
	if len(payload) > 0 {
		msg.SetData(append([]byte(nil), payload...))
	}

	// Add status code metadata (eb-status) and content-format if present
	meta := map[string]string{
		"eb-status": code.String(),
	}
	// Derive Content-Format textual representation if we got payload and format was known
	if contentFormat != 0 { // 0 = text/plain in our mapping but safe to expose
		meta["coap-content-format"] = contentFormat.String()
	}
	msg.MergeMetadata(meta)

	r.slog.Debug("CoAP runner request completed", "code", code, "resbodysize", len(payload))
	return nil
}

func (r *CoAPRunner) sendUDP(ctx context.Context, method, path, address string, contentFormat coapmessage.MediaType, data []byte) (coapcodes.Code, []byte, error) {
	client, err := coapudp.Dial(address)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to dial coap server: %w", err)
	}
	defer func() {
		if e := client.Close(); e != nil {
			r.slog.Error("error closing coap client", "err", e)
		}
	}()
	switch method {
	case "POST":
		resp, err := client.Post(ctx, path, contentFormat, bytes.NewReader(data))
		if err != nil {
			return 0, nil, err
		}
		b, _ := resp.ReadBody()
		return resp.Code(), b, nil
	case "PUT":
		resp, err := client.Put(ctx, path, contentFormat, bytes.NewReader(data))
		if err != nil {
			return 0, nil, err
		}
		b, _ := resp.ReadBody()
		return resp.Code(), b, nil
	case "GET":
		resp, err := client.Get(ctx, path)
		if err != nil {
			return 0, nil, err
		}
		b, _ := resp.ReadBody()
		return resp.Code(), b, nil
	default:
		return 0, nil, fmt.Errorf(errUnsupportedCoapMethod, method)
	}
}

func (r *CoAPRunner) sendTCP(ctx context.Context, method, path, address string, contentFormat coapmessage.MediaType, data []byte) (coapcodes.Code, []byte, error) {
	client, err := coaptcp.Dial(address)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to dial coap server: %w", err)
	}
	defer func() {
		if e := client.Close(); e != nil {
			r.slog.Error("error closing coap client", "err", e)
		}
	}()
	switch method {
	case "POST":
		resp, err := client.Post(ctx, path, contentFormat, bytes.NewReader(data))
		if err != nil {
			return 0, nil, err
		}
		b, _ := resp.ReadBody()
		return resp.Code(), b, nil
	case "PUT":
		resp, err := client.Put(ctx, path, contentFormat, bytes.NewReader(data))
		if err != nil {
			return 0, nil, err
		}
		b, _ := resp.ReadBody()
		return resp.Code(), b, nil
	case "GET":
		resp, err := client.Get(ctx, path)
		if err != nil {
			return 0, nil, err
		}
		b, _ := resp.ReadBody()
		return resp.Code(), b, nil
	default:
		return 0, nil, fmt.Errorf(errUnsupportedCoapMethod, method)
	}
}

func (r *CoAPRunner) Close() error { //nolint:revive
	r.slog.Info("closing coap runner")
	return nil
}
