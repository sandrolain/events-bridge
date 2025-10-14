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
	coapudpclient "github.com/plgd-dev/go-coap/v3/udp/client"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

type TargetConfig struct {
	Protocol CoAPProtocol  `mapstructure:"protocol" default:"udp" validate:"oneof=udp tcp dtls"`
	Address  string        `mapstructure:"address" validate:"required"`
	Path     string        `mapstructure:"path" validate:"required"`
	Method   string        `mapstructure:"method" validate:"required,oneof=GET POST PUT"`
	Timeout  time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	// DTLS fields
	PSKIdentity string `mapstructure:"pskIdentity"`
	PSK         string `mapstructure:"psk"` // supports plain, env:VAR, or file:/abs/path
	TLSCertFile string `mapstructure:"tlsCertFile"`
	TLSKeyFile  string `mapstructure:"tlsKeyFile"`
}

func NewTargetConfig() any {
	return new(TargetConfig)
}

// NewTarget creates a CoAP target from options map.
func NewTarget(anyCfg any) (connectors.Target, error) {
	cfg, ok := anyCfg.(*TargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}
	if err := validateTargetSecurity(cfg); err != nil {
		return nil, err
	}
	t := &CoAPTarget{
		cfg:    cfg,
		slog:   slog.Default().With("context", "CoAP Target"),
		stopCh: make(chan struct{}),
	}
	return t, nil
}

type CoAPTarget struct {
	cfg    *TargetConfig
	slog   *slog.Logger
	stopCh chan struct{}
}

func (t *CoAPTarget) Consume(msg *message.RunnerMessage) error {
	metadata, data, err := msg.GetMetadataAndData()
	if err != nil {
		return fmt.Errorf("error getting metadata and data: %w", err)
	}

	contentFormat := coapmessage.TextPlain
	if metadata != nil {
		contentFormat = coapTypeFromMetadata(metadata)
	}

	method := strings.ToUpper(t.cfg.Method)
	path := t.cfg.Path
	address := t.cfg.Address
	protocol := string(t.cfg.Protocol)
	t.slog.Debug("sending coap message", "protocol", protocol, "address", address, "path", path, "method", method)

	ctx, cancel := context.WithTimeout(context.Background(), t.cfg.Timeout)
	defer cancel()

	switch protocol {
	case "udp":
		err = t.sendUDP(ctx, method, path, address, contentFormat, data)
	case "tcp":
		err = t.sendTCP(ctx, method, path, address, contentFormat, data)
	case "dtls":
		err = t.sendDTLS(ctx, method, path, address, contentFormat, data)
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

func (t *CoAPTarget) sendDTLS(ctx context.Context, method, path, address string, contentFormat coapmessage.MediaType, data []byte) error {
	var client *coapudpclient.Conn
	var err error

	if t.cfg.PSK != "" {
		client, err = buildDTLSClientPSK(t.cfg.PSKIdentity, t.cfg.PSK, address)
	} else {
		client, err = buildDTLSClientCert(t.cfg.TLSCertFile, t.cfg.TLSKeyFile, address)
	}

	if err != nil {
		return fmt.Errorf("failed to create DTLS client: %w", err)
	}
	defer func() {
		if e := client.Close(); e != nil {
			t.slog.Error("error closing dtls client", "err", e)
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
	if t.stopCh != nil {
		close(t.stopCh)
	}
	return nil
}

const errUnsupportedCoapMethod = "unsupported coap method: %s"

// validateTargetSecurity validates DTLS-related security configuration for targets.
func validateTargetSecurity(cfg *TargetConfig) error {
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
