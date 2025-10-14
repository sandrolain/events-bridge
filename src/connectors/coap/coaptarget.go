// Implementation of a configurable CoAP target (endpoint, path, protocol)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	coapmessage "github.com/plgd-dev/go-coap/v3/message"
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
		err = t.sendCoAP(ctx, method, path, address, contentFormat, data, "udp")
	case "tcp":
		err = t.sendCoAP(ctx, method, path, address, contentFormat, data, "tcp")
	case "dtls":
		err = t.sendCoAP(ctx, method, path, address, contentFormat, data, "dtls")
	default:
		return fmt.Errorf("unsupported coap protocol: %s", protocol)
	}

	if err != nil {
		return fmt.Errorf("error sending coap request: %w", err)
	}

	t.slog.Debug("coap message sent")
	return nil
}

func (t *CoAPTarget) sendCoAP(ctx context.Context, method, path, address string, contentFormat coapmessage.MediaType, data []byte, protocol string) error {
	var client coapClient
	var err error

	switch protocol {
	case "udp":
		client, err = createUDPClient(address, t.slog)
	case "tcp":
		client, err = createTCPClient(address, t.slog)
	case "dtls":
		client, err = createDTLSClient(t.cfg.PSKIdentity, t.cfg.PSK, t.cfg.TLSCertFile, t.cfg.TLSKeyFile, address, t.slog)
	default:
		return fmt.Errorf("unsupported coap protocol: %s", protocol)
	}

	if err != nil {
		return err
	}
	defer client.Close()

	_, err = sendCoAPRequest(ctx, client, method, path, contentFormat, data)
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
