package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure CoAPRunner implements connectors.Runner
var _ connectors.Runner = (*CoAPRunner)(nil)

type CoAPRunnerConfig struct {
	Protocol    CoAPProtocol  `mapstructure:"protocol" default:"udp" validate:"oneof=udp tcp dtls"`
	Address     string        `mapstructure:"address" validate:"required"`
	Path        string        `mapstructure:"path" validate:"required"`
	Method      string        `mapstructure:"method" validate:"required,oneof=GET POST PUT"`
	Timeout     time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	PSKIdentity string        `mapstructure:"pskIdentity"`
	PSK         string        `mapstructure:"psk"` // supports plain, env:VAR, or file:/abs/path
	TLSCertFile string        `mapstructure:"tlsCertFile"`
	TLSKeyFile  string        `mapstructure:"tlsKeyFile"`
}

func NewRunnerConfig() any { //nolint:revive
	return new(CoAPRunnerConfig)
}

func NewRunner(anyCfg any) (connectors.Runner, error) { //nolint:revive
	cfg, ok := anyCfg.(*CoAPRunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}
	if err := validateRunnerSecurity(cfg); err != nil {
		return nil, err
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
	case string(CoAPProtocolUDP):
		code, payload, err = r.sendCoAP(ctx, method, path, address, contentFormat, data, string(CoAPProtocolUDP))
	case string(CoAPProtocolTCP):
		code, payload, err = r.sendCoAP(ctx, method, path, address, contentFormat, data, string(CoAPProtocolTCP))
	case string(CoAPProtocolDTLS):
		code, payload, err = r.sendCoAP(ctx, method, path, address, contentFormat, data, string(CoAPProtocolDTLS))
	default:
		return fmt.Errorf("unsupported coap protocol: %s", protocol)
	}
	if err != nil {
		return fmt.Errorf("error performing coap protocol: %w", err)
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

func (r *CoAPRunner) sendCoAP(ctx context.Context, method, path, address string, contentFormat coapmessage.MediaType, data []byte, protocol string) (coapcodes.Code, []byte, error) {
	var client coapClient
	var err error

	switch protocol {
	case string(CoAPProtocolUDP):
		client, err = createUDPClient(address, r.slog)
	case string(CoAPProtocolTCP):
		client, err = createTCPClient(address, r.slog)
	case string(CoAPProtocolDTLS):
		client, err = createDTLSClient(r.cfg.PSKIdentity, r.cfg.PSK, r.cfg.TLSCertFile, r.cfg.TLSKeyFile, address, r.slog)
	default:
		return 0, nil, fmt.Errorf("unsupported coap protocol: %s", protocol)
	}

	if err != nil {
		return 0, nil, err
	}
	defer client.Close()

	resp, err := sendCoAPRequest(ctx, client, method, path, contentFormat, data)
	if err != nil {
		return 0, nil, err
	}

	return resp.Code, resp.Payload, nil
}

func (r *CoAPRunner) Close() error { //nolint:revive
	r.slog.Info("closing coap runner")
	return nil
}

// validateRunnerSecurity mirrors target/source DTLS validation logic for runner.
func validateRunnerSecurity(cfg *CoAPRunnerConfig) error {
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
