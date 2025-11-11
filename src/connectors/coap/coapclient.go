package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"

	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	coaptcp "github.com/plgd-dev/go-coap/v3/tcp"
	coaptcpclient "github.com/plgd-dev/go-coap/v3/tcp/client"
	coapudp "github.com/plgd-dev/go-coap/v3/udp"
	coapudpclient "github.com/plgd-dev/go-coap/v3/udp/client"
)

// CoAPClientResponse holds the response from a CoAP request
type CoAPClientResponse struct {
	Code    coapcodes.Code
	Payload []byte
}

// coapClient is an interface that abstracts UDP, TCP, and DTLS clients
type coapClient interface {
	Post(ctx context.Context, path string, contentFormat coapmessage.MediaType, body []byte) (*CoAPClientResponse, error)
	Put(ctx context.Context, path string, contentFormat coapmessage.MediaType, body []byte) (*CoAPClientResponse, error)
	Get(ctx context.Context, path string) (*CoAPClientResponse, error)
	Close() error
}

// coapClientConn is the interface that both UDP and TCP CoAP client connections implement
// This allows us to use a single generic wrapper for both protocols
type coapClientConn interface {
	Post(ctx context.Context, path string, contentFormat coapmessage.MediaType, body io.ReadSeeker, opts ...coapmessage.Option) (*pool.Message, error)
	Put(ctx context.Context, path string, contentFormat coapmessage.MediaType, body io.ReadSeeker, opts ...coapmessage.Option) (*pool.Message, error)
	Get(ctx context.Context, path string, opts ...coapmessage.Option) (*pool.Message, error)
	Close() error
}

// clientWrapper is a generic wrapper for CoAP client connections (UDP/TCP/DTLS)
// It eliminates code duplication by providing a single implementation for all protocols
type clientWrapper[T coapClientConn] struct {
	client   T
	logger   *slog.Logger
	protocol string // "udp", "tcp", or "dtls" for logging purposes
}

func (w *clientWrapper[T]) Post(ctx context.Context, path string, contentFormat coapmessage.MediaType, body []byte) (*CoAPClientResponse, error) {
	resp, err := w.client.Post(ctx, path, contentFormat, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	payload, err := resp.ReadBody()
	if err != nil {
		w.logger.Warn("failed to read response body", "error", err)
	}
	return &CoAPClientResponse{Code: resp.Code(), Payload: payload}, nil
}

func (w *clientWrapper[T]) Put(ctx context.Context, path string, contentFormat coapmessage.MediaType, body []byte) (*CoAPClientResponse, error) {
	resp, err := w.client.Put(ctx, path, contentFormat, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	payload, err := resp.ReadBody()
	if err != nil {
		w.logger.Warn("failed to read response body", "error", err)
	}
	return &CoAPClientResponse{Code: resp.Code(), Payload: payload}, nil
}

func (w *clientWrapper[T]) Get(ctx context.Context, path string) (*CoAPClientResponse, error) {
	resp, err := w.client.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	payload, err := resp.ReadBody()
	if err != nil {
		w.logger.Warn("failed to read response body", "error", err)
	}
	return &CoAPClientResponse{Code: resp.Code(), Payload: payload}, nil
}

func (w *clientWrapper[T]) Close() error {
	if err := w.client.Close(); err != nil {
		w.logger.Error("error closing "+w.protocol+" client", "err", err)
		return err
	}
	return nil
}

// sendCoAPRequest executes a CoAP request and returns the response
func sendCoAPRequest(ctx context.Context, client coapClient, method, path string, contentFormat coapmessage.MediaType, data []byte) (*CoAPClientResponse, error) {
	switch method {
	case "POST":
		return client.Post(ctx, path, contentFormat, data)
	case "PUT":
		return client.Put(ctx, path, contentFormat, data)
	case "GET":
		return client.Get(ctx, path)
	default:
		return nil, fmt.Errorf("unsupported coap method: %s", method)
	}
}

// createUDPClient creates a UDP CoAP client wrapper
func createUDPClient(address string, logger *slog.Logger) (coapClient, error) {
	client, err := coapudp.Dial(address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial coap udp server: %w", err)
	}
	return &clientWrapper[*coapudpclient.Conn]{
		client:   client,
		logger:   logger,
		protocol: "udp",
	}, nil
}

// createTCPClient creates a TCP CoAP client wrapper
func createTCPClient(address string, logger *slog.Logger) (coapClient, error) {
	client, err := coaptcp.Dial(address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial coap tcp server: %w", err)
	}
	return &clientWrapper[*coaptcpclient.Conn]{
		client:   client,
		logger:   logger,
		protocol: "tcp",
	}, nil
}

// createDTLSClient creates a DTLS CoAP client wrapper
func createDTLSClient(pskIdentity, psk, certFile, keyFile, address string, logger *slog.Logger) (coapClient, error) {
	var client *coapudpclient.Conn
	var err error

	if psk != "" {
		client, err = buildDTLSClientPSK(pskIdentity, psk, address)
	} else {
		client, err = buildDTLSClientCert(certFile, keyFile, address)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create DTLS client: %w", err)
	}
	return &clientWrapper[*coapudpclient.Conn]{
		client:   client,
		logger:   logger,
		protocol: "dtls",
	}, nil
}
