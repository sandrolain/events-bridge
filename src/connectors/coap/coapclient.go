package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
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

// udpClientWrapper wraps a UDP client
type udpClientWrapper struct {
	client *coapudpclient.Conn
	slog   *slog.Logger
}

func (w *udpClientWrapper) Post(ctx context.Context, path string, contentFormat coapmessage.MediaType, body []byte) (*CoAPClientResponse, error) {
	resp, err := w.client.Post(ctx, path, contentFormat, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	payload, _ := resp.ReadBody()
	return &CoAPClientResponse{Code: resp.Code(), Payload: payload}, nil
}

func (w *udpClientWrapper) Put(ctx context.Context, path string, contentFormat coapmessage.MediaType, body []byte) (*CoAPClientResponse, error) {
	resp, err := w.client.Put(ctx, path, contentFormat, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	payload, _ := resp.ReadBody()
	return &CoAPClientResponse{Code: resp.Code(), Payload: payload}, nil
}

func (w *udpClientWrapper) Get(ctx context.Context, path string) (*CoAPClientResponse, error) {
	resp, err := w.client.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	payload, _ := resp.ReadBody()
	return &CoAPClientResponse{Code: resp.Code(), Payload: payload}, nil
}

func (w *udpClientWrapper) Close() error {
	if err := w.client.Close(); err != nil {
		w.slog.Error("error closing udp client", "err", err)
		return err
	}
	return nil
}

// tcpClientWrapper wraps a TCP client
type tcpClientWrapper struct {
	client *coaptcpclient.Conn
	slog   *slog.Logger
}

func (w *tcpClientWrapper) Post(ctx context.Context, path string, contentFormat coapmessage.MediaType, body []byte) (*CoAPClientResponse, error) {
	resp, err := w.client.Post(ctx, path, contentFormat, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	payload, _ := resp.ReadBody()
	return &CoAPClientResponse{Code: resp.Code(), Payload: payload}, nil
}

func (w *tcpClientWrapper) Put(ctx context.Context, path string, contentFormat coapmessage.MediaType, body []byte) (*CoAPClientResponse, error) {
	resp, err := w.client.Put(ctx, path, contentFormat, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	payload, _ := resp.ReadBody()
	return &CoAPClientResponse{Code: resp.Code(), Payload: payload}, nil
}

func (w *tcpClientWrapper) Get(ctx context.Context, path string) (*CoAPClientResponse, error) {
	resp, err := w.client.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	payload, _ := resp.ReadBody()
	return &CoAPClientResponse{Code: resp.Code(), Payload: payload}, nil
}

func (w *tcpClientWrapper) Close() error {
	if err := w.client.Close(); err != nil {
		w.slog.Error("error closing tcp client", "err", err)
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
		return nil, fmt.Errorf(errUnsupportedCoapMethod, method)
	}
}

// createUDPClient creates a UDP CoAP client wrapper
func createUDPClient(address string, logger *slog.Logger) (coapClient, error) {
	client, err := coapudp.Dial(address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial coap udp server: %w", err)
	}
	return &udpClientWrapper{client: client, slog: logger}, nil
}

// createTCPClient creates a TCP CoAP client wrapper
func createTCPClient(address string, logger *slog.Logger) (coapClient, error) {
	client, err := coaptcp.Dial(address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial coap tcp server: %w", err)
	}
	return &tcpClientWrapper{client: client, slog: logger}, nil
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
	return &udpClientWrapper{client: client, slog: logger}, nil
}
