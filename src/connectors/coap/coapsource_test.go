package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
	coaptcp "github.com/plgd-dev/go-coap/v3/tcp"
	coapudp "github.com/plgd-dev/go-coap/v3/udp"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/utils"
)

const (
	testPath = "/ingest"
)

const (
	errPostFailed    = "post failed: %v"
	errUDPDial       = "udp dial failed: %v"
	errTCPDial       = "tcp dial failed: %v"
	errNilRunnerMsg  = "received nil runner message"
	errClientTimeout = "timeout waiting for client response"
)

func startCoAPSource(t *testing.T, protocol CoAPProtocol, addr string, method string, timeout time.Duration) (<-chan *message.RunnerMessage, func()) {
	t.Helper()
	opts := map[string]any{
		"protocol": string(protocol),
		"address":  addr,
		"path":     testPath,
		"method":   method,
		"timeout":  timeout.String(),
	}
	cfg := new(SourceConfig)
	if err := utils.ParseConfig(opts, cfg); err != nil {
		t.Fatalf("failed to parse source config: %v", err)
	}
	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	ch, err := src.Produce(1)
	if err != nil {
		t.Fatalf("failed to start source: %v", err)
	}
	// small delay to ensure server bind
	time.Sleep(150 * time.Millisecond)
	return ch, func() { _ = src.Close() }
}

func TestCoAPSourceUDPAckChanged(t *testing.T) {
	addr := "127.0.0.1:56841"
	ch, stop := startCoAPSource(t, CoAPProtocolUDP, addr, "POST", 2*time.Second)
	defer stop()

	// Start client request in background
	respCh := make(chan coapcodes.Code, 1)
	errCh := make(chan error, 1)
	go func() {
		cli, err := coapudp.Dial(addr)
		if err != nil {
			errCh <- err
			return
		}
		defer cli.Close()
		payload := []byte("hello-udp-ack")
		resp, err := cli.Post(context.Background(), testPath, coapmessage.AppJSON, bytes.NewReader(payload))
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp.Code()
	}()

	rm := <-ch
	if rm == nil {
		t.Fatal(errNilRunnerMsg)
	}
	_ = rm.Ack()

	select {
	case err := <-errCh:
		t.Fatalf(errPostFailed, err)
	case code := <-respCh:
		if code != coapcodes.Changed {
			t.Fatalf("expected Changed, got: %v", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal(errClientTimeout)
	}
}

func TestCoAPSourceUDPNakInternalError(t *testing.T) {
	addr := "127.0.0.1:56843"
	ch, stop := startCoAPSource(t, CoAPProtocolUDP, addr, "POST", 2*time.Second)
	defer stop()

	respCh := make(chan coapcodes.Code, 1)
	errCh := make(chan error, 1)
	go func() {
		cli, err := coapudp.Dial(addr)
		if err != nil {
			errCh <- err
			return
		}
		defer cli.Close()
		resp, err := cli.Post(context.Background(), testPath, coapmessage.AppJSON, bytes.NewReader([]byte("nak")))
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp.Code()
	}()

	rm := <-ch
	if rm == nil {
		t.Fatal(errNilRunnerMsg)
	}
	_ = rm.Nak()

	select {
	case err := <-errCh:
		t.Fatalf(errPostFailed, err)
	case code := <-respCh:
		if code != coapcodes.InternalServerError {
			t.Fatalf("expected InternalServerError, got: %v", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal(errClientTimeout)
	}
}

func TestCoAPSourceUDPReplyContentJSON(t *testing.T) {
	addr := "127.0.0.1:56845"
	ch, stop := startCoAPSource(t, CoAPProtocolUDP, addr, "POST", 2*time.Second)
	defer stop()

	expected := []byte(`{"ok":true}`)

	respCh := make(chan coapcodes.Code, 1)
	errCh := make(chan error, 1)
	go func() {
		cli, err := coapudp.Dial(addr)
		if err != nil {
			errCh <- err
			return
		}
		defer cli.Close()
		resp, err := cli.Post(context.Background(), testPath, coapmessage.AppJSON, bytes.NewReader([]byte("ping")))
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp.Code()
	}()

	rm := <-ch
	if rm == nil {
		t.Fatal(errNilRunnerMsg)
	}
	rm.SetData(expected)
	rm.AddMetadata("Content-Type", "application/json")
	_ = rm.Reply()

	select {
	case err := <-errCh:
		t.Fatalf(errPostFailed, err)
	case code := <-respCh:
		if code != coapcodes.Content {
			t.Fatalf("expected Content, got: %v", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal(errClientTimeout)
	}
}

func TestCoAPSourceUDPTimeoutGatewayTimeout(t *testing.T) {
	addr := "127.0.0.1:56847"
	ch, stop := startCoAPSource(t, CoAPProtocolUDP, addr, "POST", 200*time.Millisecond)
	defer stop()
	_ = ch // we will read once to avoid blocking

	respCh := make(chan coapcodes.Code, 1)
	errCh := make(chan error, 1)
	go func() {
		cli, err := coapudp.Dial(addr)
		if err != nil {
			errCh <- err
			return
		}
		defer cli.Close()
		resp, err := cli.Post(context.Background(), testPath, coapmessage.AppJSON, bytes.NewReader([]byte("timeout")))
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp.Code()
	}()

	// Read the message but do not respond to trigger timeout
	rm := <-ch
	if rm == nil {
		t.Fatal(errNilRunnerMsg)
	}

	select {
	case err := <-errCh:
		t.Fatalf(errPostFailed, err)
	case code := <-respCh:
		if code != coapcodes.GatewayTimeout {
			t.Fatalf("expected GatewayTimeout, got: %v", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal(errClientTimeout)
	}
}

func TestCoAPSourceTCPAckChanged(t *testing.T) {
	addr := "127.0.0.1:56842"
	ch, stop := startCoAPSource(t, CoAPProtocolTCP, addr, "POST", 2*time.Second)
	defer stop()

	respCh := make(chan coapcodes.Code, 1)
	errCh := make(chan error, 1)
	go func() {
		cli, err := coaptcp.Dial(addr)
		if err != nil {
			errCh <- err
			return
		}
		defer cli.Close()
		payload := []byte("hello-tcp-ack")
		resp, err := cli.Post(context.Background(), testPath, coapmessage.AppJSON, bytes.NewReader(payload))
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp.Code()
	}()

	rm := <-ch
	if rm == nil {
		t.Fatalf(errNilRunnerMsg)
	}
	_ = rm.Ack()

	select {
	case err := <-errCh:
		t.Fatalf(errPostFailed, err)
	case code := <-respCh:
		if code != coapcodes.Changed {
			t.Fatalf("expected Changed, got: %v", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal(errClientTimeout)
	}
}

func TestCoAPSourceTCPReplyContentJSON(t *testing.T) {
	addr := "127.0.0.1:56846"
	ch, stop := startCoAPSource(t, CoAPProtocolTCP, addr, "POST", 2*time.Second)
	defer stop()

	expected := []byte(`{"ok":true}`)

	respCh := make(chan coapcodes.Code, 1)
	errCh := make(chan error, 1)
	go func() {
		cli, err := coaptcp.Dial(addr)
		if err != nil {
			errCh <- err
			return
		}
		defer cli.Close()
		resp, err := cli.Post(context.Background(), testPath, coapmessage.AppJSON, bytes.NewReader([]byte("ping")))
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp.Code()
	}()

	rm := <-ch
	if rm == nil {
		t.Fatal(errNilRunnerMsg)
	}
	rm.SetData(expected)
	rm.AddMetadata("Content-Type", "application/json")
	_ = rm.Reply()

	select {
	case err := <-errCh:
		t.Fatalf(errPostFailed, err)
	case code := <-respCh:
		if code != coapcodes.Content {
			t.Fatalf("expected Content, got: %v", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for client response")
	}
}

func TestCoAPSourceTCPTimeoutGatewayTimeout(t *testing.T) {
	addr := "127.0.0.1:56848"
	ch, stop := startCoAPSource(t, CoAPProtocolTCP, addr, "POST", 200*time.Millisecond)
	defer stop()
	_ = ch

	respCh := make(chan coapcodes.Code, 1)
	errCh := make(chan error, 1)
	go func() {
		cli, err := coaptcp.Dial(addr)
		if err != nil {
			errCh <- err
			return
		}
		defer cli.Close()
		resp, err := cli.Post(context.Background(), testPath, coapmessage.AppJSON, bytes.NewReader([]byte("timeout")))
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp.Code()
	}()

	rm := <-ch
	if rm == nil {
		t.Fatalf("received nil runner message")
	}

	select {
	case err := <-errCh:
		t.Fatalf(errPostFailed, err)
	case code := <-respCh:
		if code != coapcodes.GatewayTimeout {
			t.Fatalf("expected GatewayTimeout, got: %v", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for client response")
	}
}
