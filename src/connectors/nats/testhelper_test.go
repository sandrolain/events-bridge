package main

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/sandrolain/events-bridge/src/message"
)

// startNATSServer starts an embedded NATS server on an ephemeral port.
// Returns address (host:port) and a cleanup function.
func startNATSServer(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot get free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	port := addr[strings.LastIndex(addr, ":")+1:]
	opts := &server.Options{
		Host:            "127.0.0.1",
		Port:            mustAtoi(port),
		NoSystemAccount: true,
		JetStream:       false,
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed creating nats server: %v", err)
	}
	go srv.Start()

	if !srv.ReadyForConnections(2 * time.Second) {
		t.Fatal("nats server not ready")
	}

	cleanup := func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	}
	return addr, cleanup
}

func mustAtoi(s string) int {
	var n int
	for i := 0; i < len(s); i++ {
		n = n*10 + int(s[i]-'0')
	}
	return n
}

// testSrcMsg is a minimal implementation of message.SourceMessage for tests
type testSrcMsg struct {
	data []byte
	meta message.MessageMetadata
}

func (t *testSrcMsg) GetID() []byte                                 { return []byte{0, 1} }
func (t *testSrcMsg) GetMetadata() (message.MessageMetadata, error) { return t.meta, nil }
func (t *testSrcMsg) GetData() ([]byte, error)                      { return t.data, nil }
func (t *testSrcMsg) Ack() error                                    { return nil }
func (t *testSrcMsg) Nak() error                                    { return nil }
func (t *testSrcMsg) Reply(_ *message.ReplyData) error              { return nil }
