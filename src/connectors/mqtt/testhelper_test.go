package main

import (
	"net"
	"strings"
	"testing"
	"time"

	mmqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/sandrolain/events-bridge/src/message"
)

// startMochi starts an in-process mochi-mqtt broker on an ephemeral port.
// Returns address (host:port) and a cleanup function.
func startMochi(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot get free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	server := mmqtt.New(nil)
	_ = server.AddHook(new(auth.AllowHook), nil)

	port := addr[strings.LastIndex(addr, ":")+1:]
	tcp := listeners.NewTCP(listeners.Config{ID: "t1", Address: ":" + port})
	if err := server.AddListener(tcp); err != nil {
		t.Fatalf("add listener: %v", err)
	}

	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond)

	cleanup := func() { _ = server.Close() }
	return addr, cleanup
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
