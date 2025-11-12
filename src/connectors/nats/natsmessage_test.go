package main

import (
	"testing"

	nats "github.com/nats-io/nats.go"
)

func TestNATSMessageBasics(t *testing.T) {
	h := nats.Header{}
	h.Set(NatsMessageIdHeader, "id-123")
	orig := &nats.Msg{Subject: "test.subject", Data: []byte("hello"), Header: h}
	m := &NATSMessage{
		msg: orig,
		metadata: map[string]string{
			"subject": "test.subject",
		},
	}

	id := m.GetID()
	if string(id) != "id-123" {
		t.Fatalf("unexpected id: %q", string(id))
	}

	meta, err := m.GetMetadata()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta["subject"] != "test.subject" {
		t.Fatalf("unexpected subject metadata: %v", meta["subject"])
	}

	data, err := m.GetData()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("unexpected data: %s", string(data))
	}

	// Reply is no-op when Reply is empty
	if err := m.Ack(nil); err != nil {
		t.Fatalf("reply error: %v", err)
	}
}
