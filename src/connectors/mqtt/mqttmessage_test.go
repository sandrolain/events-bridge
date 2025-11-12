package main

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
)

// mock implementing mqtt.Message
type mockMQTTMessage struct {
	id      uint16
	topic   string
	payload []byte
}

func (m *mockMQTTMessage) Duplicate() bool            { return false }
func (m *mockMQTTMessage) Qos() byte                  { return 0 }
func (m *mockMQTTMessage) Retained() bool             { return false }
func (m *mockMQTTMessage) Topic() string              { return m.topic }
func (m *mockMQTTMessage) MessageID() uint16          { return m.id }
func (m *mockMQTTMessage) Payload() []byte            { return m.payload }
func (m *mockMQTTMessage) Ack()                       { /* no-op for tests */ }
func (m *mockMQTTMessage) Read(_ []byte) (int, error) { return 0, nil }

func TestMQTTMessageBasics(t *testing.T) {
	orig := &mockMQTTMessage{id: 0x1234, topic: "test/topic", payload: []byte("hello")}
	mm := &MQTTMessage{
		orig: orig,
		done: make(chan message.ResponseStatus, 3),
		metadata: map[string]string{
			"topic": "test/topic",
		},
	}

	id := mm.GetID()
	if len(id) != 2 || id[0] != 0x12 || id[1] != 0x34 {
		t.Fatalf("unexpected id bytes: %v", id)
	}

	meta, err := mm.GetMetadata()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta["topic"] != "test/topic" {
		t.Fatalf("unexpected metadata topic: %v", meta["topic"])
	}

	data, err := mm.GetData()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("unexpected data: %s", string(data))
	}

	// Ack/Nak should be non-blocking thanks to buffered chan
	if err := mm.Ack(nil); err != nil {
		t.Fatalf("ack error: %v", err)
	}
	if err := mm.Nak(); err != nil {
		t.Fatalf("nak error: %v", err)
	}

	// Reply is a no-op, must not error
	if err := mm.Ack(&message.ReplyData{Data: []byte("resp")}); err != nil {
		t.Fatalf("reply error: %v", err)
	}
}
