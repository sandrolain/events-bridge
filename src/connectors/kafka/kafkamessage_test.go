package main

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestKafkaMessageGetters(t *testing.T) {
	km := &KafkaMessage{
		msg: &kafka.Message{
			Topic:     "test-topic",
			Partition: 3,
			Offset:    42,
			Key:       []byte("key123"),
			Value:     []byte("payload"),
		},
		reader: nil, // don't test Ack here to avoid network calls
	}

	if string(km.GetID()) != "key123" {
		t.Fatalf("unexpected id: %s", string(km.GetID()))
	}

	meta, err := km.GetMetadata()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta["topic"] != "test-topic" || meta["partition"] != "3" || meta["offset"] != "42" {
		t.Fatalf("unexpected metadata: %#v", meta)
	}

	data, err := km.GetData()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("unexpected data: %s", string(data))
	}

	if err := km.Nak(); err != nil {
		t.Fatalf("unexpected Nak error: %v", err)
	}
	if err := km.Ack(nil); err != nil {
		t.Fatalf("unexpected Reply error: %v", err)
	}
}
