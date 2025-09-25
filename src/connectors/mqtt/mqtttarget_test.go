package main

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

func TestMQTTTargetNewTargetValidation(t *testing.T) {
	// missing address
	if _, err := NewTarget(map[string]any{"address": "", "topic": "t"}); err == nil {
		t.Fatal("expected error when address is empty")
	}
	// missing topic
	if _, err := NewTarget(map[string]any{"address": "localhost:1883", "topic": ""}); err == nil {
		t.Fatal("expected error when topic is empty")
	}
}

func TestMQTTTargetCloseWithoutStart(t *testing.T) {
	// Construct target with minimal config but don't connect a real broker
	tgt := &MQTTTarget{}
	if err := tgt.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestMQTTEndToEndTargetToSourceIntegration(t *testing.T) {
	addr, cleanup := startMochi(t)
	defer cleanup()

	// Start source on topic
	sIface, err := NewSource(map[string]any{"address": addr, "topic": "ab/#", "client_id": "src1"})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close()

	// Target publishes to topic
	tIface, err := NewTarget(map[string]any{"address": addr, "topic": "ab/cd", "clientID": "tgt1", "qos": 0})
	if err != nil {
		t.Fatalf("NewTarget: %v", err)
	}
	defer tIface.Close()

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("ping"), meta: message.MessageMetadata{"topic": "ab/cd"}})
	if err := tIface.Consume(rm); err != nil {
		t.Fatalf("target consume: %v", err)
	}

	select {
	case got := <-ch:
		data, _ := got.GetSourceData()
		if string(data) != "ping" {
			t.Fatalf("unexpected payload: %s", string(data))
		}
		_ = got.Ack()
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestMQTTTargetDynamicTopicFromMetadataIntegration(t *testing.T) {
	addr, cleanup := startMochi(t)
	defer cleanup()

	sIface, err := NewSource(map[string]any{"address": addr, "topic": "dyn/+", "client_id": "src2"})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close()

	tIface, err := NewTarget(map[string]any{"address": addr, "topic": "unused", "topicFromMetadataKey": "topic"})
	if err != nil {
		t.Fatalf("NewTarget: %v", err)
	}
	defer tIface.Close()

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("dyn")})
	rm.SetMetadata("topic", "dyn/x")
	if err := tIface.Consume(rm); err != nil {
		t.Fatalf("target consume: %v", err)
	}

	select {
	case got := <-ch:
		data, _ := got.GetSourceData()
		if string(data) != "dyn" {
			t.Fatalf("unexpected payload: %s", string(data))
		}
		_ = got.Ack()
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}
