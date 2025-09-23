package main

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/sandrolain/events-bridge/src/targets"
)

func TestMQTTTargetNewTargetValidation(t *testing.T) {
	// missing address
	if _, err := NewTarget(&targets.TargetMQTTConfig{Address: "", Topic: "t"}); err == nil {
		t.Fatal("expected error when address is empty")
	}
	// missing topic
	if _, err := NewTarget(&targets.TargetMQTTConfig{Address: "localhost:1883", Topic: ""}); err == nil {
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
	srcCfg := &sources.SourceMQTTConfig{Address: addr, Topic: "ab/#", ClientID: "src1"}
	sIface, err := NewSource(srcCfg)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close()

	// Target publishes to topic
	tgtCfg := &targets.TargetMQTTConfig{Address: addr, Topic: "ab/cd", ClientID: "tgt1", QoS: 0}
	tIface, err := NewTarget(tgtCfg)
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

	srcCfg := &sources.SourceMQTTConfig{Address: addr, Topic: "dyn/+", ClientID: "src2"}
	sIface, err := NewSource(srcCfg)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close()

	tgtCfg := &targets.TargetMQTTConfig{Address: addr, Topic: "unused", TopicFromMetadataKey: "topic"}
	tIface, err := NewTarget(tgtCfg)
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
