package main

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/utils"
)

func TestMQTTTargetNewTargetValidation(t *testing.T) {
	// missing address
	if err := utils.ParseConfig(map[string]any{"address": "", "topic": "t", "clientId": "cid", "topicFromMetadataKey": "topic", "qos": 1}, new(TargetConfig)); err == nil {
		t.Fatal("expected error when address is empty")
	}
	// missing topic
	if err := utils.ParseConfig(map[string]any{"address": "localhost:1883", "topic": "", "clientId": "cid", "topicFromMetadataKey": "topic", "qos": 1}, new(TargetConfig)); err == nil {
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
	sIface := mustNewMQTTSource(t, map[string]any{"address": addr, "topic": "ab/#", "clientId": "src1", "consumerGroup": "grp"})
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close() //nolint:errcheck

	// Target publishes to topic
	tIface := mustNewMQTTTarget(t, map[string]any{"address": addr, "topic": "ab/cd", "clientId": "tgt1", "topicFromMetadataKey": "topic", "qos": 1})
	defer tIface.Close() //nolint:errcheck

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("ping"), meta: map[string]string{"topic": "ab/cd"}})
	if err := tIface.Consume(rm); err != nil {
		t.Fatalf("target consume: %v", err)
	}

	select {
	case got := <-ch:
		data, _ := got.GetData()
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

	sIface := mustNewMQTTSource(t, map[string]any{"address": addr, "topic": "dyn/+", "clientId": "src2", "consumerGroup": "grp"})
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close() //nolint:errcheck

	tIface := mustNewMQTTTarget(t, map[string]any{"address": addr, "topic": "unused", "clientId": "tgt2", "topicFromMetadataKey": "topic", "qos": 1})
	defer tIface.Close() //nolint:errcheck

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("dyn")})
	rm.AddMetadata("topic", "dyn/x")
	if err := tIface.Consume(rm); err != nil {
		t.Fatalf("target consume: %v", err)
	}

	select {
	case got := <-ch:
		data, _ := got.GetData()
		if string(data) != "dyn" {
			t.Fatalf("unexpected payload: %s", string(data))
		}
		_ = got.Ack()
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func mustNewMQTTTarget(t *testing.T, opts map[string]any) *MQTTTarget {
	t.Helper()
	cfg := new(TargetConfig)
	if err := utils.ParseConfig(opts, cfg); err != nil {
		t.Fatalf("failed to parse target config: %v", err)
	}
	tgt, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mqttTgt, ok := tgt.(*MQTTTarget)
	if !ok {
		t.Fatalf("expected *MQTTTarget, got %T", tgt)
	}
	return mqttTgt
}
