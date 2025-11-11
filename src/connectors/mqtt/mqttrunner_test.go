package main

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/utils"
)

func TestMQTTRunnerNewRunnerValidation(t *testing.T) {
	// missing address
	if err := utils.ParseConfig(map[string]any{"address": "", "topic": "t", "clientId": "cid", "topicFromMetadataKey": "topic", "qos": 1}, new(RunnerConfig)); err == nil {
		t.Fatal("expected error when address is empty")
	}
	// missing topic
	if err := utils.ParseConfig(map[string]any{"address": "localhost:1883", "topic": "", "clientId": "cid", "topicFromMetadataKey": "topic", "qos": 1}, new(RunnerConfig)); err == nil {
		t.Fatal("expected error when topic is empty")
	}
}

func TestMQTTRunnerCloseWithoutStart(t *testing.T) {
	// Construct runner with minimal config but don't connect a real broker
	tgt := &MQTTRunner{}
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

	// Runner publishes to topic
	tIface := mustNewMQTTRunner(t, map[string]any{"address": addr, "topic": "ab/cd", "clientId": "tgt1", "topicFromMetadataKey": "topic", "qos": 1})
	defer tIface.Close() //nolint:errcheck

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("ping"), meta: map[string]string{"topic": "ab/cd"}})
	if err := tIface.Process(rm); err != nil {
		t.Fatalf("runner process: %v", err)
	}

	select {
	case got := <-ch:
		data, err := got.GetData()
		if err != nil {
			t.Fatalf("failed to get data: %v", err)
		}
		if string(data) != "ping" {
			t.Fatalf("unexpected payload: %s", string(data))
		}
		if err := got.Ack(nil); err != nil {
			t.Logf("failed to ack: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestMQTTRunnerDynamicTopicFromMetadataIntegration(t *testing.T) {
	addr, cleanup := startMochi(t)
	defer cleanup()

	sIface := mustNewMQTTSource(t, map[string]any{"address": addr, "topic": "dyn/+", "clientId": "src2", "consumerGroup": "grp"})
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close() //nolint:errcheck

	tIface := mustNewMQTTRunner(t, map[string]any{"address": addr, "topic": "unused", "clientId": "tgt2", "topicFromMetadataKey": "topic", "qos": 1})
	defer tIface.Close() //nolint:errcheck

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("dyn")})
	rm.AddMetadata("topic", "dyn/x")
	if err := tIface.Process(rm); err != nil {
		t.Fatalf("runner process: %v", err)
	}

	select {
	case got := <-ch:
		data, err := got.GetData()
		if err != nil {
			t.Fatalf("failed to get data: %v", err)
		}
		if string(data) != "dyn" {
			t.Fatalf("unexpected payload: %s", string(data))
		}
		if err := got.Ack(nil); err != nil {
			t.Logf("failed to ack: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func mustNewMQTTRunner(t *testing.T, opts map[string]any) *MQTTRunner {
	t.Helper()
	cfg := new(RunnerConfig)
	if err := utils.ParseConfig(opts, cfg); err != nil {
		t.Fatalf("failed to parse runner config: %v", err)
	}
	tgt, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mqttTgt, ok := tgt.(*MQTTRunner)
	if !ok {
		t.Fatalf("expected *MQTTRunner, got %T", tgt)
	}
	return mqttTgt
}
