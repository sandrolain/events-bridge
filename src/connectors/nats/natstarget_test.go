package main

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/utils"
)

func TestNATSTargetNewTargetValidation(t *testing.T) {
	if err := utils.ParseConfig(map[string]any{"address": "", "subject": "s"}, new(TargetConfig)); err == nil {
		t.Fatal("expected error when address is empty")
	}
	if err := utils.ParseConfig(map[string]any{"address": "127.0.0.1:4222", "subject": ""}, new(TargetConfig)); err == nil {
		t.Fatal("expected error when subject is empty")
	}
}

func TestNATSTargetCloseWithoutStart(t *testing.T) {
	tgt := &NATSTarget{}
	if err := tgt.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestNATSEndToEndTargetToSourceIntegration(t *testing.T) {
	addr, cleanup := startNATSServer(t)
	defer cleanup()

	sIface := mustNewNATSSource(t, map[string]any{"address": addr, "subject": "ab.*"})
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close() //nolint:errcheck

	tIface := mustNewNATSTarget(t, map[string]any{"address": addr, "subject": "ab.cd"})
	defer tIface.Close() //nolint:errcheck

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("ping"), meta: message.MessageMetadata{"subject": "ab.cd"}})
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

func TestNATSTargetDynamicSubjectFromMetadataIntegration(t *testing.T) {
	addr, cleanup := startNATSServer(t)
	defer cleanup()

	sIface := mustNewNATSSource(t, map[string]any{"address": addr, "subject": "dyn.*"})
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close() //nolint:errcheck

	tIface := mustNewNATSTarget(t, map[string]any{"address": addr, "subject": "unused", "subjectFromMetadataKey": "subject"})
	defer tIface.Close() //nolint:errcheck

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("dyn")})
	rm.AddMetadata("subject", "dyn.x")
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

func mustNewNATSTarget(t *testing.T, opts map[string]any) *NATSTarget {
	t.Helper()
	cfg := new(TargetConfig)
	if err := utils.ParseConfig(opts, cfg); err != nil {
		t.Fatalf("failed to parse target config: %v", err)
	}
	tgt, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	natsTgt, ok := tgt.(*NATSTarget)
	if !ok {
		t.Fatalf("expected *NATSTarget, got %T", tgt)
	}
	return natsTgt
}
