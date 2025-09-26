package main

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

func TestNATSTargetNewTargetValidation(t *testing.T) {
	if _, err := NewTarget(map[string]any{"address": "", "subject": "s"}); err == nil {
		t.Fatal("expected error when address is empty")
	}
	if _, err := NewTarget(map[string]any{"address": "127.0.0.1:4222", "subject": ""}); err == nil {
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

	sIface, err := NewSource(map[string]any{"address": addr, "subject": "ab.*"})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close()

	tIface, err := NewTarget(map[string]any{"address": addr, "subject": "ab.cd"})
	if err != nil {
		t.Fatalf("NewTarget: %v", err)
	}
	defer tIface.Close()

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("ping"), meta: message.MessageMetadata{"subject": "ab.cd"}})
	if err := tIface.Consume(rm); err != nil {
		t.Fatalf("target consume: %v", err)
	}

	select {
	case got := <-ch:
		data, _ := got.GetTargetData()
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

	sIface, err := NewSource(map[string]any{"address": addr, "subject": "dyn.*"})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close()

	tIface, err := NewTarget(map[string]any{"address": addr, "subject": "unused", "subjectFromMetadataKey": "subject"})
	if err != nil {
		t.Fatalf("NewTarget: %v", err)
	}
	defer tIface.Close()

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("dyn")})
	rm.SetMetadata("subject", "dyn.x")
	if err := tIface.Consume(rm); err != nil {
		t.Fatalf("target consume: %v", err)
	}

	select {
	case got := <-ch:
		data, _ := got.GetTargetData()
		if string(data) != "dyn" {
			t.Fatalf("unexpected payload: %s", string(data))
		}
		_ = got.Ack()
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}
