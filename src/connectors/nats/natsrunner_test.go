package main

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/utils"
)

func TestNATSRunnerNewRunnerValidation(t *testing.T) {
	if err := utils.ParseConfig(map[string]any{"address": "", "subject": "s"}, new(RunnerConfig)); err == nil {
		t.Fatal("expected error when address is empty")
	}
	if err := utils.ParseConfig(map[string]any{"address": "127.0.0.1:4222", "subject": ""}, new(RunnerConfig)); err == nil {
		t.Fatal("expected error when subject is empty")
	}
}

func TestNATSRunnerCloseWithoutStart(t *testing.T) {
	tgt := &NATSRunner{}
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

	tIface := mustNewNATSRunner(t, map[string]any{"address": addr, "subject": "ab.cd"})
	defer tIface.Close() //nolint:errcheck

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("ping"), meta: map[string]string{"subject": "ab.cd"}})
	if err := tIface.Process(rm); err != nil {
		t.Fatalf("target consume: %v", err)
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

func TestNATSRunnerDynamicSubjectFromMetadataIntegration(t *testing.T) {
	addr, cleanup := startNATSServer(t)
	defer cleanup()

	sIface := mustNewNATSSource(t, map[string]any{"address": addr, "subject": "dyn.*"})
	ch, err := sIface.Produce(1)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	defer sIface.Close() //nolint:errcheck

	tIface := mustNewNATSRunner(t, map[string]any{"address": addr, "subject": "unused", "subjectFromMetadataKey": "subject"})
	defer tIface.Close() //nolint:errcheck

	rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("dyn")})
	rm.AddMetadata("subject", "dyn.x")
	if err := tIface.Process(rm); err != nil {
		t.Fatalf("target consume: %v", err)
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
			t.Logf("failed to ack message: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func mustNewNATSRunner(t *testing.T, opts map[string]any) *NATSRunner {
	t.Helper()
	cfg := new(RunnerConfig)
	if err := utils.ParseConfig(opts, cfg); err != nil {
		t.Fatalf("failed to parse target config: %v", err)
	}
	tgt, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	natsTgt, ok := tgt.(*NATSRunner)
	if !ok {
		t.Fatalf("expected *NATSRunner, got %T", tgt)
	}
	return natsTgt
}
