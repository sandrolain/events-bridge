package main

import (
    "testing"
    "time"

    "github.com/sandrolain/events-bridge/src/message"
    "github.com/sandrolain/events-bridge/src/sources"
    "github.com/sandrolain/events-bridge/src/targets"
)

func TestNATSTargetNewTargetValidation(t *testing.T) {
    if _, err := NewTarget(&targets.TargetNATSConfig{Address: "", Subject: "s"}); err == nil {
        t.Fatal("expected error when address is empty")
    }
    if _, err := NewTarget(&targets.TargetNATSConfig{Address: "127.0.0.1:4222", Subject: ""}); err == nil {
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

    srcCfg := &sources.SourceNATSConfig{Address: addr, Subject: "ab.*"}
    sIface, err := NewSource(srcCfg)
    if err != nil { t.Fatalf("NewSource: %v", err) }
    ch, err := sIface.Produce(1)
    if err != nil { t.Fatalf("Produce: %v", err) }
    defer sIface.Close()

    tgtCfg := &targets.TargetNATSConfig{Address: addr, Subject: "ab.cd"}
    tIface, err := NewTarget(tgtCfg)
    if err != nil { t.Fatalf("NewTarget: %v", err) }
    defer tIface.Close()

    rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("ping"), meta: message.MessageMetadata{"subject": "ab.cd"}})
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

func TestNATSTargetDynamicSubjectFromMetadataIntegration(t *testing.T) {
    addr, cleanup := startNATSServer(t)
    defer cleanup()

    srcCfg := &sources.SourceNATSConfig{Address: addr, Subject: "dyn.*"}
    sIface, err := NewSource(srcCfg)
    if err != nil { t.Fatalf("NewSource: %v", err) }
    ch, err := sIface.Produce(1)
    if err != nil { t.Fatalf("Produce: %v", err) }
    defer sIface.Close()

    tgtCfg := &targets.TargetNATSConfig{Address: addr, Subject: "unused", SubjectFromMetadataKey: "subject"}
    tIface, err := NewTarget(tgtCfg)
    if err != nil { t.Fatalf("NewTarget: %v", err) }
    defer tIface.Close()

    rm := message.NewRunnerMessage(&testSrcMsg{data: []byte("dyn")})
    rm.SetMetadata("subject", "dyn.x")
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
