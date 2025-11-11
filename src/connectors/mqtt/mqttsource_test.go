package main

import (
	"fmt"
	"testing"
	"time"

	mqttc "github.com/eclipse/paho.mqtt.golang"

	"github.com/sandrolain/events-bridge/src/utils"
)

func TestMQTTSourceNewSourceValidation(t *testing.T) {
	// missing address
	if err := utils.ParseConfig(map[string]any{"address": "", "topic": "t", "clientId": "cid", "consumerGroup": "grp"}, new(SourceConfig)); err == nil {
		t.Fatal("expected error when address is empty")
	}
	// missing topic
	if err := utils.ParseConfig(map[string]any{"address": "localhost:1883", "topic": "", "clientId": "cid", "consumerGroup": "grp"}, new(SourceConfig)); err == nil {
		t.Fatal("expected error when topic is empty")
	}
}

func TestMQTTSourceCloseWithoutStart(t *testing.T) {
	src := mustNewMQTTSource(t, map[string]any{"address": "localhost:1883", "topic": "t", "clientId": "cid", "consumerGroup": "grp"})
	if err := src.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestMQTTSharedSubscriptionBasic(t *testing.T) {
	addr, cleanup := startMochi(t)
	defer cleanup()

	s1 := mustNewMQTTSource(t, map[string]any{"address": addr, "topic": "share/#", "clientId": "s1", "consumerGroup": "grp"})
	ch1, err := s1.Produce(10)
	if err != nil {
		t.Fatalf("s1 produce: %v", err)
	}
	defer s1.Close() //nolint:errcheck

	s2 := mustNewMQTTSource(t, map[string]any{"address": addr, "topic": "share/#", "clientId": "s2", "consumerGroup": "grp"})
	ch2, err := s2.Produce(10)
	if err != nil {
		t.Fatalf("s2 produce: %v", err)
	}
	defer s2.Close() //nolint:errcheck

	time.Sleep(500 * time.Millisecond)

	// publisher
	opts := mqttc.NewClientOptions().AddBroker("tcp://" + addr)
	opts.SetClientID(fmt.Sprintf("test-pub-shared-%d", time.Now().UnixNano()))
	pc := mqttc.NewClient(opts)
	ct := pc.Connect()
	if ok := ct.WaitTimeout(3 * time.Second); !ok || ct.Error() != nil {
		t.Fatalf("publisher connect: %v", ct.Error())
	}
	for i := 0; i < 10; i++ {
		pt := pc.Publish("share/x", 0, false, []byte(fmt.Sprintf("m%d", i)))
		pt.Wait()
		if pt.Error() != nil {
			t.Fatalf("publish err: %v", pt.Error())
		}
		time.Sleep(50 * time.Millisecond)
	}
	pc.Disconnect(250)

	got1, got2 := 0, 0
	timeout := time.After(5 * time.Second)
	for got1+got2 < 10 {
		select {
		case m := <-ch1:
			if err := m.Ack(nil); err != nil {
				t.Logf("failed to ack message: %v", err)
			}
			got1++
		case m := <-ch2:
			if err := m.Ack(nil); err != nil {
				t.Logf("failed to ack message: %v", err)
			}
			got2++
		case <-timeout:
			t.Fatalf("timeout waiting shared messages, got1=%d got2=%d", got1, got2)
		}
	}
	if got1 == 0 || got2 == 0 {
		t.Fatalf("expected distribution across consumers, got1=%d got2=%d", got1, got2)
	}
}

func mustNewMQTTSource(t *testing.T, opts map[string]any) *MQTTSource {
	t.Helper()
	cfg := new(SourceConfig)
	if err := utils.ParseConfig(opts, cfg); err != nil {
		t.Fatalf("failed to parse source config: %v", err)
	}
	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mqttSrc, ok := src.(*MQTTSource)
	if !ok {
		t.Fatalf("expected *MQTTSource, got %T", src)
	}
	return mqttSrc
}
