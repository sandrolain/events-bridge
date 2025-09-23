package main

import (
	"fmt"
	"testing"
	"time"

	mqttc "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/sources"
)

func TestMQTTSourceNewSourceValidation(t *testing.T) {
	// missing address
	if _, err := NewSource(&sources.SourceMQTTConfig{Address: "", Topic: "t"}); err == nil {
		t.Fatal("expected error when address is empty")
	}
	// missing topic
	if _, err := NewSource(&sources.SourceMQTTConfig{Address: "localhost:1883", Topic: ""}); err == nil {
		t.Fatal("expected error when topic is empty")
	}
}

func TestMQTTSourceCloseWithoutStart(t *testing.T) {
	src, err := NewSource(&sources.SourceMQTTConfig{Address: "localhost:1883", Topic: "t"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestMQTTSharedSubscriptionBasic(t *testing.T) {
	addr, cleanup := startMochi(t)
	defer cleanup()

	srcCfg1 := &sources.SourceMQTTConfig{Address: addr, Topic: "share/#", ClientID: "s1", ConsumerGroup: "grp"}
	s1, _ := NewSource(srcCfg1)
	ch1, err := s1.Produce(10)
	if err != nil {
		t.Fatalf("s1 produce: %v", err)
	}
	defer s1.Close()

	srcCfg2 := &sources.SourceMQTTConfig{Address: addr, Topic: "share/#", ClientID: "s2", ConsumerGroup: "grp"}
	s2, _ := NewSource(srcCfg2)
	ch2, err := s2.Produce(10)
	if err != nil {
		t.Fatalf("s2 produce: %v", err)
	}
	defer s2.Close()

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
			_ = m.Ack()
			got1++
		case m := <-ch2:
			_ = m.Ack()
			got2++
		case <-timeout:
			t.Fatalf("timeout waiting shared messages, got1=%d got2=%d", got1, got2)
		}
	}
	if got1 == 0 || got2 == 0 {
		t.Fatalf("expected distribution across consumers, got1=%d got2=%d", got1, got2)
	}
}
