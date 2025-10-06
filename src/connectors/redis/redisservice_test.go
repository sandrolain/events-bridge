package main

import (
	"context"
	"testing"
	"time"
)

const (
	errFmtNewService   = "NewService returned error: %v"
	errFmtCloseService = "failed to close service: %v"
)

func TestRedisServiceCallSetGet(t *testing.T) {
	srv := newMiniredis(t)

	cfg := &ServiceConfig{
		Address: srv.Addr(),
		Timeout: time.Second,
	}

	serviceAny, err := NewService(cfg)
	if err != nil {
		t.Fatalf(errFmtNewService, err)
	}
	service := serviceAny.(*RedisService)
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Fatalf(errFmtCloseService, err)
		}
	})

	if _, err := service.Call("SET", []any{"key", "value"}); err != nil {
		t.Fatalf("SET call returned error: %v", err)
	}

	data, err := service.Call("GET", []any{"key"})
	if err != nil {
		t.Fatalf("GET call returned error: %v", err)
	}

	if string(data) != "value" {
		t.Fatalf("expected GET result 'value', got %q", string(data))
	}
}

func TestRedisServiceCallHandlesNumbers(t *testing.T) {
	srv := newMiniredis(t)

	cfg := &ServiceConfig{
		Address: srv.Addr(),
		Timeout: time.Second,
	}

	serviceAny, err := NewService(cfg)
	if err != nil {
		t.Fatalf(errFmtNewService, err)
	}
	service := serviceAny.(*RedisService)
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Fatalf(errFmtCloseService, err)
		}
	})

	if _, err := service.Call("SET", []any{"counter", 42}); err != nil {
		t.Fatalf("SET counter returned error: %v", err)
	}

	data, err := service.Call("GET", []any{"counter"})
	if err != nil {
		t.Fatalf("GET counter returned error: %v", err)
	}

	if string(data) != "42" {
		t.Fatalf("expected GET counter '42', got %q", string(data))
	}
}

func TestRedisServiceCallInvalidCommand(t *testing.T) {
	srv := newMiniredis(t)

	cfg := &ServiceConfig{
		Address: srv.Addr(),
		Timeout: time.Second,
	}

	serviceAny, err := NewService(cfg)
	if err != nil {
		t.Fatalf(errFmtNewService, err)
	}
	service := serviceAny.(*RedisService)
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Fatalf(errFmtCloseService, err)
		}
	})

	if _, err := service.Call("NOSUCH", nil); err == nil {
		t.Fatal("expected error calling NOSUCH command")
	}
}

func TestRedisServiceClose(t *testing.T) {
	srv := newMiniredis(t)

	cfg := &ServiceConfig{
		Address: srv.Addr(),
		Timeout: time.Second,
	}

	serviceAny, err := NewService(cfg)
	if err != nil {
		t.Fatalf(errFmtNewService, err)
	}
	service := serviceAny.(*RedisService)

	if err := service.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	if err := service.client.Ping(context.Background()).Err(); err == nil {
		t.Fatalf("expected ping after close to fail")
	}
}

func TestNewServiceConfigReturnsPointer(t *testing.T) {
	if _, ok := NewServiceConfig().(*ServiceConfig); !ok {
		t.Fatalf("NewServiceConfig should return *ServiceConfig")
	}
}
