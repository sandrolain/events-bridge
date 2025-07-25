package main

import (
	"testing"

	"github.com/valyala/fasthttp"
)

type dummyResponseStatus int

func newHTTPMessageWithCtx(ctx *fasthttp.RequestCtx) *HTTPMessage {
	return &HTTPMessage{
		httpCtx: ctx,
		done:    make(chan responseStatus, 1),
	}
}

func TestHTTPMessageGetID(t *testing.T) {
	req := fasthttp.AcquireRequest()
	req.Header.Set("X-Request-ID", "test-id")
	ctx := &fasthttp.RequestCtx{}
	ctx.Init(req, nil, nil)
	msg := newHTTPMessageWithCtx(ctx)
	id := msg.GetID()
	if string(id) != "test-id" {
		t.Errorf("expected 'test-id', got '%s'", string(id))
	}
}

func TestHTTPMessageGetMetadata(t *testing.T) {
	req := fasthttp.AcquireRequest()
	req.Header.Set("Foo", "bar")
	req.Header.Add("Foo", "baz")
	req.Header.Set("Bar", "qux")
	ctx := &fasthttp.RequestCtx{}
	ctx.Init(req, nil, nil)
	msg := newHTTPMessageWithCtx(ctx)
	meta, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(meta["Foo"]) != 2 || meta["Foo"][0] != "bar" || meta["Foo"][1] != "baz" {
		t.Errorf("unexpected Foo values: %v", meta["Foo"])
	}
	if len(meta["Bar"]) != 1 || meta["Bar"][0] != "qux" {
		t.Errorf("unexpected Bar values: %v", meta["Bar"])
	}
}

func TestHTTPMessageGetData(t *testing.T) {
	req := fasthttp.AcquireRequest()
	req.SetBody([]byte("payload"))
	ctx := &fasthttp.RequestCtx{}
	ctx.Init(req, nil, nil)
	msg := newHTTPMessageWithCtx(ctx)
	data, err := msg.GetData()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "payload" {
		t.Errorf("expected 'payload', got '%s'", string(data))
	}
}

func TestHTTPMessageAckNak(t *testing.T) {
	req := fasthttp.AcquireRequest()
	ctx := &fasthttp.RequestCtx{}
	ctx.Init(req, nil, nil)
	msg := newHTTPMessageWithCtx(ctx)
	go func() {
		if err := msg.Ack(); err != nil {
			t.Errorf("Ack error: %v", err)
		}
	}()
	if <-msg.done != statusAck {
		t.Error("expected statusAck")
	}
	go func() {
		if err := msg.Nak(); err != nil {
			t.Errorf("Nak error: %v", err)
		}
	}()
	if <-msg.done != statusNak {
		t.Error("expected statusNak")
	}
}
