package main

import (
	"strings"
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/valyala/fasthttp"
)

func newHTTPMessageWithCtx(ctx *fasthttp.RequestCtx) *HTTPMessage {
	return &HTTPMessage{
		httpCtx: ctx,
		done:    make(chan message.ResponseStatus, 1),
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
	// Validate Foo header has both "bar" and "baz"
	fooValsRaw := meta["Foo"]
	var fooVals []string
	if fooValsRaw != "" {
		for _, v := range strings.Split(fooValsRaw, ",") {
			v = strings.TrimSpace(v)
			if v != "" {
				fooVals = append(fooVals, v)
			}
		}
	}
	if len(fooVals) != 2 {
		t.Errorf("unexpected Foo values count: %v", fooVals)
	} else {
		hasBar := false
		hasBaz := false
		for _, v := range fooVals {
			if v == "bar" {
				hasBar = true
			}
			// Validate Bar header has single "qux"
			barValsRaw := meta["Bar"]
			var barVals []string
			if barValsRaw != "" {
				for _, v := range strings.Split(barValsRaw, ",") {
					v = strings.TrimSpace(v)
					if v != "" {
						barVals = append(barVals, v)
					}
				}
			}
			if len(barVals) != 1 || barVals[0] != "qux" {
				t.Errorf("unexpected Bar values: %v", barVals)
			}
			t.Errorf("unexpected Foo values: %v", fooVals)
		}

		if !hasBar || !hasBaz {
			t.Errorf("missing Foo values: %v", fooVals)
		}
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
	if <-msg.done != message.ResponseStatusAck {
		t.Error("expected statusAck")
	}
	go func() {
		if err := msg.Nak(); err != nil {
			t.Errorf("Nak error: %v", err)
		}
	}()
	if <-msg.done != message.ResponseStatusNak {
		t.Error("expected statusNak")
	}
}
