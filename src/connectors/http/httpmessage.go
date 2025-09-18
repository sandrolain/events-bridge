package main

import (
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/valyala/fasthttp"
)

var _ message.SourceMessage = &HTTPMessage{}

type HTTPMessage struct {
	httpCtx *fasthttp.RequestCtx
	done    chan message.ResponseStatus
	reply   chan *message.ReplyData
}

func (m *HTTPMessage) GetID() []byte {
	return m.httpCtx.Request.Header.Peek("X-Request-ID")
}

func (m HTTPMessage) GetMetadata() (res map[string][]string, err error) {
	res = make(map[string][]string)
	header := &m.httpCtx.Request.Header
	keys := header.PeekKeys()
	for _, k := range keys {
		key := string(k)
		v := header.PeekAll(key)
		values := make([]string, len(v))
		for i, val := range v {
			values[i] = string(val)
		}
		res[key] = values
	}
	return
}

func (m HTTPMessage) GetData() ([]byte, error) {
	return m.httpCtx.Request.Body(), nil
}

func (m *HTTPMessage) Ack() error {
	if m.done != nil {
		m.done <- message.ResponseStatusAck
	}
	return nil
}

func (m *HTTPMessage) Nak() error {
	if m.done != nil {
		m.done <- message.ResponseStatusNak
	}
	return nil
}

func (m *HTTPMessage) Reply(data *message.ReplyData) error {
	if m.reply != nil {
		m.reply <- data
	}
	return nil
}
