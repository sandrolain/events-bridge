package main

import (
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/valyala/fasthttp"
)

var _ message.Message = &HTTPMessage{}

type HTTPMessage struct {
	httpCtx *fasthttp.RequestCtx
	done    chan message.ResponseStatus
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
	m.done <- message.ResponseStatusAck
	return nil
}

func (m *HTTPMessage) Nak() error {
	m.done <- message.ResponseStatusNak
	return nil
}

func (m *HTTPMessage) Reply(data []byte, metadata map[string][]string) error {
	for k, v := range metadata {
		for _, val := range v {
			m.httpCtx.Response.Header.Add(k, val)
		}
	}
	m.httpCtx.SetStatusCode(fasthttp.StatusOK)
	m.httpCtx.SetBody(data)
	m.Ack()
	m.done <- message.ResponseStatusReply
	return nil
}
