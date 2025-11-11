package main

import (
	"strings"

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

func (m HTTPMessage) GetMetadata() (res map[string]string, err error) {
	res = make(map[string]string)
	header := &m.httpCtx.Request.Header
	keys := header.PeekKeys()
	for _, k := range keys {
		key := string(k)
		v := header.PeekAll(key)
		values := make([]string, len(v))
		for i, val := range v {
			values[i] = string(val)
		}
		res[key] = strings.Join(values, ",")
	}
	res["method"] = string(m.httpCtx.Method())
	res["path"] = string(m.httpCtx.Path())
	return
}

func (m HTTPMessage) GetData() ([]byte, error) {
	return m.httpCtx.Request.Body(), nil
}

func (m *HTTPMessage) Ack(data *message.ReplyData) error {
	if data != nil {
		message.SendReply(m.reply, data)
	}
	message.SendResponseStatus(m.done, message.ResponseStatusAck)
	return nil
}

func (m *HTTPMessage) Nak() error {
	message.SendResponseStatus(m.done, message.ResponseStatusNak)
	return nil
}
