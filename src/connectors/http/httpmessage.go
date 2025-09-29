package main

import (
	"strings"

	"github.com/sandrolain/events-bridge/src/common"
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

func (m HTTPMessage) GetMetadata() (res message.MessageMetadata, err error) {
	res = make(message.MessageMetadata)
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

func (m *HTTPMessage) Ack() error {
	common.SendResponseStatus(m.done, message.ResponseStatusAck)
	return nil
}

func (m *HTTPMessage) Nak() error {
	common.SendResponseStatus(m.done, message.ResponseStatusNak)
	return nil
}

func (m *HTTPMessage) Reply(data *message.ReplyData) error {
	common.SendReply(m.reply, data)
	return nil
}
