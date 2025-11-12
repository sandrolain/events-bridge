package main

import (
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/valyala/fasthttp"
)

var _ message.SourceMessage = &HTTPMessage{}

type HTTPMessage struct {
	httpCtx  *fasthttp.RequestCtx
	done     chan message.ResponseStatus
	reply    chan *message.ReplyData
	metadata map[string]string
}

func (m *HTTPMessage) GetID() []byte {
	return m.httpCtx.Request.Header.Peek("X-Request-ID")
}

func (m HTTPMessage) GetMetadata() (map[string]string, error) {
	// Return pre-validated metadata (already enriched with JWT claims if configured)
	return m.metadata, nil
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
