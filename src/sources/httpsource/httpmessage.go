package httpsource

import (
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/valyala/fasthttp"
)

var _ message.Message = &HTTPMessage{}

type HTTPMessage struct {
	httpCtx *fasthttp.RequestCtx
	done    chan responseStatus
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

type responseStatus int

const (
	statusAck responseStatus = iota
	statusNak
)

func (m *HTTPMessage) Ack() error {
	m.done <- statusAck
	return nil
}

func (m *HTTPMessage) Nak() error {
	m.done <- statusNak
	return nil
}
