package main

import (
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/valyala/fasthttp"
)

var _ message.SourceMessage = &HTTPMessage{}

type HTTPMessage struct {
	httpCtx    *fasthttp.RequestCtx
	done       chan message.ResponseStatus
	reply      chan *message.ReplyData
	metadata   map[string]string
	filesystem fsutil.Filesystem
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

// GetFilesystem returns a filesystem for uploaded files (multipart) or a virtual file for body data.
func (m *HTTPMessage) GetFilesystem() (fsutil.Filesystem, error) {
	if m.filesystem != nil {
		return m.filesystem, nil
	}

	// If no multipart data, create virtual filesystem with /data
	data, err := m.GetData()
	if err != nil {
		return nil, err
	}
	return fsutil.NewVirtualFS("/data", data), nil
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
