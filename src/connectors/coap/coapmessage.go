package main

import (
	"bytes"
	"io"

	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapmux "github.com/plgd-dev/go-coap/v3/mux"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = &CoAPMessage{}

type CoAPMessage struct {
	req   *coapmux.Message
	w     coapmux.ResponseWriter
	done  chan message.ResponseStatus
	reply chan *message.ReplyData
	data  []byte
}

func (m *CoAPMessage) GetID() []byte {
	if m.req == nil {
		return nil
	}
	return m.req.Token()
}

func (m *CoAPMessage) GetMetadata() (map[string]string, error) {
	res := make(map[string]string)
	for _, opt := range m.req.Options() {
		key := opt.ID.String()
		var val string
		if opt.ID == coapmessage.ContentFormat && len(opt.Value) == 1 {
			b := opt.Value[0]
			val = coapmessage.MediaType(b).String()
		} else {
			val = string(opt.Value)
		}
		res[key] = val
	}
	return res, nil
}

func (m *CoAPMessage) GetData() ([]byte, error) {
	if m.data != nil {
		return m.data, nil
	}
	body := m.req.Body()
	if body == nil {
		return nil, nil
	}
	var buf bytes.Buffer
	_, err := io.Copy(&buf, body)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GetFilesystem returns a virtual filesystem with message data accessible at /data.
func (m *CoAPMessage) GetFilesystem() (fsutil.Filesystem, error) {
	data, err := m.GetData()
	if err != nil {
		return nil, err
	}
	return fsutil.NewVirtualFS("/data", data), nil
}

func (m *CoAPMessage) Ack(data *message.ReplyData) error {
	if data != nil {
		message.SendReply(m.reply, data)
	} else {
		message.SendResponseStatus(m.done, message.ResponseStatusAck)
	}
	return nil
}

func (m *CoAPMessage) Nak() error {
	message.SendResponseStatus(m.done, message.ResponseStatusNak)
	return nil
}
