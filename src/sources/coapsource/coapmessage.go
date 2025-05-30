package coapsource

import (
	"bytes"
	"io"

	"github.com/plgd-dev/go-coap/v3/message"
	coapmux "github.com/plgd-dev/go-coap/v3/mux"
	msg "github.com/sandrolain/events-bridge/src/message"
)

type CoAPMessage struct {
	req  *coapmux.Message
	w    coapmux.ResponseWriter
	done chan responseStatus
}

var _ msg.Message = &CoAPMessage{}

func (m *CoAPMessage) GetMetadata() (map[string][]string, error) {
	res := make(map[string][]string)
	for _, opt := range m.req.Options() {
		key := opt.ID.String()
		var val string
		if opt.ID == message.ContentFormat && len(opt.Value) == 1 {
			b := opt.Value[0]
			val = message.MediaType(b).String()
		} else {
			val = string(opt.Value)
		}
		res[key] = append(res[key], val)
	}
	return res, nil
}

func (m *CoAPMessage) GetData() ([]byte, error) {
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

func (m *CoAPMessage) Ack() error {
	m.done <- statusAck
	return nil
}

func (m *CoAPMessage) Nak() error {
	m.done <- statusNak
	return nil
}
