package main

import (
	"sync"

	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/connectors/grpc/proto"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = (*GRPCMessage)(nil)

// GRPCMessage wraps a gRPC EventMessage and implements the SourceMessage interface.
type GRPCMessage struct {
	msg   *proto.EventMessage
	done  chan message.ResponseStatus
	reply chan *message.ReplyData
	once  sync.Once
}

// NewGRPCMessage creates a new GRPCMessage from a proto EventMessage.
func NewGRPCMessage(msg *proto.EventMessage) *GRPCMessage {
	return &GRPCMessage{
		msg:   msg,
		done:  make(chan message.ResponseStatus, 1),
		reply: make(chan *message.ReplyData, 1),
	}
}

// GetID returns the message UUID.
func (m *GRPCMessage) GetID() []byte {
	return m.msg.Uuid
}

// GetMetadata returns the message metadata.
func (m *GRPCMessage) GetMetadata() (map[string]string, error) {
	return m.msg.Metadata, nil
}

// GetData returns the message payload data.
func (m *GRPCMessage) GetData() ([]byte, error) {
	return m.msg.Data, nil
}

// GetFilesystem returns nil as this message type does not provide filesystem access.
func (m *GRPCMessage) GetFilesystem() (fsutil.Filesystem, error) {
	return nil, nil
}

// Ack sends an acknowledgment signal with optional reply data.
func (m *GRPCMessage) Ack(data *message.ReplyData) error {
	m.once.Do(func() {
		if data != nil {
			m.reply <- data
		} else {
			m.done <- message.ResponseStatusAck
		}
	})
	return nil
}

// Nak sends a negative acknowledgment signal.
func (m *GRPCMessage) Nak() error {
	m.once.Do(func() {
		m.done <- message.ResponseStatusNak
	})
	return nil
}
