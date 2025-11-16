package testutil

import (
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
)

// Adapter wraps a StubSourceMessage to implement message.SourceMessage
// with the correct Ack signature.
type Adapter struct {
	*StubSourceMessage
}

// NewAdapter creates an adapter that wraps a StubSourceMessage.
func NewAdapter(data []byte, metadata map[string]string) *Adapter {
	return &Adapter{
		StubSourceMessage: NewStubSourceMessage(data, metadata),
	}
}

// Ack implements the message.SourceMessage interface with the correct signature.
func (a *Adapter) Ack(d *message.ReplyData) error {
	a.AckCalls++
	a.AckData = d
	return a.AckErr
}

// GetFilesystem implements message.SourceMessage interface with proper type.
func (a *Adapter) GetFilesystem() (fsutil.Filesystem, error) {
	if a.Filesystem == nil {
		return nil, a.FsErr
	}
	if fs, ok := a.Filesystem.(fsutil.Filesystem); ok {
		return fs, a.FsErr
	}
	return nil, a.FsErr
}
