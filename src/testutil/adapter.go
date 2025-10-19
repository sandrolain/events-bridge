package testutil

import "github.com/sandrolain/events-bridge/src/message"

// Adapter wraps a StubSourceMessage to implement message.SourceMessage
// with the correct Reply signature.
type Adapter struct {
	*StubSourceMessage
}

// NewAdapter creates an adapter that wraps a StubSourceMessage.
func NewAdapter(data []byte, metadata map[string]string) *Adapter {
	return &Adapter{
		StubSourceMessage: NewStubSourceMessage(data, metadata),
	}
}

// Reply implements the message.SourceMessage interface with the correct signature.
func (a *Adapter) Reply(d *message.ReplyData) error {
	a.ReplyCalls++
	a.ReplyData = d
	return a.ReplyErr
}
