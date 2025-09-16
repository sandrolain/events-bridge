package clirunner

import "github.com/sandrolain/events-bridge/src/message"

var _ message.Message = &cliMessage{}

type cliMessage struct {
	original message.Message
	meta     map[string][]string
	data     []byte
}

func (m *cliMessage) GetID() []byte {
	return m.original.GetID()
}

func (m *cliMessage) GetMetadata() (map[string][]string, error) {
	return m.meta, nil
}

func (m *cliMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *cliMessage) Ack() error {
	return m.original.Ack()
}

func (m *cliMessage) Nak() error {
	return m.original.Nak()
}

func (m *cliMessage) Reply(data []byte, metadata map[string][]string) error {
	return m.original.Reply(data, metadata)
}

func (c *CLIRunner) Close() error {
	c.slog.Info("closing cli runner")
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.stopCh:
		// already closed
	default:
		close(c.stopCh)
	}
	return nil
}
