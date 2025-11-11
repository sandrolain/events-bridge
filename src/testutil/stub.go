// Package testutil provides test utilities for events-bridge.
// This package contains shared test stubs and helpers to reduce
// code duplication across test files.
package testutil

// StubSourceMessage provides a configurable test stub for SourceMessage.
// It implements the message.SourceMessage interface for testing purposes.
// Uses interface{} types to avoid import cycles with the message package.
type StubSourceMessage struct {
	ID       []byte
	Data     []byte
	Metadata map[string]string
	DataErr  error
	MetaErr  error
	AckErr   error
	NakErr   error

	// Call counters for verification in tests
	AckCalls int
	NakCalls int
	AckData  any // Stores any ack data passed
}

// NewStubSourceMessage creates a stub with sensible defaults.
// This is the recommended way to create a stub for most test cases.
func NewStubSourceMessage(data []byte, metadata map[string]string) *StubSourceMessage {
	return &StubSourceMessage{
		ID:       []byte("test-id"),
		Data:     data,
		Metadata: metadata,
	}
}

// WithError configures the stub to return errors for various operations.
// This is useful for testing error handling paths.
func (s *StubSourceMessage) WithError(dataErr, metaErr, ackErr, nakErr error) *StubSourceMessage {
	s.DataErr = dataErr
	s.MetaErr = metaErr
	s.AckErr = ackErr
	s.NakErr = nakErr
	return s
}

// GetID returns the message ID.
func (s *StubSourceMessage) GetID() []byte {
	return s.ID
}

// GetMetadata returns the message metadata or configured error.
func (s *StubSourceMessage) GetMetadata() (map[string]string, error) {
	return s.Metadata, s.MetaErr
}

// GetData returns the message data or configured error.
func (s *StubSourceMessage) GetData() ([]byte, error) {
	return s.Data, s.DataErr
}

// Ack acknowledges the message and increments the counter.
func (s *StubSourceMessage) Ack(d any) error {
	s.AckCalls++
	s.AckData = d
	return s.AckErr
}

// Nak negatively acknowledges the message and increments the counter.
func (s *StubSourceMessage) Nak() error {
	s.NakCalls++
	return s.NakErr
}
