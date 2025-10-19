package message

import (
	"errors"
	"sync"
	"testing"
)

const (
	errMsgExpectedError    = "expected error %v, got %v"
	errMsgUnexpectedError  = "unexpected error: %v"
	errMsgExpectedNilError = "expected error from %s, got nil"
)

// stubSourceMessage is a local test stub for SourceMessage.
// Note: this is kept here to avoid import cycles. For tests outside
// the message package, use github.com/sandrolain/events-bridge/src/testutil.StubSourceMessage
type stubSourceMessage struct {
	id          []byte
	metadata    map[string]string
	metadataErr error
	data        []byte
	dataErr     error
	ackErr      error
	ackCalls    int
	nakErr      error
	nakCalls    int
	replyErr    error
	replyCalls  int
	replyData   *ReplyData
}

func (s *stubSourceMessage) GetID() []byte {
	return s.id
}

func (s *stubSourceMessage) GetMetadata() (map[string]string, error) {
	if s.metadataErr != nil {
		return nil, s.metadataErr
	}
	return s.metadata, nil
}

func (s *stubSourceMessage) GetData() ([]byte, error) {
	if s.dataErr != nil {
		return nil, s.dataErr
	}
	return s.data, nil
}

func (s *stubSourceMessage) Ack() error {
	s.ackCalls++
	return s.ackErr
}

func (s *stubSourceMessage) Nak() error {
	s.nakCalls++
	return s.nakErr
}

func (s *stubSourceMessage) Reply(d *ReplyData) error {
	s.replyCalls++
	s.replyData = d
	return s.replyErr
}

func TestRunnerMessageDataFallback(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{data: []byte("origin")}
	msg := NewRunnerMessage(original)

	data, err := msg.GetData()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if string(data) != "origin" {
		t.Fatalf("unexpected target data: %q", data)
	}
}

func TestRunnerMessageDataOverride(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{data: []byte("origin")}
	msg := NewRunnerMessage(original)
	msg.SetData([]byte("override"))

	data, err := msg.GetData()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if string(data) != "override" {
		t.Fatalf("unexpected target data: %q", data)
	}
}

func TestRunnerMessageMetadataPreferredLocal(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{metadata: map[string]string{"foo": "bar"}}
	msg := NewRunnerMessage(original)
	msg.AddMetadata("foo", "override")
	msg.AddMetadata("baz", "qux")

	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if len(metadata) != 2 {
		t.Fatalf("unexpected metadata size: %d", len(metadata))
	}
	if metadata["foo"] != "override" {
		t.Fatalf("expected override for foo, got %q", metadata["foo"])
	}
	if metadata["baz"] != "qux" {
		t.Fatalf("expected qux for baz, got %q", metadata["baz"])
	}
}

func TestRunnerMessageMetadataFallback(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{metadata: map[string]string{"foo": "bar"}}
	msg := NewRunnerMessage(original)

	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if len(metadata) != 1 || metadata["foo"] != "bar" {
		t.Fatalf("unexpected metadata: %#v", metadata)
	}
}

func TestRunnerMessageMergeMetadata(t *testing.T) {
	t.Parallel()

	msg := NewRunnerMessage(&stubSourceMessage{})
	msg.MergeMetadata(map[string]string{"a": "1"})
	msg.MergeMetadata(map[string]string{"b": "2", "a": "3"})

	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if len(metadata) != 2 {
		t.Fatalf("unexpected metadata size: %d", len(metadata))
	}
	if metadata["a"] != "3" {
		t.Fatalf("expected merged value 3 for key a, got %q", metadata["a"])
	}
	if metadata["b"] != "2" {
		t.Fatalf("expected value 2 for key b, got %q", metadata["b"])
	}
}

func TestRunnerMessageReplyDelegates(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{}
	msg := NewRunnerMessage(original)
	msg.SetData([]byte("payload"))
	msg.AddMetadata("k", "v")

	if err := msg.ReplySource(); err != nil {
		t.Fatalf("unexpected error replying: %v", err)
	}
	if original.replyCalls != 1 {
		t.Fatalf("expected reply to be invoked once, got %d", original.replyCalls)
	}
	if original.replyData == nil {
		t.Fatalf("expected reply payload to be forwarded")
	}
	if string(original.replyData.Data) != "payload" {
		t.Fatalf("unexpected reply data: %q", original.replyData.Data)
	}
	if original.replyData.Metadata["k"] != "v" {
		t.Fatalf("unexpected reply metadata: %#v", original.replyData.Metadata)
	}
}

func TestRunnerMessageAckNakDelegate(t *testing.T) {
	t.Parallel()

	ackErr := errors.New("ack")
	nakErr := errors.New("nak")

	original := &stubSourceMessage{ackErr: ackErr, nakErr: nakErr}
	msg := NewRunnerMessage(original)

	if err := msg.Ack(); !errors.Is(err, ackErr) {
		t.Fatalf("expected ack error, got %v", err)
	}
	if original.ackCalls != 1 {
		t.Fatalf("unexpected ack call count: %d", original.ackCalls)
	}

	if err := msg.Nak(); !errors.Is(err, nakErr) {
		t.Fatalf("expected nak error, got %v", err)
	}
	if original.nakCalls != 1 {
		t.Fatalf("unexpected nak call count: %d", original.nakCalls)
	}
}

func TestRunnerMessageGetters(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{id: []byte("id"), metadata: map[string]string{"foo": "bar"}, data: []byte("src")}
	msg := NewRunnerMessage(original)
	msg.SetData([]byte("dst"))
	msg.AddMetadata("foo", "baz")

	if string(msg.GetID()) != "id" {
		t.Fatalf("unexpected id: %q", msg.GetID())
	}
	data, err := msg.GetData()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if string(data) != "dst" {
		t.Fatalf("unexpected direct data: %q", data)
	}
	if metadata["foo"] != "baz" {
		t.Fatalf("unexpected metadata value: %q", metadata["foo"])
	}
}

// TestRunnerMessageSetMetadata tests the SetMetadata method that replaces all metadata
func TestRunnerMessageSetMetadata(t *testing.T) {
	t.Parallel()

	msg := NewRunnerMessage(&stubSourceMessage{})
	msg.AddMetadata("a", "1")
	msg.AddMetadata("b", "2")

	// Replace with new metadata
	msg.SetMetadata(map[string]string{"c": "3", "d": "4"})

	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if len(metadata) != 2 {
		t.Fatalf("expected 2 metadata entries after SetMetadata, got %d", len(metadata))
	}
	if metadata["c"] != "3" {
		t.Fatalf("expected value 3 for key c, got %q", metadata["c"])
	}
	if metadata["d"] != "4" {
		t.Fatalf("expected value 4 for key d, got %q", metadata["d"])
	}
	// Verify old metadata was removed
	if _, exists := metadata["a"]; exists {
		t.Fatal("expected old metadata key 'a' to be removed")
	}
}

// TestRunnerMessageGetSourceMetadata tests retrieving the original source metadata
func TestRunnerMessageGetSourceMetadata(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{metadata: map[string]string{"source": "original"}}
	msg := NewRunnerMessage(original)
	msg.AddMetadata("local", "added")

	metadata, err := msg.GetSourceMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if len(metadata) != 1 {
		t.Fatalf("expected 1 source metadata entry, got %d", len(metadata))
	}
	if metadata["source"] != "original" {
		t.Fatalf("expected original source metadata, got %#v", metadata)
	}
}

// TestRunnerMessageGetSourceMetadataError tests error handling when source metadata fails
func TestRunnerMessageGetSourceMetadataError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("metadata fetch failed")
	original := &stubSourceMessage{metadataErr: expectedErr}
	msg := NewRunnerMessage(original)

	metadata, err := msg.GetSourceMetadata()
	if err == nil {
		t.Fatalf(errMsgExpectedNilError, "GetSourceMetadata")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf(errMsgExpectedError, expectedErr, err)
	}
	if metadata != nil {
		t.Fatalf("expected nil metadata on error, got %#v", metadata)
	}
}

// TestRunnerMessageGetSourceData tests retrieving the original source data
func TestRunnerMessageGetSourceData(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{data: []byte("source-data")}
	msg := NewRunnerMessage(original)
	msg.SetData([]byte("local-data"))

	data, err := msg.GetSourceData()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if string(data) != "source-data" {
		t.Fatalf("expected source-data, got %q", string(data))
	}
}

// TestRunnerMessageGetSourceDataError tests error handling when source data fails
func TestRunnerMessageGetSourceDataError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("data fetch failed")
	original := &stubSourceMessage{dataErr: expectedErr}
	msg := NewRunnerMessage(original)

	data, err := msg.GetSourceData()
	if err == nil {
		t.Fatalf(errMsgExpectedNilError, "GetSourceData")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf(errMsgExpectedError, expectedErr, err)
	}
	if data != nil {
		t.Fatalf("expected nil data on error, got %#v", data)
	}
}

// TestRunnerMessageMetadataError tests error handling when target metadata fetch fails
func TestRunnerMessageMetadataError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("metadata error")
	original := &stubSourceMessage{metadataErr: expectedErr}
	msg := NewRunnerMessage(original)
	// No local metadata, should fall back to original

	metadata, err := msg.GetMetadata()
	if err == nil {
		t.Fatalf(errMsgExpectedNilError, "GetMetadata")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf(errMsgExpectedError, expectedErr, err)
	}
	if metadata != nil {
		t.Fatalf("expected nil metadata on error, got %#v", metadata)
	}
}

// TestRunnerMessageDataError tests error handling when target data fetch fails
func TestRunnerMessageDataError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("data error")
	original := &stubSourceMessage{dataErr: expectedErr}
	msg := NewRunnerMessage(original)
	// No local data, should fall back to original

	data, err := msg.GetData()
	if err == nil {
		t.Fatalf(errMsgExpectedNilError, "GetData")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf(errMsgExpectedError, expectedErr, err)
	}
	if data != nil {
		t.Fatalf("expected nil data on error, got %#v", data)
	}
}

// TestRunnerMessageReplyError tests error handling when reply fails
func TestRunnerMessageReplyError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("reply failed")
	original := &stubSourceMessage{replyErr: expectedErr}
	msg := NewRunnerMessage(original)
	msg.SetData([]byte("test"))

	err := msg.ReplySource()
	if err == nil {
		t.Fatalf(errMsgExpectedNilError, "Reply")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf(errMsgExpectedError, expectedErr, err)
	}
}

// TestRunnerMessageConcurrentMetadataAccess tests thread-safe metadata operations
func TestRunnerMessageConcurrentMetadataAccess(t *testing.T) {
	t.Parallel()

	msg := NewRunnerMessage(&stubSourceMessage{})
	var wg sync.WaitGroup
	iterations := 100

	// Concurrent AddMetadata
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func(idx int) {
			defer wg.Done()
			msg.AddMetadata("concurrent", "value")
		}(i)
	}
	wg.Wait()

	// Verify metadata was set (at least once)
	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if metadata["concurrent"] != "value" {
		t.Fatalf("expected concurrent metadata to be set")
	}
}

// TestRunnerMessageConcurrentMergeMetadata tests thread-safe MergeMetadata operations
func TestRunnerMessageConcurrentMergeMetadata(t *testing.T) {
	t.Parallel()

	msg := NewRunnerMessage(&stubSourceMessage{})
	var wg sync.WaitGroup
	iterations := 50

	// Concurrent MergeMetadata with different keys
	wg.Add(iterations * 2)
	for i := 0; i < iterations; i++ {
		go func(idx int) {
			defer wg.Done()
			msg.MergeMetadata(map[string]string{"key1": "value1"})
		}(i)
		go func(idx int) {
			defer wg.Done()
			msg.MergeMetadata(map[string]string{"key2": "value2"})
		}(i)
	}
	wg.Wait()

	// Verify both keys exist
	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if metadata["key1"] != "value1" {
		t.Fatalf("expected key1 to be set, got %#v", metadata)
	}
	if metadata["key2"] != "value2" {
		t.Fatalf("expected key2 to be set, got %#v", metadata)
	}
}

// TestRunnerMessageConcurrentSetMetadata tests thread-safe SetMetadata operations
func TestRunnerMessageConcurrentSetMetadata(t *testing.T) {
	t.Parallel()

	msg := NewRunnerMessage(&stubSourceMessage{})
	var wg sync.WaitGroup
	iterations := 50

	// Concurrent SetMetadata
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func(idx int) {
			defer wg.Done()
			msg.SetMetadata(map[string]string{"concurrent": "set"})
		}(i)
	}
	wg.Wait()

	// Verify final state is consistent
	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if len(metadata) != 1 {
		t.Fatalf("expected exactly 1 metadata entry, got %d: %#v", len(metadata), metadata)
	}
	if metadata["concurrent"] != "set" {
		t.Fatalf("expected concurrent=set, got %#v", metadata)
	}
}

// TestRunnerMessageConcurrentReadWrite tests concurrent reads and writes
func TestRunnerMessageConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	msg := NewRunnerMessage(&stubSourceMessage{metadata: map[string]string{"initial": "value"}})
	var wg sync.WaitGroup
	iterations := 100

	// Concurrent reads and writes
	wg.Add(iterations * 2)
	for i := 0; i < iterations; i++ {
		go func() {
			defer wg.Done()
			msg.AddMetadata("writer", "test")
		}()
		go func() {
			defer wg.Done()
			_, _ = msg.GetMetadata()
		}()
	}
	wg.Wait()

	// Should not panic and should have valid state
	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if metadata == nil {
		t.Fatal("expected metadata to not be nil after concurrent operations")
	}
}

// TestRunnerMessageNilMetadataInitialization tests that metadata is properly initialized
func TestRunnerMessageNilMetadataInitialization(t *testing.T) {
	t.Parallel()

	msg := NewRunnerMessage(&stubSourceMessage{})

	// Metadata should be nil initially
	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if metadata != nil {
		t.Fatal("expected initial metadata to be nil")
	}

	// AddMetadata should initialize it
	msg.AddMetadata("key", "value")
	if metadata, err = msg.GetMetadata(); err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if metadata == nil {
		t.Fatal("expected metadata to be initialized after AddMetadata")
	}
}

// TestRunnerMessageEmptyData tests handling of empty/nil data
func TestRunnerMessageEmptyData(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{data: []byte{}}
	msg := NewRunnerMessage(original)

	data, err := msg.GetData()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if data == nil {
		t.Fatal("expected empty slice, got nil")
	}
	if len(data) != 0 {
		t.Fatalf("expected empty data, got %d bytes", len(data))
	}
}

// TestRunnerMessageNilID tests handling of nil ID
func TestRunnerMessageNilID(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{id: nil}
	msg := NewRunnerMessage(original)

	id := msg.GetID()
	if id != nil {
		t.Fatalf("expected nil ID, got %v", id)
	}
}

// TestRunnerMessageReplyWithNilData tests Reply with nil data and metadata
func TestRunnerMessageReplyWithNilData(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{}
	msg := NewRunnerMessage(original)
	// Don't set any data or metadata

	err := msg.ReplySource()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}

	if original.replyCalls != 1 {
		t.Fatalf("expected Reply to be called once, got %d", original.replyCalls)
	}
	if original.replyData == nil {
		t.Fatal("expected ReplyData to be set")
	}
	if original.replyData.Data != nil {
		t.Fatalf("expected nil data in reply, got %v", original.replyData.Data)
	}
	if original.replyData.Metadata != nil {
		t.Fatalf("expected nil metadata in reply, got %v", original.replyData.Metadata)
	}
}

// TestRunnerMessageMultipleAckNakCalls tests that Ack/Nak can be called multiple times
func TestRunnerMessageMultipleAckNakCalls(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{}
	msg := NewRunnerMessage(original)

	// Call Ack multiple times
	_ = msg.Ack()
	_ = msg.Ack()
	if original.ackCalls != 2 {
		t.Fatalf("expected 2 Ack calls, got %d", original.ackCalls)
	}

	// Call Nak multiple times
	_ = msg.Nak()
	_ = msg.Nak()
	_ = msg.Nak()
	if original.nakCalls != 3 {
		t.Fatalf("expected 3 Nak calls, got %d", original.nakCalls)
	}
}

// TestRunnerMessageMetadataIsolation tests that local metadata doesn't affect source
func TestRunnerMessageMetadataIsolation(t *testing.T) {
	t.Parallel()

	sourceMetadata := map[string]string{"source": "value"}
	original := &stubSourceMessage{metadata: sourceMetadata}
	msg := NewRunnerMessage(original)

	// Modify local metadata
	msg.AddMetadata("local", "value")
	msg.AddMetadata("source", "modified")

	// Source metadata should be unchanged
	srcMeta, err := msg.GetSourceMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if srcMeta["source"] != "value" {
		t.Fatalf("source metadata was modified: %#v", srcMeta)
	}
	if _, exists := srcMeta["local"]; exists {
		t.Fatal("local metadata leaked into source metadata")
	}

	// Target metadata should have local changes
	targetMeta, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(errMsgUnexpectedError, err)
	}
	if targetMeta["source"] != "modified" {
		t.Fatalf("expected modified source in target metadata, got %#v", targetMeta)
	}
	if targetMeta["local"] != "value" {
		t.Fatalf("expected local metadata in target, got %#v", targetMeta)
	}
}
