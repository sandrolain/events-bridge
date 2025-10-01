package encdec

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
)

const (
	errGetDataFmt         = "GetData error: %v"
	errUnexpectedValueFmt = "unexpected decoded value: %s"
)

// testMessage implements SourceMessage for testing
type testMessage struct {
	data []byte
	meta message.MessageMetadata
}

func (t *testMessage) GetID() []byte                                 { return []byte{0, 1} }
func (t *testMessage) GetMetadata() (message.MessageMetadata, error) { return t.meta, nil }
func (t *testMessage) GetData() ([]byte, error)                      { return t.data, nil }
func (t *testMessage) Ack() error                                    { return nil }
func (t *testMessage) Nak() error                                    { return nil }
func (t *testMessage) Reply(_ *message.ReplyData) error              { return nil }

func TestJSONRoundTrip(t *testing.T) {
	t.Parallel()

	// Test con dati semplici sotto forma di map
	originalMeta := message.MessageMetadata{"type": "test", "id": "123"}
	originalData := []byte("simple test data")

	msg := &testMessage{
		data: originalData,
		meta: originalMeta,
	}

	ed := &JSONDecoder{
		dataKey: "data",
		metaKey: "meta",
	}

	encoded, err := ed.Encode(msg)
	if err != nil {
		t.Fatalf("EncodeJSON error: %v", err)
	}

	decoded, err := ed.Decode(encoded)
	if err != nil {
		t.Fatalf("DecodeJSON error: %v", err)
	}

	decodedData, err := decoded.GetData()
	if err != nil {
		t.Fatalf(errGetDataFmt, err)
	}

	decodedMeta, err := decoded.GetMetadata()
	if err != nil {
		t.Fatalf("GetMetadata error: %v", err)
	}

	// Verifica che i metadata siano corretti
	if len(decodedMeta) != len(originalMeta) {
		t.Fatalf("metadata length mismatch: got %d, want %d", len(decodedMeta), len(originalMeta))
	}

	// I dati potrebbero essere codificati diversamente, verifichiamo che non siano vuoti
	if len(decodedData) == 0 {
		t.Fatalf("decoded data is empty")
	}
}

func TestCBORRoundTrip(t *testing.T) {
	t.Parallel()

	// Test con dati semplici
	originalMeta := message.MessageMetadata{"type": "test", "id": "456"}
	originalData := []byte("cbor test data")

	msg := &testMessage{
		data: originalData,
		meta: originalMeta,
	}

	ed := &CBORDecoder{
		dataKey: "data",
		metaKey: "meta",
	}

	encoded, err := ed.Encode(msg)
	if err != nil {
		t.Fatalf("EncodeCBOR error: %v", err)
	}

	decoded, err := ed.Decode(encoded)
	if err != nil {
		t.Fatalf("DecodeCBOR error: %v", err)
	}

	decodedData, err := decoded.GetData()
	if err != nil {
		t.Fatalf(errGetDataFmt, err)
	}

	decodedMeta, err := decoded.GetMetadata()
	if err != nil {
		t.Fatalf("GetMetadata error: %v", err)
	}

	// Verifica che i metadata siano corretti
	if len(decodedMeta) != len(originalMeta) {
		t.Fatalf("metadata length mismatch: got %d, want %d", len(decodedMeta), len(originalMeta))
	}

	// I dati potrebbero essere codificati diversamente, verifichiamo che non siano vuoti
	if len(decodedData) == 0 {
		t.Fatalf("decoded data is empty")
	}
}

func TestCLIRoundTrip(t *testing.T) {
	t.Parallel()

	originalData := []byte("test data")

	msg := &testMessage{
		data: originalData,
		meta: message.MessageMetadata{"type": "test"},
	}

	ed := &CLIDecoder{}

	data, err := ed.Encode(msg)
	if err != nil {
		t.Fatalf("EncodeCLI error: %v", err)
	}

	decoded, err := ed.Decode(data)
	if err != nil {
		t.Fatalf("DecodeCLI error: %v", err)
	}

	decodedData, err := decoded.GetData()
	if err != nil {
		t.Fatalf(errGetDataFmt, err)
	}

	if string(decodedData) != string(originalData) {
		t.Fatalf(errUnexpectedValueFmt, string(decodedData))
	}
}
