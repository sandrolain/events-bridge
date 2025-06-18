package message

type Message interface {
	GetID() []byte
	GetMetadata() (map[string][]string, error)
	GetData() ([]byte, error)
	Ack() error
	Nak() error
}
