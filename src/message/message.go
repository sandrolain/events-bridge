package message

type Message interface {
	GetMetadata() (map[string][]string, error)
	GetData() ([]byte, error)
	Ack() error
	Nak() error
}
