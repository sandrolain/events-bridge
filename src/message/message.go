package message

type Message interface {
	GetID() []byte
	GetMetadata() (map[string][]string, error)
	GetData() ([]byte, error)
	Ack() error
	Nak() error
	Reply(data []byte, metadata map[string][]string) error
}

type ResponseStatus int

const (
	ResponseStatusAck ResponseStatus = iota
	ResponseStatusNak
	ResponseStatusReply
)
