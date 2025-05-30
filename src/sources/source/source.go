package source

import "github.com/sandrolain/events-bridge/src/message"

type Source interface {
	Produce(int) (<-chan message.Message, error)
	Close() error
}
