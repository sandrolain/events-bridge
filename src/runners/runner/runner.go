package runner

import "github.com/sandrolain/events-bridge/src/message"

type Runner interface {
	Ingest(<-chan message.Message) (<-chan message.Message, error)
	Close() error
}
