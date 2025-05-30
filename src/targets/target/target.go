package target

import "github.com/sandrolain/events-bridge/src/message"

type Target interface {
	Consume(<-chan message.Message) error
	Close() error
}
