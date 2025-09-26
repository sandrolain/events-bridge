package common

import (
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

// AwaitReplyOrStatus waits for either a reply, a status (Ack/Nak) or a timeout.
// Returns: reply (if any), status (if any), timedOut flag.
func AwaitReplyOrStatus(timeout time.Duration, done <-chan message.ResponseStatus, reply <-chan *message.ReplyData) (*message.ReplyData, *message.ResponseStatus, bool) {
	select {
	case r := <-reply:
		return r, nil, false
	case s := <-done:
		return nil, &s, false
	case <-time.After(timeout):
		return nil, nil, true
	}
}
