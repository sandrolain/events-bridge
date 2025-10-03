package message

import (
	"time"
)

// ResolveFromMetadata returns the value from metadata if metaKey is set and non-empty,
// otherwise returns the provided fallback value.
func ResolveFromMetadata(msg *RunnerMessage, metaKey string, fallback string) string {
	if metaKey == "" {
		return fallback
	}
	if meta, err := msg.GetMetadata(); err == nil {
		if v, ok := meta[metaKey]; ok && len(v) > 0 {
			return v
		}
	}
	return fallback
}

// AwaitReplyOrStatus waits for either a reply, a status (Ack/Nak) or a timeout.
// Returns: reply (if any), status (if any), timedOut flag.
func AwaitReplyOrStatus(timeout time.Duration, done <-chan ResponseStatus, reply <-chan *ReplyData) (*ReplyData, *ResponseStatus, bool) {
	select {
	case r := <-reply:
		return r, nil, false
	case s := <-done:
		return nil, &s, false
	case <-time.After(timeout):
		return nil, nil, true
	}
}

// SendResponseStatus sends the provided response status to the channel if it is not nil.
func SendResponseStatus(ch chan ResponseStatus, status ResponseStatus) {
	if ch == nil {
		return
	}
	ch <- status
}

// SendReply forwards the reply data to the channel if it is not nil.
func SendReply(ch chan *ReplyData, reply *ReplyData) {
	if ch == nil {
		return
	}
	ch <- reply
}
