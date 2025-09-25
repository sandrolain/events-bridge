package common

import "github.com/sandrolain/events-bridge/src/message"

// SendResponseStatus sends the provided response status to the channel if it is not nil.
func SendResponseStatus(ch chan message.ResponseStatus, status message.ResponseStatus) {
	if ch == nil {
		return
	}
	ch <- status
}

// SendReply forwards the reply data to the channel if it is not nil.
func SendReply(ch chan *message.ReplyData, reply *message.ReplyData) {
	if ch == nil {
		return
	}
	ch <- reply
}
