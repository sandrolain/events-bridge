package common

import "github.com/sandrolain/events-bridge/src/message"

// ResolveFromMetadata returns the value from metadata if metaKey is set and non-empty,
// otherwise returns the provided fallback value.
func ResolveFromMetadata(msg *message.RunnerMessage, metaKey string, fallback string) string {
	if metaKey == "" {
		return fallback
	}
	if meta, err := msg.GetTargetMetadata(); err == nil {
		if v, ok := meta[metaKey]; ok && len(v) > 0 {
			return v
		}
	}
	return fallback
}
