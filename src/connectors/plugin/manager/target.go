package manager

import (
	"context"

	"github.com/sandrolain/events-bridge/src/connectors/plugin/proto"
)

func (p *Plugin) Target(ctx context.Context, id []byte, metadata map[string]string, data []byte) (err error) {
	_, err = p.client.Target(ctx, &proto.PluginMessage{
		Uuid:     id,
		Data:     data,
		Metadata: metadata,
	})

	return
}
