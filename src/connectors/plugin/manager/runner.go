package manager

import (
	"context"
	"fmt"

	"github.com/sandrolain/events-bridge/src/connectors/plugin/proto"
)

func (p *Plugin) Runner(ctx context.Context, id []byte, metadata map[string]string, data []byte) (*PluginMessage, error) {
	runRes, err := p.client.Runner(ctx, &proto.PluginMessage{
		Uuid:     id,
		Metadata: metadata,
		Data:     data,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}

	return &PluginMessage{res: runRes}, nil
}
