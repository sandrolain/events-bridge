package plugin

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin/proto"
)

func (p *Plugin) Target(ctx context.Context, msg message.Message) (err error) {
	uid := uuid.New().String()

	data, e := msg.GetData()
	if e != nil {
		err = fmt.Errorf("failed to get message data: %w", e)
		return
	}

	metadata, e := msg.GetMetadata()
	if e != nil {
		err = fmt.Errorf("failed to get message metadata: %w", e)
		return
	}

	md := make([]*proto.Metadata, 0)
	for k, v := range metadata {
		for _, vv := range v {
			md = append(md, &proto.Metadata{
				Name:  k,
				Value: vv,
			})
		}
	}

	_, err = p.client.Target(ctx, &proto.PluginMessage{
		Uuid:     uid,
		Data:     data,
		Metadata: md,
	})

	return
}
