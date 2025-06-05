package plugin

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin/proto"
)

func (p *Plugin) Runner(msg message.Message) (res message.Message, err error) {
	uid := uuid.New().String()

	data, err := msg.GetData()
	if err != nil {
		err = fmt.Errorf("failed to get message data: %w", err)
		return
	}

	var metadata []*proto.Metadata
	meta, e := msg.GetMetadata()
	if e != nil {
		err = fmt.Errorf("failed to get message metadata: %w", e)
		return
	}
	if meta != nil {
		metadata = make([]*proto.Metadata, 0, len(meta))
		for k, m := range meta {
			for _, v := range m {
				metadata = append(metadata, &proto.Metadata{
					Name:  k,
					Value: v,
				})
			}
		}
	}

	runRes, e := p.client.Runner(context.TODO(), &proto.PluginMessage{
		Uuid:     uid,
		Metadata: metadata,
		Data:     data,
	})
	if e != nil {
		err = fmt.Errorf("failed to execute command: %w", e)
		return
	}

	res = &PluginMessage{
		res: runRes,
	}

	return
}
