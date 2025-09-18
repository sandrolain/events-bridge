package plugin

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin/proto"
)

func (p *Plugin) Runner(msg *message.RunnerMessage) (res *message.RunnerMessage, err error) {
	uid := uuid.New().String()

	data, err := msg.GetSourceData()
	if err != nil {
		err = fmt.Errorf("failed to get message data: %w", err)
		return
	}

	var metadata []*proto.Metadata
	meta, e := msg.GetSourceMetadata()
	if e != nil {
		err = fmt.Errorf("failed to get message metadata: %w", e)
		return
	}
	if meta != nil {
		metadata = make([]*proto.Metadata, 0, len(meta))
		for k, v := range meta {
			metadata = append(metadata, &proto.Metadata{
				Name:  k,
				Value: v,
			})
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

	return message.NewRunnerMessage(&PluginMessage{res: runRes}), nil
}
