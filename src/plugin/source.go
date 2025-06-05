package plugin

import (
	"context"
	"fmt"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin/proto"
)

func (p *Plugin) Source(buffer int, config map[string]string) (res <-chan message.Message, err error) {
	cfg := []*proto.Config{}
	for k, v := range config {
		cfg = append(cfg, &proto.Config{
			Name:  k,
			Value: v,
		})
	}

	stream, e := p.client.Source(context.TODO(), &proto.SourceReq{
		Configs: cfg,
	})
	if e != nil {
		err = fmt.Errorf("failed to execute input: %w", e)
		return
	}

	resChan := make(chan message.Message, buffer)

	go func() {
		for !p.stopped {
			streamRes, e := stream.Recv()
			if e != nil {
				p.slog.Error("failed to receive input", "error", e)
				continue
			}
			resChan <- &PluginMessage{
				res: streamRes,
			}
		}
	}()

	return resChan, nil
}
