package plugin

import (
	"context"
	"fmt"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin/proto"
)

func (p *Plugin) Source(buffer int, config map[string]string) (res <-chan message.Message, closeFn func(), err error) {
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
	stopChan := make(chan struct{})

	go func() {
		for {
			select {
			case <-stopChan:
				close(resChan)
				return
			default:
				if p.stopped {
					close(resChan)
					return
				}
				streamRes, e := stream.Recv()
				if e != nil {
					time.Sleep(100 * time.Millisecond) // Wait a bit before retrying
					p.slog.Error("failed to receive input", "error", e)
					continue
				}
				resChan <- &PluginMessage{
					res: streamRes,
				}
			}
		}
	}()

	res = resChan
	closeFn = func() {
		// close stopChan only if not already closed
		select {
		case <-stopChan:
			// already closed
		default:
			// proper close without arguments
			close(stopChan)
		}
	}

	return
}
