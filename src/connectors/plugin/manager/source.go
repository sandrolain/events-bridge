package manager

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors/plugin/proto"
)

func (p *Plugin) Source(buffer int, config map[string]string) (<-chan *PluginMessage, func(), error) {
	cfg := []*proto.Config{}
	for k, v := range config {
		cfg = append(cfg, &proto.Config{
			Name:  k,
			Value: v,
		})
	}

	ctx, cancel := context.WithCancel(context.Background())

	stream, e := p.client.Source(ctx, &proto.SourceReq{
		Configs: cfg,
	})
	if e != nil {
		cancel()
		return nil, nil, fmt.Errorf("failed to execute input: %w", e)
	}

	resChan := make(chan *PluginMessage, buffer)
	stopChan := make(chan struct{})

	var closeOnce sync.Once
	closeFn := func() {
		closeOnce.Do(func() {
			close(stopChan)
			cancel()
			close(resChan)
		})
	}

	go func() {
		for {
			select {
			case <-stopChan:
				return
			default:
				streamRes, e := stream.Recv()
				if e != nil {
					if e == io.EOF {
						closeFn()
						return
					}

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

	return resChan, closeFn, nil
}
