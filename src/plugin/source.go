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
					time.Sleep(100 * time.Millisecond) // Attendi un po' prima di riprovare
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
		// chiude stopChan solo se non è già chiuso
		select {
		case <-stopChan:
			// già chiuso
		default:
			// chiusura corretta senza argomenti
			close(stopChan)
		}
	}

	return
}
