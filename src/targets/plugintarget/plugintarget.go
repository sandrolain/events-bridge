package plugintarget

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/targets"
)

type PluginTarget struct {
	config  *targets.TargetPluginConfig
	timeout time.Duration
	slog    *slog.Logger
	mgr     *plugin.PluginManager
	plg     *plugin.Plugin
	stopped bool
	stopCh  chan struct{}
}

func New(mgr *plugin.PluginManager, cfg *targets.TargetPluginConfig) (targets.Target, error) {
	if mgr == nil {
		return nil, fmt.Errorf("plugin manager cannot be nil")
	}
	if cfg == nil {
		return nil, fmt.Errorf("plugin target config cannot be nil")
	}

	plg, err := mgr.GetPlugin(cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin %s: %w", cfg.Name, err)
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = targets.DefaultTimeout
	}

	t := &PluginTarget{
		config:  cfg,
		timeout: timeout,
		slog:    slog.Default().With("context", "Plugin Target", "name", cfg.Name),
		mgr:     mgr,
		plg:     plg,
		stopCh:  make(chan struct{}),
	}
	return t, nil
}

func (t *PluginTarget) Consume(c <-chan message.Message) error {
	t.slog.Info("starting plugin target", "name", t.config.Name, "plugin", t.plg.Config.Name)
	go func() {
		for {
			select {
			case <-t.stopCh:
				return
			case msg, ok := <-c:
				if !ok {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
				err := t.plg.Target(ctx, msg)
				if err != nil {
					msg.Nak()
					t.slog.Error("plugin target error", "err", err)
				} else {
					msg.Ack()
				}
				cancel()
			}
		}
	}()
	return nil
}

func (t *PluginTarget) Close() error {
	t.stopped = true
	if t.stopCh != nil {
		close(t.stopCh)
	}
	if t.plg != nil {
		t.plg.Stop()
	}
	return nil
}
