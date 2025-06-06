package plugintarget

import (
	"fmt"
	"log/slog"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/targets/target"
)

type TargetPluginConfig struct {
	Name   string            `yaml:"name" json:"name" validate:"required"`
	Config map[string]string `yaml:"config" json:"config"`
}

type PluginTarget struct {
	config  *TargetPluginConfig
	slog    *slog.Logger
	mgr     *plugin.PluginManager
	plg     *plugin.Plugin
	stopped bool
	stopCh  chan struct{} // canale di stop
}

func New(mgr *plugin.PluginManager, cfg *TargetPluginConfig) (target.Target, error) {
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

	t := &PluginTarget{
		config: cfg,
		slog:   slog.Default().With("context", "Plugin Target", "name", cfg.Name),
		mgr:    mgr,
		plg:    plg,
		stopCh: make(chan struct{}), // inizializza canale di stop
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
				err := t.plg.Target(msg)
				if err != nil {
					msg.Nak()
					t.slog.Error("plugin target error", "err", err)
				} else {
					msg.Ack()
				}
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
