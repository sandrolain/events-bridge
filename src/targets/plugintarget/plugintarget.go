package plugintarget

import (
	"fmt"
	"log/slog"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/targets/target"
)

type TargetPluginConfig struct {
	ID     string            `yaml:"id" json:"id" validate:"required"`
	Config map[string]string `yaml:"config" json:"config" validate:"required"`
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
	plg, err := mgr.GetPlugin(cfg.ID)
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin %s: %w", cfg.ID, err)
	}

	t := &PluginTarget{
		config: cfg,
		slog:   slog.Default().With("context", "PLUGIN-TARGET"),
		mgr:    mgr,
		plg:    plg,
		stopCh: make(chan struct{}), // inizializza canale di stop
	}
	return t, nil
}

func (t *PluginTarget) Consume(c <-chan message.Message) error {
	t.slog.Info("starting plugin target", "id", t.config.ID, "plugin", t.plg.Name)
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
