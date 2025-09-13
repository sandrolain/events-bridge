package pluginrunner

import (
	"fmt"
	"log/slog"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/runners"
)

// Assicura che PluginRunner implementi runner.Runner
var _ runners.Runner = &PluginRunner{}

// Configurazione per il runner plugin
// Usa solo l'ID del plugin da avviare
// Eventuali altre opzioni possono essere aggiunte in futuro

type PluginRunner struct {
	cfg    *runners.RunnerPluginConfig
	slog   *slog.Logger
	mgr    *plugin.PluginManager
	plg    *plugin.Plugin
	stopCh chan struct{} // canale di stop
}

// New crea una nuova istanza di PluginRunner
func New(mgr *plugin.PluginManager, cfg *runners.RunnerPluginConfig) (runners.Runner, error) {
	if mgr == nil {
		return nil, fmt.Errorf("plugin manager cannot be nil")
	}
	if cfg == nil {
		return nil, fmt.Errorf("runner plugin config cannot be nil")
	}

	plg, err := mgr.GetPlugin(cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin %s: %w", cfg.Name, err)
	}
	return &PluginRunner{
		cfg:    cfg,
		slog:   slog.Default().With("context", "Plugin Runner", "id", cfg.Name),
		mgr:    mgr,
		plg:    plg,
		stopCh: make(chan struct{}),
	}, nil
}

func (p *PluginRunner) Process(msg message.Message) (message.Message, error) {
	return p.plg.Runner(msg)
}

func (p *PluginRunner) Close() error {
	p.slog.Info("closing plugin runner")
	select {
	case <-p.stopCh:
		// già chiuso
	default:
		close(p.stopCh)
	}
	return nil
}
