package pluginrunner

import (
	"fmt"
	"log/slog"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/runners/runner"
)

// Assicura che PluginRunner implementi runner.Runner
var _ runner.Runner = &PluginRunner{}

// Configurazione per il runner plugin
// Usa solo l'ID del plugin da avviare
// Eventuali altre opzioni possono essere aggiunte in futuro

type RunnerPluginConfig struct {
	ID string `yaml:"id" json:"id" validate:"required"`
}

type PluginRunner struct {
	cfg    *RunnerPluginConfig
	slog   *slog.Logger
	mgr    *plugin.PluginManager
	plg    *plugin.Plugin
	start  bool
	stopCh chan struct{} // canale di stop
}

// New crea una nuova istanza di PluginRunner
func New(mgr *plugin.PluginManager, cfg *RunnerPluginConfig) (runner.Runner, error) {
	plg, err := mgr.GetPlugin(cfg.ID)
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin %s: %w", cfg.ID, err)
	}
	return &PluginRunner{
		cfg:    cfg,
		slog:   slog.Default().With("context", "Plugin Runner", "id", cfg.ID),
		mgr:    mgr,
		plg:    plg,
		stopCh: make(chan struct{}),
	}, nil
}

// Ingest riceve i messaggi, li processa tramite il plugin e restituisce i messaggi processati
func (p *PluginRunner) Ingest(in <-chan message.Message) (<-chan message.Message, error) {
	if !p.start {
		err := p.plg.Start()
		if err != nil {
			return nil, fmt.Errorf("cannot start plugin %s: %w", p.cfg.ID, err)
		}
		p.start = true
	}
	p.slog.Info("starting plugin runner ingest", "id", p.cfg.ID)
	out := make(chan message.Message)
	go func() {
		defer close(out)
		for {
			select {
			case msg, ok := <-in:
				if !ok {
					return
				}
				res, err := p.plg.Runner(msg)
				if err != nil {
					p.slog.Error("plugin runner error", "error", err)
					msg.Nak()
					continue
				}
				out <- res
			case <-p.stopCh:
				p.slog.Info("plugin runner stopped via stopCh")
				return
			}
		}
	}()
	return out, nil
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
