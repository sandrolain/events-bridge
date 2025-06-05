package plugin

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

var globManager *PluginManager

func GetPluginManager() (res *PluginManager, err error) {
	if globManager != nil {
		return globManager, nil
	}

	res, err = NewPluginManager()
	if err != nil {
		return nil, fmt.Errorf("cannot create plugin manager: %w", err)
	}

	globManager = res
	return res, nil
}

func NewPluginManager() (res *PluginManager, err error) {

	l := slog.Default().With("context", "Plugin Manager")

	l.Info("starting plugin manager")

	res = &PluginManager{
		slog:    l,
		plugins: make(map[string][]*Plugin),
	}
	return
}

type PluginManager struct {
	slog    *slog.Logger
	plugins map[string][]*Plugin
	server  *grpc.Server
}

func (p *PluginManager) Stop() (err error) {
	for _, list := range p.plugins {
		for _, plugin := range list {
			plugin.Stop()
		}
	}
	p.server.Stop()
	return
}

func (p *PluginManager) CreatePlugin(cfg PluginConfig) (res *Plugin, err error) {
	p.slog.Info("creating plugin", "id", cfg.ID)

	host := "localhost"
	port, err := GetFreePort()
	if err != nil {
		err = fmt.Errorf("cannot get free port: %w", err)
		return
	}

	delay, err := time.ParseDuration(cfg.Delay)
	if err != nil {
		err = fmt.Errorf("cannot parse delay: %w", err)
		return
	}

	id := uuid.New().String()

	slog.Info("creating plugin", "id", id, "host", host, "port", port)

	res = &Plugin{
		Config:    cfg,
		ID:        id,
		Host:      host,
		Port:      port,
		Exec:      cfg.Exec,
		Name:      cfg.ID,
		Args:      cfg.Args,
		Env:       cfg.Env,
		ConnRetry: cfg.Retry,
		ConnDelay: delay,
		Output:    cfg.Output,
		slog:      p.slog.With("plugin", cfg.ID, "id", id),
	}

	pluginsList, ok := p.plugins[cfg.ID]
	if !ok {
		pluginsList = make([]*Plugin, 0)
	}
	p.plugins[cfg.ID] = append(pluginsList, res)

	return
}

func (p *PluginManager) GetPlugin(id string) (res *Plugin, err error) {
	pluginsList, ok := p.plugins[id]
	if !ok {
		err = fmt.Errorf("plugin not found")
		return
	}
	if len(pluginsList) == 0 {
		err = fmt.Errorf("plugin not found")
		return
	}
	// TODO: implement weighted
	res = pluginsList[0]
	return
}
