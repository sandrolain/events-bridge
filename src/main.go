package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
	"github.com/sandrolain/events-bridge/src/config"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/models"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/runners"
	"github.com/sandrolain/events-bridge/src/runners/clirunner"
	"github.com/sandrolain/events-bridge/src/runners/es5runner"
	"github.com/sandrolain/events-bridge/src/runners/pluginrunner"
	"github.com/sandrolain/events-bridge/src/runners/runner"
	"github.com/sandrolain/events-bridge/src/runners/wasmrunner"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/sandrolain/events-bridge/src/sources/coapsource"
	"github.com/sandrolain/events-bridge/src/sources/httpsource"
	"github.com/sandrolain/events-bridge/src/sources/mqttsource"
	"github.com/sandrolain/events-bridge/src/sources/natssource"
	"github.com/sandrolain/events-bridge/src/sources/pluginsource"
	"github.com/sandrolain/events-bridge/src/sources/source"
	"github.com/sandrolain/events-bridge/src/targets"
	"github.com/sandrolain/events-bridge/src/targets/coaptarget"
	"github.com/sandrolain/events-bridge/src/targets/httptarget"
	"github.com/sandrolain/events-bridge/src/targets/mqtttarget"
	"github.com/sandrolain/events-bridge/src/targets/plugintarget"
	"github.com/sandrolain/events-bridge/src/targets/target"
)

func main() {
	w := os.Stdout

	// Set global logger with custom options
	slog.SetDefault(slog.New(
		tint.NewHandler(w, &tint.Options{
			Level:      slog.LevelDebug,
			TimeFormat: time.Kitchen,
		}),
	))

	envCfg, err := config.LoadEnvConfigFile[models.EnvConfig]()

	if err != nil {
		slog.Error("failed to load environment configuration", "error", err)
		os.Exit(1)
	}

	// This is the main entry point for the application.
	// The actual implementation would typically involve initializing
	// the configuration, setting up sources and targets, and starting
	// the event processing loop.

	slog.Info("loading configuration file", "path", envCfg.ConfigFilePath)

	cfg, err := config.LoadConfigFile[models.Config](envCfg.ConfigFilePath)
	if err != nil {
		slog.Error("failed to load configuration file", "error", err)
		os.Exit(1)
	}

	var source source.Source
	var runner runner.Runner
	var target target.Target

	// Plugin manager initialization
	plgMan, err := plugin.GetPluginManager()
	if err != nil {
		slog.Error("failed to get plugin manager", "error", err)
		os.Exit(1)
	}

	if cfg.Plugins != nil {
		slog.Info("loading plugins", "count", len(cfg.Plugins))
		for _, p := range cfg.Plugins {
			slog.Info("loading plugin", "id", p.Name, "exec", p.Exec)
			plg, err := plgMan.CreatePlugin(p)
			if err != nil {
				slog.Error("failed to load plugin", "id", p.Name, "error", err)
				os.Exit(1)
			}

			err = plg.Start()
			if err != nil {
				slog.Error("failed to start plugin", "id", p.Name, "error", err)
				os.Exit(1)
			}
		}
	} else {
		slog.Info("no plugins configured")
	}

	switch cfg.Source.Type {
	case sources.SourceTypeHTTP:
		slog.Info("using HTTP source", "address", cfg.Source.HTTP.Address, "path", cfg.Source.HTTP.Path, "method", cfg.Source.HTTP.Method)
		source, err = httpsource.New(cfg.Source.HTTP)
	case sources.SourceTypeCoAP:
		slog.Info("using CoAP source", "address", cfg.Source.CoAP.Address, "path", cfg.Source.CoAP.Path, "method", cfg.Source.CoAP.Method)
		source, err = coapsource.New(cfg.Source.CoAP)
	case sources.SourceTypeMQTT:
		slog.Info("using MQTT source", "address", cfg.Source.MQTT.Address, "topic", cfg.Source.MQTT.Topic, "consumerGroup", cfg.Source.MQTT.ConsumerGroup)
		source, err = mqttsource.New(cfg.Source.MQTT)
	case sources.SourceTypeNATS:
		slog.Info("using NATS source", "address", cfg.Source.NATS.Address, "subject", cfg.Source.NATS.Subject, "queueGroup", cfg.Source.NATS.QueueGroup)
		source, err = natssource.New(cfg.Source.NATS)
	case sources.SourceTypePlugin:
		slog.Info("using Plugin source", "path", cfg.Source.Plugin.Name)
		source, err = pluginsource.New(plgMan, cfg.Source.Plugin)
	default:
		slog.Error("unsupported source type", "type", cfg.Source.Type)
		os.Exit(1)
	}

	if err != nil {
		slog.Error("failed to create source", "error", err)
		os.Exit(1)
	}
	defer source.Close()

	// Target
	switch cfg.Target.Type {
	case targets.TargetTypeHTTP:
		slog.Info("using HTTP target", "method", cfg.Target.HTTP.Method, "url", cfg.Target.HTTP.URL, "headers", cfg.Target.HTTP.Headers)
		target, err = httptarget.New(cfg.Target.HTTP)
	case targets.TargetTypeCoAP:
		slog.Info("using CoAP target", "address", cfg.Target.CoAP.Address, "path", cfg.Target.CoAP.Path, "method", cfg.Target.CoAP.Method)
		target, err = coaptarget.New(cfg.Target.CoAP)
	case targets.TargetTypeMQTT:
		slog.Info("using MQTT target", "address", cfg.Target.MQTT.Address, "topic", cfg.Target.MQTT.Topic, "qos", cfg.Target.MQTT.QoS)
		target, err = mqtttarget.New(cfg.Target.MQTT)
	case targets.TargetTypePlugin:
		target, err = plugintarget.New(plgMan, cfg.Target.Plugin)
	default:
		slog.Error("unsupported target type", "type", cfg.Target.Type)
		os.Exit(1)
	}

	if err != nil {
		slog.Error("failed to create target", "error", err)
		os.Exit(1)
	}

	defer target.Close()

	switch cfg.Runner.Type {
	case runners.RunnerTypeWASM:
		slog.Info("using WASM runner", "path", cfg.Runner.WASM.Path)
		runner, err = wasmrunner.New(cfg.Runner.WASM)
	case runners.RunnerTypeES5:
		slog.Info("using ES5 runner", "path", cfg.Runner.ES5.Path)
		runner, err = es5runner.New(cfg.Runner.ES5)
	case runners.RunnerTypePlugin:
		slog.Info("using Plugin runner", "id", cfg.Runner.Plugin.Name)
		runner, err = pluginrunner.New(plgMan, cfg.Runner.Plugin)
	case runners.RunnerTypeCLI:
		slog.Info("using CLI runner", "command", cfg.Runner.CLI.Command, "args", cfg.Runner.CLI.Args)
		runner, err = clirunner.New(cfg.Runner.CLI)
	case runners.RunnerTypeNone:
		slog.Info("no runner configured, messages will be passed through without processing")
	default:
		slog.Error("unsupported runner type", "type", cfg.Runner.Type)
		os.Exit(1)
	}

	if err != nil {
		slog.Error("failed to create runner", "error", err)
		os.Exit(1)
	}
	if runner != nil {
		defer runner.Close()
	}

	c, err := source.Produce(cfg.Source.Buffer)
	if err != nil {
		slog.Error("failed to produce messages from source", "error", err)
		os.Exit(1)
	}

	var r <-chan message.Message
	if runner != nil {
		slog.Info("runner starting to consume messages from source")
		r, err = runner.Ingest(c)
		if err != nil {
			slog.Error("failed to ingest messages with runner", "error", err)
			os.Exit(1)
		}
	} else {
		slog.Info("no runner configured, passing messages through without processing")
		r = c
	}

	slog.Info("target starting to consume messages from source")

	err = target.Consume(r)
	if err != nil {
		slog.Error("failed to consume messages from target", "error", err)
		os.Exit(1)
	}

	select {}

}
