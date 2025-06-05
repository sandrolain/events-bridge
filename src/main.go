package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
	"github.com/sandrolain/events-bridge/src/config"
	"github.com/sandrolain/events-bridge/src/models"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/runners"
	"github.com/sandrolain/events-bridge/src/runners/es5runner"
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

	cfg, err := config.LoadConfigFile[models.Config](envCfg.ConfigFilePath)
	if err != nil {
		slog.Error("failed to load configuration file", "error", err)
		os.Exit(1)
	}

	var source source.Source
	var runner runner.Runner
	var target target.Target

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
		slog.Info("using Plugin source", "path", cfg.Source.Plugin.ID)
		mng, err := plugin.GetPluginManager()
		if err != nil {
			slog.Error("failed to get plugin manager", "error", err)
			os.Exit(1)
		}
		source, err = pluginsource.New(mng, cfg.Source.Plugin)
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
		target, err = httptarget.New(cfg.Target.HTTP)
	case targets.TargetTypeCoAP:
		target, err = coaptarget.New(cfg.Target.CoAP)
	case targets.TargetTypeMQTT:
		target, err = mqtttarget.New(cfg.Target.MQTT)
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
	default:
		slog.Error("unsupported runner type", "type", cfg.Runner.Type)
		os.Exit(1)
	}

	if err != nil {
		slog.Error("failed to create runner", "error", err)
		os.Exit(1)
	}

	defer runner.Close()

	c, err := source.Produce(cfg.Source.Buffer)
	if err != nil {
		slog.Error("failed to produce messages from source", "error", err)
		os.Exit(1)
	}

	slog.Info("starting to consume messages from source")

	r, err := runner.Ingest(c)
	if err != nil {
		slog.Error("failed to ingest messages with runner", "error", err)
		os.Exit(1)
	}

	err = target.Consume(r)
	if err != nil {
		slog.Error("failed to consume messages from target", "error", err)
		os.Exit(1)
	}
	slog.Info("started consuming messages from source, processing...")

	select {}

}
