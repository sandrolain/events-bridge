package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/destel/rill"
	"github.com/lmittmann/tint"
	"github.com/sandrolain/events-bridge/src/config"
	pluginconn "github.com/sandrolain/events-bridge/src/connectors/plugin"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/runners"
	"github.com/sandrolain/events-bridge/src/runners/clirunner"
	"github.com/sandrolain/events-bridge/src/runners/pluginrunner"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/sandrolain/events-bridge/src/targets"
	"github.com/sandrolain/events-bridge/src/utils"
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

	envCfg, err := config.LoadEnvConfigFile[config.EnvConfig]()

	if err != nil {
		fatal(err, "failed to load environment configuration")
	}

	// This is the main entry point for the application.
	// The actual implementation would typically involve initializing
	// the configuration, setting up sources and targets, and starting
	// the event processing loop.

	slog.Info("loading configuration file", "path", envCfg.ConfigFilePath)

	cfg, err := config.LoadConfigFile[config.Config](envCfg.ConfigFilePath)
	if err != nil {
		fatal(err, "failed to load configuration file")
	}

	runnerRoutines := cfg.Runner.Routines
	if runnerRoutines < 1 {
		runnerRoutines = 1
	}

	targetRoutines := cfg.Target.Routines
	if targetRoutines < 1 {
		targetRoutines = 1
	}

	var source sources.Source
	var runner runners.Runner
	var target targets.Target

	// Plugin manager initialization
	plgMan, err := plugin.GetPluginManager()
	if err != nil {
		fatal(err, "failed to get plugin manager")
	}

	if cfg.Plugins != nil {
		slog.Info("loading plugins", "count", len(cfg.Plugins))
		for _, p := range cfg.Plugins {
			slog.Info("loading plugin", "id", p.Name, "exec", p.Exec)
			plg, err := plgMan.CreatePlugin(p)
			if err != nil {
				fatal(err, "failed to load plugin")
			}

			err = plg.Start()
			if err != nil {
				fatal(err, "failed to start plugin")
				os.Exit(1)
			}
		}
	} else {
		slog.Info("no plugins configured")
	}

	// Setup source, runner, and target

	source, err = createSource(cfg.Source)
	if err != nil {
		fatal(err, "failed to create source")
	}
	defer func() {
		if err := source.Close(); err != nil {
			slog.Error("failed to close source", "error", err)
		}
	}()

	runner, err = createRunner(cfg.Runner)
	if err != nil {
		fatal(err, "failed to create runner")
	}
	if runner != nil {
		slog.Info("runner created, deferring close")
		defer func() {
			if err := runner.Close(); err != nil {
				slog.Error("failed to close runner", "error", err)
			}
		}()
	}

	target, err = createTarget(cfg.Target)
	if err != nil {
		fatal(err, "failed to create target")
	}
	if target != nil {
		slog.Info("target created, deferring close")
		defer func() {
			if err := target.Close(); err != nil {
				slog.Error("failed to close target", "error", err)
			}
		}()
	}

	// Start the event incoming loop

	c, err := source.Produce(cfg.Source.Buffer)
	if err != nil {
		fatal(err, "failed to produce messages from source")
	}
	defer func() {
		if err := source.Close(); err != nil {
			slog.Error("failed to close source", "error", err)
		}
	}()

	out := rill.FromChan(c, nil)

	if runner != nil {
		slog.Info("runner starting to consume messages from source")

		out = rill.OrderedFilterMap(out, runnerRoutines, func(msg *message.RunnerMessage) (*message.RunnerMessage, bool, error) {
			res, err := runner.Process(msg)
			if err != nil {
				msg.Nak()
				slog.Error("error processing message", "error", err)
				return nil, false, nil
			}
			return res, true, nil
		})
	} else {
		slog.Info("no runner configured, passing messages through without processing")
	}

	if target != nil {
		slog.Info("target starting to consume messages")

		rill.ForEach(out, targetRoutines, func(msg *message.RunnerMessage) error {
			err := target.Consume(msg)
			if err != nil {
				slog.Error("failed to consume message", "error", err)
				msg.Nak()
			} else {
				msg.Ack()
			}
			return nil
		})
	} else {
		rill.ForEach(out, 1, func(msg *message.RunnerMessage) error {
			msg.Reply()
			return nil
		})
	}

	rill.Drain(out)

	select {}
}

func createSource(cfg sources.SourceConfig) (source sources.Source, err error) {
	slog.Info("creating source", "type", cfg.Type, "buffer", cfg.Buffer)

	switch cfg.Type {
	case sources.SourceTypeHTTP:
		source, err = utils.LoadPlugin[*sources.SourceHTTPConfig, sources.Source]("./connectors/http.so", sources.NewMethodName, cfg.HTTP)
	case sources.SourceTypeCoAP:
		source, err = utils.LoadPlugin[*sources.SourceCoAPConfig, sources.Source]("./connectors/coap.so", sources.NewMethodName, cfg.CoAP)
	case sources.SourceTypeMQTT:
		source, err = utils.LoadPlugin[*sources.SourceMQTTConfig, sources.Source]("./connectors/mqtt.so", sources.NewMethodName, cfg.MQTT)
	case sources.SourceTypeNATS:
		source, err = utils.LoadPlugin[*sources.SourceNATSConfig, sources.Source]("./connectors/nats.so", sources.NewMethodName, cfg.NATS)
	case sources.SourceTypeKafka:
		source, err = utils.LoadPlugin[*sources.SourceKafkaConfig, sources.Source]("./connectors/kafka.so", sources.NewMethodName, cfg.Kafka)
	case sources.SourceTypeRedis:
		source, err = utils.LoadPlugin[*sources.SourceRedisConfig, sources.Source]("./connectors/redis.so", sources.NewMethodName, cfg.Redis)
	case sources.SourceTypePGSQL:
		source, err = utils.LoadPlugin[*sources.SourcePGSQLConfig, sources.Source]("./connectors/pgsql.so", sources.NewMethodName, cfg.PgSQL)
	case sources.SourceTypeGit:
		source, err = utils.LoadPlugin[*sources.SourceGitConfig, sources.Source]("./connectors/git.so", sources.NewMethodName, cfg.Git)
	case sources.SourceTypePlugin:
		slog.Info("using Plugin source", "path", cfg.Plugin.Name)
		plgMan, e := plugin.GetPluginManager()
		if e != nil {
			err = fmt.Errorf("failed to get plugin manager: %w", e)
			return
		}
		source, err = pluginconn.NewSource(plgMan, cfg.Plugin)
	default:
		err = fmt.Errorf("unsupported source type: %s", cfg.Type)
	}
	return
}

func createTarget(cfg targets.TargetConfig) (target targets.Target, err error) {
	slog.Info("creating target", "type", cfg.Type)

	switch cfg.Type {
	case targets.TargetTypeHTTP:
		target, err = utils.LoadPlugin[*targets.TargetHTTPConfig, targets.Target]("./connectors/http.so", targets.NewMethodName, cfg.HTTP)
	case targets.TargetTypeCoAP:
		target, err = utils.LoadPlugin[*targets.TargetCoAPConfig, targets.Target]("./connectors/coap.so", targets.NewMethodName, cfg.CoAP)
	case targets.TargetTypeMQTT:
		target, err = utils.LoadPlugin[*targets.TargetMQTTConfig, targets.Target]("./connectors/mqtt.so", targets.NewMethodName, cfg.MQTT)
	case targets.TargetTypeNATS:
		target, err = utils.LoadPlugin[*targets.TargetNATSConfig, targets.Target]("./connectors/nats.so", targets.NewMethodName, cfg.NATS)
	case targets.TargetTypeKafka:
		target, err = utils.LoadPlugin[*targets.TargetKafkaConfig, targets.Target]("./connectors/kafka.so", targets.NewMethodName, cfg.Kafka)
	case targets.TargetTypeRedis:
		target, err = utils.LoadPlugin[*targets.TargetRedisConfig, targets.Target]("./connectors/redis.so", targets.NewMethodName, cfg.Redis)
	case targets.TargetTypePlugin:
		plgMan, e := plugin.GetPluginManager()
		if e != nil {
			err = fmt.Errorf("failed to get plugin manager: %w", e)
			return
		}
		target, err = pluginconn.NewTarget(plgMan, cfg.Plugin)
	case targets.TargetTypeNone:
		slog.Info("no target configured, messages will be replyed to source if supported")
		target = nil
	default:
		err = fmt.Errorf("unsupported target type: %s", cfg.Type)
	}
	return
}

func createRunner(cfg runners.RunnerConfig) (runner runners.Runner, err error) {
	slog.Info("creating runner", "type", cfg.Type)

	switch cfg.Type {
	case runners.RunnerTypeWASM:
		runner, err = utils.LoadPlugin[*runners.RunnerWASMConfig, runners.Runner]("./runners/wasmrunner.so", runners.NewMethodName, cfg.WASM)
	case runners.RunnerTypeES5:
		runner, err = utils.LoadPlugin[*runners.RunnerES5Config, runners.Runner]("./runners/es5runner.so", runners.NewMethodName, cfg.ES5)
	case runners.RunnerTypeGPT:
		runner, err = utils.LoadPlugin[*runners.RunnerGPTRunnerConfig, runners.Runner]("./runners/gptrunner.so", runners.NewMethodName, cfg.GPT)
	case runners.RunnerTypeJSONLogic:
		runner, err = utils.LoadPlugin[*runners.RunnerJSONLogicConfig, runners.Runner]("./runners/jlorunner.so", runners.NewMethodName, cfg.JSONLogic)
	case runners.RunnerTypeCLI:
		runner, err = clirunner.New(cfg.CLI)
	case runners.RunnerTypePlugin:
		plgMan, e := plugin.GetPluginManager()
		if e != nil {
			err = fmt.Errorf("failed to get plugin manager: %w", e)
			return
		}
		runner, err = pluginrunner.New(plgMan, cfg.Plugin)
	case runners.RunnerTypeNone:
		slog.Info("no runner configured, messages will be passed through without processing")
		runner = nil
	default:
		err = fmt.Errorf("unsupported runner type: %s", cfg.Type)
	}
	return
}

func fatal(err error, log string) {
	slog.Error(log, "error", err)
	os.Exit(1)
}
