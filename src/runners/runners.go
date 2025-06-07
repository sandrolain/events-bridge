package runners

import (
	"github.com/sandrolain/events-bridge/src/runners/clirunner"
	"github.com/sandrolain/events-bridge/src/runners/es5runner"
	"github.com/sandrolain/events-bridge/src/runners/pluginrunner"
	"github.com/sandrolain/events-bridge/src/runners/wasmrunner"
)

type RunnerType string

const (
	RunnerTypeNone   RunnerType = "none"
	RunnerTypeWASM   RunnerType = "wasm"
	RunnerTypeES5    RunnerType = "es5"
	RunnerTypePlugin RunnerType = "plugin"
	RunnerTypeCLI    RunnerType = "cli"
)

type RunnerConfig struct {
	Type   RunnerType                       `yaml:"type" json:"type" validate:"required,oneof=wasm es5 plugin cli"`
	WASM   *wasmrunner.RunnerWASMConfig     `yaml:"wasm" json:"wasm"`
	ES5    *es5runner.RunnerES5Config       `yaml:"es5" json:"es5"`
	Plugin *pluginrunner.RunnerPluginConfig `yaml:"plugin" json:"plugin"`
	CLI    *clirunner.RunnerCLIConfig       `yaml:"cli" json:"cli"`
}
