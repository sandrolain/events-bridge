package runners

import (
	"github.com/sandrolain/events-bridge/src/runners/es5runner"
	"github.com/sandrolain/events-bridge/src/runners/pluginrunner"
	"github.com/sandrolain/events-bridge/src/runners/wasmrunner"
)

type RunnerType string

const (
	RunnerTypeWASM RunnerType = "wasm"
	RunnerTypeES5  RunnerType = "es5"
)

type RunnerConfig struct {
	Type   RunnerType                       `yaml:"type" json:"type" validate:"required,oneof=wasm es5"`
	WASM   *wasmrunner.RunnerWASMConfig     `yaml:"wasm" json:"wasm"`
	ES5    *es5runner.RunnerES5Config       `yaml:"es5" json:"es5"`
	Plugin *pluginrunner.RunnerPluginConfig `yaml:"plugin" json:"plugin"`
}
