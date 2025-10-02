package connectors

import (
	"github.com/sandrolain/events-bridge/src/message"
)

const NewRunnerMethodName = "NewRunner"
const NewRunnerConfigName = "NewRunnerConfig"

type Runner interface {
	Process(*message.RunnerMessage) (*message.RunnerMessage, error)
	Close() error
}

type RunnerConfig struct {
	Type       string         `yaml:"type" json:"type"`
	Routines   int            `yaml:"routines" json:"routines" validate:"omitempty,min=1"`
	Options    map[string]any `yaml:"options" json:"options"`
	IfExpr     string         `yaml:"ifExpr" json:"ifExpr" validate:"omitempty"`
	FilterExpr string         `yaml:"filterExpr" json:"filterExpr" validate:"omitempty"`
}
