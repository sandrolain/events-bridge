package connectors

import (
	"github.com/sandrolain/events-bridge/src/message"
)

const NewRunnerMethodName = "NewRunner"

type Runner interface {
	Process(*message.RunnerMessage) (*message.RunnerMessage, error)
	Close() error
}

type RunnerConfig struct {
	Type     string         `yaml:"type" json:"type" validate:"required"`
	Routines int            `yaml:"routines" json:"routines" validate:"omitempty,min=1"`
	Options  map[string]any `yaml:"options" json:"options"`
}
