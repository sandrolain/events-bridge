package connectors

import (
	"github.com/sandrolain/events-bridge/src/message"
)

const NewTargetMethodName = "NewTarget"

type Target interface {
	Consume(*message.RunnerMessage) error
	Close() error
}

type TargetConfig struct {
	Type     string         `yaml:"type" json:"type" validate:"required"`
	Buffer   int            `yaml:"buffer" json:"buffer"`
	Routines int            `yaml:"routines" json:"routines" validate:"omitempty,min=1"`
	Options  map[string]any `yaml:"options" json:"options"`
}
