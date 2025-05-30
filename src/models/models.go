package models

import "github.com/sandrolain/events-bridge/src/message"

type Runner interface {
	Ingest(<-chan message.Message) (<-chan message.Message, error)
	Close() error
}

type RunnerType string

const (
	RunnerTypeWASM RunnerType = "wasm"
	RunnerTypeES5  RunnerType = "es5"
)
