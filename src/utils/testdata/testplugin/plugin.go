package main

import "fmt"

type Runner interface {
	GetName() string
	GetValue() int
}

type TestRunner struct {
	Name  string
	Value int
}

func (tr *TestRunner) GetName() string {
	return tr.Name
}

func (tr *TestRunner) GetValue() int {
	return tr.Value
}

type TestConfig struct {
	Name  string `mapstructure:"name"`
	Value int    `mapstructure:"value"`
}

func NewConfig() any {
	return &TestConfig{}
}

func NewRunner(cfg any) (Runner, error) {
	config, ok := cfg.(*TestConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type")
	}
	return &TestRunner{
		Name:  config.Name,
		Value: config.Value,
	}, nil
}

func NewInvalidRunner(cfg any) (string, error) {
	return "invalid", nil
}

func InvalidConfig() string {
	return "invalid"
}
