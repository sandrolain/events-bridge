package common

import (
	"github.com/creasty/defaults"
	"github.com/go-playground/validator/v10"
	"github.com/mitchellh/mapstructure"
)

func ParseConfig[T any](opts map[string]any) (res *T, err error) {
	res = new(T)
	if err = defaults.Set(res); err != nil {
		return
	}

	if err = mapstructure.Decode(opts, res); err != nil {
		return
	}

	validate := validator.New()
	if err = validate.Struct(res); err != nil {
		return
	}

	return
}
