package config

import (
	"fmt"
	"log/slog"
	"os"

	"path/filepath"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/caarlos0/env/v11"
	"github.com/go-playground/validator/v10"

	"github.com/goccy/go-yaml"
)

func LoadEnvConfigFile[T any]() (cfg *T, err error) {
	cfg = new(T)
	err = env.Parse(cfg)
	if err != nil {
		return
	}
	validate := validator.New()
	err = validate.Struct(cfg)

	return
}

func LoadConfigFile[T any](path string) (cfg *T, err error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return
	}

	file, err := os.Open(absPath)
	if err != nil {
		err = fmt.Errorf("error opening config file: %w", err)
		return
	}
	defer func ()  {
		err = file.Close()
		if err != nil {
			slog.Error("error closing config file", "path", absPath, "err", err)
		}
	}()

	ext := strings.ToLower(filepath.Ext(absPath))
	cfg = new(T)

	switch ext {
	case ".yaml", ".yml":
		decoder := yaml.NewDecoder(file)
		err = decoder.Decode(cfg)
	case ".json":
		decoder := sonic.ConfigDefault.NewDecoder(file)
		err = decoder.Decode(cfg)
	default:
		err = &UnsupportedExtensionError{Extension: ext}
	}

	if err != nil {
		err = fmt.Errorf("error decoding config file: %w", err)
		return
	}

	validate := validator.New()
	err = validate.Struct(cfg)

	return
}

type UnsupportedExtensionError struct {
	Extension string
}

func (e *UnsupportedExtensionError) Error() string {
	return "unsupported config file extension: " + e.Extension
}
