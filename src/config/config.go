package config

import (
	"fmt"
	"log/slog"
	"os"

	"path/filepath"
	"strings"

	"github.com/go-playground/validator/v10"
	kjson "github.com/knadh/koanf/parsers/json"
	kyaml "github.com/knadh/koanf/parsers/yaml"
	kenv "github.com/knadh/koanf/providers/env"
	kfile "github.com/knadh/koanf/providers/file"
	kraw "github.com/knadh/koanf/providers/rawbytes"
	kfn "github.com/knadh/koanf/v2"
)

func LoadConfig() (cfg *Config, err error) {
	// Precedence: CLI > Env
	envCfg, err := loadEnvConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load environment configuration: %w", err)
	}

	// Apply CLI overrides on top of envs
	if err := applyCLIOverrides(envCfg); err != nil {
		return nil, fmt.Errorf("failed to apply CLI overrides: %w", err)
	}

	// Validate after merging
	validate := validator.New()
	if err = validate.Struct(envCfg); err != nil {
		return nil, fmt.Errorf("failed to validate configuration options: %w", err)
	}

	if envCfg.ConfigContent != "" {
		slog.Info("loading configuration from content", "format", envCfg.ConfigFormat)
		return loadConfigContent(envCfg.ConfigContent, envCfg.ConfigFormat)
	}

	slog.Info("loading configuration file", "path", envCfg.ConfigFilePath)
	return loadConfigFile(envCfg.ConfigFilePath)
}

// applyCLIOverrides sets EnvConfig fields from CLI flags if provided.
// Supported flags (long form only):
//
//	--config-file-path <path> | --config-file-path=<path>
//	--config-content <yaml|json string> | --config-content=<...>
//	--config-format <yaml|yml|json> | --config-format=<yaml|yml|json>
//
// CLI values take precedence over environment variables.
func applyCLIOverrides(cfg *EnvConfig) error {
	args := os.Args[1:]
	supportedFormats := map[string]bool{"yaml": true, "yml": true, "json": true}

	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case strings.HasPrefix(a, "--config-file-path="):
			path := strings.TrimPrefix(a, "--config-file-path=")
			if strings.TrimSpace(path) == "" {
				return fmt.Errorf("--config-file-path cannot be empty")
			}
			cfg.ConfigFilePath = path
		case a == "--config-file-path":
			if i+1 < len(args) {
				path := args[i+1]
				if strings.TrimSpace(path) == "" {
					return fmt.Errorf("--config-file-path cannot be empty")
				}
				cfg.ConfigFilePath = path
				i++
			} else {
				return fmt.Errorf("--config-file-path requires a value")
			}
		case strings.HasPrefix(a, "--config-content="):
			content := strings.TrimPrefix(a, "--config-content=")
			if strings.TrimSpace(content) == "" {
				return fmt.Errorf("--config-content cannot be empty")
			}
			cfg.ConfigContent = content
		case a == "--config-content":
			if i+1 < len(args) {
				content := args[i+1]
				if strings.TrimSpace(content) == "" {
					return fmt.Errorf("--config-content cannot be empty")
				}
				cfg.ConfigContent = content
				i++
			} else {
				return fmt.Errorf("--config-content requires a value")
			}
		case strings.HasPrefix(a, "--config-format="):
			format := strings.TrimPrefix(a, "--config-format=")
			if !supportedFormats[strings.ToLower(format)] {
				return fmt.Errorf("unsupported config format: %s (supported: yaml, yml, json)", format)
			}
			cfg.ConfigFormat = format
		case a == "--config-format":
			if i+1 < len(args) {
				format := args[i+1]
				if !supportedFormats[strings.ToLower(format)] {
					return fmt.Errorf("unsupported config format: %s (supported: yaml, yml, json)", format)
				}
				cfg.ConfigFormat = format
				i++
			} else {
				return fmt.Errorf("--config-format requires a value")
			}
		}
	}
	return nil

}

// loadEnvConfig loads EnvConfig using Koanf env provider.
// It reads environment variables and fills EnvConfig based on struct `env` tags.
// Defaults: if neither content nor path is provided, sets ConfigFilePath to "/etc/events-bridge/config.yaml".
func loadEnvConfig() (*EnvConfig, error) {
	k := kfn.New(".")
	// Load all envs; unmarshal will pick only those matching the struct tags
	if err := k.Load(kenv.Provider("", ".", func(s string) string { return s }), nil); err != nil {
		return nil, fmt.Errorf("failed to load env variables: %w", err)
	}
	ec := &EnvConfig{}
	if err := k.UnmarshalWithConf("", ec, kfn.UnmarshalConf{Tag: "env"}); err != nil {
		return nil, fmt.Errorf("failed to unmarshal env config: %w", err)
	}
	// Apply default path if nothing provided
	if ec.ConfigContent == "" && strings.TrimSpace(ec.ConfigFilePath) == "" {
		ec.ConfigFilePath = "/etc/events-bridge/config.yaml"
	}
	return ec, nil
}

// LoadConfigFile loads configuration from a file (YAML or JSON) and merges environment overrides.
// Environment variables use the prefix "EB_" and map to keys by:
// - trimming the prefix
// - lowercasing
// - replacing "__" with "." (double underscore denotes nesting)
// Arrays can be indexed with segments like "__0".
func loadConfigFile(path string) (cfg *Config, err error) {
	absPath, e := filepath.Abs(path)
	if e != nil {
		return nil, e
	}

	if _, e = os.Stat(absPath); e != nil {
		return nil, fmt.Errorf("error opening config file: %w", e)
	}

	ext := strings.ToLower(filepath.Ext(absPath))
	var parser kfn.Parser
	switch ext {
	case ".yaml", ".yml":
		parser = kyaml.Parser()
	case ".json":
		parser = kjson.Parser()
	default:
		return nil, &UnsupportedExtensionError{Extension: ext}
	}

	k := kfn.New(".")
	if e = k.Load(kfile.Provider(absPath), parser); e != nil {
		return nil, fmt.Errorf("error loading config file: %w", e)
	}

	// Env overrides (optional, prefix EB_)
	loadEnv(k)

	cfg = &Config{}
	if e = k.UnmarshalWithConf("", cfg, kfn.UnmarshalConf{Tag: "yaml"}); e != nil {
		return nil, fmt.Errorf("error unmarshalling config: %w", e)
	}

	validate := validator.New()
	if e = validate.Struct(cfg); e != nil {
		return nil, e
	}
	return cfg, nil
}

// LoadConfigContent loads configuration from raw YAML/JSON content and merges environment overrides.
// If format is empty, attempts to auto-detect (JSON if trimmed content starts with '{').
func loadConfigContent(content string, format string) (cfg *Config, err error) {
	trimmed := strings.TrimSpace(content)
	f := strings.ToLower(strings.TrimSpace(format))
	var parser kfn.Parser
	switch f {
	case "yaml", "yml":
		parser = kyaml.Parser()
	case "json":
		parser = kjson.Parser()
	case "":
		if strings.HasPrefix(trimmed, "{") {
			parser = kjson.Parser()
		} else {
			parser = kyaml.Parser()
		}
	default:
		return nil, &UnsupportedExtensionError{Extension: f}
	}

	k := kfn.New(".")
	if err = k.Load(kraw.Provider([]byte(content)), parser); err != nil {
		return nil, fmt.Errorf("error loading config content: %w", err)
	}

	// Env overrides (optional, prefix EB_)
	loadEnv(k)

	cfg = &Config{}
	if err = k.UnmarshalWithConf("", cfg, kfn.UnmarshalConf{Tag: "yaml"}); err != nil {
		return nil, fmt.Errorf("error unmarshalling config: %w", err)
	}

	validate := validator.New()
	if err = validate.Struct(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func loadEnv(k *kfn.Koanf) {
	// Allow overriding config via environment variables with prefix EB_.
	// Example: EB_SOURCE__TYPE=http
	// Array example: EB_SOURCE__KAFKA__BROKERS__0=localhost:9092
	const prefix = "EB_"
	_ = k.Load(kenv.Provider(prefix, ".", func(s string) string {
		// Transform: EB_FOO__BAR__BAZ -> foo.bar.baz
		noPrefix := strings.TrimPrefix(s, prefix)
		noPrefix = strings.ToLower(noPrefix)
		// Double underscore becomes dot for nesting
		noPrefix = strings.ReplaceAll(noPrefix, "__", ".")
		return noPrefix
	}), nil)
}

type UnsupportedExtensionError struct {
	Extension string
}

func (e *UnsupportedExtensionError) Error() string {
	return "unsupported config file extension: " + e.Extension
}
