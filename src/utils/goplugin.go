package utils

import (
	"fmt"
	"os"
	"path/filepath"
	goplugin "plugin"

	"github.com/creasty/defaults"
	"github.com/go-playground/validator/v10"
	"github.com/go-viper/mapstructure/v2"
	"github.com/sandrolain/events-bridge/src/security/crypto"
	"github.com/sandrolain/events-bridge/src/security/validation"
)

// PluginSecurityOptions contains security-related options for plugin loading
type PluginSecurityOptions struct {
	// AllowedPluginsDir restricts plugin loading to a specific directory
	AllowedPluginsDir string
	// ExpectedSHA256 is the expected SHA256 hash of the plugin file (optional)
	ExpectedSHA256 string
	// VerifyHash enables hash verification (requires ExpectedSHA256)
	VerifyHash bool
}

// LoadPluginAndConfigSecure loads a plugin with security validations
func LoadPluginAndConfigSecure[R any](relPath string, method string, configMethod string, options map[string]any, secOpts *PluginSecurityOptions) (res R, err error) {
	exePath, e := os.Executable()
	if e != nil {
		err = fmt.Errorf("failed to get executable path: %w", e)
		return
	}

	exeDir := filepath.Dir(exePath)
	absPath := relPath
	if !os.IsPathSeparator(relPath[0]) {
		absPath = fmt.Sprintf("%s/%s", exeDir, relPath)
	}

	// Security validations
	if secOpts != nil {
		// Validate plugin path
		allowedDir := secOpts.AllowedPluginsDir
		if allowedDir == "" {
			allowedDir = exeDir // Default to executable directory
		}
		if err = validation.ValidatePluginPath(absPath, allowedDir); err != nil {
			return res, fmt.Errorf("plugin path validation failed: %w", err)
		}

		// Verify hash if requested
		if secOpts.VerifyHash && secOpts.ExpectedSHA256 != "" {
			if err = crypto.VerifySHA256(absPath, secOpts.ExpectedSHA256); err != nil {
				return res, fmt.Errorf("plugin hash verification failed: %w", err)
			}
		}
	}

	p, e := goplugin.Open(absPath)
	if e != nil {
		err = fmt.Errorf("failed to open plugin: %w", e)
		return
	}

	configSym, e := p.Lookup(configMethod)
	if e != nil {
		err = fmt.Errorf("failed to find config constructor for %s: %w", configMethod, e)
		return
	}

	configConstr, ok := configSym.(NewConfigMethodFunc)
	if !ok {
		err = fmt.Errorf("plugin has invalid signature for %s", configMethod)
		return
	}

	config := configConstr()

	sym, err := p.Lookup(method)
	if err != nil {
		return res, fmt.Errorf("failed to find constructor for %s: %w", method, err)
	}

	err = ParseConfig(options, config)
	if err != nil {
		return res, fmt.Errorf("failed to parse config for %s: %w", method, err)
	}

	constr, ok := sym.(NewConstructorMethodFunc[R])
	if !ok {
		return res, fmt.Errorf("plugin has invalid signature for %s", method)
	}

	res, err = constr(config)
	return
}

func LoadPluginAndConfig[R any](relPath string, method string, configMethod string, options map[string]any) (res R, err error) {
	exePath, e := os.Executable()
	if e != nil {
		err = fmt.Errorf("failed to get executable path: %w", e)
		return
	}

	exeDir := filepath.Dir(exePath)
	absPath := relPath
	if !os.IsPathSeparator(relPath[0]) {
		absPath = fmt.Sprintf("%s/%s", exeDir, relPath)
	}

	p, e := goplugin.Open(absPath)
	if e != nil {
		err = fmt.Errorf("failed to open plugin: %w", e)
		return
	}

	configSym, e := p.Lookup(configMethod)
	if e != nil {
		err = fmt.Errorf("failed to find config constructor for %s: %w", configMethod, e)
		return
	}

	configConstr, ok := configSym.(NewConfigMethodFunc)
	if !ok {
		err = fmt.Errorf("plugin has invalid signature for %s", configMethod)
		return
	}

	config := configConstr()

	sym, err := p.Lookup(method)
	if err != nil {
		return res, fmt.Errorf("failed to find constructor for %s: %w", method, err)
	}

	err = ParseConfig(options, config)
	if err != nil {
		return res, fmt.Errorf("failed to parse config for %s: %w", method, err)
	}

	constr, ok := sym.(NewConstructorMethodFunc[R])
	if !ok {
		return res, fmt.Errorf("plugin has invalid signature for %s", method)
	}

	res, err = constr(config)
	return
}

type NewConfigMethodFunc = func() any
type NewConstructorMethodFunc[R any] = func(any) (R, error)

func ParseConfig(opts map[string]any, res any) (err error) {
	if e := defaults.Set(res); e != nil {
		err = fmt.Errorf("failed to set default values: %w", e)
		return
	}

	decoderConfig := &mapstructure.DecoderConfig{
		Metadata:         nil,
		Result:           res,
		WeaklyTypedInput: false, // Use strict typing for security
		ErrorUnused:      false, // Allow extra fields for flexibility
		TagName:          "mapstructure",
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
		),
	}

	decoder, e := mapstructure.NewDecoder(decoderConfig)
	if e != nil {
		err = fmt.Errorf("failed to create decoder: %w", e)
		return
	}

	if e := decoder.Decode(opts); e != nil {
		err = fmt.Errorf("failed to decode options: %w", e)
		return
	}

	if e := validator.New().Struct(res); e != nil {
		err = fmt.Errorf("failed to validate config: %w", e)
		return
	}

	return
}
