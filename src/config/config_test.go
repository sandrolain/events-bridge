package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// helper to temporarily set os.Args and restore on cleanup
func withArgs(t *testing.T, args []string) {
	t.Helper()
	old := os.Args
	os.Args = append([]string{"events-bridge"}, args...)
	t.Cleanup(func() { os.Args = old })
}

func TestApplyCLIOverridesLongAndEqualsForms(t *testing.T) {
	// long form
	withArgs(t, []string{
		"--config-file-path", "/tmp/eb.yaml",
		"--config-content", "{\"a\":1}",
		"--config-format", "json",
	})
	ec := &EnvConfig{}
	applyCLIOverrides(ec)
	require.Equal(t, "/tmp/eb.yaml", ec.ConfigFilePath)
	require.Equal(t, "{\"a\":1}", ec.ConfigContent)
	require.Equal(t, "json", ec.ConfigFormat)

	// equals form
	withArgs(t, []string{
		"--config-file-path=/var/lib/eb/config.yml",
		"--config-content=source: {}",
		"--config-format=yaml",
	})
	ec2 := &EnvConfig{}
	applyCLIOverrides(ec2)
	require.Equal(t, "/var/lib/eb/config.yml", ec2.ConfigFilePath)
	require.Equal(t, "source: {}", ec2.ConfigContent)
	require.Equal(t, "yaml", ec2.ConfigFormat)
}

func TestLoadEnvConfigDefaultPathWhenEmpty(t *testing.T) {
	t.Setenv("EB_CONFIG_FILE_PATH", "")
	t.Setenv("EB_CONFIG_CONTENT", "")
	t.Setenv("EB_CONFIG_FORMAT", "")

	ec, err := loadEnvConfig()
	require.NoError(t, err)
	require.Equal(t, "/etc/events-bridge/config.yaml", ec.ConfigFilePath)
	require.Empty(t, ec.ConfigContent)
	require.Empty(t, ec.ConfigFormat)
}

func TestLoadConfigFileYAMLWithEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	yaml := "" +
		"source:\n" +
		"  type: nats\n" +
		"  options:\n" +
		"    address: 127.0.0.1:4222\n" +
		"    subject: fromfile\n" +
		"runner:\n" +
		"  type: cli\n" +
		"  cli:\n" +
		"    command: echo\n" +
		"target:\n" +
		"  type: nats\n" +
		"  options:\n" +
		"    address: 127.0.0.1:4222\n" +
		"    subject: will-be-overridden\n"
	require.NoError(t, os.WriteFile(cfgPath, []byte(yaml), 0o600))

	// override via env (prefix EB_ with __ for nesting)
	t.Setenv("EB_SOURCE__OPTIONS__SUBJECT", "fromenv")
	t.Setenv("EB_TARGET__OPTIONS__SUBJECT", "outenv")

	cfg, err := loadConfigFile(cfgPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "nats", string(cfg.Source.Type))
	if subj, ok := cfg.Source.Options["subject"].(string); ok {
		require.Equal(t, "fromenv", subj)
	} else {
		t.Fatalf("expected source options.subject to be string, got %#v", cfg.Source.Options["subject"])
	}
	if subj, ok := cfg.Target.Options["subject"].(string); ok {
		require.Equal(t, "outenv", subj)
	} else {
		t.Fatalf("expected target options.subject to be string, got %#v", cfg.Target.Options["subject"])
	}
}

func TestLoadConfigFileUnsupportedExtension(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(path, []byte("key='value'"), 0o600))

	_, err := loadConfigFile(path)
	require.Error(t, err)
	var ue *UnsupportedExtensionError
	require.ErrorAs(t, err, &ue)
	require.Equal(t, ".toml", ue.Extension)
}

func TestLoadConfigFileFileNotFound(t *testing.T) {
	_, err := loadConfigFile(filepath.Join(t.TempDir(), "missing.yaml"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "error opening config file")
}

func TestLoadConfigContentYAMLAndJSONAutoDetectAndExplicit(t *testing.T) {
	// YAML explicit
	yaml := strings.Join([]string{
		"source:",
		"  type: nats",
		"  options:",
		"    address: 127.0.0.1:4222",
		"    subject: a",
		"runner:",
		"  type: cli",
		"  cli:",
		"    command: echo",
		"target:",
		"  type: nats",
		"  options:",
		"    address: 127.0.0.1:4222",
		"    subject: b",
	}, "\n")

	cfg, err := loadConfigContent(yaml, "yaml")
	require.NoError(t, err)
	require.Equal(t, "a", cfg.Source.Options["subject"])
	require.Equal(t, "b", cfg.Target.Options["subject"])

	// JSON auto-detect
	json := `{"source":{"type":"nats","options":{"address":"127.0.0.1:4222","subject":"ja"}},"runner":{"type":"cli","cli":{"command":"echo"}},"target":{"type":"nats","options":{"address":"127.0.0.1:4222","subject":"jb"}}}`
	cfg2, err := loadConfigContent(json, "")
	require.NoError(t, err)
	require.Equal(t, "ja", cfg2.Source.Options["subject"])
	require.Equal(t, "jb", cfg2.Target.Options["subject"])
}

func TestLoadConfigContentUnsupportedFormat(t *testing.T) {
	_, err := loadConfigContent("key: val", "toml")
	require.Error(t, err)
	var ue *UnsupportedExtensionError
	require.ErrorAs(t, err, &ue)
	require.Equal(t, "toml", ue.Extension)
}

func TestLoadConfigUsesEnvAndCLIPrecedence(t *testing.T) {
	// Provide minimal env content to avoid default path (and its filepath validation),
	// then override via CLI which should take precedence over env.
	t.Setenv("EB_CONFIG_CONTENT", `{"source":{"type":"nats","options":{"address":"127.0.0.1:4222","subject":"fromenv"}},"runner":{"type":"cli"},"target":{"type":"nats"}}`)
	t.Setenv("EB_CONFIG_FORMAT", "json")

	// CLI should override env by providing different inline JSON content
	json := `{"source":{"type":"nats","options":{"address":"127.0.0.1:4222","subject":"fromcli"}},"runner":{"type":"cli"},"target":{"type":"nats"}}`
	withArgs(t, []string{"--config-content", json, "--config-format", "json"})

	cfg, err := LoadConfig()
	// On some systems, validator tag "filepath" might not be registered; ensure this doesn't fail here.
	// If it does, the error will be about validation of EnvConfig, not parsing; this test ensures happy path works.
	require.NoError(t, err)
	if subj, ok := cfg.Source.Options["subject"].(string); ok {
		require.Equal(t, "fromcli", subj)
	} else {
		t.Fatalf("expected source options.subject to be string, got %#v", cfg.Source.Options["subject"])
	}
}

func TestUnsupportedExtensionErrorError(t *testing.T) {
	e := &UnsupportedExtensionError{Extension: ".weird"}
	require.Equal(t, "unsupported config file extension: .weird", e.Error())
}

// Ensure tests are not skipped on older OS where /bin/true may not exist
func TestEnvironment(t *testing.T) {
	require.NotEmpty(t, runtime.GOOS)
}
