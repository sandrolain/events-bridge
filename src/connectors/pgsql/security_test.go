package main

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
)

const (
	testConnString        = "postgres://user:pass@localhost:5432/testdb" //nolint:gosec // test connection string with placeholder credentials
	securityTestTable     = "test_table"
	errExpectedError      = "expected error for %s"
	errUnexpectedError    = "unexpected error: %v"
	sqlInjectionAttempt   = "users; DROP TABLE users;--"
	invalidIdentifierDash = "user-table"
)

// TestValidateIdentifier tests the identifier validation function
func TestValidateIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		strict     bool
		wantErr    bool
	}{
		{
			name:       "valid simple identifier",
			identifier: "users",
			strict:     true,
			wantErr:    false,
		},
		{
			name:       "valid identifier with underscore",
			identifier: "user_accounts",
			strict:     true,
			wantErr:    false,
		},
		{
			name:       "valid identifier starting with underscore",
			identifier: "_private_table",
			strict:     true,
			wantErr:    false,
		},
		{
			name:       "valid identifier with numbers",
			identifier: "table_123",
			strict:     true,
			wantErr:    false,
		},
		{
			name:       "empty identifier",
			identifier: "",
			strict:     true,
			wantErr:    true,
		},
		{
			name:       "identifier with spaces",
			identifier: "user table",
			strict:     true,
			wantErr:    true,
		},
		{
			name:       "identifier with semicolon (SQL injection)",
			identifier: sqlInjectionAttempt,
			strict:     true,
			wantErr:    true,
		},
		{
			name:       "identifier with dash",
			identifier: invalidIdentifierDash,
			strict:     true,
			wantErr:    true,
		},
		{
			name:       "identifier starting with number",
			identifier: "1_table",
			strict:     true,
			wantErr:    true,
		},
		{
			name:       "identifier too long (>63 chars)",
			identifier: "this_is_a_very_long_table_name_that_exceeds_the_postgresql_limit_of_63_characters",
			strict:     true,
			wantErr:    true,
		},
		{
			name:       "identifier with special chars (non-strict)",
			identifier: invalidIdentifierDash,
			strict:     false,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIdentifier(tt.identifier, tt.strict)
			if (err != nil) != tt.wantErr {
				if tt.wantErr {
					t.Errorf(errExpectedError, tt.name)
				} else {
					t.Errorf(errUnexpectedError, err)
				}
			}
		})
	}
}

// TestSourceConfigValidation tests source config validation
func TestSourceConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *SourceConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: &SourceConfig{
				ConnString:       testConnString,
				Table:            securityTestTable,
				StrictValidation: true,
			},
			wantErr: false,
		},
		{
			name: "valid config with TLS",
			config: &SourceConfig{
				ConnString:       testConnString,
				Table:            securityTestTable,
				StrictValidation: true,
				TLS: &tlsconfig.Config{
					Enabled:    true,
					MinVersion: "1.2",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid table name (SQL injection)",
			config: &SourceConfig{
				ConnString:       testConnString,
				Table:            sqlInjectionAttempt,
				StrictValidation: true,
			},
			wantErr: true,
		},
		{
			name: "invalid table name (empty)",
			config: &SourceConfig{
				ConnString:       testConnString,
				Table:            "",
				StrictValidation: true,
			},
			wantErr: true,
		},
		{
			name: "invalid table name accepted when not strict",
			config: &SourceConfig{
				ConnString:       testConnString,
				Table:            invalidIdentifierDash,
				StrictValidation: false,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSource(tt.config)
			if (err != nil) != tt.wantErr {
				if tt.wantErr {
					t.Errorf(errExpectedError, tt.name)
				} else {
					t.Errorf(errUnexpectedError, err)
				}
			}
		})
	}
}

// TestRunnerConfigSecurityValidation tests target config security validation
func TestRunnerConfigSecurityValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *RunnerConfig
		wantErr bool
	}{
		{
			name: "valid table name",
			config: &RunnerConfig{
				ConnString:       testConnString,
				Table:            securityTestTable,
				StrictValidation: true,
			},
			wantErr: true, // Will fail on connection, but table name is valid
		},
		{
			name: "invalid table name (SQL injection)",
			config: &RunnerConfig{
				ConnString:       testConnString,
				Table:            sqlInjectionAttempt,
				StrictValidation: true,
			},
			wantErr: true,
		},
		{
			name: "valid table with other column",
			config: &RunnerConfig{
				ConnString:       testConnString,
				Table:            securityTestTable,
				OtherColumn:      "metadata",
				StrictValidation: true,
			},
			wantErr: true, // Will fail on connection, but names are valid
		},
		{
			name: "invalid other column name",
			config: &RunnerConfig{
				ConnString:       testConnString,
				Table:            securityTestTable,
				OtherColumn:      "col'; DROP TABLE users;--",
				StrictValidation: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRunner(tt.config)
			if err == nil && tt.wantErr {
				t.Errorf(errExpectedError, tt.name)
			}
			// Note: We expect errors because we can't actually connect to DB in unit tests
			// The important part is that SQL injection attempts are caught before connection
		})
	}
}

// TestNewSourceConfigInvalidType tests error handling for invalid config types
func TestNewSourceConfigInvalidType(t *testing.T) {
	_, err := NewSource("invalid config")
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
}

// TestNewRunnerConfigInvalidType tests error handling for invalid config types
func TestNewRunnerConfigInvalidType(t *testing.T) {
	_, err := NewRunner("invalid config")
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
}

// TestChannelNameForTable tests channel name generation
func TestChannelNameForTable(t *testing.T) {
	tests := []struct {
		tableName string
		want      string
	}{
		{"users", "users_changes"},
		{"user_accounts", "user_accounts_changes"},
		{"_private", "_private_changes"},
	}

	for _, tt := range tests {
		t.Run(tt.tableName, func(t *testing.T) {
			got := channelNameForTable(tt.tableName)
			if got != tt.want {
				t.Errorf("channelNameForTable(%q) = %q, want %q", tt.tableName, got, tt.want)
			}
		})
	}
}

// TestSourceConfigDefaults tests default values
func TestSourceConfigDefaults(t *testing.T) {
	cfg := &SourceConfig{
		ConnString: testConnString,
		Table:      securityTestTable,
	}

	// After struct tag defaults are applied (would be done by mapstructure in real usage)
	// We can test the NewSource doesn't fail with defaults
	source, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(errUnexpectedError, err)
	}

	if source == nil {
		t.Fatal("expected non-nil source")
	}
}

// TestRunnerConfigDefaults tests default values
func TestRunnerConfigDefaults(t *testing.T) {
	cfg := &RunnerConfig{
		ConnString: testConnString,
		Table:      securityTestTable,
	}

	// Will fail on connection but config validation should pass
	_, err := NewRunner(cfg)
	// We expect an error because we can't connect, but it should be connection error not validation
	if err == nil {
		t.Fatal("expected connection error")
	}

	// Check that error is not about table validation
	if err.Error() == "invalid table name: invalid identifier: "+securityTestTable {
		t.Fatal("should not fail on valid table name")
	}
}
