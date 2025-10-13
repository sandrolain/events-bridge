package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// identifierRegex validates PostgreSQL identifiers (tables, functions, etc.)
// Allows alphanumeric, underscore, starts with letter or underscore
var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// SourceConfig defines the configuration for PostgreSQL source connector
type SourceConfig struct {
	// Database connection string
	// Format: postgres://user:password@host:port/database?sslmode=disable
	// For security, use environment variables or secret managers for credentials
	ConnString string `mapstructure:"connString" validate:"required"`

	// Table name to monitor for changes
	// Must be a valid PostgreSQL identifier (alphanumeric + underscore)
	Table string `mapstructure:"table" validate:"required"`

	// TLS configuration for encrypted connections
	TLS *tlsconfig.Config `mapstructure:"tls"`

	// Maximum number of connections in the pool
	MaxConns int32 `mapstructure:"maxConns" default:"4" validate:"omitempty,min=1,max=100"`

	// Minimum number of connections in the pool
	MinConns int32 `mapstructure:"minConns" default:"1" validate:"omitempty,min=0,max=10"`

	// Enable strict identifier validation (recommended: true)
	// When true, table/function names are validated against a strict regex
	StrictValidation bool `mapstructure:"strictValidation" default:"true"`
}

type PGSQLSource struct {
	cfg  *SourceConfig
	slog *slog.Logger
	c    chan *message.RunnerMessage
	conn *pgx.Conn
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

// validateIdentifier checks if a string is a valid PostgreSQL identifier
// Returns error if the identifier contains SQL injection attempts
func validateIdentifier(name string, strict bool) error {
	if name == "" {
		return fmt.Errorf("identifier cannot be empty")
	}

	if len(name) > 63 {
		return fmt.Errorf("identifier exceeds PostgreSQL maximum length of 63 characters")
	}

	if strict {
		if !identifierRegex.MatchString(name) {
			return fmt.Errorf("invalid identifier: %s (must start with letter/underscore, contain only alphanumeric and underscore)", name)
		}
	}

	return nil
}

// buildTLSConfig creates TLS configuration from source config
func (s *PGSQLSource) buildTLSConfig() (*tls.Config, error) {
	if s.cfg.TLS == nil || !s.cfg.TLS.Enabled {
		return nil, nil
	}

	return s.cfg.TLS.BuildClientConfig()
}

// buildPoolConfig creates pgxpool configuration with TLS and connection limits
func (s *PGSQLSource) buildPoolConfig() (*pgxpool.Config, error) {
	config, err := pgxpool.ParseConfig(s.cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Apply TLS configuration
	tlsConf, err := s.buildTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	if tlsConf != nil {
		config.ConnConfig.TLSConfig = tlsConf
	}

	// Apply connection pool limits
	config.MaxConns = s.cfg.MaxConns
	config.MinConns = s.cfg.MinConns

	return config, nil
}

// NewSource creates a PGSQL source from options map.
func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Validate table name to prevent SQL injection
	if err := validateIdentifier(cfg.Table, cfg.StrictValidation); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}

	return &PGSQLSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "PGSQL Source"),
	}, nil
}

func (s *PGSQLSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	tlsEnabled := s.cfg.TLS != nil && s.cfg.TLS.Enabled
	s.slog.Info("starting PGSQL source",
		"table", s.cfg.Table,
		"tls", tlsEnabled,
		"strictValidation", s.cfg.StrictValidation,
	)

	ctx := context.Background()

	// Build pool configuration with TLS
	poolConfig, err := s.buildPoolConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build pool config: %w", err)
	}

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Get a connection from the pool for LISTEN
	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	s.conn = conn.Conn()

	query, err := s.setupTrigger(ctx, s.conn, s.cfg.Table)
	if err != nil {
		conn.Release()
		pool.Close()
		return nil, fmt.Errorf("failed to setup trigger for table %s: %w", s.cfg.Table, err)
	}

	_, err = s.conn.Exec(ctx, query)
	if err != nil {
		conn.Release()
		pool.Close()
		return nil, fmt.Errorf("failed to LISTEN on channel: %w", err)
	}

	go s.listenLoop()

	return s.c, nil
}

func (s *PGSQLSource) listenLoop() {
	ctx := context.Background()
	for {
		n, err := s.conn.WaitForNotification(ctx)
		if err != nil {
			s.slog.Error("error waiting for notification", "err", err)
			time.Sleep(time.Second)
			continue
		}
		s.slog.Debug("received notification", "channel", n.Channel, "payload", n.Payload)

		m := &PGSQLMessage{
			notification: n,
		}
		s.c <- message.NewRunnerMessage(m)
	}
}

func (s *PGSQLSource) Close() error {
	if s.conn != nil {
		return s.conn.Close(context.Background())
	}
	return nil
}

func channelNameForTable(tableName string) string {
	return fmt.Sprintf("%s_changes", tableName)
}

func (s *PGSQLSource) setupTrigger(ctx context.Context, conn *pgx.Conn, tableName string) (string, error) {
	// Validate table name again (defense in depth)
	if err := validateIdentifier(tableName, s.cfg.StrictValidation); err != nil {
		return "", fmt.Errorf("invalid table name: %w", err)
	}

	// Use pgx.Identifier for safe SQL construction
	tableIdent := pgx.Identifier{tableName}
	funcName := fmt.Sprintf("eb_notify_%s_change", tableName)
	triggerName := fmt.Sprintf("eb_change_%s", tableName)
	channelName := channelNameForTable(tableName)

	// Validate generated names
	if err := validateIdentifier(funcName, s.cfg.StrictValidation); err != nil {
		return "", fmt.Errorf("generated function name invalid: %w", err)
	}
	if err := validateIdentifier(triggerName, s.cfg.StrictValidation); err != nil {
		return "", fmt.Errorf("generated trigger name invalid: %w", err)
	}
	if err := validateIdentifier(channelName, s.cfg.StrictValidation); err != nil {
		return "", fmt.Errorf("generated channel name invalid: %w", err)
	}

	funcIdent := pgx.Identifier{funcName}
	triggerIdent := pgx.Identifier{triggerName}
	channelIdent := pgx.Identifier{channelName}

	s.slog.Debug("setting up trigger", "table", tableName, "function", funcName, "trigger", triggerName, "channel", channelName)

	// Check if function exists using parameterized query
	var exists bool
	err := conn.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM pg_proc WHERE proname = $1
        )
    `, funcName).Scan(&exists)
	if err != nil {
		return "", fmt.Errorf("check function existence failed: %w", err)
	}

	if !exists {
		// Create function using pgx.Identifier for safe quoting
		createFuncQuery := fmt.Sprintf(`
			CREATE FUNCTION %s()
			RETURNS trigger AS $$
			BEGIN
					IF TG_OP = 'DELETE' THEN
						PERFORM pg_notify(%s, json_build_object('table', TG_TABLE_NAME, 'operation', TG_OP, 'data', row_to_json(OLD))::text);
					ELSE
						PERFORM pg_notify(%s, json_build_object('table', TG_TABLE_NAME, 'operation', TG_OP, 'data', row_to_json(NEW))::text);
					END IF;
					RETURN COALESCE(NEW, OLD);
			END;
			$$ LANGUAGE plpgsql; 
		`, funcIdent.Sanitize(), channelIdent.Sanitize(), channelIdent.Sanitize())

		s.slog.Debug("creating function", "query", createFuncQuery)

		_, err := conn.Exec(ctx, createFuncQuery)
		if err != nil {
			return "", fmt.Errorf("create function failed: %w", err)
		}
	} else {
		s.slog.Debug("function already exists", "function", funcName)
	}

	// Create the trigger using parameterized check and safe identifiers
	createTriggerQuery := fmt.Sprintf(`
DO $$
BEGIN
		IF NOT EXISTS (
				SELECT 1 FROM pg_trigger WHERE tgname = '%s'
		) THEN
				CREATE TRIGGER %s
				AFTER INSERT OR UPDATE OR DELETE ON %s
				FOR EACH ROW
				EXECUTE FUNCTION %s();
		END IF;
END;
$$;
`, triggerName, triggerIdent.Sanitize(), tableIdent.Sanitize(), funcIdent.Sanitize())

	s.slog.Debug("creating trigger", "query", createTriggerQuery)

	_, err = conn.Exec(ctx, createTriggerQuery)
	if err != nil {
		return "", fmt.Errorf("create trigger failed: %w", err)
	}

	// Use pgx.Identifier.Sanitize() for LISTEN command
	query := fmt.Sprintf("LISTEN %s", channelIdent.Sanitize())

	s.slog.Debug("listening on channel", "query", query)

	return query, nil
}
