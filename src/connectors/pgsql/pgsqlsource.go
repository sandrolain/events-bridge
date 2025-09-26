package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/common"
	"github.com/sandrolain/events-bridge/src/message"
)

type SourceConfig struct {
	ConnString string `mapstructure:"conn_string" validate:"required"`
	Table      string `mapstructure:"table" validate:"required"`
}

type PGSQLSource struct {
	cfg  *SourceConfig
	slog *slog.Logger
	c    chan *message.RunnerMessage
	conn *pgx.Conn
}

// NewSource creates a PGSQL source from options map.
func NewSource(opts map[string]any) (connectors.Source, error) {
	cfg, err := common.ParseConfig[SourceConfig](opts)
	if err != nil {
		return nil, err
	}
	return &PGSQLSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "PGSQL Source"),
	}, nil
}

func (s *PGSQLSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting PGSQL source", "connString", s.cfg.ConnString, "table", s.cfg.Table)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	s.conn = conn

	query, err := s.setupTrigger(ctx, conn, s.cfg.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to setup trigger for table %s: %w", s.cfg.Table, err)
	}

	_, err = conn.Exec(ctx, query)
	if err != nil {
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
	funcName := fmt.Sprintf("eb_notify_%s_change", tableName)
	triggerName := fmt.Sprintf("eb_change_%s", tableName)
	channelName := channelNameForTable(tableName)

	s.slog.Debug("setting up trigger", "table", tableName, "function", funcName, "trigger", triggerName, "channel", channelName)

	var exists bool
	err := conn.QueryRow(ctx, fmt.Sprintf(`
        SELECT EXISTS (
            SELECT 1 FROM pg_proc WHERE proname = '%s'
        )
    `, funcName)).Scan(&exists)
	if err != nil {
		return "", fmt.Errorf("check function existence failed: %w", err)
	}

	if !exists {
		createFuncQuery := fmt.Sprintf(`
			CREATE FUNCTION %s()
			RETURNS trigger AS $$
			BEGIN
					IF TG_OP = 'DELETE' THEN
						PERFORM pg_notify('%s', json_build_object('table', TG_TABLE_NAME, 'operation', TG_OP, 'data', row_to_json(OLD))::text);
					ELSE
						PERFORM pg_notify('%s', json_build_object('table', TG_TABLE_NAME, 'operation', TG_OP, 'data', row_to_json(NEW))::text);
					END IF;
					RETURN COALESCE(NEW, OLD);
			END;
			$$ LANGUAGE plpgsql; 
		`, funcName, channelName, channelName)

		s.slog.Debug("creating function", "query", createFuncQuery)

		_, err := conn.Exec(ctx, createFuncQuery)
		if err != nil {
			return "", fmt.Errorf("create function failed: %w", err)
		}
	} else {
		s.slog.Debug("function already exists", "function", funcName)
	}

	// 2. Create the trigger if it does not exist
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
`, triggerName, triggerName, tableName, funcName)

	s.slog.Debug("creating trigger", "query", createTriggerQuery)

	_, err = conn.Exec(ctx, createTriggerQuery)
	if err != nil {
		return "", fmt.Errorf("create trigger failed: %w", err)
	}

	query := fmt.Sprintf("LISTEN %s", pgx.Identifier{channelName}.Sanitize())

	s.slog.Debug("listening on channel", "query", query)

	return query, nil
}
