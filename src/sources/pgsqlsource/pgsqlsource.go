package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
)

type PGSQLSource struct {
	config  *sources.SourcePGSQLConfig
	slog    *slog.Logger
	c       chan message.Message
	conn    *pgx.Conn
	started bool
}

func New(cfg *sources.SourcePGSQLConfig) (sources.Source, error) {
	if cfg.ConnString == "" || cfg.Table == "" {
		return nil, fmt.Errorf("connString and table are required for PGSQL source")
	}

	return &PGSQLSource{
		config: cfg,
		slog:   slog.Default().With("context", "PGSQL"),
	}, nil
}

func (s *PGSQLSource) Produce(buffer int) (<-chan message.Message, error) {
	s.c = make(chan message.Message, buffer)

	s.slog.Info("starting PGSQL source", "connString", s.config.ConnString, "table", s.config.Table)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.config.ConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	s.conn = conn

	channelName := channelNameForTable(s.config.Table)

	query := fmt.Sprintf("LISTEN %s", pgx.Identifier{channelName}.Sanitize())

	_, err = conn.Exec(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to LISTEN on channel: %w", err)
	}

	go s.listenLoop()

	s.started = true
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
		m := &PGSQLMessage{
			channel: n.Channel,
			payload: n.Payload,
		}
		s.c <- m
	}
}

func (s *PGSQLSource) Close() error {
	if s.c != nil {
		close(s.c)
	}
	if s.conn != nil {
		return s.conn.Close(context.Background())
	}
	return nil
}

// PGSQLMessage implements message.Message
var _ message.Message = &PGSQLMessage{}

type PGSQLMessage struct {
	channel string
	payload string
}

func (m *PGSQLMessage) GetMetadata() (map[string][]string, error) {
	return map[string][]string{"channel": {m.channel}}, nil
}

func (m *PGSQLMessage) GetData() ([]byte, error) {
	return []byte(m.payload), nil
}

func (m *PGSQLMessage) Ack() error {
	// Nessuna azione necessaria per Ack su NOTIFY
	return nil
}

func (m *PGSQLMessage) Nak() error {
	// Nessuna azione necessaria per Nak su NOTIFY
	return nil
}

func channelNameForTable(tableName string) string {
	return fmt.Sprintf("%s_changes", tableName)
}

func setupTrigger(ctx context.Context, conn *pgx.Conn, tableName string) error {
	funcName := fmt.Sprintf("eb_notify_%s_change", tableName)
	triggerName := fmt.Sprintf("eb_change_%s", tableName)
	channelName := channelNameForTable(tableName)

	// 1. Crea la funzione se non esiste
	createFuncQuery := `DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_proc WHERE proname = $1
    ) THEN
        EXECUTE format('
            CREATE FUNCTION %s() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify(''%s'', json_build_object(''table'', TG_TABLE_NAME, ''operation'', TG_OP, ''data'', row_to_json(NEW))::text);
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;', $1, $2);
    END IF;
END;
$$;`
	_, err := conn.Exec(ctx, createFuncQuery, funcName, channelName)
	if err != nil {
		return fmt.Errorf("create function failed: %w", err)
	}

	// 2. Crea il trigger se non esiste
	createTriggerQuery := `DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = $1
    ) THEN
        EXECUTE format('
            CREATE TRIGGER %s
            AFTER INSERT OR UPDATE OR DELETE ON %s
            FOR EACH ROW
            EXECUTE FUNCTION %s();', $1, $2, $3);
    END IF;
END;
$$;`
	_, err = conn.Exec(ctx, createTriggerQuery, triggerName, tableName, funcName)
	if err != nil {
		return fmt.Errorf("create trigger failed: %w", err)
	}

	return nil
}
