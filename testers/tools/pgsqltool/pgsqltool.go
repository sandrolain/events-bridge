package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	toolutil "github.com/sandrolain/events-bridge/testers/toolutil"
	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "pgsqlcli",
		Short: "PostgreSQL source tester",
		Long:  "A simple PostgreSQL CLI with only a send command that inserts rows periodically.",
	}

	var (
		connStr  string
		table    string
		interval string
		payload  string
	)

	sendCmd := &cobra.Command{
		Use:   "send",
		Short: "Periodically insert rows into PostgreSQL",
		RunE: func(cmd *cobra.Command, args []string) error {
			dur, err := time.ParseDuration(interval)
			if err != nil {
				return fmt.Errorf("invalid interval: %w", err)
			}

			db, err := sql.Open("postgres", connStr)
			if err != nil {
				return fmt.Errorf("DB open error: %w", err)
			}
			defer func() {
				if err := db.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to close DB connection: %v\n", err)
				}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			createTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
				id SERIAL PRIMARY KEY,
				created_at TIMESTAMP NOT NULL DEFAULT NOW(),
				data TEXT
			)`, table)
			if _, err := db.ExecContext(ctx, createTable); err != nil {
				return fmt.Errorf("table creation error: %w", err)
			}
			fmt.Printf("Table '%s' ready.\n", table)

			ticker := time.NewTicker(dur)
			defer ticker.Stop()
			fmt.Printf("Inserting into %s every %s\n", table, dur)
			for range ticker.C {
				b, _, err := toolutil.BuildPayload(payload, toolutil.CTText)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				insert := fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", table) // #nosec G201 -- test tool with controlled table name
				if _, err := db.Exec(insert, string(b)); err != nil {
					fmt.Fprintf(os.Stderr, "Insert error: %v\n", err)
				} else {
					fmt.Printf("Inserted: %s\n", time.Now().Format(time.RFC3339))
				}
			}
			return nil
		},
	}

	sendCmd.Flags().StringVar(&connStr, "conn", "postgres://user:pass@localhost:5432/postgres?sslmode=disable", "PostgreSQL connection string")
	sendCmd.Flags().StringVar(&table, "table", "test_table", "Table name")
	toolutil.AddPayloadFlags(sendCmd, &payload, "{nowtime}", new(string), "")
	toolutil.AddIntervalFlag(sendCmd, &interval, "5s")

	root.AddCommand(sendCmd)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
