package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	connStr := flag.String("conn", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", "PostgreSQL connection string")
	table := flag.String("table", "test_table", "Table name")
	interval := flag.Int("interval", 5, "Insert interval in seconds")
	flag.Parse()

	db, err := sql.Open("postgres", *connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "DB open error: %v\n", err)
		os.Exit(1)
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
	)`, *table)
	if _, err := db.ExecContext(ctx, createTable); err != nil {
		fmt.Fprintf(os.Stderr, "Table creation error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Table '%s' ready.\n", *table)

	ticker := time.NewTicker(time.Duration(*interval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		insert := fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", *table)
		msg := fmt.Sprintf("test at %s", time.Now().Format(time.RFC3339))
		_, err := db.Exec(insert, msg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Insert error: %v\n", err)
		} else {
			fmt.Printf("Inserted: %s\n", msg)
		}
	}
}
