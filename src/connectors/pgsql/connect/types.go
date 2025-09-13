package dbstore

import "time"

// Column represents a column of a PostgreSQL table.
type Column struct {
	Name     string // Column name
	Type     string // Data type
	Nullable bool   // True if the column is nullable
}

// CacheEntry stores column metadata with an expiration for caching.
type CacheEntry struct {
	Columns   []Column  // Columns list
	ExpiresAt time.Time // Expiration
}

// Record represents a generic row as a map column-name -> value.
type Record map[string]interface{}

// InsertRecordOnConflict defines the ON CONFLICT behavior for inserts.
type InsertRecordOnConflict string

const (
	DoNothing InsertRecordOnConflict = "DO NOTHING" // Ignore conflicts
	DoUpdate  InsertRecordOnConflict = "DO UPDATE"  // Update on conflict
)

// InsertRecordArgs contains all arguments for InsertRecord.
// Uses *pgxpool.Pool as the connection.
type InsertRecordArgs struct {
	TableName          string                 // Table name
	OtherColumn        string                 // Column for extra fields (JSON)
	BatchRecords       []Record               // Records to insert
	OnConflict         InsertRecordOnConflict // ON CONFLICT
	ConflictConstraint string                 // Optional constraint name
	ConflictColumns    string                 // Columns for ON CONFLICT
	BatchSize          int                    // Batch size
	CacheExpiration    int64                  // Metadata cache (seconds)
}
