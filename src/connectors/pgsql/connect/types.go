package dbstore

import "time"

// Column rappresenta una colonna di una tabella PostgreSQL.
type Column struct {
	Name     string // Nome colonna
	Type     string // Tipo dati
	Nullable bool   // True se la colonna è nullable
}

// CacheEntry memorizza i metadati delle colonne con scadenza per la cache.
type CacheEntry struct {
	Columns   []Column  // Lista colonne
	ExpiresAt time.Time // Scadenza
}

// Record rappresenta una riga generica come mappa nome colonna -> valore.
type Record map[string]interface{}

// InsertRecordOnConflict definisce il comportamento ON CONFLICT per gli insert.
type InsertRecordOnConflict string

const (
	DoNothing InsertRecordOnConflict = "DO NOTHING" // Ignora i conflitti
	DoUpdate  InsertRecordOnConflict = "DO UPDATE"  // Aggiorna in caso di conflitto
)

// InsertRecordArgs contiene tutti gli argomenti per InsertRecord.
// Usa *pgxpool.Pool come connessione.
type InsertRecordArgs struct {
	TableName          string                 // Nome tabella
	OtherColumn        string                 // Colonna per extra fields (JSON)
	BatchRecords       []Record               // Record da inserire
	OnConflict         InsertRecordOnConflict // ON CONFLICT
	ConflictConstraint string                 // Nome constraint opzionale
	ConflictColumns    string                 // Colonne per ON CONFLICT
	BatchSize          int                    // Batch size
	CacheExpiration    int64                  // Cache metadati (secondi)
}
