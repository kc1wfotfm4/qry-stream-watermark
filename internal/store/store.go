// Package store owns the SQLite file and every write the service performs.
package store

import (
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB
}

// Stream is the registered windowing configuration and current watermark of one data stream.
type Stream struct {
	StreamID               string
	WindowType             string
	WindowSizeSeconds      float64
	AllowedLatenessSeconds float64
	Watermark              int64
}

// Business outcomes the API layer maps onto the published error codes.
var (
	ErrStreamExists        = errors.New("store: stream already exists")
	ErrStreamNotFound      = errors.New("store: stream not found")
	ErrWatermarkOutOfOrder = errors.New("store: watermark moves backwards")
)

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite allows one writer at a time; a single connection serializes transactions so a
	// watermark compare-and-write never interleaves with a concurrent writer.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable wal: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Ping reports whether the storage layer is usable.
func (s *Store) Ping() error { return s.db.Ping() }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// CreateStream registers a new data stream with a zero watermark. It returns ErrStreamExists
// when the stream_id is already registered and leaves the existing record untouched.
func (s *Store) CreateStream(stream Stream) error {
	result, err := s.db.Exec(
		`INSERT OR IGNORE INTO streams
			(stream_id, window_type, window_size_seconds, allowed_lateness_seconds, watermark)
			VALUES (?, ?, ?, ?, 0)`,
		stream.StreamID, stream.WindowType, stream.WindowSizeSeconds, stream.AllowedLatenessSeconds,
	)
	if err != nil {
		return fmt.Errorf("insert stream: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("insert stream: %w", err)
	}
	if inserted == 0 {
		return ErrStreamExists
	}
	return nil
}

// GetStream loads one registered stream by id.
func (s *Store) GetStream(streamID string) (Stream, error) {
	var stream Stream
	err := s.db.QueryRow(
		`SELECT stream_id, window_type, window_size_seconds, allowed_lateness_seconds, watermark
			FROM streams WHERE stream_id = ?`,
		streamID,
	).Scan(&stream.StreamID, &stream.WindowType, &stream.WindowSizeSeconds, &stream.AllowedLatenessSeconds, &stream.Watermark)
	if errors.Is(err, sql.ErrNoRows) {
		return Stream{}, ErrStreamNotFound
	}
	if err != nil {
		return Stream{}, fmt.Errorf("query stream: %w", err)
	}
	return stream, nil
}

// AdvanceWatermark compares the submitted watermark with the stored one and writes the new
// value inside the same transaction, so concurrent submitters can never move the watermark
// backwards. It returns the watermark stored after the call.
func (s *Store) AdvanceWatermark(streamID string, watermark int64) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin watermark update: %w", err)
	}
	defer tx.Rollback()

	var current int64
	err = tx.QueryRow(`SELECT watermark FROM streams WHERE stream_id = ?`, streamID).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrStreamNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("read watermark: %w", err)
	}
	if watermark < current {
		return 0, ErrWatermarkOutOfOrder
	}
	if watermark > current {
		if _, err := tx.Exec(`UPDATE streams SET watermark = ? WHERE stream_id = ?`, watermark, streamID); err != nil {
			return 0, fmt.Errorf("update watermark: %w", err)
		}
		current = watermark
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit watermark update: %w", err)
	}
	return current, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS streams (
	stream_id                TEXT PRIMARY KEY,
	window_type              TEXT NOT NULL,
	window_size_seconds      REAL NOT NULL CHECK (window_size_seconds > 0),
	allowed_lateness_seconds REAL NOT NULL CHECK (allowed_lateness_seconds >= 0),
	watermark                INTEGER NOT NULL DEFAULT 0 CHECK (watermark >= 0)
);
`
