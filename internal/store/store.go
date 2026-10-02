// Package store owns the SQLite file and every write the service performs.
package store

import (
	"database/sql"
	"errors"
	"fmt"

	"modernc.org/sqlite"
)

// Sentinel errors the API layer maps onto the published error codes.
var (
	ErrStreamExists        = errors.New("stream already exists")
	ErrStreamNotFound      = errors.New("stream not found")
	ErrWatermarkOutOfOrder = errors.New("watermark out of order")
)

// SQLite extended result codes for a violated PRIMARY KEY or UNIQUE constraint.
const (
	sqliteConstraintPrimaryKey = 1555
	sqliteConstraintUnique     = 2067
)

// Stream is one registered data stream and its current watermark.
type Stream struct {
	StreamID               string
	WindowType             string
	WindowSizeSeconds      float64
	AllowedLatenessSeconds float64
	Watermark              int64
}

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB
}

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// A single connection serializes every statement, so concurrent writers
	// queue in the pool instead of failing with SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable wal: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
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

// CreateStream registers a new data stream with a zero watermark. It returns
// ErrStreamExists when the stream id is already registered.
func (s *Store) CreateStream(stream Stream) error {
	_, err := s.db.Exec(
		`INSERT INTO streams (stream_id, window_type, window_size_seconds, allowed_lateness_seconds, watermark)
		 VALUES (?, ?, ?, ?, 0)`,
		stream.StreamID, stream.WindowType, stream.WindowSizeSeconds, stream.AllowedLatenessSeconds,
	)
	if err != nil {
		var sqliteErr *sqlite.Error
		if errors.As(err, &sqliteErr) &&
			(sqliteErr.Code() == sqliteConstraintPrimaryKey || sqliteErr.Code() == sqliteConstraintUnique) {
			return ErrStreamExists
		}
		return fmt.Errorf("insert stream: %w", err)
	}
	return nil
}

// GetWatermark returns the current watermark of a registered stream. It
// returns ErrStreamNotFound when the stream id is unknown.
func (s *Store) GetWatermark(streamID string) (int64, error) {
	var watermark int64
	err := s.db.QueryRow(`SELECT watermark FROM streams WHERE stream_id = ?`, streamID).Scan(&watermark)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrStreamNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("query watermark: %w", err)
	}
	return watermark, nil
}

// AdvanceWatermark moves the stream watermark forward to watermark. The
// comparison and the write run as one atomic UPDATE, so concurrent callers
// can never move the stored value backwards and the surviving value is always
// the highest submitted watermark. An equal value succeeds without changing
// the record, a lower value fails with ErrWatermarkOutOfOrder, and an unknown
// stream fails with ErrStreamNotFound.
func (s *Store) AdvanceWatermark(streamID string, watermark int64) error {
	result, err := s.db.Exec(
		`UPDATE streams SET watermark = ? WHERE stream_id = ? AND watermark <= ?`,
		watermark, streamID, watermark,
	)
	if err != nil {
		return fmt.Errorf("advance watermark: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("advance watermark: %w", err)
	}
	if affected > 0 {
		return nil
	}
	if _, err := s.GetWatermark(streamID); err != nil {
		return err
	}
	return ErrWatermarkOutOfOrder
}

const schema = `
CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS streams (
	stream_id                TEXT PRIMARY KEY,
	window_type              TEXT NOT NULL,
	window_size_seconds      REAL NOT NULL,
	allowed_lateness_seconds REAL NOT NULL,
	watermark                INTEGER NOT NULL DEFAULT 0
);
`
