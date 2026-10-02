package store

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestOpenCreatesUsableStore(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	if err := st.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestCreateStreamRejectsDuplicateAndKeepsOriginal(t *testing.T) {
	st := openTestStore(t)

	original := Stream{StreamID: "orders", WindowType: "tumbling", WindowSizeSeconds: 60, AllowedLatenessSeconds: 10}
	if err := st.CreateStream(original); err != nil {
		t.Fatalf("create: %v", err)
	}
	err := st.CreateStream(Stream{StreamID: "orders", WindowType: "session", WindowSizeSeconds: 300, AllowedLatenessSeconds: 30})
	if !errors.Is(err, ErrStreamExists) {
		t.Fatalf("err = %v, want ErrStreamExists", err)
	}

	var windowType string
	var windowSize, lateness float64
	var watermark int64
	row := st.db.QueryRow(`SELECT window_type, window_size_seconds, allowed_lateness_seconds, watermark FROM streams WHERE stream_id = 'orders'`)
	if err := row.Scan(&windowType, &windowSize, &lateness, &watermark); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if windowType != "tumbling" || windowSize != 60 || lateness != 10 || watermark != 0 {
		t.Fatalf("record = (%q, %v, %v, %v), want the original registration untouched", windowType, windowSize, lateness, watermark)
	}
}

func TestAdvanceWatermarkOnlyMovesForward(t *testing.T) {
	st := openTestStore(t)
	if err := st.CreateStream(Stream{StreamID: "orders", WindowType: "tumbling", WindowSizeSeconds: 60, AllowedLatenessSeconds: 10}); err != nil {
		t.Fatalf("create: %v", err)
	}

	if got, err := st.GetWatermark("orders"); err != nil || got != 0 {
		t.Fatalf("initial watermark = %d, %v; want 0, nil", got, err)
	}
	if err := st.AdvanceWatermark("orders", 100); err != nil {
		t.Fatalf("advance to 100: %v", err)
	}
	if err := st.AdvanceWatermark("orders", 100); err != nil {
		t.Fatalf("re-submitting the current watermark must succeed: %v", err)
	}
	if err := st.AdvanceWatermark("orders", 99); !errors.Is(err, ErrWatermarkOutOfOrder) {
		t.Fatalf("err = %v, want ErrWatermarkOutOfOrder", err)
	}
	if got, err := st.GetWatermark("orders"); err != nil || got != 100 {
		t.Fatalf("watermark = %d, %v; want 100, nil", got, err)
	}
}

func TestWatermarkEntriesRejectUnknownStreams(t *testing.T) {
	st := openTestStore(t)
	if _, err := st.GetWatermark("ghost"); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("err = %v, want ErrStreamNotFound", err)
	}
	if err := st.AdvanceWatermark("ghost", 1); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("err = %v, want ErrStreamNotFound", err)
	}
}

func TestConcurrentAdvancesKeepHighestWatermark(t *testing.T) {
	st := openTestStore(t)
	if err := st.CreateStream(Stream{StreamID: "orders", WindowType: "tumbling", WindowSizeSeconds: 60, AllowedLatenessSeconds: 10}); err != nil {
		t.Fatalf("create: %v", err)
	}

	const submitters = 32
	errs := make(chan error, submitters)
	var wait sync.WaitGroup
	wait.Add(submitters)
	for i := 1; i <= submitters; i++ {
		go func(value int64) {
			defer wait.Done()
			if err := st.AdvanceWatermark("orders", value); err != nil && !errors.Is(err, ErrWatermarkOutOfOrder) {
				errs <- err
			}
		}(int64(i))
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("advance: %v", err)
	}

	if got, err := st.GetWatermark("orders"); err != nil || got != submitters {
		t.Fatalf("watermark = %d, %v; want %d, nil", got, err, submitters)
	}
}

func TestStoreKeepsRecordsAcrossReopen(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "store.db")

	st, err := Open(databasePath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.CreateStream(Stream{StreamID: "orders", WindowType: "tumbling", WindowSizeSeconds: 60, AllowedLatenessSeconds: 10}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := st.AdvanceWatermark("orders", 42); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(databasePath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if got, err := reopened.GetWatermark("orders"); err != nil || got != 42 {
		t.Fatalf("watermark = %d, %v; want 42, nil", got, err)
	}
	if err := reopened.CreateStream(Stream{StreamID: "orders", WindowType: "session", WindowSizeSeconds: 300, AllowedLatenessSeconds: 30}); !errors.Is(err, ErrStreamExists) {
		t.Fatalf("err = %v, want ErrStreamExists", err)
	}
}
