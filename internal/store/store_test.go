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

func TestCreateAndGetStream(t *testing.T) {
	st := openTestStore(t)
	stream := Stream{StreamID: "orders", WindowType: "tumbling", WindowSizeSeconds: 300, AllowedLatenessSeconds: 60}
	if err := st.CreateStream(stream); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := st.GetStream("orders")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != stream {
		t.Fatalf("stream = %+v, want %+v", got, stream)
	}
}

func TestCreateDuplicateStreamLeavesOriginalUntouched(t *testing.T) {
	st := openTestStore(t)
	original := Stream{StreamID: "orders", WindowType: "tumbling", WindowSizeSeconds: 300, AllowedLatenessSeconds: 60}
	if err := st.CreateStream(original); err != nil {
		t.Fatalf("create: %v", err)
	}
	replacement := Stream{StreamID: "orders", WindowType: "hopping", WindowSizeSeconds: 60, AllowedLatenessSeconds: 5}
	if err := st.CreateStream(replacement); !errors.Is(err, ErrStreamExists) {
		t.Fatalf("duplicate create err = %v, want %v", err, ErrStreamExists)
	}
	got, err := st.GetStream("orders")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != original {
		t.Fatalf("stream = %+v, want untouched %+v", got, original)
	}
}

func TestGetMissingStream(t *testing.T) {
	st := openTestStore(t)
	if _, err := st.GetStream("ghost"); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("get err = %v, want %v", err, ErrStreamNotFound)
	}
}

func TestAdvanceWatermarkRules(t *testing.T) {
	st := openTestStore(t)
	if err := st.CreateStream(Stream{StreamID: "orders", WindowType: "tumbling", WindowSizeSeconds: 300}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if got, err := st.AdvanceWatermark("orders", 100); err != nil || got != 100 {
		t.Fatalf("advance = %d, %v; want 100, nil", got, err)
	}
	if got, err := st.AdvanceWatermark("orders", 100); err != nil || got != 100 {
		t.Fatalf("equal advance = %d, %v; want 100, nil", got, err)
	}
	if _, err := st.AdvanceWatermark("orders", 50); !errors.Is(err, ErrWatermarkOutOfOrder) {
		t.Fatalf("backwards advance err = %v, want %v", err, ErrWatermarkOutOfOrder)
	}
	if _, err := st.AdvanceWatermark("ghost", 1); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("missing stream advance err = %v, want %v", err, ErrStreamNotFound)
	}
	stream, err := st.GetStream("orders")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stream.Watermark != 100 {
		t.Fatalf("watermark = %d, want 100", stream.Watermark)
	}
}

func TestConcurrentAdvanceKeepsHighest(t *testing.T) {
	st := openTestStore(t)
	if err := st.CreateStream(Stream{StreamID: "orders", WindowType: "tumbling", WindowSizeSeconds: 300}); err != nil {
		t.Fatalf("create: %v", err)
	}
	const submitters = 64
	var wg sync.WaitGroup
	failures := make(chan error, submitters)
	for i := 1; i <= submitters; i++ {
		wg.Add(1)
		go func(watermark int64) {
			defer wg.Done()
			if _, err := st.AdvanceWatermark("orders", watermark); err != nil && !errors.Is(err, ErrWatermarkOutOfOrder) {
				failures <- err
			}
		}(int64(i))
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatalf("advance: %v", err)
	}
	stream, err := st.GetStream("orders")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stream.Watermark != submitters {
		t.Fatalf("watermark = %d, want %d", stream.Watermark, submitters)
	}
}

func TestRecordsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.CreateStream(Stream{StreamID: "orders", WindowType: "tumbling", WindowSizeSeconds: 300, AllowedLatenessSeconds: 60}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := st.AdvanceWatermark("orders", 42); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.GetStream("orders")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	want := Stream{StreamID: "orders", WindowType: "tumbling", WindowSizeSeconds: 300, AllowedLatenessSeconds: 60, Watermark: 42}
	if got != want {
		t.Fatalf("stream = %+v, want %+v", got, want)
	}
}
