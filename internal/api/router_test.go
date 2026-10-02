package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kc1wfotfm4/qry-stream-watermark/internal/store"
)

func TestHealthzReportsOK(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	NewRouter(st).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Body.String(); got != `{"database":"ok","status":"ok"}` {
		t.Fatalf("body = %s", got)
	}
}

func TestUnknownRouteUsesPublishedErrorShape(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/missing", nil)
	NewRouter(st).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewRouter(st)
}

func serveJSON(t *testing.T, router http.Handler, method string, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func requireErrorResponse(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", recorder.Code, status, recorder.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("error body = %v, want a single top-level error object", body)
	}
	errorObject, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("error body = %v, want a top-level error object", body)
	}
	if len(errorObject) != 2 {
		t.Fatalf("error object = %v, want only code and message", errorObject)
	}
	if errorObject["code"] != code {
		t.Fatalf("error code = %v, want %q", errorObject["code"], code)
	}
	if message, ok := errorObject["message"].(string); !ok || message == "" {
		t.Fatalf("error message = %v, want a non-empty string", errorObject["message"])
	}
}

func requireWatermarkResponse(t *testing.T, recorder *httptest.ResponseRecorder, streamID string, watermark int64) {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body %s)", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if len(body) != 2 || body["stream_id"] != streamID || body["watermark"] != float64(watermark) {
		t.Fatalf("body = %v, want stream_id=%q watermark=%d", body, streamID, watermark)
	}
}

func registerStream(t *testing.T, router http.Handler, streamID string) *httptest.ResponseRecorder {
	t.Helper()
	return serveJSON(t, router, http.MethodPost, "/api/v1/streams",
		fmt.Sprintf(`{"stream_id":%q,"window_type":"tumbling","window_size_seconds":60,"allowed_lateness_seconds":10}`, streamID))
}

func TestRegisterStreamEchoesSavedFields(t *testing.T) {
	router := newTestRouter(t)
	recorder := serveJSON(t, router, http.MethodPost, "/api/v1/streams",
		`{"stream_id":"orders","window_type":"tumbling","window_size_seconds":60,"allowed_lateness_seconds":10}`)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d (body %s)", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if len(body) != 4 {
		t.Fatalf("body = %v, want exactly the four saved fields", body)
	}
	if body["stream_id"] != "orders" || body["window_type"] != "tumbling" ||
		body["window_size_seconds"] != 60.0 || body["allowed_lateness_seconds"] != 10.0 {
		t.Fatalf("body = %v, want the registered values echoed back", body)
	}
}

func TestRegisterStreamRejectsInvalidBodies(t *testing.T) {
	router := newTestRouter(t)
	cases := map[string]string{
		"empty body":             ``,
		"not json":               `not-json`,
		"missing stream_id":      `{"window_type":"tumbling","window_size_seconds":60,"allowed_lateness_seconds":10}`,
		"empty stream_id":        `{"stream_id":"","window_type":"tumbling","window_size_seconds":60,"allowed_lateness_seconds":10}`,
		"stream_id wrong type":   `{"stream_id":42,"window_type":"tumbling","window_size_seconds":60,"allowed_lateness_seconds":10}`,
		"missing window_type":    `{"stream_id":"a","window_size_seconds":60,"allowed_lateness_seconds":10}`,
		"empty window_type":      `{"stream_id":"a","window_type":"","window_size_seconds":60,"allowed_lateness_seconds":10}`,
		"missing window size":    `{"stream_id":"a","window_type":"tumbling","allowed_lateness_seconds":10}`,
		"zero window size":       `{"stream_id":"a","window_type":"tumbling","window_size_seconds":0,"allowed_lateness_seconds":10}`,
		"negative window size":   `{"stream_id":"a","window_type":"tumbling","window_size_seconds":-5,"allowed_lateness_seconds":10}`,
		"window size wrong type": `{"stream_id":"a","window_type":"tumbling","window_size_seconds":"60","allowed_lateness_seconds":10}`,
		"missing lateness":       `{"stream_id":"a","window_type":"tumbling","window_size_seconds":60}`,
		"negative lateness":      `{"stream_id":"a","window_type":"tumbling","window_size_seconds":60,"allowed_lateness_seconds":-1}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			requireErrorResponse(t, serveJSON(t, router, http.MethodPost, "/api/v1/streams", body), http.StatusBadRequest, "invalid_request")
		})
	}
}

func TestRegisterDuplicateStreamKeepsOriginalRecord(t *testing.T) {
	router := newTestRouter(t)
	if recorder := registerStream(t, router, "orders"); recorder.Code != http.StatusCreated {
		t.Fatalf("register: status = %d, want %d", recorder.Code, http.StatusCreated)
	}

	recorder := serveJSON(t, router, http.MethodPost, "/api/v1/streams",
		`{"stream_id":"orders","window_type":"session","window_size_seconds":300,"allowed_lateness_seconds":30}`)
	requireErrorResponse(t, recorder, http.StatusConflict, "stream_already_exists")

	// The original registration is untouched: the watermark still starts at 0.
	requireWatermarkResponse(t, serveJSON(t, router, http.MethodGet, "/api/v1/streams/orders/watermark", ""), "orders", 0)
}

func TestWatermarkLifecycle(t *testing.T) {
	router := newTestRouter(t)
	registerStream(t, router, "orders")

	// A freshly registered stream reports a zero watermark.
	requireWatermarkResponse(t, serveJSON(t, router, http.MethodGet, "/api/v1/streams/orders/watermark", ""), "orders", 0)

	// A higher watermark advances the record.
	requireWatermarkResponse(t, serveJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", `{"watermark":100}`), "orders", 100)
	requireWatermarkResponse(t, serveJSON(t, router, http.MethodGet, "/api/v1/streams/orders/watermark", ""), "orders", 100)

	// Re-submitting the current watermark succeeds without changing anything.
	requireWatermarkResponse(t, serveJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", `{"watermark":100}`), "orders", 100)

	// An earlier watermark is rejected and the record stays put.
	requireErrorResponse(t, serveJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", `{"watermark":99}`), http.StatusConflict, "watermark_out_of_order")
	requireWatermarkResponse(t, serveJSON(t, router, http.MethodGet, "/api/v1/streams/orders/watermark", ""), "orders", 100)
}

func TestWatermarkEntriesRejectUnknownStreams(t *testing.T) {
	router := newTestRouter(t)
	requireErrorResponse(t, serveJSON(t, router, http.MethodGet, "/api/v1/streams/ghost/watermark", ""), http.StatusNotFound, "stream_not_found")
	requireErrorResponse(t, serveJSON(t, router, http.MethodPost, "/api/v1/streams/ghost/watermarks", `{"watermark":1}`), http.StatusNotFound, "stream_not_found")
}

func TestSubmitWatermarkRejectsInvalidValues(t *testing.T) {
	router := newTestRouter(t)
	registerStream(t, router, "orders")
	cases := map[string]string{
		"missing watermark": `{}`,
		"negative":          `{"watermark":-1}`,
		"fractional":        `{"watermark":1.5}`,
		"string":            `{"watermark":"100"}`,
		"boolean":           `{"watermark":true}`,
		"null":              `{"watermark":null}`,
		"not json":          `nope`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			requireErrorResponse(t, serveJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", body), http.StatusBadRequest, "invalid_request")
		})
	}

	// The rejected submissions must not have moved the watermark.
	requireWatermarkResponse(t, serveJSON(t, router, http.MethodGet, "/api/v1/streams/orders/watermark", ""), "orders", 0)
}

func TestSubmitWatermarkAcceptsIntegralNumberForms(t *testing.T) {
	router := newTestRouter(t)
	registerStream(t, router, "orders")
	requireWatermarkResponse(t, serveJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", `{"watermark":100.0}`), "orders", 100)
	requireWatermarkResponse(t, serveJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", `{"watermark":1e3}`), "orders", 1000)
}

func TestConcurrentWatermarkSubmissionsKeepHighest(t *testing.T) {
	router := newTestRouter(t)
	registerStream(t, router, "orders")

	const submitters = 32
	var wait sync.WaitGroup
	wait.Add(submitters)
	for i := 1; i <= submitters; i++ {
		go func(value int) {
			defer wait.Done()
			serveJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", fmt.Sprintf(`{"watermark":%d}`, value))
		}(i)
	}
	wait.Wait()

	requireWatermarkResponse(t, serveJSON(t, router, http.MethodGet, "/api/v1/streams/orders/watermark", ""), "orders", submitters)
}

func TestStreamsSurviveRestart(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "service.db")

	st, err := store.Open(databasePath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	router := NewRouter(st)
	registerStream(t, router, "orders")
	requireWatermarkResponse(t, serveJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", `{"watermark":42}`), "orders", 42)
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := store.Open(databasePath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	router = NewRouter(reopened)

	requireWatermarkResponse(t, serveJSON(t, router, http.MethodGet, "/api/v1/streams/orders/watermark", ""), "orders", 42)
	requireErrorResponse(t, registerStream(t, router, "orders"), http.StatusConflict, "stream_already_exists")
}
