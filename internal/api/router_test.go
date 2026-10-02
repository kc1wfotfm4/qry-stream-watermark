package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

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

func newTestRouter(t *testing.T) *gin.Engine {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewRouter(st)
}

func doJSON(t *testing.T, router *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, reader)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	router.ServeHTTP(recorder, request)
	return recorder
}

func decodeBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode body %q: %v", recorder.Body.String(), err)
	}
	return payload
}

func assertErrorShape(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", recorder.Code, status, recorder.Body.String())
	}
	payload := decodeBody(t, recorder)
	if len(payload) != 1 {
		t.Fatalf("error body keys = %v, want only error", payload)
	}
	errorObject, ok := payload["error"].(map[string]any)
	if !ok {
		t.Fatalf("error field = %v, want object", payload["error"])
	}
	if len(errorObject) != 2 {
		t.Fatalf("error object keys = %v, want only code and message", errorObject)
	}
	if errorObject["code"] != code {
		t.Fatalf("error code = %v, want %q", errorObject["code"], code)
	}
	if message, ok := errorObject["message"].(string); !ok || message == "" {
		t.Fatalf("error message = %v, want non-empty string", errorObject["message"])
	}
}

func TestRegisterStreamEchoesSavedConfig(t *testing.T) {
	router := newTestRouter(t)
	recorder := doJSON(t, router, http.MethodPost, "/api/v1/streams",
		`{"stream_id":"orders","window_type":"tumbling","window_size_seconds":300,"allowed_lateness_seconds":60}`)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d (body %s)", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	want := map[string]any{
		"stream_id":                "orders",
		"window_type":              "tumbling",
		"window_size_seconds":      float64(300),
		"allowed_lateness_seconds": float64(60),
	}
	if got := decodeBody(t, recorder); !reflect.DeepEqual(got, want) {
		t.Fatalf("body = %v, want %v", got, want)
	}
}

func TestRegisterStreamRejectsInvalidBodies(t *testing.T) {
	router := newTestRouter(t)
	bodies := map[string]string{
		"empty body":            "",
		"not json":              "not-json",
		"missing stream_id":     `{"window_type":"tumbling","window_size_seconds":300,"allowed_lateness_seconds":60}`,
		"empty stream_id":       `{"stream_id":"","window_type":"tumbling","window_size_seconds":300,"allowed_lateness_seconds":60}`,
		"stream_id wrong type":  `{"stream_id":7,"window_type":"tumbling","window_size_seconds":300,"allowed_lateness_seconds":60}`,
		"missing window_type":   `{"stream_id":"a","window_size_seconds":300,"allowed_lateness_seconds":60}`,
		"empty window_type":     `{"stream_id":"a","window_type":"","window_size_seconds":300,"allowed_lateness_seconds":60}`,
		"missing window size":   `{"stream_id":"a","window_type":"tumbling","allowed_lateness_seconds":60}`,
		"zero window size":      `{"stream_id":"a","window_type":"tumbling","window_size_seconds":0,"allowed_lateness_seconds":60}`,
		"negative window size":  `{"stream_id":"a","window_type":"tumbling","window_size_seconds":-5,"allowed_lateness_seconds":60}`,
		"window size as string": `{"stream_id":"a","window_type":"tumbling","window_size_seconds":"300","allowed_lateness_seconds":60}`,
		"missing lateness":      `{"stream_id":"a","window_type":"tumbling","window_size_seconds":300}`,
		"negative lateness":     `{"stream_id":"a","window_type":"tumbling","window_size_seconds":300,"allowed_lateness_seconds":-1}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			recorder := doJSON(t, router, http.MethodPost, "/api/v1/streams", body)
			assertErrorShape(t, recorder, http.StatusBadRequest, "invalid_request")
		})
	}
}

func TestRegisterDuplicateStreamConflicts(t *testing.T) {
	router := newTestRouter(t)
	first := doJSON(t, router, http.MethodPost, "/api/v1/streams",
		`{"stream_id":"orders","window_type":"tumbling","window_size_seconds":300,"allowed_lateness_seconds":60}`)
	if first.Code != http.StatusCreated {
		t.Fatalf("first register status = %d", first.Code)
	}
	second := doJSON(t, router, http.MethodPost, "/api/v1/streams",
		`{"stream_id":"orders","window_type":"hopping","window_size_seconds":60,"allowed_lateness_seconds":5}`)
	assertErrorShape(t, second, http.StatusConflict, "stream_already_exists")
}

func TestWatermarkLifecycle(t *testing.T) {
	router := newTestRouter(t)
	if recorder := doJSON(t, router, http.MethodPost, "/api/v1/streams",
		`{"stream_id":"orders","window_type":"tumbling","window_size_seconds":300,"allowed_lateness_seconds":60}`); recorder.Code != http.StatusCreated {
		t.Fatalf("register status = %d", recorder.Code)
	}

	initial := doJSON(t, router, http.MethodGet, "/api/v1/streams/orders/watermark", "")
	if initial.Code != http.StatusOK {
		t.Fatalf("initial status = %d", initial.Code)
	}
	if got := decodeBody(t, initial); got["stream_id"] != "orders" || got["watermark"] != float64(0) {
		t.Fatalf("initial body = %v", got)
	}

	advanced := doJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", `{"watermark":100}`)
	if advanced.Code != http.StatusOK {
		t.Fatalf("advance status = %d (body %s)", advanced.Code, advanced.Body.String())
	}
	if got := decodeBody(t, advanced); got["stream_id"] != "orders" || got["watermark"] != float64(100) {
		t.Fatalf("advance body = %v", got)
	}

	equal := doJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", `{"watermark":100}`)
	if equal.Code != http.StatusOK {
		t.Fatalf("equal status = %d (body %s)", equal.Code, equal.Body.String())
	}
	if got := decodeBody(t, equal); got["watermark"] != float64(100) {
		t.Fatalf("equal body = %v", got)
	}

	backwards := doJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", `{"watermark":50}`)
	assertErrorShape(t, backwards, http.StatusConflict, "watermark_out_of_order")

	current := doJSON(t, router, http.MethodGet, "/api/v1/streams/orders/watermark", "")
	if got := decodeBody(t, current); got["watermark"] != float64(100) {
		t.Fatalf("current body = %v", got)
	}
}

func TestWatermarkUnknownStream(t *testing.T) {
	router := newTestRouter(t)
	get := doJSON(t, router, http.MethodGet, "/api/v1/streams/ghost/watermark", "")
	assertErrorShape(t, get, http.StatusNotFound, "stream_not_found")
	post := doJSON(t, router, http.MethodPost, "/api/v1/streams/ghost/watermarks", `{"watermark":10}`)
	assertErrorShape(t, post, http.StatusNotFound, "stream_not_found")
}

func TestSubmitWatermarkRejectsInvalidBodies(t *testing.T) {
	router := newTestRouter(t)
	if recorder := doJSON(t, router, http.MethodPost, "/api/v1/streams",
		`{"stream_id":"orders","window_type":"tumbling","window_size_seconds":300,"allowed_lateness_seconds":60}`); recorder.Code != http.StatusCreated {
		t.Fatalf("register status = %d", recorder.Code)
	}
	bodies := map[string]string{
		"empty body":        "",
		"missing watermark": `{}`,
		"null watermark":    `{"watermark":null}`,
		"negative":          `{"watermark":-1}`,
		"fractional":        `{"watermark":1.5}`,
		"string":            `{"watermark":"100"}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			recorder := doJSON(t, router, http.MethodPost, "/api/v1/streams/orders/watermarks", body)
			assertErrorShape(t, recorder, http.StatusBadRequest, "invalid_request")
		})
	}
}
