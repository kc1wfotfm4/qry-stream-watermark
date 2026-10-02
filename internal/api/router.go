package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/kc1wfotfm4/qry-stream-watermark/internal/store"
)

// NewRouter wires the public HTTP surface described in README.md. Every
// failure response keeps the single-error-object contract.
func NewRouter(st *store.Store) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.CustomRecovery(func(c *gin.Context, recovered any) {
		writeError(c, http.StatusInternalServerError, "internal_error", "the request could not be completed")
	}))

	handler := &streamHandler{store: st}

	router.GET("/healthz", func(c *gin.Context) {
		if err := st.Ping(); err != nil {
			writeError(c, http.StatusServiceUnavailable, "storage_unavailable", "database is not available")
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "database": "ok"})
	})

	v1 := router.Group("/api/v1")
	v1.POST("/streams", handler.createStream)
	v1.POST("/streams/:id/watermarks", handler.advanceWatermark)
	v1.GET("/streams/:id/watermark", handler.getWatermark)

	router.NoRoute(func(c *gin.Context) {
		writeError(c, http.StatusNotFound, "route_not_found", "no route matches this path")
	})
	return router
}

// streamHandler serves the stream registration and watermark entries.
type streamHandler struct {
	store *store.Store
}

// registerStreamRequest is the JSON contract for POST /api/v1/streams.
// Pointer fields tell a missing field apart from a zero value.
type registerStreamRequest struct {
	StreamID               *string  `json:"stream_id"`
	WindowType             *string  `json:"window_type"`
	WindowSizeSeconds      *float64 `json:"window_size_seconds"`
	AllowedLatenessSeconds *float64 `json:"allowed_lateness_seconds"`
}

func (h *streamHandler) createStream(c *gin.Context) {
	var request registerStreamRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "request body must be a JSON object with stream_id, window_type, window_size_seconds and allowed_lateness_seconds")
		return
	}
	switch {
	case request.StreamID == nil || *request.StreamID == "":
		writeError(c, http.StatusBadRequest, "invalid_request", "stream_id must be a non-empty string")
		return
	case request.WindowType == nil || *request.WindowType == "":
		writeError(c, http.StatusBadRequest, "invalid_request", "window_type must be a non-empty string")
		return
	case request.WindowSizeSeconds == nil || *request.WindowSizeSeconds <= 0:
		writeError(c, http.StatusBadRequest, "invalid_request", "window_size_seconds must be a number greater than zero")
		return
	case request.AllowedLatenessSeconds == nil || *request.AllowedLatenessSeconds < 0:
		writeError(c, http.StatusBadRequest, "invalid_request", "allowed_lateness_seconds must be a non-negative number")
		return
	}

	stream := store.Stream{
		StreamID:               *request.StreamID,
		WindowType:             *request.WindowType,
		WindowSizeSeconds:      *request.WindowSizeSeconds,
		AllowedLatenessSeconds: *request.AllowedLatenessSeconds,
	}
	switch err := h.store.CreateStream(stream); {
	case err == nil:
		c.JSON(http.StatusCreated, gin.H{
			"stream_id":                stream.StreamID,
			"window_type":              stream.WindowType,
			"window_size_seconds":      stream.WindowSizeSeconds,
			"allowed_lateness_seconds": stream.AllowedLatenessSeconds,
		})
	case errors.Is(err, store.ErrStreamExists):
		writeError(c, http.StatusConflict, "stream_already_exists", "a stream with this stream_id is already registered")
	default:
		writeError(c, http.StatusInternalServerError, "internal_error", "the request could not be completed")
	}
}

// watermarkRequest is the JSON contract for POST /api/v1/streams/:id/watermarks.
type watermarkRequest struct {
	Watermark json.RawMessage `json:"watermark"`
}

func (h *streamHandler) advanceWatermark(c *gin.Context) {
	streamID := c.Param("id")

	var request watermarkRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "request body must be a JSON object with a numeric watermark")
		return
	}
	watermark, ok := parseWatermark(request.Watermark)
	if !ok {
		writeError(c, http.StatusBadRequest, "invalid_request", "watermark must be a non-negative integer unix timestamp in seconds")
		return
	}

	switch err := h.store.AdvanceWatermark(streamID, watermark); {
	case err == nil:
		c.JSON(http.StatusOK, gin.H{"stream_id": streamID, "watermark": watermark})
	case errors.Is(err, store.ErrStreamNotFound):
		writeError(c, http.StatusNotFound, "stream_not_found", "no stream is registered with this stream_id")
	case errors.Is(err, store.ErrWatermarkOutOfOrder):
		writeError(c, http.StatusConflict, "watermark_out_of_order", "watermark is earlier than the current watermark")
	default:
		writeError(c, http.StatusInternalServerError, "internal_error", "the request could not be completed")
	}
}

func (h *streamHandler) getWatermark(c *gin.Context) {
	streamID := c.Param("id")

	watermark, err := h.store.GetWatermark(streamID)
	switch {
	case err == nil:
		c.JSON(http.StatusOK, gin.H{"stream_id": streamID, "watermark": watermark})
	case errors.Is(err, store.ErrStreamNotFound):
		writeError(c, http.StatusNotFound, "stream_not_found", "no stream is registered with this stream_id")
	default:
		writeError(c, http.StatusInternalServerError, "internal_error", "the request could not be completed")
	}
}

// parseWatermark accepts a JSON number literal whose value is a non-negative
// integer that fits an int64, so 1e3 and 100.0 are as valid as 100. Anything
// else, including a quoted number, is rejected.
func parseWatermark(raw json.RawMessage) (int64, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] == '"' {
		return 0, false
	}
	var value json.Number
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, false
	}
	if parsed, err := value.Int64(); err == nil {
		return parsed, parsed >= 0
	}
	parsed, err := value.Float64()
	if err != nil || parsed < 0 || parsed >= 1<<63 || math.Trunc(parsed) != parsed {
		return 0, false
	}
	return int64(parsed), true
}

// writeError renders the single error object every failure response shares.
func writeError(c *gin.Context, status int, code string, message string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}
