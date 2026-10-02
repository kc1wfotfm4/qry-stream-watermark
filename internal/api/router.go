package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/kc1wfotfm4/qry-stream-watermark/internal/store"
)

// NewRouter wires the public HTTP surface. Every entry keeps the error shape published in
// README.md: a single top-level error object with code and message string fields.
func NewRouter(st *store.Store) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/healthz", func(c *gin.Context) {
		if err := st.Ping(); err != nil {
			writeError(c, http.StatusServiceUnavailable, "storage_unavailable", "database is not available")
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "database": "ok"})
	})

	v1 := router.Group("/api/v1")
	v1.POST("/streams", registerStream(st))
	v1.POST("/streams/:id/watermarks", submitWatermark(st))
	v1.GET("/streams/:id/watermark", getWatermark(st))

	router.NoRoute(func(c *gin.Context) {
		writeError(c, http.StatusNotFound, "route_not_found", "no route matches this path")
	})
	return router
}

// registerStreamRequest mirrors the JSON body of POST /api/v1/streams. Pointer fields make
// missing keys distinguishable from zero values.
type registerStreamRequest struct {
	StreamID               *string  `json:"stream_id"`
	WindowType             *string  `json:"window_type"`
	WindowSizeSeconds      *float64 `json:"window_size_seconds"`
	AllowedLatenessSeconds *float64 `json:"allowed_lateness_seconds"`
}

func registerStream(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body registerStreamRequest
		if err := json.NewDecoder(c.Request.Body).Decode(&body); err != nil {
			writeError(c, http.StatusBadRequest, "invalid_request", "body must be a JSON object with stream_id, window_type, window_size_seconds and allowed_lateness_seconds")
			return
		}
		if body.StreamID == nil || *body.StreamID == "" ||
			body.WindowType == nil || *body.WindowType == "" ||
			body.WindowSizeSeconds == nil || *body.WindowSizeSeconds <= 0 ||
			body.AllowedLatenessSeconds == nil || *body.AllowedLatenessSeconds < 0 {
			writeError(c, http.StatusBadRequest, "invalid_request", "stream_id and window_type must be non-empty strings, window_size_seconds must be greater than zero and allowed_lateness_seconds must not be negative")
			return
		}
		stream := store.Stream{
			StreamID:               *body.StreamID,
			WindowType:             *body.WindowType,
			WindowSizeSeconds:      *body.WindowSizeSeconds,
			AllowedLatenessSeconds: *body.AllowedLatenessSeconds,
		}
		if err := st.CreateStream(stream); err != nil {
			if errors.Is(err, store.ErrStreamExists) {
				writeError(c, http.StatusConflict, "stream_already_exists", "a stream with this stream_id is already registered")
				return
			}
			writeError(c, http.StatusInternalServerError, "internal_error", "the service could not complete the request")
			return
		}
		c.JSON(http.StatusCreated, gin.H{
			"stream_id":                stream.StreamID,
			"window_type":              stream.WindowType,
			"window_size_seconds":      stream.WindowSizeSeconds,
			"allowed_lateness_seconds": stream.AllowedLatenessSeconds,
		})
	}
}

// submitWatermarkRequest mirrors the JSON body of POST /api/v1/streams/:id/watermarks.
type submitWatermarkRequest struct {
	Watermark *int64 `json:"watermark"`
}

func submitWatermark(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body submitWatermarkRequest
		if err := json.NewDecoder(c.Request.Body).Decode(&body); err != nil {
			writeError(c, http.StatusBadRequest, "invalid_request", "body must be a JSON object with a non-negative integer watermark")
			return
		}
		if body.Watermark == nil || *body.Watermark < 0 {
			writeError(c, http.StatusBadRequest, "invalid_request", "watermark must be a non-negative integer unix timestamp in seconds")
			return
		}
		streamID := c.Param("id")
		current, err := st.AdvanceWatermark(streamID, *body.Watermark)
		if err != nil {
			switch {
			case errors.Is(err, store.ErrStreamNotFound):
				writeError(c, http.StatusNotFound, "stream_not_found", "no stream is registered with this stream_id")
			case errors.Is(err, store.ErrWatermarkOutOfOrder):
				writeError(c, http.StatusConflict, "watermark_out_of_order", "watermark must not move backwards")
			default:
				writeError(c, http.StatusInternalServerError, "internal_error", "the service could not complete the request")
			}
			return
		}
		c.JSON(http.StatusOK, gin.H{"stream_id": streamID, "watermark": current})
	}
}

func getWatermark(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		stream, err := st.GetStream(c.Param("id"))
		if err != nil {
			if errors.Is(err, store.ErrStreamNotFound) {
				writeError(c, http.StatusNotFound, "stream_not_found", "no stream is registered with this stream_id")
				return
			}
			writeError(c, http.StatusInternalServerError, "internal_error", "the service could not complete the request")
			return
		}
		c.JSON(http.StatusOK, gin.H{"stream_id": stream.StreamID, "watermark": stream.Watermark})
	}
}

// writeError renders the single error shape every endpoint shares.
func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}
