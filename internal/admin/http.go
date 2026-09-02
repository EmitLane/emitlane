package admin

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

const maxRequestBody = 1 << 20

type HTTPConfig struct {
	Addr          string
	Token         string
	ExposePayload bool
	Logger        *slog.Logger
}

func NewHTTPServer(cfg HTTPConfig, service *Service) (*http.Server, error) {
	if service == nil {
		return nil, fmt.Errorf("admin: service is required")
	}
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, fmt.Errorf("admin: address is required")
	}
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           newHandler(service, cfg.Token, cfg.ExposePayload, cfg.Logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}, nil
}

type handler struct {
	service       *Service
	tokenHash     [sha256.Size]byte
	requireAuth   bool
	exposePayload bool
	log           *slog.Logger
}

func newHandler(service *Service, token string, exposePayload bool, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	h := &handler{service: service, exposePayload: exposePayload, log: logger}
	if token != "" {
		h.requireAuth = true
		h.tokenHash = sha256.Sum256([]byte(token))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/stats", h.stats)
	mux.HandleFunc("GET /v1/events", h.events)
	mux.HandleFunc("GET /v1/events/{id}", h.event)
	mux.HandleFunc("POST /v1/events/{id}/retry", h.retry)
	mux.HandleFunc("POST /v1/events/{id}/replay", h.replayEvent)
	mux.HandleFunc("GET /v1/relays", h.relays)
	mux.HandleFunc("GET /v1/relays/{id}", h.relay)
	mux.HandleFunc("GET /v1/relay", h.control)
	mux.HandleFunc("POST /v1/relay/pause", h.pause)
	mux.HandleFunc("POST /v1/relay/resume", h.resume)
	mux.HandleFunc("POST /v1/replays/preview", h.previewReplay)
	mux.HandleFunc("POST /v1/replays", h.replayBatch)
	mux.HandleFunc("GET /v1/audit", h.audit)
	return h.requestID(h.recover(h.authenticate(mux)))
}

type contextKey string

const requestIDKey contextKey = "request-id"

func (h *handler) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := sanitizeRequestID(r.Header.Get("X-Request-ID"))
		if requestID == "" {
			if id, err := uuid.NewV7(); err == nil {
				requestID = id.String()
			} else {
				requestID = uuid.NewString()
			}
		}
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, requestID)))
	})
}

func sanitizeRequestID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return ""
	}
	for _, r := range value {
		if r > unicode.MaxASCII || unicode.IsControl(r) {
			return ""
		}
	}
	return value
}

func (h *handler) authenticate(next http.Handler) http.Handler {
	if !h.requireAuth {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") || len(auth) <= len("Bearer ") {
			h.writeError(w, r, http.StatusUnauthorized, "unauthorized", "valid bearer token required")
			return
		}
		provided := sha256.Sum256([]byte(auth[len("Bearer "):]))
		if subtle.ConstantTimeCompare(provided[:], h.tokenHash[:]) != 1 {
			h.writeError(w, r, http.StatusUnauthorized, "unauthorized", "valid bearer token required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *handler) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				h.log.Error("admin request panic", "request_id", requestID(r), "method", r.Method, "path", r.URL.Path)
				h.writeError(w, r, http.StatusInternalServerError, "internal", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func requestID(r *http.Request) string {
	if value, ok := r.Context().Value(requestIDKey).(string); ok {
		return value
	}
	return r.Header.Get("X-Request-ID")
}

type errorEnvelope struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

func (h *handler) writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	var envelope errorEnvelope
	envelope.Error.Code = code
	envelope.Error.Message = message
	envelope.Error.RequestID = requestID(r)
	h.writeJSON(w, status, envelope)
}

func (h *handler) serviceError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, message := http.StatusInternalServerError, "internal", "internal server error"
	switch {
	case errors.Is(err, ErrInvalid):
		status, code, message = http.StatusBadRequest, "invalid_request", err.Error()
	case errors.Is(err, ErrNotFound):
		code = "not_found"
		if strings.Contains(r.URL.Path, "/events/") {
			code = "event_not_found"
		}
		status, message = http.StatusNotFound, err.Error()
	case errors.Is(err, ErrConflict):
		code = "invalid_event_state"
		if strings.Contains(err.Error(), "exceeds") {
			code = "replay_limit_exceeded"
		}
		status, message = http.StatusConflict, err.Error()
	default:
		h.log.Error("admin request failed", "request_id", requestID(r), "method", r.Method, "path", r.URL.Path, "error", err)
	}
	h.writeError(w, r, status, code, message)
}

func (h *handler) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (h *handler) decodeJSON(w http.ResponseWriter, r *http.Request, value any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(value); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.writeError(w, r, http.StatusRequestEntityTooLarge, "invalid_request", "request body is too large")
			return false
		}
		h.writeError(w, r, http.StatusBadRequest, "invalid_json", "request body must be one valid JSON object")
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		h.writeError(w, r, http.StatusBadRequest, "invalid_json", "request body must contain one JSON object")
		return false
	}
	return true
}

func (h *handler) stats(w http.ResponseWriter, r *http.Request) {
	value, err := h.service.Stats(r.Context())
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.writeJSON(w, http.StatusOK, value)
}

func (h *handler) events(w http.ResponseWriter, r *http.Request) {
	filter, err := filterFromQuery(r)
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	page, err := h.service.ListEvents(r.Context(), filter)
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.writeJSON(w, http.StatusOK, page)
}

func (h *handler) event(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, "invalid_request", "event id must be a UUID")
		return
	}
	include := r.URL.Query().Get("payload") == "true"
	if include && !h.exposePayload {
		h.writeError(w, r, http.StatusForbidden, "payload_disabled", "payload exposure is disabled")
		return
	}
	value, err := h.service.InspectEvent(r.Context(), id, include)
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.writeJSON(w, http.StatusOK, value)
}

func (h *handler) relays(w http.ResponseWriter, r *http.Request) {
	value, err := h.service.ListRelays(r.Context())
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]any{"relays": value})
}

func (h *handler) control(w http.ResponseWriter, r *http.Request) {
	value, err := h.service.ControlState(r.Context())
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.writeJSON(w, http.StatusOK, value)
}

func (h *handler) relay(w http.ResponseWriter, r *http.Request) {
	value, err := h.service.GetRelay(r.Context(), r.PathValue("id"))
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.writeJSON(w, http.StatusOK, value)
}

type mutationBody struct {
	Reason string `json:"reason"`
}

func (h *handler) mutation(r *http.Request, reason string) Mutation {
	return Mutation{Actor: "admin-api", Reason: reason, RequestID: requestID(r)}
}

func (h *handler) pause(w http.ResponseWriter, r *http.Request)  { h.setPause(w, r, true) }
func (h *handler) resume(w http.ResponseWriter, r *http.Request) { h.setPause(w, r, false) }

func (h *handler) setPause(w http.ResponseWriter, r *http.Request, paused bool) {
	var body mutationBody
	if !h.decodeJSON(w, r, &body) {
		return
	}
	state, err := h.service.SetPaused(r.Context(), paused, h.mutation(r, body.Reason))
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.log.Info("admin relay control changed", "request_id", requestID(r), "paused", paused, "actor", "admin-api")
	h.writeJSON(w, http.StatusOK, state)
}

func (h *handler) retry(w http.ResponseWriter, r *http.Request) {
	id, ok := h.eventID(w, r)
	if !ok {
		return
	}
	var body mutationBody
	if !h.decodeJSON(w, r, &body) {
		return
	}
	if err := h.service.RetryDead(r.Context(), id, h.mutation(r, body.Reason)); err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.log.Info("admin event retry committed", "request_id", requestID(r), "event_id", id, "actor", "admin-api")
	h.writeJSON(w, http.StatusOK, map[string]any{"event_id": id, "status": "pending"})
}

func (h *handler) replayEvent(w http.ResponseWriter, r *http.Request) {
	id, ok := h.eventID(w, r)
	if !ok {
		return
	}
	var body mutationBody
	if !h.decodeJSON(w, r, &body) {
		return
	}
	result, err := h.service.ReplayEvent(r.Context(), id, h.mutation(r, body.Reason))
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.log.Info("admin event replay committed", "request_id", requestID(r), "source_event_id", id,
		"replay_batch_id", result.ReplayBatchID, "created", result.Created, "actor", "admin-api")
	h.writeJSON(w, http.StatusCreated, result)
}

func (h *handler) eventID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, "invalid_request", "event id must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

type replayBody struct {
	Statuses    []string   `json:"statuses"`
	Destination string     `json:"destination"`
	EventType   string     `json:"event_type"`
	CreatedFrom *time.Time `json:"created_after"`
	CreatedTo   *time.Time `json:"created_before"`
	Limit       int        `json:"limit"`
	Reason      string     `json:"reason"`
}

func (b replayBody) filter() EventFilter {
	return EventFilter{Statuses: b.Statuses, Destination: b.Destination, EventType: b.EventType,
		CreatedFrom: b.CreatedFrom, CreatedTo: b.CreatedTo, Limit: b.Limit}
}

func (h *handler) previewReplay(w http.ResponseWriter, r *http.Request) {
	var body replayBody
	if !h.decodeJSON(w, r, &body) {
		return
	}
	preview, err := h.service.PreviewReplay(r.Context(), body.filter())
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.writeJSON(w, http.StatusOK, preview)
}

func (h *handler) replayBatch(w http.ResponseWriter, r *http.Request) {
	var body replayBody
	if !h.decodeJSON(w, r, &body) {
		return
	}
	result, err := h.service.ReplayBatch(r.Context(), body.filter(), h.mutation(r, body.Reason))
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.log.Info("admin replay batch committed", "request_id", requestID(r), "replay_batch_id", result.ReplayBatchID,
		"created", result.Created, "actor", "admin-api")
	h.writeJSON(w, http.StatusCreated, result)
}

func (h *handler) audit(w http.ResponseWriter, r *http.Request) {
	limit, err := queryLimit(r, DefaultPageSize)
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	cursor, err := DecodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	page, err := h.service.ListAudit(r.Context(), AuditFilter{Action: r.URL.Query().Get("action"), Limit: limit, Cursor: cursor})
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	h.writeJSON(w, http.StatusOK, page)
}

func filterFromQuery(r *http.Request) (EventFilter, error) {
	q := r.URL.Query()
	statuses := make([]string, 0)
	for _, value := range q["status"] {
		statuses = append(statuses, strings.Split(value, ",")...)
	}
	limit, err := queryLimit(r, DefaultPageSize)
	if err != nil {
		return EventFilter{}, err
	}
	cursor, err := DecodeCursor(q.Get("cursor"))
	if err != nil {
		return EventFilter{}, err
	}
	from, err := queryTime(q.Get("created_after"), "created_after")
	if err != nil {
		return EventFilter{}, err
	}
	to, err := queryTime(q.Get("created_before"), "created_before")
	if err != nil {
		return EventFilter{}, err
	}
	var replayBatchID *uuid.UUID
	if raw := strings.TrimSpace(q.Get("replay_batch_id")); raw != "" {
		id, parseErr := uuid.Parse(raw)
		if parseErr != nil {
			return EventFilter{}, fmt.Errorf("%w: replay_batch_id must be a UUID", ErrInvalid)
		}
		replayBatchID = &id
	}
	return EventFilter{Statuses: statuses, Destination: q.Get("destination"), EventType: q.Get("event_type"),
		CreatedFrom: from, CreatedTo: to, ReplayBatchID: replayBatchID, Cursor: cursor, Limit: limit}, nil
}

func queryLimit(r *http.Request, fallback int) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return fallback, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: limit must be an integer", ErrInvalid)
	}
	return limit, nil
}

func queryTime(raw, name string) (*time.Time, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %s must be RFC3339", ErrInvalid, name)
	}
	return &value, nil
}
