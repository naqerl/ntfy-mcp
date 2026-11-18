package mcpx

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// SessionHeader is the Streamable-HTTP session header.
const SessionHeader = "Mcp-Session-Id"

const (
	maxRequestBody = 8 << 20
	defaultSessTTL = 24 * time.Hour
)

type session struct {
	id       string
	lastSeen time.Time
}

// Handler is the Streamable-HTTP endpoint. It requires bearer authentication,
// creates and validates Mcp-Session-Id sessions, and owns the SSE writer so a
// POST carrying events/stream stays open on the originating request.
type Handler struct {
	server     *Server
	authToken  string
	sessionTTL time.Duration
	now        func() time.Time

	mu       sync.Mutex
	sessions map[string]*session
}

// NewHandler constructs a Streamable-HTTP handler. authToken must be non-empty;
// startup validation enforces that.
func NewHandler(server *Server, authToken string) *Handler {
	return &Handler{
		server:     server,
		authToken:  authToken,
		sessionTTL: defaultSessTTL,
		now:        time.Now,
		sessions:   make(map[string]*session),
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="ntfy-mcp"`)
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}

	switch r.Method {
	case http.MethodPost:
		h.handlePost(w, r)
	case http.MethodGet:
		h.handleGet(w, r)
	case http.MethodDelete:
		h.handleDelete(w, r)
	default:
		w.Header().Set("Allow", "POST, GET, DELETE")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

func (h *Handler) authorized(r *http.Request) bool {
	got, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok || h.authToken == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(h.authToken)) == 1
}

func bearerToken(header string) (string, bool) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" || strings.ContainsAny(token, " \t") {
		return "", false
	}
	return token, true
}

func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "cannot read request body"})
		return
	}
	if len(body) > maxRequestBody {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "request body too large"})
		return
	}

	sessionID := strings.TrimSpace(r.Header.Get(SessionHeader))
	if sessionID != "" {
		if !h.touchSession(sessionID) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown or expired session"})
			return
		}
	}

	batch, incoming, parseErr := Decode(body)
	if parseErr != nil {
		if sessionID != "" {
			w.Header().Set(SessionHeader, sessionID)
		}
		writeJSON(w, http.StatusOK, ErrorResponse(nil, parseErr))
		return
	}
	if sessionID == "" {
		var newErr error
		sessionID, newErr = h.newSession()
		if newErr != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "cannot create session"})
			return
		}
	}
	w.Header().Set(SessionHeader, sessionID)

	if batch {
		h.handleBatch(w, r, incoming)
		return
	}
	h.handleSingle(w, r, incoming[0], sessionID)
}

func (h *Handler) handleBatch(w http.ResponseWriter, r *http.Request, incoming []Incoming) {
	responses := make([]Response, 0, len(incoming))
	for _, inc := range incoming {
		req := inc.Request
		if inc.Err != nil {
			responses = append(responses, ErrorResponse(req.ID, inc.Err))
			continue
		}
		if req.Method == "events/stream" || req.Method == "events/subscribe" {
			responses = append(responses, ErrorResponse(req.ID,
				NewError(CodeInvalidRequest, req.Method+" cannot be sent in a batch", nil)))
			continue
		}
		if req.IsNotification() {
			// Notifications (including the legacy notifications/initialized) have
			// no response body; their handler result is deliberately dropped.
			_, _ = h.server.Handle(r.Context(), req, &jsonResponder{})
			continue
		}
		responder := &jsonResponder{}
		result, rpcErr := h.server.Handle(r.Context(), req, responder)
		if rpcErr != nil {
			responses = append(responses, ErrorResponse(req.ID, rpcErr))
			continue
		}
		responses = append(responses, SuccessResponse(req.ID, result))
	}
	if len(responses) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	writeJSON(w, http.StatusOK, responses)
}

func (h *Handler) handleSingle(w http.ResponseWriter, r *http.Request, inc Incoming, sessionID string) {
	req := inc.Request
	if inc.Err != nil {
		writeJSON(w, http.StatusOK, ErrorResponse(req.ID, inc.Err))
		return
	}
	if req.IsNotification() {
		_, _ = h.server.Handle(r.Context(), req, &jsonResponder{})
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// events/subscribe also keeps the POST open: it writes its result and then
	// pushes notifications on the same SSE stream.
	isStream := req.Method == "events/stream" || req.Method == "events/subscribe"
	accept := strings.ToLower(r.Header.Get("Accept"))
	acceptsSSE := accept == "" || strings.Contains(accept, "text/event-stream")
	acceptsJSON := accept == "" || strings.Contains(accept, "application/json") || strings.Contains(accept, "*/*")
	useSSE := (isStream && acceptsSSE) || (acceptsSSE && !acceptsJSON)

	var responder *jsonResponder
	if useSSE {
		responder = &jsonResponder{sse: newSSEWriter(w)}
	} else {
		responder = &jsonResponder{}
	}

	result, rpcErr := h.server.Handle(r.Context(), req, responder)

	if isStream {
		if rpcErr != nil {
			if responder.started() {
				_ = responder.sendResponse(ErrorResponse(req.ID, rpcErr))
				return
			}
			writeJSON(w, http.StatusOK, ErrorResponse(req.ID, rpcErr))
			return
		}
		if !responder.started() {
			_ = responder.Start()
		}
		return
	}

	if useSSE {
		if rpcErr != nil {
			_ = responder.sendResponse(ErrorResponse(req.ID, rpcErr))
			return
		}
		_ = responder.sendResponse(SuccessResponse(req.ID, result))
		return
	}

	if rpcErr != nil {
		writeJSON(w, http.StatusOK, ErrorResponse(req.ID, rpcErr))
		return
	}
	writeJSON(w, http.StatusOK, SuccessResponse(req.ID, result))
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.Header.Get(SessionHeader))
	if sessionID == "" || !h.touchSession(sessionID) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown or expired session"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	_, _ = io.WriteString(w, ": connected\n\n")
	_ = rc.Flush()

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
		}
	}
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.Header.Get(SessionHeader))
	if sessionID == "" || !h.touchSession(sessionID) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown or expired session"})
		return
	}
	h.mu.Lock()
	delete(h.sessions, sessionID)
	h.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) newSession() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(buf[:])
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pruneLocked()
	h.sessions[id] = &session{id: id, lastSeen: h.now()}
	return id, nil
}

func (h *Handler) touchSession(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pruneLocked()
	s, ok := h.sessions[id]
	if !ok {
		return false
	}
	s.lastSeen = h.now()
	return true
}

func (h *Handler) pruneLocked() {
	cutoff := h.now().Add(-h.sessionTTL)
	for id, s := range h.sessions {
		if s.lastSeen.Before(cutoff) {
			delete(h.sessions, id)
		}
	}
}

// jsonResponder is both a JSON response holder and the callback target for
// push notifications. When sse is non-nil notifications are data frames.
type jsonResponder struct {
	sse *sseWriter

	mu      sync.Mutex
	didSend bool
}

func (j *jsonResponder) SSE() bool { return j.sse != nil }

func (j *jsonResponder) Start() error {
	if j.sse == nil {
		return nil
	}
	return j.sse.Start()
}

func (j *jsonResponder) Respond(resp Response) error {
	return j.sendResponse(resp)
}

func (j *jsonResponder) Send(n Notification) error {
	if j.sse == nil {
		return errors.New("notifications require an SSE response stream")
	}
	j.mu.Lock()
	j.didSend = true
	j.mu.Unlock()
	return j.sse.Send(n)
}

func (j *jsonResponder) sendResponse(resp Response) error {
	if j.sse == nil {
		return errors.New("SSE response stream is not available")
	}
	j.mu.Lock()
	j.didSend = true
	j.mu.Unlock()
	return j.sse.SendRaw(resp)
}

func (j *jsonResponder) started() bool {
	if j.sse == nil {
		return false
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.didSend || j.sse.Started()
}

// sseWriter writes JSON-RPC messages as SSE data frames and flushes after every
// frame. It is safe to use from the single provider goroutine plus the handler.
type sseWriter struct {
	w  http.ResponseWriter
	rc *http.ResponseController

	mu      sync.Mutex
	started bool
}

func newSSEWriter(w http.ResponseWriter) *sseWriter {
	return &sseWriter{w: w, rc: http.NewResponseController(w)}
}

func (s *sseWriter) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	s.w.WriteHeader(http.StatusOK)
	s.started = true
	return s.rc.Flush()
}

func (s *sseWriter) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

func (s *sseWriter) Send(n Notification) error {
	return s.SendRaw(n)
}

// SendRaw marshals v and writes it as one SSE data frame, then flushes.
func (s *sseWriter) SendRaw(v any) error {
	frame, err := encodeSSE(v)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		h := s.w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no")
		s.w.WriteHeader(http.StatusOK)
		s.started = true
	}
	if _, err := s.w.Write(frame); err != nil {
		return err
	}
	return s.rc.Flush()
}

// encodeSSE encodes a JSON-RPC message as a single data frame. JSON encoding
// never introduces the newlines that would require multiple data: lines.
func encodeSSE(v any) ([]byte, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteString("data: ")
	buf.Write(payload)
	buf.WriteString("\n\n")
	return buf.Bytes(), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
