package mcpx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

// Tool is a tool advertised by tools/list.
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// ToolProvider supplies the ntfy tools.
type ToolProvider interface {
	ListTools() []Tool
	CallTool(ctx context.Context, name string, args json.RawMessage) (any, error)
}

// EventDescriptor describes one event type advertised by events/list.
type EventDescriptor struct {
	Name          string         `json:"name"`
	Delivery      []string       `json:"delivery"`
	InputSchema   map[string]any `json:"inputSchema"`
	PayloadSchema map[string]any `json:"payloadSchema"`
}

// EventProvider supplies the SEP-3415 events extension. subscriptionID is the
// caller's default subscription id (the JSON-RPC request id for a stream) and
// is used when the request params do not carry one of their own.
type EventProvider interface {
	ListEvents() []EventDescriptor
	PollEvents(ctx context.Context, params json.RawMessage) (any, error)
	StreamEvents(ctx context.Context, params json.RawMessage, subscriptionID string, emit func(Notification) error) error
	// SubscribeEvents registers a push subscription, calls respond with the
	// subscription result before the first event frame, and blocks until the
	// subscription ends. Events are pushed through emit.
	SubscribeEvents(ctx context.Context, params json.RawMessage, subscriptionID string, respond func(result any) error, emit func(Notification) error) error
	// UnsubscribeEvents cancels a subscription by id or by event selector.
	UnsubscribeEvents(ctx context.Context, params json.RawMessage) (any, error)
}

// Responder is the transport side of a single JSON-RPC request. Streaming
// methods use Respond for the immediate result and Send to push notifications
// on the originating request stream.
type Responder interface {
	SSE() bool
	Start() error
	Respond(Response) error
	Send(Notification) error
}

// Server dispatches MCP methods. It owns no transport state; Handler does.
type Server struct {
	Version string
	Tools   ToolProvider
	Events  EventProvider
}

// NewServer constructs a dispatcher.
func NewServer(version string, tools ToolProvider, events EventProvider) *Server {
	return &Server{Version: version, Tools: tools, Events: events}
}

// legacyRevisions are the initialize-handshake revisions served as a
// compatibility path on top of the native 2026-07-28 protocol. A client that
// asks for one of these revisions in the top-level params.protocolVersion gets
// its own version echoed back. Anything else, including versions newer than
// ProtocolRevision, is answered with ProtocolRevision.
var legacyRevisions = map[string]bool{
	"2025-11-25": true,
	"2025-06-18": true,
	"2025-03-26": true,
	"2024-11-05": true,
}

// Handle dispatches one request. For events/stream it blocks until the stream
// ends or ctx is cancelled and returns (nil, nil); all notifications have been
// written through r.
func (s *Server) Handle(ctx context.Context, req Request, r Responder) (any, *Error) {
	if err := s.negotiate(req); err != nil {
		return nil, err
	}
	switch req.Method {
	case "initialize":
		return s.initializeResult(req.Params)

	case "notifications/initialized":
		// The legacy handshake's follow-up notification carries no request and
		// no result. The transport never writes a body for notifications.
		return nil, nil

	case "server/discover":
		return complete(s.discoverResult()), nil

	case "ping":
		return complete(map[string]any{}), nil

	case "tools/list":
		if s.Tools == nil {
			return nil, methodNotFound(req.Method)
		}
		// Start from an empty slice so an empty provider encodes as [] rather
		// than null.
		tools := append([]Tool{}, s.Tools.ListTools()...)
		return complete(map[string]any{"tools": tools}), nil

	case "tools/call":
		if s.Tools == nil {
			return nil, methodNotFound(req.Method)
		}
		return s.callTool(ctx, req.Params)

	case "events/list":
		if s.Events == nil {
			return nil, methodNotFound(req.Method)
		}
		events := s.Events.ListEvents()
		if events == nil {
			events = []EventDescriptor{}
		}
		return complete(map[string]any{"events": events}), nil

	case "events/poll":
		if s.Events == nil {
			return nil, methodNotFound(req.Method)
		}
		result, err := s.Events.PollEvents(ctx, req.Params)
		if err != nil {
			return nil, asRPCError(err)
		}
		return completeMap(result), nil

	case "events/stream":
		if s.Events == nil {
			return nil, methodNotFound(req.Method)
		}
		if !r.SSE() {
			return nil, NewError(CodeUnsupported,
				"events/stream requires text/event-stream", map[string]any{
					"feature": "events/stream",
					"value":   "Accept",
				})
		}
		if err := s.Events.StreamEvents(ctx, req.Params, requestSubscriptionID(req), r.Send); err != nil {
			return nil, asRPCError(err)
		}
		return nil, nil

	case "events/subscribe":
		if s.Events == nil {
			return nil, methodNotFound(req.Method)
		}
		if !r.SSE() {
			return nil, NewError(CodeUnsupported,
				"events/subscribe requires text/event-stream", map[string]any{
					"feature": "events/subscribe",
					"value":   "Accept",
				})
		}
		respond := func(result any) error {
			return r.Respond(SuccessResponse(req.ID, completeMap(result)))
		}
		if err := s.Events.SubscribeEvents(ctx, req.Params, requestSubscriptionID(req), respond, r.Send); err != nil {
			return nil, asRPCError(err)
		}
		return nil, nil

	case "events/unsubscribe":
		if s.Events == nil {
			return nil, methodNotFound(req.Method)
		}
		result, err := s.Events.UnsubscribeEvents(ctx, req.Params)
		if err != nil {
			return nil, asRPCError(err)
		}
		return completeMap(result), nil

	case "subscriptions/listen":
		// SEP-3415 listChanged is false: a listening client is served through
		// events/subscribe instead of the subscription-list method.
		return nil, methodNotFound(req.Method)
	}

	return nil, methodNotFound(req.Method)
}

// negotiate applies the per-request capability envelope described by the
// 2026 protocol. Clients that omit _meta are still served (the smoke script and
// curl examples rely on that), but an explicitly older revision or an explicit
// capability set without the events extension is rejected. The legacy
// initialize handshake is exempt from the revision floor: its version is
// negotiated in initializeResult, not here.
func (s *Server) negotiate(req Request) *Error {
	trimmed := bytes.TrimSpace(req.Params)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil
	}
	var params struct {
		Meta struct {
			ProtocolVersion    string          `json:"protocolVersion"`
			ClientCapabilities json.RawMessage `json:"io.modelcontextprotocol/clientCapabilities"`
		} `json:"_meta"`
	}
	if err := json.Unmarshal(trimmed, &params); err != nil {
		return nil
	}
	// The 2026 methods keep the revision floor. initialize is the compatibility
	// path: a legacy client puts its version at the top level of params and
	// sends no _meta, and initializeResult negotiates that version down, so an
	// explicit older _meta value must not reject the handshake itself.
	if v := params.Meta.ProtocolVersion; v != "" && v < ProtocolRevision && req.Method != "initialize" {
		return NewError(CodeUnsupported,
			"protocol revision is older than "+ProtocolRevision,
			map[string]any{"feature": "protocolVersion", "value": v})
	}
	capabilities := bytes.TrimSpace(params.Meta.ClientCapabilities)
	if len(capabilities) == 0 || !isExtensionMethod(req.Method) {
		return nil
	}
	// An explicit null is equivalent to omitting the field. Any other explicit
	// capability object must advertise the events extension.
	if bytes.Equal(capabilities, []byte("null")) {
		return nil
	}
	var caps struct {
		Extensions json.RawMessage `json:"extensions"`
	}
	if err := json.Unmarshal(capabilities, &caps); err != nil {
		return NewError(CodeInvalidParams,
			"invalid io.modelcontextprotocol/clientCapabilities", nil)
	}
	extensions := bytes.TrimSpace(caps.Extensions)
	if len(extensions) == 0 || bytes.Equal(extensions, []byte("null")) {
		return NewError(CodeUnsupported,
			"client did not advertise the events extension",
			map[string]any{"feature": EventsExtension, "value": req.Method})
	}
	var advertised map[string]json.RawMessage
	if err := json.Unmarshal(extensions, &advertised); err != nil {
		return NewError(CodeInvalidParams,
			"invalid io.modelcontextprotocol/clientCapabilities.extensions", nil)
	}
	if _, ok := advertised[EventsExtension]; !ok {
		return NewError(CodeUnsupported,
			"client did not advertise the events extension",
			map[string]any{"feature": EventsExtension, "value": req.Method})
	}
	return nil
}

func isExtensionMethod(method string) bool {
	switch method {
	case "events/list", "events/poll", "events/stream", "events/subscribe", "events/unsubscribe":
		return true
	}
	return false
}

// initializeResult answers the legacy 2025-11-25 initialize handshake. The
// negotiated version is the client's own when it is one this server serves as a
// compatibility path, otherwise ProtocolRevision. The capabilities and
// serverInfo members are exactly the ones server/discover advertises.
func (s *Server) initializeResult(raw json.RawMessage) (any, *Error) {
	var params struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) > 0 {
		if err := json.Unmarshal(trimmed, &params); err != nil {
			return nil, NewError(CodeInvalidParams, "invalid params: "+err.Error(), nil)
		}
	}
	version := ProtocolRevision
	if legacyRevisions[params.ProtocolVersion] {
		version = params.ProtocolVersion
	}
	return complete(map[string]any{
		"protocolVersion": version,
		"capabilities":    s.capabilities(),
		"serverInfo":      s.serverInfo(),
	}), nil
}

func (s *Server) serverInfo() map[string]any {
	version := s.Version
	if version == "" {
		version = "dev"
	}
	return map[string]any{
		"name":    "ntfy-mcp",
		"version": version,
	}
}

func (s *Server) capabilities() map[string]any {
	return map[string]any{
		"tools": map[string]any{
			"listChanged": false,
		},
		"extensions": map[string]any{
			EventsExtension: map[string]any{
				"listChanged": false,
			},
		},
	}
}

func (s *Server) discoverResult() map[string]any {
	return map[string]any{
		"protocolVersion": ProtocolRevision,
		"serverInfo":      s.serverInfo(),
		"capabilities":    s.capabilities(),
	}
}

func (s *Server) callTool(ctx context.Context, raw json.RawMessage) (any, *Error) {
	var params struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := decodeParams(raw, &params); err != nil {
		return nil, err
	}
	if params.Name == "" {
		return nil, NewError(CodeInvalidParams, "tools/call: name is required", nil)
	}
	args := params.Arguments
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	result, err := s.Tools.CallTool(ctx, params.Name, args)
	if err != nil {
		return nil, asRPCError(err)
	}
	text, merr := json.Marshal(result)
	if merr != nil {
		return nil, NewError(CodeInternalError, "tools/call: cannot encode result", nil)
	}
	return complete(map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": string(text)},
		},
		"structuredContent": result,
		"isError":           false,
	}), nil
}

func decodeParams(raw json.RawMessage, dst any) *Error {
	if len(raw) == 0 {
		return NewError(CodeInvalidParams, "params are required", nil)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return NewError(CodeInvalidParams, "invalid params: "+err.Error(), nil)
	}
	return nil
}

func methodNotFound(method string) *Error {
	return NewError(CodeMethodNotFound, fmt.Sprintf("method %q not found", method), nil)
}

// requestSubscriptionID is the default subscription id for a streaming
// request. SEP-3415 demultiplexes notifications on
// _meta["io.modelcontextprotocol/subscriptionId"], whose value is the stream
// request's JSON-RPC id verbatim. Using that id when the client does not send a
// subscriptionId keeps the stream addressable by the client that opened it.
func requestSubscriptionID(req Request) string {
	trimmed := bytes.TrimSpace(req.ID)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	var s string
	if err := json.Unmarshal(trimmed, &s); err == nil {
		return s
	}
	return string(trimmed)
}

func complete(result map[string]any) map[string]any {
	result["resultType"] = "complete"
	return result
}

func completeMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return complete(m)
	}
	// Providers return maps, but keep a defensive path for other map-like
	// values without hiding an encoding failure.
	raw, err := json.Marshal(v)
	if err != nil {
		return complete(map[string]any{"result": v})
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return complete(map[string]any{"result": v})
	}
	return complete(m)
}

func asRPCError(err error) *Error {
	if rpcErr, ok := err.(*Error); ok {
		return rpcErr
	}
	if rpcErr, ok := err.(interface{ RPCError() *Error }); ok {
		return rpcErr.RPCError()
	}
	return NewError(CodeInternalError, err.Error(), nil)
}
