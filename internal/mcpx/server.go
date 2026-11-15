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

// EventProvider supplies the SEP-3415 events extension.
type EventProvider interface {
	ListEvents() []EventDescriptor
	PollEvents(ctx context.Context, params json.RawMessage) (any, error)
	StreamEvents(ctx context.Context, params json.RawMessage, emit func(Notification) error) error
}

// Responder is the transport side of a single JSON-RPC request. StreamEvents
// uses Send to push notifications on the originating request stream.
type Responder interface {
	SSE() bool
	Start() error
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

// Handle dispatches one request. For events/stream it blocks until the stream
// ends or ctx is cancelled and returns (nil, nil); all notifications have been
// written through r.
func (s *Server) Handle(ctx context.Context, req Request, r Responder) (any, *Error) {
	if err := s.negotiate(req); err != nil {
		return nil, err
	}
	switch req.Method {
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
		if err := s.Events.StreamEvents(ctx, req.Params, r.Send); err != nil {
			return nil, asRPCError(err)
		}
		return nil, nil

	case "events/subscribe", "events/unsubscribe", "subscriptions/listen":
		// SEP-3415 allows servers that only implement push+poll to omit the
		// subscription control methods. listChanged is false, so these stay
		// method-not-found rather than being silently accepted.
		return nil, methodNotFound(req.Method)
	}

	return nil, methodNotFound(req.Method)
}

// negotiate applies the per-request capability envelope described by the
// 2026 protocol. Clients that omit _meta are still served (the smoke script and
// curl examples rely on that), but an explicitly older revision or an explicit
// capability set without the events extension is rejected.
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
	if v := params.Meta.ProtocolVersion; v != "" && v < ProtocolRevision {
		return NewError(CodeUnsupported,
			"protocol revision is older than "+ProtocolRevision,
			map[string]any{"feature": "protocolVersion", "value": v})
	}
	if len(params.Meta.ClientCapabilities) == 0 || !isExtensionMethod(req.Method) {
		return nil
	}
	var caps struct {
		Extensions map[string]json.RawMessage `json:"extensions"`
	}
	if err := json.Unmarshal(params.Meta.ClientCapabilities, &caps); err != nil {
		return NewError(CodeInvalidParams,
			"invalid io.modelcontextprotocol/clientCapabilities", nil)
	}
	if caps.Extensions != nil {
		if _, ok := caps.Extensions[EventsExtension]; !ok {
			return NewError(CodeUnsupported,
				"client did not advertise the events extension",
				map[string]any{"feature": EventsExtension, "value": req.Method})
		}
	}
	return nil
}

func isExtensionMethod(method string) bool {
	return method == "events/list" || method == "events/poll" || method == "events/stream"
}

func (s *Server) discoverResult() map[string]any {
	version := s.Version
	if version == "" {
		version = "dev"
	}
	return map[string]any{
		"protocolVersion": ProtocolRevision,
		"serverInfo": map[string]any{
			"name":    "ntfy-mcp",
			"version": version,
		},
		"capabilities": map[string]any{
			"tools": map[string]any{
				"listChanged": false,
			},
			"extensions": map[string]any{
				EventsExtension: map[string]any{
					"listChanged": false,
				},
			},
		},
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
