// Package mcpx implements the JSON-RPC 2.0 and Streamable-HTTP core used by
// the ntfy MCP server. It deliberately owns the transport instead of relying
// on an SDK: SEP-3415 requires the server to write extension notifications on
// the originating HTTP request stream, which the pinned Go SDK does not expose.
package mcpx

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// JSON-RPC and MCP error codes used by this server.
const (
	CodeParseError        = -32700
	CodeInvalidRequest    = -32600
	CodeMethodNotFound    = -32601
	CodeInvalidParams     = -32602
	CodeInternalError     = -32603
	CodeNotFound          = -32023
	CodeResourceExhausted = -32025
	CodeUnsupported       = -32026
)

// ProtocolRevision is the SEP-3415 revision this server targets. Only the
// 2026-07-28 protocol shape (per-request capabilities, server/discover, and
// capability extensions) is implemented.
const ProtocolRevision = "2026-07-28"

// EventsExtension is the SEP-3415 extension identifier.
const EventsExtension = "io.modelcontextprotocol/events"

// Error is a JSON-RPC error object. Data is optional and is serialised as the
// "data" member when non-nil.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("json-rpc error %d: %s", e.Code, e.Message)
}

// NewError builds a JSON-RPC error.
func NewError(code int, message string, data any) *Error {
	return &Error{Code: code, Message: message, Data: data}
}

// Request is a JSON-RPC request or notification. A nil or absent ID means the
// message is a notification.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// HasID reports whether the request carries an ID (including an explicit null).
func (r Request) HasID() bool {
	return len(bytes.TrimSpace(r.ID)) > 0
}

// IsNotification reports whether the request has no ID.
func (r Request) IsNotification() bool {
	return !r.HasID()
}

// Response is a JSON-RPC response.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Notification is a server-initiated JSON-RPC notification. It is written as
// an SSE data frame on the originating Streamable-HTTP request stream.
type Notification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

// NewNotification constructs a JSON-RPC notification.
func NewNotification(method string, params any) Notification {
	return Notification{JSONRPC: "2.0", Method: method, Params: params}
}

// Incoming is one decoded request (or the error produced while decoding it).
type Incoming struct {
	Request Request
	Err     *Error
}

// Decode decodes a single JSON-RPC message or a batch. batch is true when the
// body was a JSON array. A body-level parse error is returned as err; malformed
// members of a batch are represented by Incoming.Err so each one can receive a
// JSON-RPC error response.
func Decode(body []byte) (batch bool, msgs []Incoming, err *Error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false, nil, NewError(CodeParseError, "parse error: empty body", nil)
	}
	if trimmed[0] != '[' && trimmed[0] != '{' {
		return false, []Incoming{{Err: NewError(CodeInvalidRequest, "invalid request: expected a JSON object", nil)}}, nil
	}
	if trimmed[0] != '[' {
		inc, rpcErr := decodeOne(trimmed)
		if rpcErr != nil {
			if rpcErr.Code == CodeParseError {
				return false, nil, rpcErr
			}
			inc.Err = rpcErr
		}
		return false, []Incoming{inc}, nil
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(trimmed, &raw); err != nil {
		return true, nil, NewError(CodeParseError, "parse error: "+err.Error(), nil)
	}
	if len(raw) == 0 {
		return true, nil, NewError(CodeInvalidRequest, "invalid request: empty batch", nil)
	}
	msgs = make([]Incoming, 0, len(raw))
	for _, item := range raw {
		inc, rpcErr := decodeOne(item)
		if rpcErr != nil && rpcErr.Code == CodeParseError {
			// A malformed element is an invalid request, not a transport-level
			// parse error; keep the batch answerable item by item.
			msgs = append(msgs, Incoming{Err: NewError(CodeInvalidRequest, "invalid request: "+rpcErr.Message, nil)})
			continue
		}
		msgs = append(msgs, inc)
	}
	return true, msgs, nil
}

func decodeOne(raw []byte) (Incoming, *Error) {
	var req Request
	if err := json.Unmarshal(raw, &req); err != nil {
		return Incoming{}, NewError(CodeParseError, "parse error: "+err.Error(), nil)
	}
	if req.JSONRPC != "2.0" {
		return Incoming{Request: req}, NewError(CodeInvalidRequest, "invalid request: jsonrpc must be \"2.0\"", nil)
	}
	if req.Method == "" {
		return Incoming{Request: req}, NewError(CodeInvalidRequest, "invalid request: method is required", nil)
	}
	if req.HasID() && !validID(req.ID) {
		return Incoming{Request: req}, NewError(CodeInvalidRequest, "invalid request: id must be a string, number, or null", nil)
	}
	if len(req.Params) > 0 {
		trimmed := bytes.TrimSpace(req.Params)
		if len(trimmed) == 0 || (trimmed[0] != '{' && trimmed[0] != '[') {
			return Incoming{Request: req}, NewError(CodeInvalidRequest, "invalid request: params must be an object or array", nil)
		}
	}
	return Incoming{Request: req}, nil
}

func validID(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return false
	}
	var v any
	if err := json.Unmarshal(trimmed, &v); err != nil {
		return false
	}
	switch v.(type) {
	case nil, string, float64:
		return true
	default:
		return false
	}
}

// ErrorResponse builds an error response echoing id (or null when id is absent).
func ErrorResponse(id json.RawMessage, err *Error) Response {
	if len(bytes.TrimSpace(id)) == 0 {
		id = json.RawMessage("null")
	}
	return Response{JSONRPC: "2.0", ID: id, Error: err}
}

// SuccessResponse builds a success response echoing id.
func SuccessResponse(id json.RawMessage, result any) Response {
	if len(bytes.TrimSpace(id)) == 0 {
		id = json.RawMessage("null")
	}
	return Response{JSONRPC: "2.0", ID: id, Result: result}
}
