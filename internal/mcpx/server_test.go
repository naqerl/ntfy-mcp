package mcpx

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type fakeTools struct {
	defs   []Tool
	result any
	err    error

	calls    int
	lastName string
	lastArgs json.RawMessage
}

func (f *fakeTools) ListTools() []Tool { return f.defs }

func (f *fakeTools) CallTool(_ context.Context, name string, args json.RawMessage) (any, error) {
	f.calls++
	f.lastName = name
	f.lastArgs = args
	return f.result, f.err
}

type fakeEvents struct {
	defs       []EventDescriptor
	pollResult any
	pollErr    error
	streamErr  error
	stream     func(ctx context.Context, raw json.RawMessage, emit func(Notification) error) error

	pollCalls   int
	streamCalls int
}

func (f *fakeEvents) ListEvents() []EventDescriptor { return f.defs }

func (f *fakeEvents) PollEvents(_ context.Context, _ json.RawMessage) (any, error) {
	f.pollCalls++
	return f.pollResult, f.pollErr
}

func (f *fakeEvents) StreamEvents(ctx context.Context, raw json.RawMessage, emit func(Notification) error) error {
	f.streamCalls++
	if f.stream != nil {
		return f.stream(ctx, raw, emit)
	}
	return f.streamErr
}

type fakeResponder struct {
	sse     bool
	started bool
	sent    []Notification
}

func (f *fakeResponder) SSE() bool    { return f.sse }
func (f *fakeResponder) Start() error { f.started = true; return nil }
func (f *fakeResponder) Send(n Notification) error {
	f.started = true
	f.sent = append(f.sent, n)
	return nil
}

func defaultTools() *fakeTools {
	return &fakeTools{
		defs: []Tool{
			{Name: "ntfy_publish", Description: "publish", InputSchema: map[string]any{"type": "object"}},
			{Name: "ntfy_read", Description: "read", InputSchema: map[string]any{"type": "object"}},
		},
		result: map[string]any{"id": "1"},
	}
}

func TestDiscoverAdvertisesEventsExtension(t *testing.T) {
	srv := NewServer("v9", defaultTools(), &fakeEvents{})
	result, rpcErr := srv.Handle(context.Background(), Request{Method: "server/discover"}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("Handle: %v", rpcErr)
	}
	m := result.(map[string]any)
	if m["resultType"] != "complete" {
		t.Fatalf("resultType = %v", m["resultType"])
	}
	if m["protocolVersion"] != ProtocolRevision {
		t.Fatalf("protocolVersion = %v", m["protocolVersion"])
	}
	caps := m["capabilities"].(map[string]any)
	exts := caps["extensions"].(map[string]any)
	events, ok := exts[EventsExtension].(map[string]any)
	if !ok {
		t.Fatalf("extensions = %+v", exts)
	}
	if events["listChanged"] != false {
		t.Fatalf("events = %+v", events)
	}
	tools := caps["tools"].(map[string]any)
	if tools["listChanged"] != false {
		t.Fatalf("tools = %+v", tools)
	}
	info := m["serverInfo"].(map[string]any)
	if info["version"] != "v9" {
		t.Fatalf("serverInfo = %+v", info)
	}
}

func TestToolsListAndCall(t *testing.T) {
	tools := defaultTools()
	srv := NewServer("test", tools, &fakeEvents{})
	resp, rpcErr := srv.Handle(context.Background(), Request{Method: "tools/list"}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("tools/list: %v", rpcErr)
	}
	list := resp.(map[string]any)
	if list["resultType"] != "complete" || len(list["tools"].([]Tool)) != 2 {
		t.Fatalf("tools/list = %+v", list)
	}

	resp, rpcErr = srv.Handle(context.Background(), Request{
		Method: "tools/call",
		Params: json.RawMessage(`{"name":"ntfy_publish","arguments":{"topic":"t","message":"m"}}`),
	}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("tools/call: %v", rpcErr)
	}
	call := resp.(map[string]any)
	if call["resultType"] != "complete" || call["isError"] != false {
		t.Fatalf("tools/call = %+v", call)
	}
	structured := call["structuredContent"].(map[string]any)
	if structured["id"] != "1" {
		t.Fatalf("structuredContent = %+v", structured)
	}
	content := call["content"].([]map[string]any)
	if content[0]["type"] != "text" {
		t.Fatalf("content = %+v", content)
	}
	if tools.lastName != "ntfy_publish" || string(tools.lastArgs) != `{"topic":"t","message":"m"}` {
		t.Fatalf("tool call args = %q %s", tools.lastName, tools.lastArgs)
	}
}

func TestToolsCallDefaultsEmptyArguments(t *testing.T) {
	tools := defaultTools()
	srv := NewServer("test", tools, nil)
	_, rpcErr := srv.Handle(context.Background(), Request{
		Method: "tools/call",
		Params: json.RawMessage(`{"name":"ntfy_read"}`),
	}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("tools/call: %v", rpcErr)
	}
	if string(tools.lastArgs) != `{}` {
		t.Fatalf("args = %s", tools.lastArgs)
	}
}

func TestToolsCallValidation(t *testing.T) {
	srv := NewServer("test", defaultTools(), nil)
	tests := []string{
		`{}`,
		`{"name":""}`,
		`{"name":1}`,
	}
	for _, params := range tests {
		_, rpcErr := srv.Handle(context.Background(), Request{Method: "tools/call", Params: json.RawMessage(params)}, &fakeResponder{})
		if rpcErr == nil || rpcErr.Code != CodeInvalidParams {
			t.Fatalf("%s: error = %v", params, rpcErr)
		}
	}
}

func TestToolAndEventErrorsPassThrough(t *testing.T) {
	tools := defaultTools()
	tools.err = NewError(CodeNotFound, "no such topic", map[string]any{"kind": "topic"})
	srv := NewServer("test", tools, nil)
	_, rpcErr := srv.Handle(context.Background(), Request{
		Method: "tools/call",
		Params: json.RawMessage(`{"name":"ntfy_read","arguments":{}}`),
	}, &fakeResponder{})
	if rpcErr == nil || rpcErr.Code != CodeNotFound {
		t.Fatalf("error = %v", rpcErr)
	}

	events := &fakeEvents{pollErr: NewError(CodeResourceExhausted, "slow down", nil)}
	srv = NewServer("test", nil, events)
	_, rpcErr = srv.Handle(context.Background(), Request{Method: "events/poll", Params: json.RawMessage(`{}`)}, &fakeResponder{})
	if rpcErr == nil || rpcErr.Code != CodeResourceExhausted {
		t.Fatalf("error = %v", rpcErr)
	}
}

func TestEventsListAndPoll(t *testing.T) {
	events := &fakeEvents{
		defs: []EventDescriptor{{
			Name:     "ntfy.message",
			Delivery: []string{"push", "poll"},
		}},
		pollResult: map[string]any{"events": []any{}, "cursor": "t1"},
	}
	srv := NewServer("test", nil, events)
	resp, rpcErr := srv.Handle(context.Background(), Request{Method: "events/list"}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("events/list: %v", rpcErr)
	}
	list := resp.(map[string]any)
	if list["resultType"] != "complete" {
		t.Fatalf("list = %+v", list)
	}
	defs := list["events"].([]EventDescriptor)
	if len(defs) != 1 || defs[0].Name != "ntfy.message" || len(defs[0].Delivery) != 2 {
		t.Fatalf("events = %+v", defs)
	}

	resp, rpcErr = srv.Handle(context.Background(), Request{Method: "events/poll", Params: json.RawMessage(`{}`)}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("events/poll: %v", rpcErr)
	}
	poll := resp.(map[string]any)
	if poll["resultType"] != "complete" || poll["cursor"] != "t1" {
		t.Fatalf("poll = %+v", poll)
	}
	if events.pollCalls != 1 {
		t.Fatalf("poll calls = %d", events.pollCalls)
	}
}

func TestEventsStreamPushRequiresSSE(t *testing.T) {
	events := &fakeEvents{stream: func(_ context.Context, _ json.RawMessage, emit func(Notification) error) error {
		return emit(NewNotification("notifications/events/active", map[string]any{"cursor": "t1"}))
	}}
	srv := NewServer("test", nil, events)

	_, rpcErr := srv.Handle(context.Background(), Request{Method: "events/stream", Params: json.RawMessage(`{}`)}, &fakeResponder{sse: false})
	if rpcErr == nil || rpcErr.Code != CodeUnsupported {
		t.Fatalf("error = %v", rpcErr)
	}
	if events.streamCalls != 0 {
		t.Fatal("provider was called without an SSE response")
	}

	resp := &fakeResponder{sse: true}
	_, rpcErr = srv.Handle(context.Background(), Request{Method: "events/stream", Params: json.RawMessage(`{}`)}, resp)
	if rpcErr != nil {
		t.Fatalf("stream: %v", rpcErr)
	}
	if len(resp.sent) != 1 || resp.sent[0].Method != "notifications/events/active" {
		t.Fatalf("sent = %+v", resp.sent)
	}
}

func TestEventsStreamErrorBeforeEmit(t *testing.T) {
	events := &fakeEvents{streamErr: NewError(CodeNotFound, "unknown event", map[string]any{"kind": "event"})}
	srv := NewServer("test", nil, events)
	_, rpcErr := srv.Handle(context.Background(), Request{Method: "events/stream", Params: json.RawMessage(`{}`)}, &fakeResponder{sse: true})
	if rpcErr == nil || rpcErr.Code != CodeNotFound {
		t.Fatalf("error = %v", rpcErr)
	}
}

func TestAbsentMethods(t *testing.T) {
	srv := NewServer("test", nil, nil)
	for _, method := range []string{
		"initialise", "initialize", "events/subscribe", "events/unsubscribe", "subscriptions/listen", "nope",
	} {
		_, rpcErr := srv.Handle(context.Background(), Request{Method: method}, &fakeResponder{})
		if rpcErr == nil || rpcErr.Code != CodeMethodNotFound {
			t.Errorf("%s: error = %v", method, rpcErr)
		}
	}
}

func TestPing(t *testing.T) {
	srv := NewServer("test", nil, nil)
	resp, rpcErr := srv.Handle(context.Background(), Request{Method: "ping"}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("ping: %v", rpcErr)
	}
	if resp.(map[string]any)["resultType"] != "complete" {
		t.Fatalf("ping = %+v", resp)
	}
}

func TestProviderErrorTypeAssertion(t *testing.T) {
	events := &fakeEvents{pollErr: errors.New("plain failure")}
	srv := NewServer("test", nil, events)
	_, rpcErr := srv.Handle(context.Background(), Request{Method: "events/poll", Params: json.RawMessage(`{}`)}, &fakeResponder{})
	if rpcErr == nil || rpcErr.Code != CodeInternalError {
		t.Fatalf("error = %v", rpcErr)
	}
}

func TestCapabilityNegotiation(t *testing.T) {
	events := &fakeEvents{
		defs:       []EventDescriptor{{Name: "ntfy.message", Delivery: []string{"push", "poll"}}},
		pollResult: map[string]any{"events": []any{}},
	}
	srv := NewServer("test", nil, events)

	// Explicitly advertising the extension succeeds.
	_, rpcErr := srv.Handle(context.Background(), Request{
		Method: "events/poll",
		Params: json.RawMessage(`{"event":"ntfy.message","filter":{"topic":"t"},"_meta":{"io.modelcontextprotocol/clientCapabilities":{"extensions":{"io.modelcontextprotocol/events":{}}}}}`),
	}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("advertised extension: %v", rpcErr)
	}

	// Every explicit capability set without the extension is unsupported.
	for _, caps := range []string{
		`{}`,
		`{"extensions":null}`,
		`{"extensions":{}}`,
		`{"extensions":{"other":{}}}`,
	} {
		_, rpcErr = srv.Handle(context.Background(), Request{
			Method: "events/poll",
			Params: json.RawMessage(`{"_meta":{"io.modelcontextprotocol/clientCapabilities":` + caps + `}}`),
		}, &fakeResponder{})
		if rpcErr == nil || rpcErr.Code != CodeUnsupported {
			t.Fatalf("capabilities %s: error = %v", caps, rpcErr)
		}
	}

	// Explicit null is equivalent to leaving capabilities out.
	_, rpcErr = srv.Handle(context.Background(), Request{
		Method: "events/poll",
		Params: json.RawMessage(`{"event":"ntfy.message","filter":{"topic":"t"},"_meta":{"io.modelcontextprotocol/clientCapabilities":null}}`),
	}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("null capabilities: %v", rpcErr)
	}

	// The 2026 revision is the floor.
	_, rpcErr = srv.Handle(context.Background(), Request{
		Method: "ping",
		Params: json.RawMessage(`{"_meta":{"protocolVersion":"2025-11-25"}}`),
	}, &fakeResponder{})
	if rpcErr == nil || rpcErr.Code != CodeUnsupported {
		t.Fatalf("old revision: %v", rpcErr)
	}

	// Malformed capabilities are invalid params.
	_, rpcErr = srv.Handle(context.Background(), Request{
		Method: "events/list",
		Params: json.RawMessage(`{"_meta":{"io.modelcontextprotocol/clientCapabilities":"bad"}}`),
	}, &fakeResponder{})
	if rpcErr == nil || rpcErr.Code != CodeInvalidParams {
		t.Fatalf("bad caps: %v", rpcErr)
	}
}
