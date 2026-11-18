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

	subscribe         func(ctx context.Context, raw json.RawMessage, respond func(any) error, emit func(Notification) error) error
	subscribeErr      error
	unsubscribe       func(ctx context.Context, raw json.RawMessage) (any, error)
	unsubscribeResult any
	unsubscribeErr    error

	pollCalls        int
	streamCalls      int
	subscribeCalls   int
	unsubscribeCalls int

	streamSubscriptionID    string
	subscribeSubscriptionID string
}

func (f *fakeEvents) ListEvents() []EventDescriptor { return f.defs }

func (f *fakeEvents) PollEvents(_ context.Context, _ json.RawMessage) (any, error) {
	f.pollCalls++
	return f.pollResult, f.pollErr
}

func (f *fakeEvents) StreamEvents(ctx context.Context, raw json.RawMessage, subscriptionID string, emit func(Notification) error) error {
	f.streamCalls++
	f.streamSubscriptionID = subscriptionID
	if f.stream != nil {
		return f.stream(ctx, raw, emit)
	}
	return f.streamErr
}

func (f *fakeEvents) SubscribeEvents(ctx context.Context, raw json.RawMessage, subscriptionID string, respond func(any) error, emit func(Notification) error) error {
	f.subscribeCalls++
	f.subscribeSubscriptionID = subscriptionID
	if f.subscribe != nil {
		return f.subscribe(ctx, raw, respond, emit)
	}
	return f.subscribeErr
}

func (f *fakeEvents) UnsubscribeEvents(ctx context.Context, raw json.RawMessage) (any, error) {
	f.unsubscribeCalls++
	if f.unsubscribe != nil {
		return f.unsubscribe(ctx, raw)
	}
	return f.unsubscribeResult, f.unsubscribeErr
}

type fakeResponder struct {
	sse       bool
	started   bool
	sent      []Notification
	responses []Response
}

func (f *fakeResponder) SSE() bool    { return f.sse }
func (f *fakeResponder) Start() error { f.started = true; return nil }
func (f *fakeResponder) Respond(resp Response) error {
	f.started = true
	f.responses = append(f.responses, resp)
	return nil
}
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

func TestInitializeNegotiatesLegacyRevisions(t *testing.T) {
	srv := NewServer("v9", defaultTools(), &fakeEvents{})
	tests := []struct {
		name    string
		params  string
		version string
	}{
		{"latest legacy", `{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"pi","version":"1"}}`, "2025-11-25"},
		{"legacy", `{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"pi","version":"1"}}`, "2025-06-18"},
		{"older legacy", `{"protocolVersion":"2025-03-26"}`, "2025-03-26"},
		{"oldest legacy", `{"protocolVersion":"2024-11-05"}`, "2024-11-05"},
		{"unknown revision", `{"protocolVersion":"2024-01-01"}`, ProtocolRevision},
		{"future revision", `{"protocolVersion":"2027-01-01"}`, ProtocolRevision},
		{"absent params", ``, ProtocolRevision},
		{"absent version", `{"capabilities":{}}`, ProtocolRevision},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, rpcErr := srv.Handle(context.Background(), Request{
				Method: "initialize",
				Params: json.RawMessage(tt.params),
			}, &fakeResponder{})
			if rpcErr != nil {
				t.Fatalf("Handle: %v", rpcErr)
			}
			m := resp.(map[string]any)
			if m["resultType"] != "complete" {
				t.Fatalf("resultType = %v", m["resultType"])
			}
			if m["protocolVersion"] != tt.version {
				t.Fatalf("protocolVersion = %v, want %s", m["protocolVersion"], tt.version)
			}
			info, ok := m["serverInfo"].(map[string]any)
			if !ok || info["name"] != "ntfy-mcp" || info["version"] != "v9" {
				t.Fatalf("serverInfo = %+v", m["serverInfo"])
			}
			caps, ok := m["capabilities"].(map[string]any)
			if !ok {
				t.Fatalf("capabilities = %+v", m["capabilities"])
			}
			tools, ok := caps["tools"].(map[string]any)
			if !ok || tools["listChanged"] != false {
				t.Fatalf("tools capability = %+v", caps["tools"])
			}
		})
	}
}

func TestInitializeOmitsVersionDefaultsToDev(t *testing.T) {
	srv := NewServer("", defaultTools(), nil)
	resp, rpcErr := srv.Handle(context.Background(), Request{Method: "initialize"}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("Handle: %v", rpcErr)
	}
	info := resp.(map[string]any)["serverInfo"].(map[string]any)
	if info["version"] != "dev" {
		t.Fatalf("serverInfo = %+v", info)
	}
}

func TestInitializeRejectsMalformedParams(t *testing.T) {
	srv := NewServer("test", defaultTools(), nil)
	_, rpcErr := srv.Handle(context.Background(), Request{
		Method: "initialize",
		Params: json.RawMessage(`{"protocolVersion":1}`),
	}, &fakeResponder{})
	if rpcErr == nil || rpcErr.Code != CodeInvalidParams {
		t.Fatalf("error = %v", rpcErr)
	}
}

func TestInitializeThenLegacyToolCalls(t *testing.T) {
	tools := defaultTools()
	srv := NewServer("test", tools, &fakeEvents{})
	_, rpcErr := srv.Handle(context.Background(), Request{
		Method: "initialize",
		Params: json.RawMessage(`{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"pi","version":"1"}}`),
	}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("initialize: %v", rpcErr)
	}

	// A legacy client sends no _meta; tools must list and call unchanged.
	resp, rpcErr := srv.Handle(context.Background(), Request{Method: "tools/list"}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("tools/list: %v", rpcErr)
	}
	listed := resp.(map[string]any)["tools"].([]Tool)
	names := make(map[string]bool, len(listed))
	for _, tool := range listed {
		names[tool.Name] = true
	}
	if len(listed) != 2 || !names["ntfy_publish"] || !names["ntfy_read"] {
		t.Fatalf("tools = %+v", listed)
	}

	resp, rpcErr = srv.Handle(context.Background(), Request{
		Method: "tools/call",
		Params: json.RawMessage(`{"name":"ntfy_read","arguments":{"topic":"t"}}`),
	}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("tools/call: %v", rpcErr)
	}
	call := resp.(map[string]any)
	if call["resultType"] != "complete" || call["isError"] != false {
		t.Fatalf("tools/call = %+v", call)
	}
	if tools.lastName != "ntfy_read" {
		t.Fatalf("tool call = %q", tools.lastName)
	}
}

func TestNotificationsInitializedIsSilent(t *testing.T) {
	srv := NewServer("test", defaultTools(), nil)
	resp, rpcErr := srv.Handle(context.Background(), Request{
		Method: "notifications/initialized",
	}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("error = %v", rpcErr)
	}
	if resp != nil {
		t.Fatalf("result = %+v, want nil", resp)
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
	_, rpcErr = srv.Handle(context.Background(), Request{ID: json.RawMessage(`42`), Method: "events/stream", Params: json.RawMessage(`{}`)}, resp)
	if rpcErr != nil {
		t.Fatalf("stream: %v", rpcErr)
	}
	if len(resp.sent) != 1 || resp.sent[0].Method != "notifications/events/active" {
		t.Fatalf("sent = %+v", resp.sent)
	}
	if events.streamSubscriptionID != "42" {
		t.Fatalf("default subscription id = %q, want the request id", events.streamSubscriptionID)
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

func TestEventsSubscribeRepliesThenStreams(t *testing.T) {
	events := &fakeEvents{subscribe: func(_ context.Context, _ json.RawMessage, respond func(any) error, emit func(Notification) error) error {
		if err := respond(map[string]any{"subscriptionId": "sub-1"}); err != nil {
			return err
		}
		if err := emit(NewNotification("notifications/events/active", map[string]any{"subscriptionId": "sub-1"})); err != nil {
			return err
		}
		return emit(NewNotification("notifications/events/event", map[string]any{"eventId": "m1"}))
	}}
	srv := NewServer("test", nil, events)
	resp := &fakeResponder{sse: true}
	result, rpcErr := srv.Handle(context.Background(), Request{
		ID:     json.RawMessage(`7`),
		Method: "events/subscribe",
		Params: json.RawMessage(`{"name":"ntfy.message","arguments":{"topic":"t"}}`),
	}, resp)
	if rpcErr != nil {
		t.Fatalf("subscribe: %v", rpcErr)
	}
	if result != nil {
		t.Fatalf("result = %+v, want nil after responding", result)
	}
	if len(resp.responses) != 1 {
		t.Fatalf("responses = %+v", resp.responses)
	}
	if string(resp.responses[0].ID) != "7" {
		t.Fatalf("response id = %s", resp.responses[0].ID)
	}
	m, ok := resp.responses[0].Result.(map[string]any)
	if !ok || m["subscriptionId"] != "sub-1" || m["resultType"] != "complete" {
		t.Fatalf("response result = %+v", resp.responses[0].Result)
	}
	if len(resp.sent) != 2 || resp.sent[0].Method != "notifications/events/active" || resp.sent[1].Method != "notifications/events/event" {
		t.Fatalf("sent = %+v", resp.sent)
	}
	if events.subscribeSubscriptionID != "7" {
		t.Fatalf("default subscription id = %q, want the request id", events.subscribeSubscriptionID)
	}
}

func TestEventsSubscribeRequiresSSE(t *testing.T) {
	events := &fakeEvents{}
	srv := NewServer("test", nil, events)
	_, rpcErr := srv.Handle(context.Background(), Request{Method: "events/subscribe", Params: json.RawMessage(`{}`)}, &fakeResponder{sse: false})
	if rpcErr == nil || rpcErr.Code != CodeUnsupported {
		t.Fatalf("error = %v", rpcErr)
	}
	if events.subscribeCalls != 0 {
		t.Fatal("provider was called without an SSE response")
	}
}

func TestEventsUnsubscribe(t *testing.T) {
	events := &fakeEvents{unsubscribeResult: map[string]any{
		"unsubscribed":    true,
		"subscriptionIds": []string{"sub-1"},
	}}
	srv := NewServer("test", nil, events)
	resp, rpcErr := srv.Handle(context.Background(), Request{
		Method: "events/unsubscribe",
		Params: json.RawMessage(`{"subscriptionId":"sub-1"}`),
	}, &fakeResponder{})
	if rpcErr != nil {
		t.Fatalf("unsubscribe: %v", rpcErr)
	}
	m := resp.(map[string]any)
	if m["unsubscribed"] != true || m["resultType"] != "complete" {
		t.Fatalf("result = %+v", m)
	}

	events.unsubscribeErr = NewError(CodeNotFound, "unknown subscription", map[string]any{"kind": "subscription"})
	_, rpcErr = srv.Handle(context.Background(), Request{Method: "events/unsubscribe", Params: json.RawMessage(`{"subscriptionId":"nope"}`)}, &fakeResponder{})
	if rpcErr == nil || rpcErr.Code != CodeNotFound {
		t.Fatalf("error = %v", rpcErr)
	}
}

func TestAbsentMethods(t *testing.T) {
	srv := NewServer("test", nil, nil)
	for _, method := range []string{
		"initialise", "events/subscribe", "events/unsubscribe", "subscriptions/listen", "nope",
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

	// A revision newer than the server knows is unsupported.
	_, rpcErr = srv.Handle(context.Background(), Request{
		Method: "ping",
		Params: json.RawMessage(`{"_meta":{"protocolVersion":"2027-01-01"}}`),
	}, &fakeResponder{})
	if rpcErr == nil || rpcErr.Code != CodeUnsupported {
		t.Fatalf("future revision: %v", rpcErr)
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

func TestNegotiateProtocolRevision(t *testing.T) {
	tests := []struct {
		name     string
		params   string
		wantCode int
	}{
		{"absent", `{}`, 0},
		{"legacy 2025-11-25", `{"_meta":{"protocolVersion":"2025-11-25"}}`, 0},
		{"legacy 2024-11-05", `{"_meta":{"protocolVersion":"2024-11-05"}}`, 0},
		{"native 2026-07-28", `{"_meta":{"protocolVersion":"2026-07-28"}}`, 0},
		{"future 2027-01-01", `{"_meta":{"protocolVersion":"2027-01-01"}}`, CodeUnsupported},
		{
			"legacy skips the native capability gate",
			`{"_meta":{"protocolVersion":"2025-11-25","io.modelcontextprotocol/clientCapabilities":{}}}`,
			0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := &fakeEvents{stream: func(_ context.Context, _ json.RawMessage, emit func(Notification) error) error {
				return emit(NewNotification("notifications/events/active", map[string]any{"cursor": "t1"}))
			}}
			srv := NewServer("test", nil, events)
			resp := &fakeResponder{sse: true}
			_, rpcErr := srv.Handle(context.Background(), Request{
				ID:     json.RawMessage(`1`),
				Method: "events/stream",
				Params: json.RawMessage(tt.params),
			}, resp)

			if tt.wantCode != 0 {
				if rpcErr == nil || rpcErr.Code != tt.wantCode {
					t.Fatalf("error = %v, want %d", rpcErr, tt.wantCode)
				}
				if events.streamCalls != 0 {
					t.Fatal("provider was called for a rejected revision")
				}
				return
			}
			if rpcErr != nil {
				t.Fatalf("stream: %v", rpcErr)
			}
			if events.streamCalls != 1 || len(resp.sent) != 1 || resp.sent[0].Method != "notifications/events/active" {
				t.Fatalf("calls=%d sent=%+v", events.streamCalls, resp.sent)
			}
			if events.streamSubscriptionID != "1" {
				t.Fatalf("default subscription id = %q", events.streamSubscriptionID)
			}
		})
	}
}
