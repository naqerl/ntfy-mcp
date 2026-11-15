package tools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/naqerl/ntfy-mcp/internal/mcpx"
	"github.com/naqerl/ntfy-mcp/internal/ntfy"
)

type fakeClient struct {
	publishCalls int
	pollCalls    int

	publishReq ntfy.PublishRequest
	pollTopic  string
	pollSince  string
	pollLimit  int
	publishErr error
	pollErr    error
	publishRes ntfy.PublishResult
	pollRes    ntfy.PollResult
}

func (f *fakeClient) Publish(_ context.Context, in ntfy.PublishRequest) (ntfy.PublishResult, error) {
	f.publishCalls++
	f.publishReq = in
	return f.publishRes, f.publishErr
}

func (f *fakeClient) Poll(_ context.Context, topic, since string, limit int) (ntfy.PollResult, error) {
	f.pollCalls++
	f.pollTopic, f.pollSince, f.pollLimit = topic, since, limit
	return f.pollRes, f.pollErr
}

func TestListToolsExactlyTwo(t *testing.T) {
	tools := New(&fakeClient{}, "")
	defs := tools.ListTools()
	if len(defs) != 2 {
		t.Fatalf("tools = %d, want 2", len(defs))
	}
	names := map[string]bool{}
	for _, def := range defs {
		names[def.Name] = true
	}
	if !names["ntfy_publish"] || !names["ntfy_read"] {
		t.Fatalf("names = %+v", names)
	}
	publishRequired, _ := defs[0].InputSchema["required"].([]string)
	if len(publishRequired) != 2 || publishRequired[0] != "topic" || publishRequired[1] != "message" {
		t.Fatalf("ntfy_publish required = %+v", publishRequired)
	}
}

func TestPublishRequestMapping(t *testing.T) {
	fake := &fakeClient{publishRes: ntfy.PublishResult{ID: "id1", Topic: "alerts", Time: 123}}
	markdown := true
	args := json.RawMessage(`{
		"topic":"alerts","message":"hello","title":"T","priority":4,
		"tags":["a","b"],"click":"https://example.com","markdown":true,"delay":"30m"
	}`)
	out, err := New(fake, "").CallTool(context.Background(), "ntfy_publish", args)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	result, ok := out.(PublishResult)
	if !ok {
		t.Fatalf("result type %T", out)
	}
	if result.ID != "id1" || result.Topic != "alerts" || result.Time != 123 {
		t.Fatalf("result = %+v", result)
	}
	want := ntfy.PublishRequest{
		Topic: "alerts", Message: "hello", Title: "T", Priority: 4,
		Tags: []string{"a", "b"}, Click: "https://example.com", Markdown: &markdown, Delay: "30m",
	}
	if fake.publishReq.Topic != want.Topic || fake.publishReq.Message != want.Message ||
		fake.publishReq.Title != want.Title || fake.publishReq.Priority != want.Priority ||
		len(fake.publishReq.Tags) != 2 || fake.publishReq.Tags[0] != "a" ||
		fake.publishReq.Click != want.Click || fake.publishReq.Delay != want.Delay ||
		fake.publishReq.Markdown == nil || *fake.publishReq.Markdown != true {
		t.Fatalf("request = %+v", fake.publishReq)
	}
}

func TestPublishValidation(t *testing.T) {
	tests := []struct {
		name string
		args string
		want string
	}{
		{"missing message", `{"topic":"t"}`, "message"},
		{"missing topic", `{"message":"m"}`, "topic"},
		{"priority too high", `{"topic":"t","message":"m","priority":6}`, "priority"},
		{"priority too low", `{"topic":"t","message":"m","priority":-1}`, "priority"},
		{"priority zero", `{"topic":"t","message":"m","priority":0}`, "priority"},
		{"reserved topic", `{"topic":"docs","message":"m"}`, "reserved"},
		{"invalid topic chars", `{"topic":"a/b","message":"m"}`, "invalid ntfy topic"},
		{"bad tag", `{"topic":"t","message":"m","tags":["a,b"]}`, "tags"},
		{"empty tag", `{"topic":"t","message":"m","tags":[""]}`, "tags"},
		{"bad click", `{"topic":"t","message":"m","click":"not-a-uri"}`, "click"},
		{"unknown field", `{"topic":"t","message":"m","bogus":true}`, "unknown field"},
		{"trailing json", `{"topic":"t","message":"m"} {}`, "multiple"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeClient{}
			_, err := New(fake, "").CallTool(context.Background(), "ntfy_publish", json.RawMessage(tc.args))
			var rpcErr *mcpx.Error
			if !errors.As(err, &rpcErr) {
				t.Fatalf("error = %v, want *mcpx.Error", err)
			}
			if rpcErr.Code != mcpx.CodeInvalidParams {
				t.Fatalf("code = %d, want %d", rpcErr.Code, mcpx.CodeInvalidParams)
			}
			if !strings.Contains(rpcErr.Message, tc.want) {
				t.Fatalf("message = %q, want contains %q", rpcErr.Message, tc.want)
			}
			if fake.publishCalls != 0 {
				t.Fatal("upstream was called for invalid input")
			}
		})
	}
}

func TestReadDefaultsAndCursor(t *testing.T) {
	fake := &fakeClient{pollRes: ntfy.PollResult{Messages: []ntfy.Message{
		{ID: "a", Time: 1, Topic: "t", Message: "one"},
		{ID: "b", Time: 2, Topic: "t", Message: "two"},
	}}}
	out, err := New(fake, "").CallTool(context.Background(), "ntfy_read", json.RawMessage(`{"topic":"t"}`))
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	result := out.(ReadResult)
	if result.Cursor != "b" || len(result.Messages) != 2 || result.Topic != "t" {
		t.Fatalf("result = %+v", result)
	}
	if fake.pollSince != "10m" || fake.pollLimit != 50 {
		t.Fatalf("poll since/limit = %q/%d", fake.pollSince, fake.pollLimit)
	}
}

func TestReadExplicitSinceLimit(t *testing.T) {
	fake := &fakeClient{pollRes: ntfy.PollResult{}}
	_, err := New(fake, "").CallTool(context.Background(), "ntfy_read",
		json.RawMessage(`{"topic":"t","since":"all","limit":200}`))
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if fake.pollSince != "all" || fake.pollLimit != 200 {
		t.Fatalf("poll since/limit = %q/%d", fake.pollSince, fake.pollLimit)
	}
}

func TestReadEmptyMessagesEncodesArray(t *testing.T) {
	fake := &fakeClient{pollRes: ntfy.PollResult{}}
	out, err := New(fake, "").CallTool(context.Background(), "ntfy_read", json.RawMessage(`{"topic":"t"}`))
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	raw, _ := json.Marshal(out)
	if !strings.Contains(string(raw), `"messages":[]`) {
		t.Fatalf("encoded result = %s", raw)
	}
	if !strings.Contains(string(raw), `"cursor":""`) {
		t.Fatalf("encoded result = %s", raw)
	}
}

func TestReadValidation(t *testing.T) {
	tests := []struct {
		name string
		args string
	}{
		{"limit too high", `{"topic":"t","limit":201}`},
		{"limit negative", `{"topic":"t","limit":-1}`},
		{"reserved topic", `{"topic":"v1"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeClient{}
			_, err := New(fake, "").CallTool(context.Background(), "ntfy_read", json.RawMessage(tc.args))
			var rpcErr *mcpx.Error
			if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeInvalidParams {
				t.Fatalf("error = %v", err)
			}
			if fake.pollCalls != 0 {
				t.Fatal("upstream was called for invalid input")
			}
		})
	}
}

func TestDefaultTopicAndUnknownTool(t *testing.T) {
	fake := &fakeClient{pollRes: ntfy.PollResult{}}
	if _, err := New(fake, "alerts").CallTool(context.Background(), "ntfy_read", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("default topic: %v", err)
	}
	if fake.pollTopic != "alerts" {
		t.Fatalf("topic = %q", fake.pollTopic)
	}
	_, err := New(fake, "").CallTool(context.Background(), "nope", json.RawMessage(`{}`))
	var rpcErr *mcpx.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeInvalidParams {
		t.Fatalf("unknown tool error = %v", err)
	}
}

func TestUpstreamErrorMapping(t *testing.T) {
	fake := &fakeClient{publishErr: &ntfy.APIError{Status: 429, Code: 42905, Message: "slow down"}}
	_, err := New(fake, "").CallTool(context.Background(), "ntfy_publish", json.RawMessage(`{"topic":"t","message":"m"}`))
	var rpcErr *mcpx.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeResourceExhausted {
		t.Fatalf("error = %v", err)
	}

	fake = &fakeClient{pollErr: &ntfy.APIError{Status: 404, Message: "missing"}}
	_, err = New(fake, "").CallTool(context.Background(), "ntfy_read", json.RawMessage(`{"topic":"t"}`))
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeNotFound {
		t.Fatalf("error = %v", err)
	}
}

func TestEmptyArguments(t *testing.T) {
	fake := &fakeClient{}
	_, err := New(fake, "").CallTool(context.Background(), "ntfy_read", nil)
	var rpcErr *mcpx.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeInvalidParams {
		t.Fatalf("error = %v", err)
	}
}
