package events

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/naqerl/ntfy-mcp/internal/config"
	"github.com/naqerl/ntfy-mcp/internal/mcpx"
	"github.com/naqerl/ntfy-mcp/internal/ntfy"
)

type fakeClient struct {
	pollFn func(ctx context.Context, topic, since string, limit int) (ntfy.PollResult, error)
	openFn func(ctx context.Context, topic, since string) (*ntfy.Stream, error)

	pollCalls int
	openCalls int
	pollSince string
	pollLimit int
	openSince string
}

func (f *fakeClient) Poll(ctx context.Context, topic, since string, limit int) (ntfy.PollResult, error) {
	f.pollCalls++
	f.pollSince, f.pollLimit = since, limit
	if f.pollFn == nil {
		panic("unexpected Poll")
	}
	return f.pollFn(ctx, topic, since, limit)
}

func (f *fakeClient) OpenStream(ctx context.Context, topic, since string) (*ntfy.Stream, error) {
	f.openCalls++
	f.openSince = since
	if f.openFn == nil {
		panic("unexpected OpenStream")
	}
	return f.openFn(ctx, topic, since)
}

func realService(t *testing.T, handler http.HandlerFunc, opts Options) (*Service, func()) {
	t.Helper()
	srv := httptest.NewServer(handler)
	base, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := ntfy.New(base, "", srv.Client())
	return New(client, opts), srv.Close
}

func encodeMessages(messages ...ntfy.Message) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		enc := json.NewEncoder(w)
		for _, m := range messages {
			if err := enc.Encode(m); err != nil {
				return
			}
		}
	}
}

func TestCursorMintParse(t *testing.T) {
	at := time.Unix(1700000000, 0)
	got := Mint(at)
	if got != "t1700000000" {
		t.Fatalf("Mint = %q", got)
	}
	cursor, err := ParseCursor(got)
	if err != nil || !cursor.IsTime || cursor.Time != 1700000000 {
		t.Fatalf("ParseCursor = %+v, %v", cursor, err)
	}
	cursor, err = ParseCursor("abc_123-XYZ")
	if err != nil || cursor.IsTime || cursor.ID != "abc_123-XYZ" {
		t.Fatalf("ParseCursor id = %+v, %v", cursor, err)
	}
	for _, bad := range []string{"", "bad id", "x:y"} {
		if _, err := ParseCursor(bad); err == nil {
			t.Errorf("ParseCursor(%q) succeeded", bad)
		}
	}
}

func TestCursorSinceRaisesReplayFloor(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cursor := Cursor{IsTime: true, Time: 1699999000}
	since, gap := cursor.Since(now, 10*time.Second)
	if since != "1699999990" || !gap {
		t.Fatalf("since/gap = %q/%v", since, gap)
	}
	since, gap = cursor.Since(now, 0)
	if since != "1699999000" || gap {
		t.Fatalf("since/gap = %q/%v", since, gap)
	}
	id := Cursor{ID: "m1"}
	since, gap = id.Since(now, time.Hour)
	if since != "m1" || gap {
		t.Fatalf("id since/gap = %q/%v", since, gap)
	}
}

func TestEventsList(t *testing.T) {
	svc := New(nil, Options{})
	defs := svc.ListEvents()
	if len(defs) != 1 {
		t.Fatalf("events = %d", len(defs))
	}
	def := defs[0]
	if def.Name != EventName {
		t.Fatalf("name = %q", def.Name)
	}
	if len(def.Delivery) != 2 || def.Delivery[0] != "push" || def.Delivery[1] != "poll" {
		t.Fatalf("delivery = %+v", def.Delivery)
	}
	required, _ := def.InputSchema["required"].([]string)
	if len(required) != 1 || required[0] != "topic" {
		t.Fatalf("input required = %+v", required)
	}
	if _, ok := def.PayloadSchema["properties"].(map[string]any)["time"]; !ok {
		t.Fatalf("payload schema = %+v", def.PayloadSchema)
	}
}

func TestPollFromNowNullCursorDoesNotCallUpstream(t *testing.T) {
	for _, cursor := range []string{`null`, ``} {
		t.Run(cursor, func(t *testing.T) {
			fake := &fakeClient{}
			svc := New(fake, Options{PollMS: 1234, MaxEvents: 5})
			svc.now = func() time.Time { return time.Unix(1700000000, 0) }
			params := `{"event":"ntfy.message","filter":{"topic":"alerts"},"subscriptionId":"sub-1"`
			if cursor != "" {
				params += `,"cursor":` + cursor
			}
			params += `}`
			out, err := svc.PollEvents(context.Background(), json.RawMessage(params))
			if err != nil {
				t.Fatalf("PollEvents: %v", err)
			}
			if fake.pollCalls != 0 {
				t.Fatal("poll with from-now cursor called upstream")
			}
			result := out.(map[string]any)
			if result["cursor"] != "t1700000000" {
				t.Fatalf("cursor = %v", result["cursor"])
			}
			if len(result["events"].([]any)) != 0 {
				t.Fatalf("events = %+v", result["events"])
			}
			if result["nextPollMs"] != 1234 || result["hasMore"] != false || result["truncated"] != false {
				t.Fatalf("result = %+v", result)
			}
			meta := result["_meta"].(map[string]any)
			if meta["subscriptionId"] != "sub-1" {
				t.Fatalf("_meta = %+v", meta)
			}
		})
	}
}

func TestPollHasMoreFromOverFullBatch(t *testing.T) {
	msgs := []ntfy.Message{
		{ID: "m1", Time: 1001, Event: "message", Topic: "alerts", Message: "1"},
		{ID: "m2", Time: 1002, Event: "message", Topic: "alerts", Message: "2"},
		{ID: "m3", Time: 1003, Event: "message", Topic: "alerts", Message: "3"},
	}
	svc, closeSrv := realService(t, encodeMessages(msgs...), Options{PollMS: 1500, MaxEvents: 2})
	defer closeSrv()
	svc.now = func() time.Time { return time.Unix(2000, 0) }
	out, err := svc.PollEvents(context.Background(), json.RawMessage(
		`{"event":"ntfy.message","cursor":"t1000","maxEvents":2,"filter":{"topic":"alerts"}}`))
	if err != nil {
		t.Fatalf("PollEvents: %v", err)
	}
	result := out.(map[string]any)
	if result["hasMore"] != true {
		t.Fatalf("hasMore = %v", result["hasMore"])
	}
	if result["cursor"] != "m2" {
		t.Fatalf("cursor = %v", result["cursor"])
	}
	if len(result["events"].([]any)) != 2 {
		t.Fatalf("events = %+v", result["events"])
	}
	if result["nextPollMs"] != 1500 {
		t.Fatalf("nextPollMs = %v", result["nextPollMs"])
	}
}

func TestPollTruncatedHeaderAndStaleCursor(t *testing.T) {
	t.Run("header", func(t *testing.T) {
		handler := func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Messages-Truncated", "1")
			encodeMessages(ntfy.Message{ID: "m1", Time: 1001, Event: "message", Topic: "t", Message: "x"})(w, r)
		}
		svc, closeSrv := realService(t, handler, Options{})
		defer closeSrv()
		out, err := svc.PollEvents(context.Background(), json.RawMessage(`{"event":"ntfy.message","cursor":"t1000","filter":{"topic":"t"}}`))
		if err != nil {
			t.Fatalf("PollEvents: %v", err)
		}
		if out.(map[string]any)["truncated"] != true {
			t.Fatalf("result = %+v", out)
		}
	})

	t.Run("time cursor older message", func(t *testing.T) {
		svc, closeSrv := realService(t, encodeMessages(ntfy.Message{ID: "m1", Time: 999, Event: "message", Topic: "t", Message: "x"}), Options{})
		defer closeSrv()
		out, err := svc.PollEvents(context.Background(), json.RawMessage(`{"event":"ntfy.message","cursor":"t1000","filter":{"topic":"t"}}`))
		if err != nil {
			t.Fatalf("PollEvents: %v", err)
		}
		if out.(map[string]any)["truncated"] != true {
			t.Fatalf("result = %+v", out)
		}
	})

	t.Run("id cursor previously seen", func(t *testing.T) {
		fake := &fakeClient{}
		svc := New(fake, Options{})
		svc.recordSeen(ntfy.Message{ID: "m0", Time: 1000})
		fake.pollFn = func(context.Context, string, string, int) (ntfy.PollResult, error) {
			return ntfy.PollResult{Messages: []ntfy.Message{{ID: "m1", Time: 999, Event: "message", Topic: "t", Message: "x"}}}, nil
		}
		out, err := svc.PollEvents(context.Background(), json.RawMessage(`{"event":"ntfy.message","cursor":"m0","filter":{"topic":"t"}}`))
		if err != nil {
			t.Fatalf("PollEvents: %v", err)
		}
		if out.(map[string]any)["truncated"] != true {
			t.Fatalf("result = %+v", out)
		}
	})
}

func TestPollTagAndPriorityFilter(t *testing.T) {
	msgs := []ntfy.Message{
		{ID: "m1", Time: 1001, Event: "message", Topic: "alerts", Message: "match", Tags: []string{"a", "b"}, Priority: 5},
		{ID: "m2", Time: 1002, Event: "message", Topic: "alerts", Message: "low", Tags: []string{"a"}, Priority: 2},
		{ID: "m3", Time: 1003, Event: "message", Topic: "alerts", Message: "tag", Tags: []string{"b"}, Priority: 5},
	}
	svc, closeSrv := realService(t, encodeMessages(msgs...), Options{})
	defer closeSrv()
	out, err := svc.PollEvents(context.Background(), json.RawMessage(
		`{"event":"ntfy.message","cursor":"t1000","filter":{"topic":"alerts","tags":["a"],"minPriority":4}}`))
	if err != nil {
		t.Fatalf("PollEvents: %v", err)
	}
	result := out.(map[string]any)
	events := result["events"].([]any)
	if len(events) != 1 {
		t.Fatalf("events = %+v", events)
	}
	event := events[0].(map[string]any)
	if event["eventId"] != "m1" || event["event"] != EventName {
		t.Fatalf("event = %+v", event)
	}
	payload := event["payload"].(Payload)
	if payload.Message != "match" || payload.Priority != 5 || payload.Time != "1970-01-01T00:16:41Z" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestPollInlineFilterAndValidation(t *testing.T) {
	svc := New(&fakeClient{}, Options{})
	svc.now = func() time.Time { return time.Unix(100, 0) }
	out, err := svc.PollEvents(context.Background(), json.RawMessage(`{"event":"ntfy.message","topic":"alerts","cursor":null}`))
	if err != nil {
		t.Fatalf("PollEvents: %v", err)
	}
	if out.(map[string]any)["cursor"] != "t100" {
		t.Fatalf("result = %+v", out)
	}

	tests := []struct {
		params string
		code   int
	}{
		{`{"event":"ntfy.message","cursor":"t1"}`, mcpx.CodeInvalidParams},
		{`{"event":"ntfy.message","cursor":"t1","filter":{"topic":"docs"}}`, mcpx.CodeInvalidParams},
		{`{"event":"ntfy.message","cursor":"t1","filter":{"topic":"t","minPriority":6}}`, mcpx.CodeInvalidParams},
		{`{"event":"other","filter":{"topic":"t"}}`, mcpx.CodeNotFound},
		{`{"event":"ntfy.message","cursor":"bad cursor","filter":{"topic":"t"}}`, mcpx.CodeInvalidParams},
		{`{"event":"ntfy.message","cursor":1,"filter":{"topic":"t"}}`, mcpx.CodeInvalidParams},
		{`{"event":"ntfy.message","maxEvents":201,"filter":{"topic":"t"}}`, mcpx.CodeInvalidParams},
		{`{"event":"ntfy.message","maxAgeMs":-1,"filter":{"topic":"t"}}`, mcpx.CodeInvalidParams},
	}
	for _, tc := range tests {
		_, err := svc.PollEvents(context.Background(), json.RawMessage(tc.params))
		var rpcErr *mcpx.Error
		if !errors.As(err, &rpcErr) || rpcErr.Code != tc.code {
			t.Errorf("%s: error = %v, want code %d", tc.params, err, tc.code)
		}
	}
}

func TestPollFromNowReservedTopicStillValidated(t *testing.T) {
	fake := &fakeClient{}
	svc := New(fake, Options{})
	_, err := svc.PollEvents(context.Background(), json.RawMessage(`{"event":"ntfy.message","filter":{"topic":"v1"}}`))
	var rpcErr *mcpx.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeInvalidParams {
		t.Fatalf("error = %v", err)
	}
	if fake.pollCalls != 0 {
		t.Fatal("upstream called")
	}
}

func TestPollMaxEventsDefaults(t *testing.T) {
	fake := &fakeClient{pollFn: func(_ context.Context, _ string, _ string, limit int) (ntfy.PollResult, error) {
		if limit != 51 {
			t.Errorf("limit = %d, want default 50 + 1", limit)
		}
		return ntfy.PollResult{}, nil
	}}
	svc := New(fake, Options{MaxEvents: 50})
	if _, err := svc.PollEvents(context.Background(), json.RawMessage(`{"event":"ntfy.message","cursor":"t1","filter":{"topic":"t"}}`)); err != nil {
		t.Fatalf("PollEvents: %v", err)
	}
}

// --- stream tests -----------------------------------------------------------

func startStream(t *testing.T, svc *Service, params string, defaultSID ...string) (context.CancelFunc, <-chan mcpx.Notification, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	notes := make(chan mcpx.Notification, 64)
	errs := make(chan error, 1)
	sid := ""
	if len(defaultSID) > 0 {
		sid = defaultSID[0]
	}
	go func() {
		errs <- svc.StreamEvents(ctx, json.RawMessage(params), sid, func(n mcpx.Notification) error {
			notes <- n
			return nil
		})
	}()
	return cancel, notes, errs
}

func waitNotification(t *testing.T, ch <-chan mcpx.Notification, method string) mcpx.Notification {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case n := <-ch:
			if n.Method == method {
				return n
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", method)
		}
	}
}

func TestStreamFromNowHasNoCachedHistory(t *testing.T) {
	at := time.Unix(1700000000, 0)
	var gotSince string
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		gotSince = r.URL.Query().Get("since")
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		http.NewResponseController(w).Flush()
		_, _ = w.Write([]byte(`{"id":"","time":1,"event":"open","topic":"alerts"}` + "\n"))
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}, Options{Heartbeat: time.Hour})
	defer closeSrv()
	svc.now = func() time.Time { return at }

	cancel, notes, errs := startStream(t, svc, `{"event":"ntfy.message","filter":{"topic":"alerts"}}`)
	defer cancel()
	active := waitNotification(t, notes, activeMethod)
	params := active.Params.(map[string]any)
	if params["cursor"] != "t1700000000" {
		t.Fatalf("active cursor = %v", params["cursor"])
	}
	if params["truncated"] != false {
		t.Fatalf("active = %+v", params)
	}
	if gotSince != strconv.FormatInt(at.Unix(), 10) {
		t.Fatalf("upstream since = %q", gotSince)
	}
	select {
	case n := <-notes:
		if n.Method == eventMethod {
			t.Fatalf("cached message delivered: %+v", n)
		}
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case <-errs:
	case <-time.After(time.Second):
		t.Fatal("stream did not stop")
	}
}

func TestStreamMessageAndHeartbeat(t *testing.T) {
	release := make(chan struct{})
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(w)
		_ = rc.Flush()
		_, _ = w.Write([]byte(`{"id":"","time":10,"event":"open","topic":"alerts"}` + "\n"))
		_ = rc.Flush()
		<-release
		m := ntfy.Message{ID: "m1", Time: 1700000001, Event: "message", Topic: "alerts",
			Message: "hello", Title: "Title", Tags: []string{"a"}, Priority: 4, Click: "https://example.com"}
		_ = json.NewEncoder(w).Encode(m)
		_ = rc.Flush()
		<-r.Context().Done()
	}, Options{Heartbeat: 40 * time.Millisecond, StreamMax: 3 * time.Second})
	defer closeSrv()

	cancel, notes, errs := startStream(t, svc, `{"event":"ntfy.message","filter":{"topic":"alerts"},"_meta":{"subscriptionId":"sub-test"}}`)
	defer cancel()
	active := waitNotification(t, notes, activeMethod)
	if active.Params.(map[string]any)["subscriptionId"] != "sub-test" {
		t.Fatalf("active = %+v", active)
	}
	close(release)

	msg := waitNotification(t, notes, eventMethod)
	params := msg.Params.(map[string]any)
	if params["eventId"] != "m1" || params["cursor"] != "m1" || params["subscriptionId"] != "sub-test" {
		t.Fatalf("message = %+v", params)
	}
	meta := params["_meta"].(map[string]any)
	if meta["subscriptionId"] != "sub-test" {
		t.Fatalf("_meta = %+v", meta)
	}
	payload := params["payload"].(Payload)
	if payload.Message != "hello" || payload.Title != "Title" || payload.Priority != 4 ||
		payload.Time != "2023-11-14T22:13:21Z" || payload.Click != "https://example.com" {
		t.Fatalf("payload = %+v", payload)
	}

	heartbeat := waitNotification(t, notes, heartbeatMethod)
	hparams := heartbeat.Params.(map[string]any)
	if hparams["subscriptionId"] != "sub-test" || !strings.HasPrefix(hparams["cursor"].(string), "t") {
		t.Fatalf("heartbeat = %+v", hparams)
	}
	cancel()
	select {
	case <-errs:
	case <-time.After(time.Second):
		t.Fatal("stream did not stop")
	}
}

func TestStreamTruncatedHeader(t *testing.T) {
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Messages-Truncated", "1")
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(w)
		_ = rc.Flush()
		_, _ = w.Write([]byte(`{"id":"","time":10,"event":"open","topic":"t"}` + "\n"))
		_ = rc.Flush()
		<-r.Context().Done()
	}, Options{})
	defer closeSrv()
	cancel, notes, _ := startStream(t, svc, `{"event":"ntfy.message","cursor":"t1000","filter":{"topic":"t"}}`)
	defer cancel()
	active := waitNotification(t, notes, activeMethod)
	if active.Params.(map[string]any)["truncated"] != true {
		t.Fatalf("active = %+v", active)
	}
}

func TestStreamStaleReplayEmitsFreshActive(t *testing.T) {
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("since") != "m0" {
			t.Errorf("since = %q", r.URL.Query().Get("since"))
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(w)
		_ = rc.Flush()
		_, _ = w.Write([]byte(`{"id":"","time":10,"event":"open","topic":"t"}` + "\n"))
		_, _ = w.Write([]byte(`{"id":"m1","time":999,"event":"message","topic":"t","message":"replayed"}` + "\n"))
		_ = rc.Flush()
		<-r.Context().Done()
	}, Options{})
	defer closeSrv()
	svc.recordSeen(ntfy.Message{ID: "m0", Time: 1000})

	cancel, notes, _ := startStream(t, svc, `{"event":"ntfy.message","cursor":"m0","filter":{"topic":"t"}}`)
	defer cancel()
	first := waitNotification(t, notes, activeMethod)
	if first.Params.(map[string]any)["truncated"] != false {
		t.Fatalf("first active = %+v", first)
	}
	second := waitNotification(t, notes, activeMethod)
	if second.Params.(map[string]any)["truncated"] != true {
		t.Fatalf("second active = %+v", second)
	}
	msg := waitNotification(t, notes, eventMethod)
	if msg.Params.(map[string]any)["eventId"] != "m1" {
		t.Fatalf("message = %+v", msg)
	}
}

func TestStreamTimeCursorMaxAgeFloor(t *testing.T) {
	at := time.Unix(1700000000, 0)
	var gotSince string
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		gotSince = r.URL.Query().Get("since")
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}, Options{})
	defer closeSrv()
	svc.now = func() time.Time { return at }
	cancel, notes, _ := startStream(t, svc, `{"event":"ntfy.message","cursor":"t1000","maxAgeMs":100000,"filter":{"topic":"t"}}`)
	defer cancel()
	active := waitNotification(t, notes, activeMethod)
	if active.Params.(map[string]any)["truncated"] != true {
		t.Fatalf("active = %+v", active)
	}
	want := strconv.FormatInt(at.Add(-100*time.Second).Unix(), 10)
	if gotSince != want {
		t.Fatalf("since = %q, want %q", gotSince, want)
	}
}

func TestStreamUnknownEventDoesNotOpenUpstream(t *testing.T) {
	fake := &fakeClient{}
	svc := New(fake, Options{})
	err := svc.StreamEvents(context.Background(), json.RawMessage(`{"event":"other","filter":{"topic":"t"}}`), "", func(mcpx.Notification) error { return nil })
	var rpcErr *mcpx.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeNotFound {
		t.Fatalf("error = %v", err)
	}
	if rpcErr.Data.(map[string]any)["kind"] != "event" {
		t.Fatalf("data = %+v", rpcErr.Data)
	}
	if fake.openCalls != 0 {
		t.Fatal("upstream opened for unknown event")
	}
}

func TestStreamOpenErrorMapsRateLimit(t *testing.T) {
	fake := &fakeClient{openFn: func(context.Context, string, string) (*ntfy.Stream, error) {
		return nil, &ntfy.APIError{Status: 429, Code: 42905, Message: "overrun"}
	}}
	svc := New(fake, Options{})
	err := svc.StreamEvents(context.Background(), json.RawMessage(`{"event":"ntfy.message","filter":{"topic":"t"}}`), "", func(mcpx.Notification) error { return nil })
	var rpcErr *mcpx.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeResourceExhausted {
		t.Fatalf("error = %v", err)
	}
}

func TestStreamUpstreamDecodeErrorIsReturned(t *testing.T) {
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(w)
		_ = rc.Flush()
		_, _ = w.Write([]byte(`{"id":"","time":1,"event":"open","topic":"t"}` + "\n"))
		_, _ = w.Write([]byte(`{not-json}` + "\n"))
		_ = rc.Flush()
		<-r.Context().Done()
	}, Options{})
	defer closeSrv()

	cancel, notes, errs := startStream(t, svc, `{"event":"ntfy.message","filter":{"topic":"t"}}`)
	defer cancel()
	_ = waitNotification(t, notes, activeMethod)

	select {
	case streamErr := <-errs:
		var rpcErr *mcpx.Error
		if !errors.As(streamErr, &rpcErr) || rpcErr.Code != mcpx.CodeInternalError {
			t.Fatalf("stream error = %v", streamErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not report the upstream decode error")
	}
}

func TestOptionsDefaultsAndHeartbeatCeiling(t *testing.T) {
	svc := New(nil, Options{Heartbeat: time.Hour, PollMS: 0, MaxEvents: 999, StreamMax: 0})
	if svc.heartbeat != config.DefaultHeartbeat {
		t.Fatalf("heartbeat = %s", svc.heartbeat)
	}
	if svc.pollMS != config.DefaultPollMS || svc.maxEvents != config.DefaultMaxEvents || svc.streamMax != config.DefaultStreamMax {
		t.Fatalf("defaults = %+v", svc)
	}
	svc = New(nil, Options{Heartbeat: 29 * time.Second})
	if svc.heartbeat != 29*time.Second {
		t.Fatalf("heartbeat = %s", svc.heartbeat)
	}
}

func TestFilterMatch(t *testing.T) {
	min := 4
	filter := Filter{Topic: "t", Tags: []string{"a", "b"}, MinPriority: &min}
	if !filter.Match(ntfy.Message{Topic: "t", Tags: []string{"a", "b", "c"}, Priority: 4}) {
		t.Fatal("expected match")
	}
	if filter.Match(ntfy.Message{Topic: "t", Tags: []string{"a"}, Priority: 5}) {
		t.Fatal("missing tag matched")
	}
	if filter.Match(ntfy.Message{Topic: "t", Tags: []string{"a", "b"}, Priority: 3}) {
		t.Fatal("low priority matched")
	}
	if filter.Match(ntfy.Message{Topic: "other", Tags: []string{"a", "b"}, Priority: 5}) {
		t.Fatal("other topic matched")
	}
	// A zero upstream priority is ntfy's normal priority (3), below min 4.
	if filter.Match(ntfy.Message{Topic: "t", Tags: []string{"a", "b"}, Priority: 0}) {
		t.Fatal("default priority 3 matched minPriority 4")
	}
}

func TestParseParamsAcceptsStreamEnvelope(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{
			name: "envelope with arguments",
			raw:  `{"name":"ntfy.message","arguments":{"topic":"t"},"cursor":null}`,
		},
		{
			name: "envelope with inline cursor",
			raw:  `{"name":"ntfy.message","arguments":{"topic":"t"},"cursor":"abc","maxAgeMs":1000}`,
		},
		{
			name: "flat form still works",
			raw:  `{"event":"ntfy.message","topic":"t"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, rpcErr := parseParams(json.RawMessage(tc.raw))
			if rpcErr != nil {
				t.Fatalf("parseParams(%s) error = %v", tc.raw, rpcErr)
			}
			if p.Event != EventName {
				t.Fatalf("event = %q", p.Event)
			}
			if p.Filter == nil || p.Filter.Topic != "t" {
				t.Fatalf("filter = %+v", p.Filter)
			}
		})
	}
}

func TestParseParamsRejectsEnvelopeWithoutArguments(t *testing.T) {
	_, rpcErr := parseParams(json.RawMessage(`{"name":"ntfy.message"}`))
	if rpcErr == nil {
		t.Fatal("expected an error for a missing arguments object")
	}
	if rpcErr.Code != mcpx.CodeNotFound {
		t.Fatalf("code = %d", rpcErr.Code)
	}
}

// --- subscribe tests --------------------------------------------------------

func startSubscribe(t *testing.T, svc *Service, params string, defaultSID ...string) (<-chan any, <-chan mcpx.Notification, <-chan error, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan any, 1)
	notes := make(chan mcpx.Notification, 64)
	errs := make(chan error, 1)
	sid := ""
	if len(defaultSID) > 0 {
		sid = defaultSID[0]
	}
	go func() {
		errs <- svc.SubscribeEvents(ctx, json.RawMessage(params), sid, func(result any) error {
			results <- result
			return nil
		}, func(n mcpx.Notification) error {
			notes <- n
			return nil
		})
	}()
	return results, notes, errs, cancel
}

func waitSubscribeResult(t *testing.T, ch <-chan any) map[string]any {
	t.Helper()
	select {
	case result := <-ch:
		m, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("result = %T", result)
		}
		return m
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the subscribe result")
		return nil
	}
}

func TestSubscribeRepliesWithIDAndPushesEvent(t *testing.T) {
	release := make(chan struct{})
	var gotSince string
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		gotSince = r.URL.Query().Get("since")
		w.Header().Set("Content-Type", "application/x-ndjson")
		rc := http.NewResponseController(w)
		_ = rc.Flush()
		_, _ = w.Write([]byte(`{"id":"","time":10,"event":"open","topic":"alerts"}` + "\n"))
		_ = rc.Flush()
		<-release
		m := ntfy.Message{ID: "m1", Time: 1700000001, Event: "message", Topic: "alerts", Message: "hello"}
		_ = json.NewEncoder(w).Encode(m)
		_ = rc.Flush()
		<-r.Context().Done()
	}, Options{Heartbeat: time.Hour})
	defer closeSrv()
	at := time.Unix(1700000000, 0)
	svc.now = func() time.Time { return at }

	results, notes, errs, cancel := startSubscribe(t, svc, `{"name":"ntfy.message","arguments":{"topic":"alerts"}}`)
	defer cancel()
	result := waitSubscribeResult(t, results)
	sid, _ := result["subscriptionId"].(string)
	if !strings.HasPrefix(sid, "sub_") || result["event"] != EventName {
		t.Fatalf("result = %+v", result)
	}
	active := waitNotification(t, notes, activeMethod)
	if active.Params.(map[string]any)["subscriptionId"] != sid {
		t.Fatalf("active = %+v", active.Params)
	}
	if gotSince != strconv.FormatInt(at.Unix(), 10) {
		t.Fatalf("upstream since = %q", gotSince)
	}

	close(release)
	msg := waitNotification(t, notes, eventMethod)
	params := msg.Params.(map[string]any)
	if params["eventId"] != "m1" || params["subscriptionId"] != sid {
		t.Fatalf("message = %+v", params)
	}

	cancel()
	select {
	case <-errs:
	case <-time.After(time.Second):
		t.Fatal("subscription did not stop")
	}
}

func TestSubscribeHonoursMaxAgeReplayFloor(t *testing.T) {
	at := time.Unix(1700000000, 0)
	var gotSince string
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		gotSince = r.URL.Query().Get("since")
		w.Header().Set("Content-Type", "application/x-ndjson")
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}, Options{Heartbeat: time.Hour})
	defer closeSrv()
	svc.now = func() time.Time { return at }

	results, notes, _, cancel := startSubscribe(t, svc, `{"name":"ntfy.message","arguments":{"topic":"t"},"cursor":"t1000","maxAgeMs":100000}`)
	defer cancel()
	_ = waitSubscribeResult(t, results)
	active := waitNotification(t, notes, activeMethod)
	if active.Params.(map[string]any)["truncated"] != true {
		t.Fatalf("active = %+v", active.Params)
	}
	want := strconv.FormatInt(at.Add(-100*time.Second).Unix(), 10)
	if gotSince != want {
		t.Fatalf("since = %q, want %q", gotSince, want)
	}
}

func TestSubscribeUnsubscribeStopsDelivery(t *testing.T) {
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		rc := http.NewResponseController(w)
		_ = rc.Flush()
		_, _ = w.Write([]byte(`{"id":"","time":10,"event":"open","topic":"t"}` + "\n"))
		_ = rc.Flush()
		<-r.Context().Done()
	}, Options{Heartbeat: time.Hour})
	defer closeSrv()

	results, notes, errs, cancel := startSubscribe(t, svc, `{"event":"ntfy.message","filter":{"topic":"t"},"subscriptionId":"fixed"}`)
	defer cancel()
	if result := waitSubscribeResult(t, results); result["subscriptionId"] != "fixed" {
		t.Fatalf("result = %+v", result)
	}
	_ = waitNotification(t, notes, activeMethod)

	out, err := svc.UnsubscribeEvents(context.Background(), json.RawMessage(`{"subscriptionId":"fixed"}`))
	if err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	m := out.(map[string]any)
	ids := m["subscriptionIds"].([]string)
	if m["unsubscribed"] != true || len(ids) != 1 || ids[0] != "fixed" {
		t.Fatalf("unsubscribe result = %+v", m)
	}
	select {
	case <-errs:
	case <-time.After(time.Second):
		t.Fatal("subscription goroutine did not exit")
	}

	_, err = svc.UnsubscribeEvents(context.Background(), json.RawMessage(`{"subscriptionId":"fixed"}`))
	var rpcErr *mcpx.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeNotFound {
		t.Fatalf("unknown id error = %v", err)
	}
	svc.subsMu.Lock()
	n := len(svc.subs)
	svc.subsMu.Unlock()
	if n != 0 {
		t.Fatalf("registry still holds %d subscription(s)", n)
	}
}

func TestSubscribeUnknownEventDoesNotOpenUpstream(t *testing.T) {
	fake := &fakeClient{}
	svc := New(fake, Options{})
	responded := false
	err := svc.SubscribeEvents(context.Background(), json.RawMessage(`{"name":"other","arguments":{"topic":"t"}}`), "",
		func(any) error { responded = true; return nil },
		func(mcpx.Notification) error { return nil })
	var rpcErr *mcpx.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeNotFound {
		t.Fatalf("error = %v", err)
	}
	if rpcErr.Data.(map[string]any)["kind"] != "event" {
		t.Fatalf("data = %+v", rpcErr.Data)
	}
	if fake.openCalls != 0 {
		t.Fatal("upstream opened for unknown event")
	}
	if responded {
		t.Fatal("responded before validation")
	}
}

func TestSubscribeTwoTopicsAreIsolated(t *testing.T) {
	release := make(chan struct{})
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		topic := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), "/json")
		w.Header().Set("Content-Type", "application/x-ndjson")
		rc := http.NewResponseController(w)
		_ = rc.Flush()
		_, _ = w.Write([]byte(`{"id":"","time":10,"event":"open"}` + "\n"))
		_ = rc.Flush()
		<-release
		m := ntfy.Message{ID: "m-" + topic, Time: 1700000001, Event: "message", Topic: topic, Message: "hello-" + topic}
		_ = json.NewEncoder(w).Encode(m)
		_ = rc.Flush()
		<-r.Context().Done()
	}, Options{Heartbeat: time.Hour})
	defer closeSrv()

	resultsA, notesA, _, cancelA := startSubscribe(t, svc, `{"event":"ntfy.message","filter":{"topic":"alerts"}}`)
	defer cancelA()
	resultsB, notesB, _, cancelB := startSubscribe(t, svc, `{"event":"ntfy.message","filter":{"topic":"other"}}`)
	defer cancelB()
	_ = waitSubscribeResult(t, resultsA)
	_ = waitSubscribeResult(t, resultsB)
	_ = waitNotification(t, notesA, activeMethod)
	_ = waitNotification(t, notesB, activeMethod)

	close(release)
	msgA := waitNotification(t, notesA, eventMethod)
	msgB := waitNotification(t, notesB, eventMethod)
	if msgA.Params.(map[string]any)["eventId"] != "m-alerts" {
		t.Fatalf("A = %+v", msgA.Params)
	}
	if msgB.Params.(map[string]any)["eventId"] != "m-other" {
		t.Fatalf("B = %+v", msgB.Params)
	}
	select {
	case n := <-notesA:
		if n.Method == eventMethod {
			t.Fatalf("A received another subscription's event: %+v", n)
		}
	case <-time.After(100 * time.Millisecond):
	}
}

func TestUnsubscribeBySelectorAndUnknownID(t *testing.T) {
	svc := New(nil, Options{})
	cancelled := map[string]int{}
	svc.subs["a"] = &activeSubscription{id: "a", cancel: func() { cancelled["a"]++ }, filter: Filter{Topic: "a", Tags: []string{"x"}}}
	svc.subs["b"] = &activeSubscription{id: "b", cancel: func() { cancelled["b"]++ }, filter: Filter{Topic: "b"}}

	out, err := svc.UnsubscribeEvents(context.Background(), json.RawMessage(`{"name":"ntfy.message","arguments":{"topic":"a","tags":["x"]}}`))
	if err != nil {
		t.Fatalf("selector unsubscribe: %v", err)
	}
	m := out.(map[string]any)
	ids := m["subscriptionIds"].([]string)
	if len(ids) != 1 || ids[0] != "a" || cancelled["a"] != 1 || cancelled["b"] != 0 {
		t.Fatalf("result=%+v cancelled=%+v", m, cancelled)
	}
	if _, ok := svc.subs["b"]; !ok {
		t.Fatal("selector removed the wrong subscription")
	}

	_, err = svc.UnsubscribeEvents(context.Background(), json.RawMessage(`{"subscriptionId":"missing"}`))
	var rpcErr *mcpx.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeNotFound {
		t.Fatalf("unknown id error = %v", err)
	}
	if rpcErr.Data.(map[string]any)["kind"] != "subscription" {
		t.Fatalf("data = %+v", rpcErr.Data)
	}

	_, err = svc.UnsubscribeEvents(context.Background(), json.RawMessage(`{"name":"ntfy.message","arguments":{"topic":"zzz"}}`))
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeNotFound {
		t.Fatalf("no-match selector error = %v", err)
	}
}

func TestSubscribeDuplicateIDRejected(t *testing.T) {
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}, Options{Heartbeat: time.Hour})
	defer closeSrv()

	results, _, _, cancel := startSubscribe(t, svc, `{"event":"ntfy.message","filter":{"topic":"t"},"subscriptionId":"dup"}`)
	defer cancel()
	_ = waitSubscribeResult(t, results)

	err := svc.SubscribeEvents(context.Background(), json.RawMessage(`{"event":"ntfy.message","filter":{"topic":"t"},"subscriptionId":"dup"}`), "",
		func(any) error { return nil }, func(mcpx.Notification) error { return nil })
	var rpcErr *mcpx.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeInvalidParams {
		t.Fatalf("duplicate id error = %v", err)
	}
}

func TestSubscribeConcurrencyCap(t *testing.T) {
	svc := New(nil, Options{})
	for i := 0; i < maxSubscriptions; i++ {
		id := "s" + strconv.Itoa(i)
		svc.subs[id] = &activeSubscription{id: id, cancel: func() {}, filter: Filter{Topic: "t"}}
	}
	err := svc.SubscribeEvents(context.Background(), json.RawMessage(`{"event":"ntfy.message","filter":{"topic":"t"}}`), "",
		func(any) error { return nil }, func(mcpx.Notification) error { return nil })
	var rpcErr *mcpx.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != mcpx.CodeResourceExhausted {
		t.Fatalf("cap error = %v", err)
	}
}

func TestSubscribeCancelReleasesRegistry(t *testing.T) {
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}, Options{Heartbeat: time.Hour})
	defer closeSrv()

	for i := 0; i < 5; i++ {
		results, _, errs, cancel := startSubscribe(t, svc, `{"event":"ntfy.message","filter":{"topic":"t"},"subscriptionId":"loop"}`)
		_ = waitSubscribeResult(t, results)
		if _, err := svc.UnsubscribeEvents(context.Background(), json.RawMessage(`{"subscriptionId":"loop"}`)); err != nil {
			t.Fatalf("iteration %d: unsubscribe: %v", i, err)
		}
		select {
		case <-errs:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: subscription goroutine did not exit", i)
		}
		cancel()
	}
	svc.subsMu.Lock()
	n := len(svc.subs)
	svc.subsMu.Unlock()
	if n != 0 {
		t.Fatalf("registry = %d entries", n)
	}
}

func TestStreamNotificationsUseSEP3415Shape(t *testing.T) {
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		rc := http.NewResponseController(w)
		_ = rc.Flush()
		_, _ = w.Write([]byte(`{"id":"","time":10,"event":"open","topic":"t"}` + "\n"))
		_ = rc.Flush()
		m := ntfy.Message{ID: "m1", Time: 1700000001, Event: "message", Topic: "t", Message: "hello"}
		_ = json.NewEncoder(w).Encode(m)
		_ = rc.Flush()
		<-r.Context().Done()
	}, Options{Heartbeat: 40 * time.Millisecond})
	defer closeSrv()

	cancel, notes, _ := startStream(t, svc, `{"event":"ntfy.message","filter":{"topic":"t"}}`, "mcp-extension-abc-0")
	defer cancel()

	active := waitNotification(t, notes, activeMethod)
	activeParams := active.Params.(map[string]any)
	activeMeta := activeParams["_meta"].(map[string]any)
	if activeMeta[subscriptionIDMetaKey] != "mcp-extension-abc-0" {
		t.Fatalf("active _meta = %+v", activeMeta)
	}
	if activeParams["subscriptionId"] != "mcp-extension-abc-0" {
		t.Fatalf("active subscriptionId = %v", activeParams["subscriptionId"])
	}

	msg := waitNotification(t, notes, eventMethod)
	params := msg.Params.(map[string]any)
	if params["name"] != EventName || params["eventId"] != "m1" || params["timestamp"] != "2023-11-14T22:13:21Z" {
		t.Fatalf("event params = %+v", params)
	}
	data, ok := params["data"].(Payload)
	if !ok || data.ID != "m1" || data.Message != "hello" || data.Topic != "t" {
		t.Fatalf("data = %+v", params["data"])
	}
	meta := params["_meta"].(map[string]any)
	if meta[subscriptionIDMetaKey] != "mcp-extension-abc-0" {
		t.Fatalf("event _meta = %+v", meta)
	}

	heartbeat := waitNotification(t, notes, heartbeatMethod)
	if got := heartbeat.Params.(map[string]any)["_meta"].(map[string]any)[subscriptionIDMetaKey]; got != "mcp-extension-abc-0" {
		t.Fatalf("heartbeat meta = %+v", heartbeat.Params)
	}
}

func TestSubscribeUsesRequestIDAsDefault(t *testing.T) {
	svc, closeSrv := realService(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}, Options{Heartbeat: time.Hour})
	defer closeSrv()

	results, notes, _, cancel := startSubscribe(t, svc, `{"name":"ntfy.message","arguments":{"topic":"t"}}`, "mcp-extension-sub-1")
	defer cancel()
	result := waitSubscribeResult(t, results)
	if result["subscriptionId"] != "mcp-extension-sub-1" {
		t.Fatalf("result = %+v", result)
	}
	meta := result["_meta"].(map[string]any)
	if meta[subscriptionIDMetaKey] != "mcp-extension-sub-1" {
		t.Fatalf("result _meta = %+v", meta)
	}
	active := waitNotification(t, notes, activeMethod)
	if got := active.Params.(map[string]any)["_meta"].(map[string]any)[subscriptionIDMetaKey]; got != "mcp-extension-sub-1" {
		t.Fatalf("active = %+v", active.Params)
	}
}
