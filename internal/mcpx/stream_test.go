package mcpx

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testHandler(events *fakeEvents) *Handler {
	if events == nil {
		events = &fakeEvents{}
	}
	return NewHandler(NewServer("test", defaultTools(), events), "secret")
}

func doRequest(h http.Handler, method, body, session, accept, auth string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/mcp", strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if session != "" {
		req.Header.Set(SessionHeader, session)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestAuthRejectedBeforeParsing(t *testing.T) {
	h := testHandler(nil)
	for _, auth := range []string{"", "wrong", "secret-extra"} {
		rr := doRequest(h, http.MethodPost, `{"jsonrpc":"2.0","id":1,"method":"server/discover"}`, "", "application/json", auth)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("auth %q: status = %d", auth, rr.Code)
		}
		if rr.Header().Get(SessionHeader) != "" {
			t.Fatal("unauthorized request created a session")
		}
		if rr.Header().Get("WWW-Authenticate") == "" {
			t.Fatal("missing WWW-Authenticate")
		}
	}
}

func TestSessionCreateReuseRejectDelete(t *testing.T) {
	h := testHandler(nil)
	body := `{"jsonrpc":"2.0","id":1,"method":"server/discover"}`
	first := doRequest(h, http.MethodPost, body, "", "application/json", "secret")
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d body=%s", first.Code, first.Body.String())
	}
	session := first.Header().Get(SessionHeader)
	if session == "" {
		t.Fatal("missing session header")
	}
	var resp Response
	if err := json.Unmarshal(first.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("response error = %v", resp.Error)
	}

	second := doRequest(h, http.MethodPost, body, session, "application/json", "secret")
	if second.Code != http.StatusOK {
		t.Fatalf("reuse status = %d", second.Code)
	}
	if second.Header().Get(SessionHeader) != session {
		t.Fatalf("session changed: %q", second.Header().Get(SessionHeader))
	}

	unknown := doRequest(h, http.MethodPost, body, "does-not-exist", "application/json", "secret")
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown session status = %d", unknown.Code)
	}

	del := doRequest(h, http.MethodDelete, "", session, "", "secret")
	if del.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d", del.Code)
	}
	after := doRequest(h, http.MethodPost, body, session, "application/json", "secret")
	if after.Code != http.StatusNotFound {
		t.Fatalf("after delete status = %d", after.Code)
	}
}

func TestBatchIDEcho(t *testing.T) {
	h := testHandler(nil)
	rr := doRequest(h, http.MethodPost,
		`[{"jsonrpc":"2.0","id":"a","method":"ping"},{"jsonrpc":"2.0","id":2,"method":"ping"}]`,
		"", "application/json", "secret")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	var responses []Response
	if err := json.Unmarshal(rr.Body.Bytes(), &responses); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("responses = %+v", responses)
	}
	if string(responses[0].ID) != `"a"` || string(responses[1].ID) != `2` {
		t.Fatalf("ids = %s %s", responses[0].ID, responses[1].ID)
	}
}

func TestBatchWithNotificationAndEmptyBatch(t *testing.T) {
	h := testHandler(nil)
	rr := doRequest(h, http.MethodPost,
		`[{"jsonrpc":"2.0","method":"notifications/initialized"},{"jsonrpc":"2.0","id":1,"method":"ping"}]`,
		"", "application/json", "secret")
	var responses []Response
	if err := json.Unmarshal(rr.Body.Bytes(), &responses); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("responses = %+v", responses)
	}

	rr = doRequest(h, http.MethodPost, `[]`, "", "application/json", "secret")
	rr = doRequest(h, http.MethodPost, `[]`, rr.Header().Get(SessionHeader), "application/json", "secret")
	if rr.Code != http.StatusOK {
		t.Fatalf("empty batch status = %d", rr.Code)
	}
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != CodeInvalidRequest {
		t.Fatalf("empty batch response = %+v", resp)
	}
}

func TestParseErrorResponse(t *testing.T) {
	h := testHandler(nil)
	rr := doRequest(h, http.MethodPost, `{"jsonrpc":`, "", "application/json", "secret")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != CodeParseError {
		t.Fatalf("response = %+v", resp)
	}
	if string(resp.ID) != "null" {
		t.Fatalf("id = %s", resp.ID)
	}
}

func TestInvalidIDResponseUsesNull(t *testing.T) {
	h := testHandler(nil)
	for _, id := range []string{`{}`, `[]`, `true`} {
		body := `{"jsonrpc":"2.0","id":` + id + `,"method":"ping"}`
		rr := doRequest(h, http.MethodPost, body, "", "application/json", "secret")
		var resp Response
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("%s: decode: %v", id, err)
		}
		if resp.Error == nil || resp.Error.Code != CodeInvalidRequest {
			t.Fatalf("%s: response = %+v", id, resp)
		}
		if string(resp.ID) != "null" {
			t.Fatalf("%s: id = %s, want null", id, resp.ID)
		}
	}
}

func TestNotificationsOnlyAccepted(t *testing.T) {
	h := testHandler(nil)
	rr := doRequest(h, http.MethodPost, `{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		"", "application/json", "secret")
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestToolsCallOverHTTP(t *testing.T) {
	h := testHandler(nil)
	rr := doRequest(h, http.MethodPost,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ntfy_publish","arguments":{"topic":"t","message":"m"}}}`,
		"", "application/json", "secret")
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("error = %v", resp.Error)
	}
	result := resp.Result.(map[string]any)
	if result["resultType"] != "complete" {
		t.Fatalf("result = %+v", result)
	}
}

func TestEventsStreamSSE(t *testing.T) {
	events := &fakeEvents{stream: func(_ context.Context, _ json.RawMessage, emit func(Notification) error) error {
		if err := emit(NewNotification("notifications/events/active", map[string]any{"cursor": "t1"})); err != nil {
			return err
		}
		return emit(NewNotification("notifications/events/event", map[string]any{"eventId": "m1"}))
	}}
	h := testHandler(events)
	rr := doRequest(h, http.MethodPost,
		`{"jsonrpc":"2.0","id":9,"method":"events/stream","params":{"event":"ntfy.message","filter":{"topic":"t"}}}`,
		"", "text/event-stream", "secret")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	if rr.Header().Get(SessionHeader) == "" {
		t.Fatal("missing session header")
	}
	body := rr.Body.String()
	if strings.Count(body, "data: ") != 2 {
		t.Fatalf("body = %q", body)
	}
	if !strings.Contains(body, "notifications/events/active") || !strings.Contains(body, "notifications/events/event") {
		t.Fatalf("body = %q", body)
	}
	if strings.Contains(body, `"result"`) {
		t.Fatalf("stream must not write a result: %q", body)
	}
}

func TestEventsStreamRequiresSSE(t *testing.T) {
	h := testHandler(&fakeEvents{})
	rr := doRequest(h, http.MethodPost,
		`{"jsonrpc":"2.0","id":9,"method":"events/stream","params":{}}`,
		"", "application/json", "secret")
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type = %q", ct)
	}
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != CodeUnsupported {
		t.Fatalf("response = %+v", resp)
	}
}

func TestEventsStreamErrorBeforeHeadersIsJSON(t *testing.T) {
	h := testHandler(&fakeEvents{streamErr: NewError(CodeNotFound, "unknown event", map[string]any{"kind": "event"})})
	rr := doRequest(h, http.MethodPost,
		`{"jsonrpc":"2.0","id":9,"method":"events/stream","params":{}}`,
		"", "text/event-stream", "secret")
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type = %q", ct)
	}
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != CodeNotFound {
		t.Fatalf("response = %+v", resp)
	}
}

func TestEventsStreamErrorAfterActiveWritesErrorFrame(t *testing.T) {
	events := &fakeEvents{stream: func(_ context.Context, _ json.RawMessage, emit func(Notification) error) error {
		if err := emit(NewNotification("notifications/events/active", map[string]any{"cursor": "t1"})); err != nil {
			return err
		}
		return NewError(CodeInternalError, "upstream reset", nil)
	}}
	h := testHandler(events)
	rr := doRequest(h, http.MethodPost,
		`{"jsonrpc":"2.0","id":11,"method":"events/stream","params":{"event":"ntfy.message","filter":{"topic":"t"}}}`,
		"", "text/event-stream", "secret")
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	body := rr.Body.String()
	if strings.Count(body, "data: ") != 2 {
		t.Fatalf("body = %q", body)
	}
	if !strings.Contains(body, `"error":{"code":-32603`) || !strings.Contains(body, `"id":11`) {
		t.Fatalf("missing downstream error frame: %q", body)
	}
}

func TestSingleResponseOverSSE(t *testing.T) {
	h := testHandler(nil)
	rr := doRequest(h, http.MethodPost, `{"jsonrpc":"2.0","id":1,"method":"ping"}`,
		"", "text/event-stream", "secret")
	if !strings.Contains(rr.Body.String(), `"result"`) || !strings.HasPrefix(rr.Body.String(), "data: ") {
		t.Fatalf("body = %q", rr.Body.String())
	}
}

func TestGETAndDELETESessionHandling(t *testing.T) {
	h := testHandler(nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	client := srv.Client()
	req, _ := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"server/discover"}`))
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	session := resp.Header.Get(SessionHeader)
	resp.Body.Close()
	if session == "" {
		t.Fatal("missing session")
	}

	// Unknown session is rejected.
	unknownReq, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	unknownReq.Header.Set("Authorization", "Bearer secret")
	unknownReq.Header.Set(SessionHeader, "missing")
	unknownResp, err := client.Do(unknownReq)
	if err != nil {
		t.Fatalf("unknown GET: %v", err)
	}
	if unknownResp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown GET status = %d", unknownResp.StatusCode)
	}
	unknownResp.Body.Close()

	// A valid GET opens the server-to-client SSE channel.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	getReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	getReq.Header.Set("Authorization", "Bearer secret")
	getReq.Header.Set(SessionHeader, session)
	getResp, err := client.Do(getReq)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if getResp.StatusCode != http.StatusOK || !strings.HasPrefix(getResp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("GET status/content-type = %d %q", getResp.StatusCode, getResp.Header.Get("Content-Type"))
	}
	line, _ := bufio.NewReader(getResp.Body).ReadString('\n')
	if !strings.HasPrefix(line, ": connected") {
		t.Fatalf("first line = %q", line)
	}
	cancel()
	getResp.Body.Close()

	// DELETE removes it.
	delReq, _ := http.NewRequest(http.MethodDelete, srv.URL, nil)
	delReq.Header.Set("Authorization", "Bearer secret")
	delReq.Header.Set(SessionHeader, session)
	delResp, err := client.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d", delResp.StatusCode)
	}
	delResp.Body.Close()
}

func TestMethodNotAllowed(t *testing.T) {
	h := testHandler(nil)
	rr := doRequest(h, http.MethodPut, "", "", "", "secret")
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rr.Code)
	}
	if rr.Header().Get("Allow") == "" {
		t.Fatal("missing Allow")
	}
}

func TestRequestTooLarge(t *testing.T) {
	h := testHandler(nil)
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"ping","params":{"x":%q}}`, strings.Repeat("a", maxRequestBody))
	rr := doRequest(h, http.MethodPost, body, "", "application/json", "secret")
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestSessionExpiry(t *testing.T) {
	h := testHandler(nil)
	now := time.Unix(1000, 0)
	h.now = func() time.Time { return now }
	first := doRequest(h, http.MethodPost, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, "", "application/json", "secret")
	session := first.Header().Get(SessionHeader)
	now = now.Add(defaultSessTTL + time.Second)
	rr := doRequest(h, http.MethodPost, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, session, "application/json", "secret")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expired session status = %d", rr.Code)
	}
}
