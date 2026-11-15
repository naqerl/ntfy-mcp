package mcpx

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestDecodeSingleAndBatch(t *testing.T) {
	batch, msgs, rpcErr := Decode([]byte(`[
		{"jsonrpc":"2.0","id":1,"method":"ping"},
		{"jsonrpc":"2.0","method":"notifications/initialized"}
	]`))
	if rpcErr != nil {
		t.Fatalf("Decode: %v", rpcErr)
	}
	if !batch || len(msgs) != 2 {
		t.Fatalf("batch=%v len=%d", batch, len(msgs))
	}
	if !msgs[0].Request.HasID() || msgs[0].Request.IsNotification() {
		t.Fatal("first request should have an ID")
	}
	if !msgs[1].Request.IsNotification() {
		t.Fatal("second request should be a notification")
	}
}

func TestDecodeParseError(t *testing.T) {
	_, _, rpcErr := Decode([]byte(`{"jsonrpc":`))
	if rpcErr == nil || rpcErr.Code != CodeParseError {
		t.Fatalf("error = %v", rpcErr)
	}
	_, _, rpcErr = Decode([]byte(`[]`))
	if rpcErr == nil || rpcErr.Code != CodeInvalidRequest {
		t.Fatalf("empty batch error = %v", rpcErr)
	}
}

func TestDecodeInvalidRequests(t *testing.T) {
	tests := []string{
		`{"jsonrpc":"1.0","id":1,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":1}`,
		`{"jsonrpc":"2.0","id":{"x":1},"method":"ping"}`,
		`{"jsonrpc":"2.0","id":1,"method":"ping","params":"bad"}`,
	}
	for _, body := range tests {
		_, msgs, rpcErr := Decode([]byte(body))
		if rpcErr != nil {
			t.Fatalf("%s: body error %v", body, rpcErr)
		}
		if len(msgs) != 1 || msgs[0].Err == nil || msgs[0].Err.Code != CodeInvalidRequest {
			t.Fatalf("%s: msgs = %+v", body, msgs)
		}
		if strings.Contains(body, `"id":{"x":1}`) && len(msgs[0].Request.ID) != 0 {
			t.Fatalf("%s: invalid id must be cleared, got %s", body, msgs[0].Request.ID)
		}
	}
}

func TestDecodeBatchMalformedMember(t *testing.T) {
	_, msgs, rpcErr := Decode([]byte(`[1,{"jsonrpc":"2.0","id":2,"method":"ping"}]`))
	if rpcErr != nil {
		t.Fatalf("Decode: %v", rpcErr)
	}
	if msgs[0].Err == nil || msgs[0].Err.Code != CodeInvalidRequest {
		t.Fatalf("first = %+v", msgs[0])
	}
	if msgs[1].Err != nil || msgs[1].Request.Method != "ping" {
		t.Fatalf("second = %+v", msgs[1])
	}
}

func TestResponseIDEcho(t *testing.T) {
	resp := SuccessResponse(json.RawMessage(`"abc"`), map[string]any{"ok": true})
	raw, _ := json.Marshal(resp)
	if !strings.Contains(string(raw), `"id":"abc"`) {
		t.Fatalf("response = %s", raw)
	}
	errResp := ErrorResponse(nil, NewError(CodeParseError, "bad", nil))
	raw, _ = json.Marshal(errResp)
	if !strings.Contains(string(raw), `"id":null`) {
		t.Fatalf("error response = %s", raw)
	}
}

func TestEncodeSSEFrame(t *testing.T) {
	frame, err := encodeSSE(NewNotification("notifications/events/active", map[string]any{"cursor": "t1"}))
	if err != nil {
		t.Fatalf("encodeSSE: %v", err)
	}
	text := string(frame)
	if !strings.HasPrefix(text, "data: {") || !strings.HasSuffix(text, "\n\n") {
		t.Fatalf("frame = %q", text)
	}
	if strings.Count(text, "\n") != 2 {
		t.Fatalf("frame should be exactly one data line: %q", text)
	}
	var decoded Notification
	payload := strings.TrimSuffix(strings.TrimPrefix(text, "data: "), "\n\n")
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("unmarshal frame: %v", err)
	}
	if decoded.Method != "notifications/events/active" {
		t.Fatalf("method = %q", decoded.Method)
	}
}

func TestBearerTokenParsing(t *testing.T) {
	tests := []struct {
		header string
		token  string
		ok     bool
	}{
		{"Bearer secret", "secret", true},
		{"bearer secret", "secret", true},
		{"Basic secret", "", false},
		{"Bearer", "", false},
		{"Bearer  ", "", false},
		{"Bearer a b", "", false},
	}
	for _, tc := range tests {
		token, ok := bearerToken(tc.header)
		if token != tc.token || ok != tc.ok {
			t.Errorf("bearerToken(%q) = %q,%v", tc.header, token, ok)
		}
	}
}

func TestAsRPCErrorPassthrough(t *testing.T) {
	want := NewError(CodeNotFound, "gone", nil)
	if got := asRPCError(want); got != want {
		t.Fatalf("got %p want %p", got, want)
	}
	got := asRPCError(errors.New("boom"))
	if got.Code != CodeInternalError || got.Message != "boom" {
		t.Fatalf("got %+v", got)
	}
}
