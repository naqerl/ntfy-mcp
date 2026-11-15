package ntfy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestPublishHeaderMapping(t *testing.T) {
	var got *http.Request
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		data, _ := io.ReadAll(r.Body)
		body = string(data)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"msg-1","time":1700000000,"topic":"alerts","event":"message"}`)
	}))
	defer srv.Close()

	base, _ := url.Parse(srv.URL)
	client := New(base, "upstream-token", srv.Client())
	markdown := false
	result, err := client.Publish(context.Background(), PublishRequest{
		Topic:    "alerts",
		Message:  "hello",
		Title:    "Title",
		Priority: 5,
		Tags:     []string{"warning", "disk"},
		Click:    "https://example.com/x",
		Markdown: &markdown,
		Delay:    "30m",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if result.ID != "msg-1" || result.Topic != "alerts" || result.Time != 1700000000 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if got.Method != http.MethodPost || got.URL.Path != "/alerts" {
		t.Fatalf("request = %s %s", got.Method, got.URL.Path)
	}
	if body != "hello" {
		t.Fatalf("body = %q", body)
	}
	if got.Header.Get("Authorization") != "Bearer upstream-token" {
		t.Fatalf("authorization = %q", got.Header.Get("Authorization"))
	}
	headers := map[string]string{
		"X-Title":    "Title",
		"X-Priority": "5",
		"X-Tags":     "warning,disk",
		"X-Click":    "https://example.com/x",
		"X-Markdown": "false",
		"X-Delay":    "30m",
	}
	for key, want := range headers {
		if have := got.Header.Get(key); have != want {
			t.Errorf("%s = %q, want %q", key, have, want)
		}
	}
}

func TestPollSinceFormsAndLimit(t *testing.T) {
	forms := []string{"10m", "all", "latest", "1700000000", "abc123"}
	for _, form := range forms {
		t.Run(form, func(t *testing.T) {
			var query url.Values
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				query = r.URL.Query()
				w.Header().Set("Content-Type", "application/x-ndjson")
				for i := 0; i < 3; i++ {
					fmt.Fprintf(w, `{"id":"m%d","time":%d,"event":"message","topic":"t","message":"x"}`+"\n", i, 100+i)
				}
			}))
			defer srv.Close()
			base, _ := url.Parse(srv.URL)
			res, err := New(base, "", srv.Client()).Poll(context.Background(), "t", form, 2)
			if err != nil {
				t.Fatalf("Poll: %v", err)
			}
			if query.Get("since") != form {
				t.Fatalf("since = %q, want %q", query.Get("since"), form)
			}
			if query.Get("poll") != "1" {
				t.Fatalf("poll = %q", query.Get("poll"))
			}
			if len(res.Messages) != 2 {
				t.Fatalf("messages = %d, want 2", len(res.Messages))
			}
			if res.Messages[0].ID != "m0" || res.Messages[1].ID != "m1" {
				t.Fatalf("messages = %+v", res.Messages)
			}
		})
	}
}

func TestPollTruncatedHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Messages-Truncated", "1")
		fmt.Fprintln(w, `{"id":"m1","time":1,"event":"message","topic":"t","message":"x"}`)
	}))
	defer srv.Close()
	base, _ := url.Parse(srv.URL)
	res, err := New(base, "", srv.Client()).Poll(context.Background(), "t", "all", 10)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if !res.Truncated {
		t.Fatal("expected truncated")
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		status  int
		code    int
		limited bool
		missing bool
	}{
		{http.StatusTooManyRequests, 42905, true, false},
		{http.StatusNotFound, 0, false, true},
		{http.StatusInternalServerError, 0, false, false},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				fmt.Fprintf(w, `{"code":%d,"error":"upstream says no"}`, tc.code)
			}))
			defer srv.Close()
			base, _ := url.Parse(srv.URL)
			_, err := New(base, "", srv.Client()).Publish(context.Background(), PublishRequest{Topic: "t", Message: "m"})
			if err == nil {
				t.Fatal("expected error")
			}
			if IsRateLimited(err) != tc.limited {
				t.Errorf("IsRateLimited = %v", IsRateLimited(err))
			}
			if IsNotFound(err) != tc.missing {
				t.Errorf("IsNotFound = %v", IsNotFound(err))
			}
			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("error type %T", err)
			}
			if apiErr.Message != "upstream says no" {
				t.Errorf("message = %q", apiErr.Message)
			}
		})
	}
}

func TestNDJSONStreamDecoder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("since") != "t123" {
			t.Errorf("since = %q", r.URL.Query().Get("since"))
		}
		fmt.Fprintln(w, `{"id":"","time":10,"event":"open","topic":"t"}`)
		fmt.Fprintln(w, `{"id":"","time":11,"event":"keepalive","topic":"t"}`)
		fmt.Fprintln(w, `{"id":"m1","time":12,"event":"message","topic":"t","message":"hi","priority":4,"tags":["a"]}`)
	}))
	defer srv.Close()
	base, _ := url.Parse(srv.URL)
	stream, err := New(base, "", srv.Client()).OpenStream(context.Background(), "t", "t123")
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer stream.Close()

	wantEvents := []string{"open", "keepalive", "message"}
	for _, want := range wantEvents {
		m, err := stream.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if m.Event != want {
			t.Fatalf("event = %q, want %q", m.Event, want)
		}
	}
	if _, err := stream.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("final Next = %v, want EOF", err)
	}
}

func TestPollDecodesAttachment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"id":"m1","time":1,"event":"message","topic":"t","message":"x","attachment":{"name":"a.txt","url":"https://x/a","type":"text/plain","size":2,"expires":9}}`)
	}))
	defer srv.Close()
	base, _ := url.Parse(srv.URL)
	res, err := New(base, "", srv.Client()).Poll(context.Background(), "t", "all", 10)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(res.Messages) != 1 || res.Messages[0].Attachment == nil || res.Messages[0].Attachment.Name != "a.txt" {
		t.Fatalf("messages = %+v", res.Messages)
	}
}

func TestClientUsesContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	base, _ := url.Parse(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := New(base, "", srv.Client()).OpenStream(ctx, "t", "all")
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestMessageJSONRoundTrip(t *testing.T) {
	raw := []byte(`{"id":"x","time":5,"event":"message","topic":"t","message":"m","tags":["a"],"priority":2}`)
	var m Message
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.ID != "x" || len(m.Tags) != 1 || m.Priority != 2 {
		t.Fatalf("message = %+v", m)
	}
}
