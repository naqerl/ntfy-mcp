package config

import (
	"strings"
	"testing"
	"time"
)

func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func validEnv() map[string]string {
	return map[string]string{
		"MCP_AUTH_TOKEN": "secret",
		"NTFY_BASE_URL":  "http://127.0.0.1:2586",
	}
}

func TestDefaults(t *testing.T) {
	cfg, err := FromEnv(env(validEnv()))
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.Addr != DefaultAddr || cfg.Path != DefaultPath {
		t.Fatalf("addr/path = %s %s", cfg.Addr, cfg.Path)
	}
	if cfg.Heartbeat != DefaultHeartbeat || cfg.PollMS != DefaultPollMS {
		t.Fatalf("heartbeat/poll = %s %d", cfg.Heartbeat, cfg.PollMS)
	}
	if cfg.MaxEvents != DefaultMaxEvents || cfg.StreamMax != DefaultStreamMax {
		t.Fatalf("maxEvents/streamMax = %d %s", cfg.MaxEvents, cfg.StreamMax)
	}
	if cfg.NtfyBaseURL.String() != "http://127.0.0.1:2586" {
		t.Fatalf("base = %s", cfg.NtfyBaseURL)
	}
}

func TestRequiredEnvironment(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]string)
		want   string
	}{
		{
			name:   "missing auth token",
			mutate: func(m map[string]string) { delete(m, "MCP_AUTH_TOKEN") },
			want:   "MCP_AUTH_TOKEN",
		},
		{
			name:   "empty auth token",
			mutate: func(m map[string]string) { m["MCP_AUTH_TOKEN"] = "  " },
			want:   "MCP_AUTH_TOKEN",
		},
		{
			name:   "missing base url",
			mutate: func(m map[string]string) { delete(m, "NTFY_BASE_URL") },
			want:   "NTFY_BASE_URL",
		},
		{
			name:   "unparsable base url",
			mutate: func(m map[string]string) { m["NTFY_BASE_URL"] = "://bad" },
			want:   "NTFY_BASE_URL",
		},
		{
			name:   "unsupported scheme",
			mutate: func(m map[string]string) { m["NTFY_BASE_URL"] = "ftp://example.com" },
			want:   "http or https",
		},
		{
			name:   "missing host",
			mutate: func(m map[string]string) { m["NTFY_BASE_URL"] = "http://" },
			want:   "host",
		},
		{
			name:   "base query",
			mutate: func(m map[string]string) { m["NTFY_BASE_URL"] = "http://example.com?x=1" },
			want:   "query",
		},
		{
			name:   "bad path",
			mutate: func(m map[string]string) { m["MCP_PATH"] = "mcp" },
			want:   "MCP_PATH",
		},
		{
			name:   "heartbeat too large",
			mutate: func(m map[string]string) { m["NTFY_HEARTBEAT"] = "31s" },
			want:   "NTFY_HEARTBEAT",
		},
		{
			name:   "heartbeat invalid",
			mutate: func(m map[string]string) { m["NTFY_HEARTBEAT"] = "soon" },
			want:   "NTFY_HEARTBEAT",
		},
		{
			name:   "poll invalid",
			mutate: func(m map[string]string) { m["NTFY_POLL_MS"] = "0" },
			want:   "NTFY_POLL_MS",
		},
		{
			name:   "max events too large",
			mutate: func(m map[string]string) { m["NTFY_MAX_EVENTS"] = "201" },
			want:   "NTFY_MAX_EVENTS",
		},
		{
			name:   "stream max invalid",
			mutate: func(m map[string]string) { m["NTFY_STREAM_MAX"] = "-1s" },
			want:   "NTFY_STREAM_MAX",
		},
		{
			name:   "reserved default topic",
			mutate: func(m map[string]string) { m["NTFY_DEFAULT_TOPIC"] = "docs" },
			want:   "reserved",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			values := validEnv()
			tc.mutate(values)
			_, err := FromEnv(env(values))
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want contains %q", err, tc.want)
			}
		})
	}
}

func TestOverridesAndCustomValues(t *testing.T) {
	values := validEnv()
	values["MCP_ADDR"] = "127.0.0.1:9000"
	values["MCP_PATH"] = "/custom"
	values["NTFY_BASE_URL"] = "https://ntfy.example.com/"
	values["NTFY_HEARTBEAT"] = "10s"
	values["NTFY_POLL_MS"] = "1500"
	values["NTFY_MAX_EVENTS"] = "100"
	values["NTFY_STREAM_MAX"] = "15m"
	values["NTFY_DEFAULT_TOPIC"] = "alerts"
	values["NTFY_TOKEN"] = "upstream"

	cfg, err := FromEnv(env(values))
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.Addr != "127.0.0.1:9000" || cfg.Path != "/custom" {
		t.Fatalf("addr/path = %s %s", cfg.Addr, cfg.Path)
	}
	if cfg.NtfyBaseURL.Path != "" {
		t.Fatalf("base path not trimmed: %q", cfg.NtfyBaseURL.Path)
	}
	if cfg.Heartbeat != 10*time.Second || cfg.PollMS != 1500 || cfg.MaxEvents != 100 || cfg.StreamMax != 15*time.Minute {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.DefaultTopic != "alerts" || cfg.NtfyToken != "upstream" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}
