// Package config parses and validates the ntfy-mcp environment. Every value
// that can break startup is validated here, before any listener is opened.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/naqerl/ntfy-mcp/internal/ntfy"
)

const (
	DefaultAddr       = ":8080"
	DefaultPath       = "/mcp"
	DefaultHeartbeat  = 25 * time.Second
	DefaultPollMS     = 3000
	DefaultMaxEvents  = 50
	DefaultStreamMax  = time.Hour
	MaxHeartbeat      = 30 * time.Second
	MaxEventsLimit    = 200
	DefaultReadSince  = "10m"
	DefaultReadLimit  = 50
	MaxReadLimit      = 200
	DefaultStreamName = "ntfy.message"
)

// Config is the validated runtime configuration.
type Config struct {
	Addr         string
	Path         string
	AuthToken    string
	NtfyBaseURL  *url.URL
	NtfyToken    string
	DefaultTopic string
	Heartbeat    time.Duration
	PollMS       int
	MaxEvents    int
	StreamMax    time.Duration
}

// Load reads configuration from the process environment.
func Load() (*Config, error) {
	return FromEnv(os.Getenv)
}

// FromEnv builds configuration using get. It is exported so tests can exercise
// every failure path without mutating the process environment.
func FromEnv(get func(string) string) (*Config, error) {
	cfg := &Config{
		Addr:         valueOr(get("MCP_ADDR"), DefaultAddr),
		Path:         valueOr(get("MCP_PATH"), DefaultPath),
		AuthToken:    get("MCP_AUTH_TOKEN"),
		NtfyToken:    get("NTFY_TOKEN"),
		DefaultTopic: strings.TrimSpace(get("NTFY_DEFAULT_TOPIC")),
		Heartbeat:    DefaultHeartbeat,
		PollMS:       DefaultPollMS,
		MaxEvents:    DefaultMaxEvents,
		StreamMax:    DefaultStreamMax,
	}

	rawBase := strings.TrimSpace(get("NTFY_BASE_URL"))
	if rawBase != "" {
		u, err := url.Parse(rawBase)
		if err != nil {
			return nil, fmt.Errorf("NTFY_BASE_URL: %w", err)
		}
		cfg.NtfyBaseURL = u
	}

	if raw := strings.TrimSpace(get("NTFY_HEARTBEAT")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("NTFY_HEARTBEAT: %w", err)
		}
		cfg.Heartbeat = d
	}
	if raw := strings.TrimSpace(get("NTFY_POLL_MS")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("NTFY_POLL_MS: %w", err)
		}
		cfg.PollMS = n
	}
	if raw := strings.TrimSpace(get("NTFY_MAX_EVENTS")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("NTFY_MAX_EVENTS: %w", err)
		}
		cfg.MaxEvents = n
	}
	if raw := strings.TrimSpace(get("NTFY_STREAM_MAX")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("NTFY_STREAM_MAX: %w", err)
		}
		cfg.StreamMax = d
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate checks the whole configuration. It is called after environment
// parsing and again if flags override Addr or Path.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.AuthToken) == "" {
		return fmt.Errorf("MCP_AUTH_TOKEN is required")
	}
	if c.NtfyBaseURL == nil {
		return fmt.Errorf("NTFY_BASE_URL is required")
	}
	u := c.NtfyBaseURL
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("NTFY_BASE_URL must use http or https")
	}
	if u.Host == "" {
		return fmt.Errorf("NTFY_BASE_URL must include a host")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("NTFY_BASE_URL must not include a query or fragment")
	}
	c.NtfyBaseURL.Path = strings.TrimSuffix(u.Path, "/")

	if c.Addr == "" {
		return fmt.Errorf("MCP_ADDR must not be empty")
	}
	if c.Path == "" || !strings.HasPrefix(c.Path, "/") {
		return fmt.Errorf("MCP_PATH must start with /")
	}
	if c.Heartbeat <= 0 || c.Heartbeat > MaxHeartbeat {
		return fmt.Errorf("NTFY_HEARTBEAT must be >0 and <=%s", MaxHeartbeat)
	}
	if c.PollMS <= 0 {
		return fmt.Errorf("NTFY_POLL_MS must be >0")
	}
	if c.MaxEvents <= 0 || c.MaxEvents > MaxEventsLimit {
		return fmt.Errorf("NTFY_MAX_EVENTS must be between 1 and %d", MaxEventsLimit)
	}
	if c.StreamMax <= 0 {
		return fmt.Errorf("NTFY_STREAM_MAX must be >0")
	}
	if c.DefaultTopic != "" {
		if err := ntfy.ValidateTopic(c.DefaultTopic); err != nil {
			return fmt.Errorf("NTFY_DEFAULT_TOPIC: %w", err)
		}
	}
	// Reject a default that would silently be unusable in the URL.
	c.NtfyBaseURL.Path = strings.TrimSuffix(c.NtfyBaseURL.Path, "/")
	return nil
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
