// Package tools implements the two MCP tools exposed by ntfy-mcp:
// ntfy_publish and ntfy_read.
package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/naqerl/ntfy-mcp/internal/config"
	"github.com/naqerl/ntfy-mcp/internal/mcpx"
	"github.com/naqerl/ntfy-mcp/internal/ntfy"
)

// Client is the ntfy surface used by the tools. It is an interface so argument
// validation can be tested without an upstream server.
type Client interface {
	Publish(ctx context.Context, in ntfy.PublishRequest) (ntfy.PublishResult, error)
	Poll(ctx context.Context, topic, since string, limit int) (ntfy.PollResult, error)
}

// Tools implements mcpx.ToolProvider.
type Tools struct {
	client       Client
	defaultTopic string
}

// New constructs the tool set.
func New(client Client, defaultTopic string) *Tools {
	return &Tools{client: client, defaultTopic: defaultTopic}
}

// ListTools returns exactly the two public tools.
func (t *Tools) ListTools() []mcpx.Tool {
	return []mcpx.Tool{
		{
			Name:        "ntfy_publish",
			Description: "Publish a message to an ntfy topic.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"topic":    map[string]any{"type": "string", "description": "ntfy topic (1-64 chars, [A-Za-z0-9_-])"},
					"message":  map[string]any{"type": "string", "description": "message body"},
					"title":    map[string]any{"type": "string"},
					"priority": map[string]any{"type": "integer", "minimum": 1, "maximum": 5},
					"tags":     map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					"click":    map[string]any{"type": "string", "format": "uri"},
					"markdown": map[string]any{"type": "boolean"},
					"delay":    map[string]any{"type": "string", "description": "ntfy delay, e.g. 30m or 2026-07-28T10:00:00Z"},
				},
				"required":             []string{"topic", "message"},
				"additionalProperties": false,
			},
		},
		{
			Name:        "ntfy_read",
			Description: "Read cached messages from an ntfy topic.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"topic": map[string]any{"type": "string", "description": "ntfy topic; defaults to NTFY_DEFAULT_TOPIC when configured"},
					"since": map[string]any{
						"type":        "string",
						"default":     config.DefaultReadSince,
						"description": "duration, unix timestamp, message id, all, or latest",
					},
					"limit": map[string]any{
						"type":    "integer",
						"default": config.DefaultReadLimit,
						"minimum": 1,
						"maximum": config.MaxReadLimit,
					},
				},
				"additionalProperties": false,
			},
		},
	}
}

// PublishResult is the ntfy_publish structured result.
type PublishResult struct {
	ID    string `json:"id"`
	Topic string `json:"topic"`
	Time  int64  `json:"time"`
}

// ReadResult is the ntfy_read structured result.
type ReadResult struct {
	Topic    string         `json:"topic"`
	Cursor   string         `json:"cursor"`
	Messages []ntfy.Message `json:"messages"`
}

// CallTool validates and executes one tool call.
func (t *Tools) CallTool(ctx context.Context, name string, raw json.RawMessage) (any, error) {
	switch name {
	case "ntfy_publish":
		return t.publish(ctx, raw)
	case "ntfy_read":
		return t.read(ctx, raw)
	default:
		return nil, mcpx.NewError(mcpx.CodeInvalidParams, fmt.Sprintf("unknown tool %q", name), nil)
	}
}

type publishArgs struct {
	Topic    string   `json:"topic"`
	Message  string   `json:"message"`
	Title    string   `json:"title"`
	Priority *int     `json:"priority"`
	Tags     []string `json:"tags"`
	Click    string   `json:"click"`
	Markdown *bool    `json:"markdown"`
	Delay    string   `json:"delay"`
}

func (t *Tools) publish(ctx context.Context, raw json.RawMessage) (any, error) {
	var args publishArgs
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	topic, err := t.resolveTopic(args.Topic)
	if err != nil {
		return nil, err
	}
	if args.Message == "" {
		return nil, invalidParams("message is required")
	}
	priority := 0
	if args.Priority != nil {
		priority = *args.Priority
		if priority < 1 || priority > 5 {
			return nil, invalidParams("priority must be between 1 and 5")
		}
	}
	if err := validateTags(args.Tags); err != nil {
		return nil, err
	}
	if args.Click != "" {
		u, perr := url.Parse(args.Click)
		if perr != nil || u.Scheme == "" {
			return nil, invalidParams("click must be an absolute URI")
		}
	}

	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res, err := t.client.Publish(callCtx, ntfy.PublishRequest{
		Topic:    topic,
		Message:  args.Message,
		Title:    args.Title,
		Priority: priority,
		Tags:     args.Tags,
		Click:    args.Click,
		Markdown: args.Markdown,
		Delay:    args.Delay,
	})
	if err != nil {
		return nil, mapUpstreamError(err)
	}
	return PublishResult{ID: res.ID, Topic: res.Topic, Time: res.Time}, nil
}

type readArgs struct {
	Topic string `json:"topic"`
	Since string `json:"since"`
	Limit int    `json:"limit"`
}

func (t *Tools) read(ctx context.Context, raw json.RawMessage) (any, error) {
	var args readArgs
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	topic, err := t.resolveTopic(args.Topic)
	if err != nil {
		return nil, err
	}
	since := strings.TrimSpace(args.Since)
	if since == "" {
		since = config.DefaultReadSince
	}
	limit := args.Limit
	if limit == 0 {
		limit = config.DefaultReadLimit
	}
	if limit < 1 || limit > config.MaxReadLimit {
		return nil, invalidParams(fmt.Sprintf("limit must be between 1 and %d", config.MaxReadLimit))
	}

	callCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	res, err := t.client.Poll(callCtx, topic, since, limit)
	if err != nil {
		return nil, mapUpstreamError(err)
	}
	messages := res.Messages
	if messages == nil {
		messages = []ntfy.Message{}
	}
	cursor := ""
	if len(messages) > 0 {
		cursor = messages[len(messages)-1].ID
	}
	return ReadResult{Topic: topic, Cursor: cursor, Messages: messages}, nil
}

func (t *Tools) resolveTopic(topic string) (string, error) {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		topic = t.defaultTopic
	}
	if topic == "" {
		return "", invalidParams("topic is required (or configure NTFY_DEFAULT_TOPIC)")
	}
	if err := ntfy.ValidateTopic(topic); err != nil {
		return "", invalidParams(err.Error())
	}
	return topic, nil
}

func validateTags(tags []string) error {
	for _, tag := range tags {
		if tag == "" || strings.ContainsAny(tag, ", \t\r\n") {
			return invalidParams("tags must be non-empty and must not contain commas or whitespace")
		}
	}
	return nil
}

func decodeArgs(raw json.RawMessage, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return invalidParams("invalid arguments: " + err.Error())
	}
	// Reject trailing junk or a second top-level JSON value.
	var extra any
	switch err := dec.Decode(&extra); {
	case err == nil:
		return invalidParams("invalid arguments: multiple JSON values")
	case err != io.EOF:
		return invalidParams("invalid arguments: " + err.Error())
	}
	return nil
}

func invalidParams(msg string) *mcpx.Error {
	return mcpx.NewError(mcpx.CodeInvalidParams, msg, nil)
}

func mapUpstreamError(err error) error {
	switch {
	case ntfy.IsRateLimited(err):
		return mcpx.NewError(mcpx.CodeResourceExhausted, "ntfy rate limit or replay cache overrun", nil)
	case ntfy.IsNotFound(err):
		return mcpx.NewError(mcpx.CodeNotFound, "ntfy topic not found", map[string]any{"kind": "topic"})
	default:
		return mcpx.NewError(mcpx.CodeInternalError, err.Error(), nil)
	}
}
