// Package ntfy is a small stdlib-only client for the ntfy HTTP API. It covers
// exactly the surface the MCP server needs: publish, cached poll, and the JSON
// (NDJSON) subscription stream.
package ntfy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Message is one ntfy message as returned by the JSON endpoints. The open and
// keepalive events use the same envelope with an empty message body.
type Message struct {
	ID         string      `json:"id"`
	Time       int64       `json:"time"`
	Event      string      `json:"event"`
	Topic      string      `json:"topic"`
	Title      string      `json:"title,omitempty"`
	Message    string      `json:"message"`
	Tags       []string    `json:"tags,omitempty"`
	Priority   int         `json:"priority,omitempty"`
	Click      string      `json:"click,omitempty"`
	Attachment *Attachment `json:"attachment,omitempty"`
}

// Attachment is ntfy's attachment metadata. Attachments are not produced by
// this gateway but are passed through when present in upstream messages.
type Attachment struct {
	Name    string `json:"name,omitempty"`
	URL     string `json:"url,omitempty"`
	Type    string `json:"type,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Expires int64  `json:"expires,omitempty"`
}

// PublishRequest describes a message to publish.
type PublishRequest struct {
	Topic    string
	Message  string
	Title    string
	Priority int
	Tags     []string
	Click    string
	Markdown *bool
	Delay    string
}

// PublishResult is the subset of ntfy's publish response exposed to tools.
type PublishResult struct {
	ID    string
	Topic string
	Time  int64
}

// PollResult is the result of one cached poll.
type PollResult struct {
	Messages  []Message
	Truncated bool
}

// APIError is an HTTP-level error returned by ntfy.
type APIError struct {
	Status  int
	Code    int
	Message string
}

// Error implements the error interface.
func (e *APIError) Error() string {
	if e.Code != 0 {
		return fmt.Sprintf("ntfy: HTTP %d (code %d): %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("ntfy: HTTP %d: %s", e.Status, e.Message)
}

// IsRateLimited reports whether err is an upstream 429 / 42905 response.
func IsRateLimited(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.Status == http.StatusTooManyRequests || apiErr.Code == 42905
}

// IsNotFound reports whether err is an upstream 404.
func IsNotFound(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.Status == http.StatusNotFound
}

// Client is an ntfy API client. BaseURL must not include the topic path.
type Client struct {
	base  *url.URL
	token string
	http  *http.Client
}

// New constructs a client. The HTTP client should not set a global Timeout:
// subscription streams are long-lived; callers apply deadlines per request.
func New(base *url.URL, token string, hc *http.Client) *Client {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{base: base, token: token, http: hc}
}

func (c *Client) endpoint(topic string, jsonEndpoint bool, q url.Values) string {
	u := *c.base
	path := strings.TrimSuffix(u.Path, "/") + "/" + url.PathEscape(topic)
	if jsonEndpoint {
		path += "/json"
	}
	u.Path = path
	u.RawPath = ""
	u.RawQuery = q.Encode()
	return u.String()
}

func (c *Client) newRequest(ctx context.Context, method, endpoint string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer resp.Body.Close()
		return nil, decodeAPIError(resp)
	}
	return resp, nil
}

func decodeAPIError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var payload struct {
		Code  int    `json:"code"`
		Error string `json:"error"`
	}
	_ = json.Unmarshal(body, &payload)
	msg := strings.TrimSpace(payload.Error)
	if msg == "" {
		msg = strings.TrimSpace(string(body))
	}
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return &APIError{Status: resp.StatusCode, Code: payload.Code, Message: msg}
}

// Publish posts a message to a topic and returns its id and server time.
func (c *Client) Publish(ctx context.Context, in PublishRequest) (PublishResult, error) {
	req, err := c.newRequest(ctx, http.MethodPost, c.endpoint(in.Topic, false, nil), strings.NewReader(in.Message))
	if err != nil {
		return PublishResult{}, err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if in.Title != "" {
		req.Header.Set("X-Title", in.Title)
	}
	if in.Priority != 0 {
		req.Header.Set("X-Priority", strconv.Itoa(in.Priority))
	}
	if len(in.Tags) > 0 {
		req.Header.Set("X-Tags", strings.Join(in.Tags, ","))
	}
	if in.Click != "" {
		req.Header.Set("X-Click", in.Click)
	}
	if in.Markdown != nil {
		req.Header.Set("X-Markdown", strconv.FormatBool(*in.Markdown))
	}
	if in.Delay != "" {
		req.Header.Set("X-Delay", in.Delay)
	}

	resp, err := c.do(req)
	if err != nil {
		return PublishResult{}, err
	}
	defer resp.Body.Close()

	var payload struct {
		ID    string `json:"id"`
		Time  int64  `json:"time"`
		Topic string `json:"topic"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		return PublishResult{}, fmt.Errorf("ntfy: decode publish response: %w", err)
	}
	if payload.Topic == "" {
		payload.Topic = in.Topic
	}
	return PublishResult{ID: payload.ID, Topic: payload.Topic, Time: payload.Time}, nil
}

// Poll reads the cached messages for a topic and closes the connection. since
// is passed verbatim so all ntfy forms (duration, unix, message id, all,
// latest) work.
func (c *Client) Poll(ctx context.Context, topic, since string, limit int) (PollResult, error) {
	q := url.Values{"poll": {"1"}}
	if since != "" {
		q.Set("since", since)
	}
	req, err := c.newRequest(ctx, http.MethodGet, c.endpoint(topic, true, q), nil)
	if err != nil {
		return PollResult{}, err
	}
	req.Header.Set("Accept", "application/x-ndjson")
	resp, err := c.do(req)
	if err != nil {
		return PollResult{}, err
	}
	defer resp.Body.Close()

	result := PollResult{
		Messages:  []Message{},
		Truncated: resp.Header.Get("X-Messages-Truncated") == "1",
	}
	dec := json.NewDecoder(resp.Body)
	for {
		if limit > 0 && len(result.Messages) >= limit {
			break
		}
		var m Message
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return PollResult{}, fmt.Errorf("ntfy: decode message: %w", err)
		}
		if m.Event == "" || m.Event == "message" {
			result.Messages = append(result.Messages, m)
		}
	}
	return result, nil
}

// Stream is an open NDJSON subscription.
type Stream struct {
	Body      io.ReadCloser
	Truncated bool

	dec *json.Decoder
}

// Next decodes the next NDJSON object.
func (s *Stream) Next() (Message, error) {
	var m Message
	if err := s.dec.Decode(&m); err != nil {
		return Message{}, err
	}
	return m, nil
}

// Close closes the underlying response body.
func (s *Stream) Close() error {
	return s.Body.Close()
}

// OpenStream subscribes to a topic using since (verbatim). The caller must
// close the returned stream.
func (c *Client) OpenStream(ctx context.Context, topic, since string) (*Stream, error) {
	q := url.Values{}
	if since != "" {
		q.Set("since", since)
	}
	req, err := c.newRequest(ctx, http.MethodGet, c.endpoint(topic, true, q), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/x-ndjson")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	return &Stream{
		Body:      resp.Body,
		Truncated: resp.Header.Get("X-Messages-Truncated") == "1",
		dec:       json.NewDecoder(resp.Body),
	}, nil
}
