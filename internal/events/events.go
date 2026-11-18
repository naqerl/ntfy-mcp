// Package events implements the SEP-3415 events extension for ntfy. It owns
// cursor semantics, filtering, poll batching, and the push stream lifecycle.
package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/naqerl/ntfy-mcp/internal/config"
	"github.com/naqerl/ntfy-mcp/internal/mcpx"
	"github.com/naqerl/ntfy-mcp/internal/ntfy"
)

// EventName is the only event type exposed by this server.
const EventName = "ntfy.message"

const (
	activeMethod    = "notifications/events/active"
	eventMethod     = "notifications/events/event"
	heartbeatMethod = "notifications/events/heartbeat"

	maxSeen = 4096

	// maxSubscriptions bounds concurrent push subscriptions so a client cannot
	// exhaust the process with repeated subscribe calls.
	maxSubscriptions = 32
)

var (
	cursorIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	tagPattern      = regexp.MustCompile(`^[^,\s]+$`)
)

// Client is the ntfy surface used by the events extension.
type Client interface {
	Poll(ctx context.Context, topic, since string, limit int) (ntfy.PollResult, error)
	OpenStream(ctx context.Context, topic, since string) (*ntfy.Stream, error)
}

// Options configures poll and push behaviour. Zero values fall back to the
// documented defaults.
type Options struct {
	Heartbeat time.Duration
	PollMS    int
	MaxEvents int
	StreamMax time.Duration
}

// Service implements mcpx.EventProvider.
type Service struct {
	client    Client
	heartbeat time.Duration
	pollMS    int
	maxEvents int
	streamMax time.Duration
	now       func() time.Time

	mu   sync.Mutex
	seen map[string]int64

	subsMu sync.Mutex
	subs   map[string]*activeSubscription
}

// activeSubscription is a registered push subscription. cancel stops the
// forwarded stream; filter is retained so events/unsubscribe can select by
// name and arguments when the client does not echo the id.
type activeSubscription struct {
	id     string
	cancel context.CancelFunc
	filter Filter
}

// New constructs the events service.
func New(client Client, opts Options) *Service {
	if opts.Heartbeat <= 0 || opts.Heartbeat > config.MaxHeartbeat {
		opts.Heartbeat = config.DefaultHeartbeat
	}
	if opts.PollMS <= 0 {
		opts.PollMS = config.DefaultPollMS
	}
	if opts.MaxEvents <= 0 || opts.MaxEvents > config.MaxEventsLimit {
		opts.MaxEvents = config.DefaultMaxEvents
	}
	if opts.StreamMax <= 0 {
		opts.StreamMax = config.DefaultStreamMax
	}
	return &Service{
		client:    client,
		heartbeat: opts.Heartbeat,
		pollMS:    opts.PollMS,
		maxEvents: opts.MaxEvents,
		streamMax: opts.StreamMax,
		now:       time.Now,
		seen:      make(map[string]int64),
		subs:      make(map[string]*activeSubscription),
	}
}

// ListEvents advertises ntfy.message for both push and poll delivery.
func (s *Service) ListEvents() []mcpx.EventDescriptor {
	return []mcpx.EventDescriptor{{
		Name:     EventName,
		Delivery: []string{"push", "poll"},
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"topic":       map[string]any{"type": "string"},
				"tags":        map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"minPriority": map[string]any{"type": "integer", "minimum": 1, "maximum": 5},
			},
			"required":             []string{"topic"},
			"additionalProperties": false,
		},
		PayloadSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id":         map[string]any{"type": "string"},
				"time":       map[string]any{"type": "string", "format": "date-time"},
				"topic":      map[string]any{"type": "string"},
				"title":      map[string]any{"type": "string"},
				"message":    map[string]any{"type": "string"},
				"tags":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"priority":   map[string]any{"type": "integer", "minimum": 1, "maximum": 5},
				"click":      map[string]any{"type": "string", "format": "uri"},
				"attachment": map[string]any{"type": "object"},
			},
			"required": []string{"id", "time", "topic", "message"},
		},
	}}
}

// Cursor is the opaque cursor used by events/poll and events/stream. Before the
// first message it is a time watermark (t<unix>); afterwards it is the ntfy
// message id.
type Cursor struct {
	IsTime bool
	Time   int64
	ID     string
}

// Mint returns a time-watermark cursor.
func Mint(t time.Time) string {
	return "t" + strconv.FormatInt(t.Unix(), 10)
}

// ParseCursor parses a cursor previously minted by this package or an ntfy
// message id.
func ParseCursor(raw string) (Cursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Cursor{}, errors.New("empty cursor")
	}
	if strings.HasPrefix(raw, "t") && len(raw) > 1 {
		digits := raw[1:]
		allDigits := true
		for _, r := range digits {
			if r < '0' || r > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			if unix, err := strconv.ParseInt(digits, 10, 64); err == nil {
				return Cursor{IsTime: true, Time: unix}, nil
			}
		}
	}
	if !cursorIDPattern.MatchString(raw) {
		return Cursor{}, fmt.Errorf("invalid cursor %q", raw)
	}
	return Cursor{ID: raw}, nil
}

// String renders the cursor in wire form.
func (c Cursor) String() string {
	if c.IsTime {
		return "t" + strconv.FormatInt(c.Time, 10)
	}
	return c.ID
}

// Since translates the cursor to an ntfy since value, raising the replay floor
// to now-maxAge when that floor is newer than a time cursor. The bool is true
// when the floor introduced a gap.
func (c Cursor) Since(now time.Time, maxAge time.Duration) (string, bool) {
	if !c.IsTime {
		return c.ID, false
	}
	if maxAge > 0 {
		floor := now.Add(-maxAge).Unix()
		if floor > c.Time {
			return strconv.FormatInt(floor, 10), true
		}
	}
	return strconv.FormatInt(c.Time, 10), false
}

// Filter is the event input schema applied to upstream messages.
type Filter struct {
	Topic       string   `json:"topic"`
	Tags        []string `json:"tags,omitempty"`
	MinPriority *int     `json:"minPriority,omitempty"`
}

func (f Filter) validate() *mcpx.Error {
	if strings.TrimSpace(f.Topic) == "" {
		return invalidParams("filter.topic is required")
	}
	if err := ntfy.ValidateTopic(f.Topic); err != nil {
		return invalidParams(err.Error())
	}
	if f.MinPriority != nil && (*f.MinPriority < 1 || *f.MinPriority > 5) {
		return invalidParams("filter.minPriority must be between 1 and 5")
	}
	for _, tag := range f.Tags {
		if !tagPattern.MatchString(tag) {
			return invalidParams("filter.tags must be non-empty and must not contain commas or whitespace")
		}
	}
	return nil
}

// Match reports whether a message satisfies the filter.
func (f Filter) Match(m ntfy.Message) bool {
	if f.Topic != "" && m.Topic != "" && m.Topic != f.Topic {
		return false
	}
	if f.MinPriority != nil {
		priority := m.Priority
		if priority == 0 {
			priority = 3 // ntfy's default priority
		}
		if priority < *f.MinPriority {
			return false
		}
	}
	for _, want := range f.Tags {
		found := false
		for _, have := range m.Tags {
			if have == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

type eventParams struct {
	Event          string          `json:"event"`
	Cursor         json.RawMessage `json:"cursor"`
	MaxEvents      int             `json:"maxEvents"`
	MaxAgeMs       int64           `json:"maxAgeMs"`
	SubscriptionID string          `json:"subscriptionId"`
	Filter         *Filter         `json:"filter"`

	// Inline filter fields are accepted as an ergonomic alternative to the
	// nested filter object; the nested object wins when both are present.
	Topic       string   `json:"topic"`
	Tags        []string `json:"tags"`
	MinPriority *int     `json:"minPriority"`

	Meta struct {
		SubscriptionID string `json:"subscriptionId"`
	} `json:"_meta"`
}

func parseParams(raw json.RawMessage) (eventParams, *mcpx.Error) {
	var p eventParams
	if len(strings.TrimSpace(string(raw))) == 0 {
		return p, invalidParams("params are required")
	}
	raw = unwrapEventEnvelope(raw)
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if err := dec.Decode(&p); err != nil {
		return p, invalidParams("invalid params: " + err.Error())
	}
	if p.Event != EventName {
		return p, mcpx.NewError(mcpx.CodeNotFound,
			fmt.Sprintf("unknown event %q", p.Event),
			map[string]any{"kind": "event", "event": p.Event})
	}
	if p.MaxEvents < 0 || p.MaxEvents > config.MaxEventsLimit {
		return p, invalidParams(fmt.Sprintf("maxEvents must be between 0 and %d", config.MaxEventsLimit))
	}
	if p.MaxAgeMs < 0 || p.MaxAgeMs > int64((1<<63-1)/int64(time.Millisecond)) {
		return p, invalidParams("maxAgeMs is out of range")
	}
	if p.SubscriptionID == "" {
		p.SubscriptionID = p.Meta.SubscriptionID
	}

	filter := Filter{}
	if p.Filter != nil {
		filter = *p.Filter
	}
	if filter.Topic == "" {
		filter.Topic = p.Topic
	}
	if len(filter.Tags) == 0 {
		filter.Tags = p.Tags
	}
	if filter.MinPriority == nil {
		filter.MinPriority = p.MinPriority
	}
	filter.Topic = strings.TrimSpace(filter.Topic)
	if err := filter.validate(); err != nil {
		return p, err
	}
	p.Filter = &filter
	return p, nil
}

// unwrapEventEnvelope accepts the `{"name":…,"arguments":{…}}` stream
// parameters that SEP-3415 clients send, in addition to the flat parameter
// object this server documents. A flat object is returned untouched, and any
// argument the envelope carries inline wins over the same key in arguments.
func unwrapEventEnvelope(raw json.RawMessage) json.RawMessage {
	var env struct {
		Name      string                     `json:"name"`
		Event     string                     `json:"event"`
		Arguments map[string]json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return raw
	}
	if env.Event != "" || strings.TrimSpace(env.Name) == "" {
		return raw
	}

	merged := make(map[string]json.RawMessage, len(env.Arguments)+5)
	for key, value := range env.Arguments {
		merged[key] = value
	}
	name, err := json.Marshal(env.Name)
	if err != nil {
		return raw
	}
	merged["event"] = name

	// cursor, maxAgeMs, maxEvents and subscriptionId live next to name in the
	// stream request, not inside arguments.
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err == nil {
		for _, key := range []string{"cursor", "maxAgeMs", "maxEvents", "subscriptionId"} {
			if _, taken := merged[key]; taken {
				continue
			}
			if value, ok := top[key]; ok && len(strings.TrimSpace(string(value))) > 0 {
				merged[key] = value
			}
		}
	}
	if env.Arguments == nil {
		// No arguments object: nothing to merge, keep the original payload so
		// the caller still sees the real parse error.
		return raw
	}
	out, err := json.Marshal(merged)
	if err != nil {
		return raw
	}
	return out
}

// parseCursorParam distinguishes absent/null (from-now) from a cursor string.
func parseCursorParam(raw json.RawMessage) (*string, *mcpx.Error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, invalidParams("cursor must be a string or null")
	}
	if value == "" {
		return nil, nil
	}
	return &value, nil
}

// PollEvents performs a cached poll. A null/absent cursor is from-now and does
// not read cached history; the returned cursor is a fresh watermark.
func (s *Service) PollEvents(ctx context.Context, raw json.RawMessage) (any, error) {
	p, err := parseParams(raw)
	if err != nil {
		return nil, err
	}
	cursorParam, err := parseCursorParam(p.Cursor)
	if err != nil {
		return nil, err
	}
	now := s.now()
	if cursorParam == nil {
		return map[string]any{
			"events":     []any{},
			"cursor":     Mint(now),
			"hasMore":    false,
			"truncated":  false,
			"nextPollMs": s.pollMS,
			"_meta":      subscriptionMeta(p.SubscriptionID),
		}, nil
	}

	cursor, perr := ParseCursor(*cursorParam)
	if perr != nil {
		return nil, invalidParams(perr.Error())
	}
	maxEvents := p.MaxEvents
	if maxEvents == 0 {
		maxEvents = s.maxEvents
	}
	if maxEvents > config.MaxEventsLimit {
		return nil, invalidParams(fmt.Sprintf("maxEvents must be <= %d", config.MaxEventsLimit))
	}
	since, floorGap := cursor.Since(now, time.Duration(p.MaxAgeMs)*time.Millisecond)

	// Read one extra to distinguish a full page from a completed poll.
	polled, nerr := s.client.Poll(ctx, p.Filter.Topic, since, maxEvents+1)
	if nerr != nil {
		return nil, mapUpstreamError(nerr)
	}
	rawMessages := polled.Messages
	if rawMessages == nil {
		rawMessages = []ntfy.Message{}
	}
	hasMore := len(rawMessages) > maxEvents
	if hasMore {
		rawMessages = rawMessages[:maxEvents]
	}
	truncated := polled.Truncated || floorGap || s.isStale(cursor, rawMessages)

	out := make([]any, 0, len(rawMessages))
	for _, m := range rawMessages {
		s.recordSeen(m)
		if !p.Filter.Match(m) {
			continue
		}
		out = append(out, map[string]any{
			"event":   EventName,
			"eventId": m.ID,
			"payload": payloadFor(m),
		})
	}
	cursorOut := *cursorParam
	if len(rawMessages) > 0 {
		cursorOut = rawMessages[len(rawMessages)-1].ID
	}
	return map[string]any{
		"events":     out,
		"cursor":     cursorOut,
		"hasMore":    hasMore,
		"truncated":  truncated,
		"nextPollMs": s.pollMS,
		"_meta":      subscriptionMeta(p.SubscriptionID),
	}, nil
}

// StreamEvents opens the upstream NDJSON subscription and pushes events until
// the client disconnects, the stream max age elapses, or upstream closes.
func (s *Service) StreamEvents(ctx context.Context, raw json.RawMessage, emit func(mcpx.Notification) error) error {
	p, err := parseParams(raw)
	if err != nil {
		return err
	}
	cursorParam, err := parseCursorParam(p.Cursor)
	if err != nil {
		return err
	}
	sid := p.SubscriptionID
	if sid == "" {
		sid = newSubscriptionID()
	}
	setup, oerr := s.openStream(ctx, p, cursorParam)
	if oerr != nil {
		return oerr
	}
	defer setup.stream.Close()

	return s.forward(ctx, setup, p, sid, emit)
}

// streamSetup is the opened upstream stream plus the cursor state derived from
// the request.
type streamSetup struct {
	stream    *ntfy.Stream
	cursor    Cursor
	cursorOut string
	truncated bool
}

// openStream resolves the cursor/maxAgeMs replay window and opens the upstream
// NDJSON stream. It performs no I/O beyond opening the stream, so callers can
// still report a clean JSON-RPC error before starting to push frames.
func (s *Service) openStream(ctx context.Context, p eventParams, cursorParam *string) (*streamSetup, error) {
	now := s.now()
	var cursor Cursor
	var since string
	truncated := false
	if cursorParam == nil {
		// From-now: never replay cache. The first active frame carries the
		// watermark so clients have a resumable cursor immediately.
		since = strconv.FormatInt(now.Unix(), 10)
	} else {
		parsed, perr := ParseCursor(*cursorParam)
		if perr != nil {
			return nil, invalidParams(perr.Error())
		}
		cursor = parsed
		since, truncated = cursor.Since(now, time.Duration(p.MaxAgeMs)*time.Millisecond)
	}

	stream, oerr := s.client.OpenStream(ctx, p.Filter.Topic, since)
	if oerr != nil {
		return nil, mapUpstreamError(oerr)
	}
	if stream.Truncated {
		truncated = true
	}
	cursorOut := Mint(now)
	if cursorParam != nil {
		cursorOut = *cursorParam
	}
	return &streamSetup{stream: stream, cursor: cursor, cursorOut: cursorOut, truncated: truncated}, nil
}

// forward pushes the active frame and then every matching message until ctx
// ends, the stream max age elapses, or upstream closes.
func (s *Service) forward(ctx context.Context, setup *streamSetup, p eventParams, sid string, emit func(mcpx.Notification) error) error {
	stream := setup.stream
	cursor := setup.cursor
	cursorOut := setup.cursorOut
	reportedGap := setup.truncated
	if eerr := emit(activeNotification(sid, EventName, cursorOut, setup.truncated)); eerr != nil {
		return eerr
	}

	lastTime := int64(0)
	if cursor.IsTime {
		lastTime = cursor.Time
	} else if cursor.ID != "" {
		lastTime = s.seenTime(cursor.ID)
	}

	type streamItem struct {
		msg ntfy.Message
		err error
	}
	items := make(chan streamItem, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		defer close(items)
		for {
			m, rerr := stream.Next()
			if rerr != nil {
				select {
				case items <- streamItem{err: rerr}:
				case <-done:
				}
				return
			}
			select {
			case items <- streamItem{msg: m}:
			case <-done:
				return
			}
		}
	}()

	ticker := time.NewTicker(s.heartbeat)
	defer ticker.Stop()
	maxTimer := time.NewTimer(s.streamMax)
	defer maxTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-maxTimer.C:
			return nil
		case <-ticker.C:
			// Advance the watermark on quiet topics so a reconnect does not
			// replay the cache unnecessarily.
			cursorOut = Mint(s.now())
			if eerr := emit(heartbeatNotification(sid, EventName, cursorOut)); eerr != nil {
				return eerr
			}
		case item, ok := <-items:
			if !ok {
				return nil
			}
			if item.err != nil {
				if errors.Is(item.err, io.EOF) || ctx.Err() != nil {
					return nil
				}
				// Report a reset connection or malformed NDJSON. The HTTP
				// handler writes this as an SSE error frame because the active
				// frame has already started the stream.
				return mapUpstreamError(item.err)
			}
			m := item.msg
			if m.Event != "" && m.Event != "message" {
				continue
			}
			if m.ID == "" {
				continue
			}
			if !reportedGap && ((lastTime > 0 && m.Time > 0 && m.Time < lastTime) || s.isStale(cursor, []ntfy.Message{m})) {
				reportedGap = true
				if eerr := emit(activeNotification(sid, EventName, cursorOut, true)); eerr != nil {
					return eerr
				}
			}
			if !p.Filter.Match(m) {
				continue
			}
			cursorOut = m.ID
			if m.Time > lastTime {
				lastTime = m.Time
			}
			s.recordSeen(m)
			if eerr := emit(messageNotification(sid, m)); eerr != nil {
				return eerr
			}
		}
	}
}

// SubscribeEvents registers a push subscription, replies with its stable
// subscription id, and then forwards events until the subscription is
// cancelled (events/unsubscribe), the caller's context ends, or upstream
// closes. Validation matches events/poll and events/stream.
func (s *Service) SubscribeEvents(ctx context.Context, raw json.RawMessage, respond func(any) error, emit func(mcpx.Notification) error) error {
	p, err := parseParams(raw)
	if err != nil {
		return err
	}
	cursorParam, err := parseCursorParam(p.Cursor)
	if err != nil {
		return err
	}
	sid := p.SubscriptionID
	if sid == "" {
		sid = newSubscriptionID()
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if rerr := s.addSubscription(sid, cancel, *p.Filter); rerr != nil {
		return rerr
	}
	defer s.removeSubscription(sid)

	// Open upstream before replying so a failure is a clean JSON-RPC error
	// rather than an error frame following a successful result.
	setup, oerr := s.openStream(subCtx, p, cursorParam)
	if oerr != nil {
		return oerr
	}
	defer setup.stream.Close()

	if rerr := respond(map[string]any{
		"subscriptionId": sid,
		"event":          EventName,
		"cursor":         setup.cursorOut,
		"_meta":          subscriptionMeta(sid),
	}); rerr != nil {
		return rerr
	}
	return s.forward(subCtx, setup, p, sid, emit)
}

// UnsubscribeEvents cancels one subscription by id, or every subscription that
// matches an event selector when no id is supplied.
func (s *Service) UnsubscribeEvents(_ context.Context, raw json.RawMessage) (any, error) {
	sel, err := parseUnsubscribe(raw)
	if err != nil {
		return nil, err
	}

	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	if sel.subscriptionID != "" {
		sub, ok := s.subs[sel.subscriptionID]
		if !ok {
			return nil, subscriptionNotFound(sel.subscriptionID)
		}
		delete(s.subs, sub.id)
		sub.cancel()
		return map[string]any{
			"unsubscribed":    true,
			"subscriptionIds": []string{sub.id},
		}, nil
	}

	ids := make([]string, 0, 1)
	for id, sub := range s.subs {
		if sel.matches(sub) {
			delete(s.subs, id)
			sub.cancel()
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil, mcpx.NewError(mcpx.CodeNotFound, "no matching subscription",
			map[string]any{"kind": "subscription"})
	}
	sort.Strings(ids)
	return map[string]any{
		"unsubscribed":    true,
		"subscriptionIds": ids,
	}, nil
}

func (s *Service) addSubscription(id string, cancel context.CancelFunc, filter Filter) error {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	if s.subs == nil {
		s.subs = make(map[string]*activeSubscription)
	}
	if _, exists := s.subs[id]; exists {
		return invalidParams(fmt.Sprintf("subscription %q already exists", id))
	}
	if len(s.subs) >= maxSubscriptions {
		return mcpx.NewError(mcpx.CodeResourceExhausted,
			fmt.Sprintf("too many concurrent subscriptions (max %d)", maxSubscriptions),
			map[string]any{"kind": "subscription", "limit": maxSubscriptions})
	}
	s.subs[id] = &activeSubscription{id: id, cancel: cancel, filter: filter}
	return nil
}

func (s *Service) removeSubscription(id string) {
	s.subsMu.Lock()
	delete(s.subs, id)
	s.subsMu.Unlock()
}

// unsubscribeSelector is either an id or an event/filter selector. An empty
// event matches every registered event type.
type unsubscribeSelector struct {
	subscriptionID string
	event          string
	filter         *Filter
}

func (sel unsubscribeSelector) matches(sub *activeSubscription) bool {
	if sel.event != "" && sel.event != EventName {
		return false
	}
	if sel.filter == nil {
		return true
	}
	if sel.filter.Topic != "" && sel.filter.Topic != sub.filter.Topic {
		return false
	}
	if sel.filter.MinPriority != nil {
		if sub.filter.MinPriority == nil || *sub.filter.MinPriority != *sel.filter.MinPriority {
			return false
		}
	}
	for _, want := range sel.filter.Tags {
		found := false
		for _, have := range sub.filter.Tags {
			if have == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// parseUnsubscribe accepts {"subscriptionId":…}, _meta.subscriptionId, or the
// SEP-3415 selector {"name":<event>,"arguments":{…}}. The stream-envelope
// unwrapping is reused so a selector looks exactly like a subscribe request.
func parseUnsubscribe(raw json.RawMessage) (unsubscribeSelector, *mcpx.Error) {
	var sel unsubscribeSelector
	if len(strings.TrimSpace(string(raw))) == 0 {
		return sel, invalidParams("params are required")
	}
	var p struct {
		SubscriptionID string   `json:"subscriptionId"`
		Event          string   `json:"event"`
		Name           string   `json:"name"`
		Filter         *Filter  `json:"filter"`
		Topic          string   `json:"topic"`
		Tags           []string `json:"tags"`
		MinPriority    *int     `json:"minPriority"`
		Meta           struct {
			SubscriptionID string `json:"subscriptionId"`
		} `json:"_meta"`
	}
	if err := json.Unmarshal(unwrapEventEnvelope(raw), &p); err != nil {
		return sel, invalidParams("invalid params: " + err.Error())
	}
	sel.subscriptionID = strings.TrimSpace(p.SubscriptionID)
	if sel.subscriptionID == "" {
		sel.subscriptionID = strings.TrimSpace(p.Meta.SubscriptionID)
	}
	sel.event = p.Event
	if sel.event == "" {
		sel.event = p.Name
	}
	if sel.subscriptionID != "" {
		return sel, nil
	}
	if sel.event != "" && sel.event != EventName {
		return sel, mcpx.NewError(mcpx.CodeNotFound,
			fmt.Sprintf("unknown event %q", sel.event),
			map[string]any{"kind": "event", "event": sel.event})
	}

	filter := Filter{}
	if p.Filter != nil {
		filter = *p.Filter
	}
	if filter.Topic == "" {
		filter.Topic = p.Topic
	}
	if len(filter.Tags) == 0 {
		filter.Tags = p.Tags
	}
	if filter.MinPriority == nil {
		filter.MinPriority = p.MinPriority
	}
	filter.Topic = strings.TrimSpace(filter.Topic)
	if filter.Topic != "" {
		if err := filter.validate(); err != nil {
			return sel, err
		}
	}
	if sel.event == "" && filter.Topic == "" && len(filter.Tags) == 0 && filter.MinPriority == nil {
		return sel, invalidParams("subscriptionId or an event selector is required")
	}
	if filter.Topic != "" || len(filter.Tags) > 0 || filter.MinPriority != nil {
		sel.filter = &filter
	}
	return sel, nil
}

func subscriptionNotFound(id string) *mcpx.Error {
	return mcpx.NewError(mcpx.CodeNotFound,
		fmt.Sprintf("unknown subscription %q", id),
		map[string]any{"kind": "subscription", "subscriptionId": id})
}

// Payload is the ntfy.message event payload.
type Payload struct {
	ID         string           `json:"id"`
	Time       string           `json:"time"`
	Topic      string           `json:"topic"`
	Title      string           `json:"title,omitempty"`
	Message    string           `json:"message"`
	Tags       []string         `json:"tags,omitempty"`
	Priority   int              `json:"priority,omitempty"`
	Click      string           `json:"click,omitempty"`
	Attachment *ntfy.Attachment `json:"attachment,omitempty"`
}

func payloadFor(m ntfy.Message) Payload {
	return Payload{
		ID:         m.ID,
		Time:       time.Unix(m.Time, 0).UTC().Format(time.RFC3339),
		Topic:      m.Topic,
		Title:      m.Title,
		Message:    m.Message,
		Tags:       m.Tags,
		Priority:   m.Priority,
		Click:      m.Click,
		Attachment: m.Attachment,
	}
}

func activeNotification(sid, event, cursor string, truncated bool) mcpx.Notification {
	params := map[string]any{
		"event":          event,
		"cursor":         cursor,
		"truncated":      truncated,
		"subscriptionId": sid,
	}
	if sid != "" {
		params["_meta"] = subscriptionMeta(sid)
	}
	return mcpx.NewNotification(activeMethod, params)
}

func messageNotification(sid string, m ntfy.Message) mcpx.Notification {
	params := map[string]any{
		"event":          EventName,
		"eventId":        m.ID,
		"cursor":         m.ID,
		"subscriptionId": sid,
		"payload":        payloadFor(m),
	}
	if sid != "" {
		params["_meta"] = subscriptionMeta(sid)
	}
	return mcpx.NewNotification(eventMethod, params)
}

func heartbeatNotification(sid, event, cursor string) mcpx.Notification {
	params := map[string]any{
		"event":          event,
		"cursor":         cursor,
		"subscriptionId": sid,
	}
	if sid != "" {
		params["_meta"] = subscriptionMeta(sid)
	}
	return mcpx.NewNotification(heartbeatMethod, params)
}

func subscriptionMeta(sid string) map[string]any {
	return map[string]any{"subscriptionId": sid}
}

func newSubscriptionID() string {
	var buf [12]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "sub_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return "sub_" + hex.EncodeToString(buf[:])
}

func (s *Service) recordSeen(m ntfy.Message) {
	if m.ID == "" || m.Time == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seen) >= maxSeen {
		for id := range s.seen {
			delete(s.seen, id)
			break
		}
	}
	s.seen[m.ID] = m.Time
}

func (s *Service) seenTime(id string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[id]
}

// isStale applies the stale-cursor heuristic: a replay containing a message
// older than the cursor watermark means the cache rolled over and the client
// missed history.
func (s *Service) isStale(cursor Cursor, messages []ntfy.Message) bool {
	if len(messages) == 0 {
		return false
	}
	var baseline int64
	if cursor.IsTime {
		baseline = cursor.Time
	} else if cursor.ID != "" {
		baseline = s.seenTime(cursor.ID)
	}
	if baseline == 0 {
		return false
	}
	for _, m := range messages {
		if m.Time > 0 && m.Time < baseline {
			return true
		}
	}
	return false
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

func invalidParams(msg string) *mcpx.Error {
	return mcpx.NewError(mcpx.CodeInvalidParams, msg, nil)
}
