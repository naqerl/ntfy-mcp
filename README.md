# ntfy-mcp

A standalone [MCP](https://modelcontextprotocol.io/) gateway for
[ntfy](https://ntfy.sh/) with two tools and the **SEP-3415 events extension**
(`io.modelcontextprotocol/events`) delivered as push notifications over
Streamable-HTTP SSE.

- Streamable HTTP only, standard library only, no MCP SDK in the request path.
- Stateless tool calls, session-scoped Streamable HTTP, bearer authentication.
- A subscription delivers only messages published **after** it starts unless the
  client supplies a cursor (see [Cursor semantics](#cursor-semantics)).
- The ntfy instance URL is always supplied through `NTFY_BASE_URL`; it is never
  baked into the binary or image.

## What SEP-3415 is

SEP-3415 defines a server-pushed **events** extension for MCP. A server
advertises event types, clients poll or stream them, and push delivery is
carried as JSON-RPC notifications on the same Streamable-HTTP request stream
that opened the subscription. The extension targets protocol revision
**`2026-07-28`** and later and relies on:

- per-request client capabilities in
  `_meta["io.modelcontextprotocol/clientCapabilities"]`,
- `server/discover` (there is no `initialize` handshake in this revision),
- the `extensions` member of server capabilities,
- `resultType: "complete"` on extension results.

This server speaks that revision natively. It **also** answers the classic
`2025-11-25` `initialize` handshake as a compatibility path so clients built on
released MCP revisions can attach; see [Compatibility](#compatibility). stdio
transport and pre-2025 revisions are out of scope.

## Quick start

```bash
docker run --rm -p 8080:8080 \
  -e MCP_AUTH_TOKEN='replace-me' \
  -e NTFY_BASE_URL='https://ntfy.example.com' \
  -e NTFY_TOKEN='optional-upstream-bearer' \
  ghcr.io/naqerl/ntfy-mcp:v0.1.0
```

Or build/run locally:

```bash
make build
MCP_AUTH_TOKEN=replace-me NTFY_BASE_URL=http://127.0.0.1:2586 ./bin/ntfy-mcp
```

`GET /healthz` is open and returns `{"status":"ok","version":"..."}`. Every
request to `MCP_PATH` requires `Authorization: Bearer <MCP_AUTH_TOKEN>`; the
comparison uses `crypto/subtle.ConstantTimeCompare` and authentication happens
before the request body is parsed.

## Configuration

All configuration is environment based and validated at startup. Invalid or
missing required values exit with status 1.

| Variable | Default | Meaning |
| --- | --- | --- |
| `MCP_ADDR` | `:8080` | listen address for `http.Server` |
| `MCP_PATH` | `/mcp` | Streamable-HTTP endpoint path |
| `MCP_AUTH_TOKEN` | **required** | bearer token required on every `/mcp` request; empty exits 1 |
| `NTFY_BASE_URL` | **required** | ntfy instance base URL, no default; empty/unparsable exits 1 |
| `NTFY_TOKEN` | empty | optional upstream `Authorization: Bearer` token |
| `NTFY_DEFAULT_TOPIC` | empty | fallback topic for tools when `topic` is omitted |
| `NTFY_HEARTBEAT` | `25s` | events/stream heartbeat; may not exceed `30s` |
| `NTFY_POLL_MS` | `3000` | default `nextPollMs` in `events/poll` |
| `NTFY_MAX_EVENTS` | `50` | default/max page size for event polls (ceiling 200) |
| `NTFY_STREAM_MAX` | `1h` | maximum lifetime of one `events/stream` request |
| `VERSION` | `dev` | injected with `-ldflags "-X main.VERSION=..."` |

The HTTP server deliberately sets `WriteTimeout: 0`: event streams are
long-lived SSE responses. `ReadHeaderTimeout` and idle timeouts remain bounded.

## Reserved ntfy topics

The ntfy application serves these route names itself. `ntfy_publish`,
`ntfy_read`, and every event filter reject them with `-32602` **before any
upstream call**:

`docs`, `static`, `file`, `app`, `metrics`, `account`, `settings`, `signup`,
`login`, `v1`

Topics remain implicit: ntfy exposes no topic-enumeration API (`/v1/topics` is
404) and there is no OpenAPI document. Topic names must match
`[-_A-Za-z0-9]{1,64}`.

## MCP surface

### `server/discover`

```json
{
  "protocolVersion": "2026-07-28",
  "serverInfo": {"name": "ntfy-mcp", "version": "v0.1.0"},
  "capabilities": {
    "tools": {"listChanged": false},
    "extensions": {
      "io.modelcontextprotocol/events": {"listChanged": false}
    }
  },
  "resultType": "complete"
}
```

Capability negotiation is per request. A `_meta.protocolVersion` at or below
`2026-07-28` is served in that client's mode: a legacy revision such as
`2025-11-25` is accepted (released clients stamp every request, extension
streams included, with their negotiated revision), and only a revision newer
than the server knows is rejected with `-32026`. On the native `2026-07-28`
revision an explicit client capability set that omits the events extension is
rejected for `events/*`; legacy clients skip that per-request gate. Omitting
`_meta` (as curl recipes and the smoke script do) is accepted. The legacy
`initialize` handshake keeps its top-level version negotiation.

## Compatibility

The server accepts two handshakes on the same endpoint.

**Native 2026-07-28.** Clients call `server/discover` (or any method) directly,
optionally sending per-request capabilities in
`_meta["io.modelcontextprotocol/clientCapabilities"]`. No session handshake is
required beyond the `Mcp-Session-Id` transport header.

**Legacy `2025-11-25` `initialize`.** Clients that always start with the
released handshake send:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{
  "protocolVersion":"2025-11-25","capabilities":{},
  "clientInfo":{"name":"client","version":"1.0"}}}
```

and receive the same `capabilities` and `serverInfo` objects `server/discover`
returns:

```json
{"protocolVersion":"2025-11-25",
 "capabilities":{"tools":{"listChanged":false},
                 "extensions":{"io.modelcontextprotocol/events":{"listChanged":false}}},
 "serverInfo":{"name":"ntfy-mcp","version":"v0.1.1"},
 "resultType":"complete"}
```

The negotiated version rule is simple: echo the client's `protocolVersion` when
it is one of `2025-11-25`, `2025-06-18`, `2025-03-26` or `2024-11-05`;
otherwise answer `2026-07-28`. The follow-up `notifications/initialized` is
accepted silently (HTTP 202, no body). After either handshake, `tools/list` and
`tools/call` behave identically — legacy clients send no `_meta`, which the
implementation treats as an empty capability envelope.

### Sessions and transport

- `POST` without `Mcp-Session-Id` creates a session and returns the header.
- `POST` with an unknown/expired session returns HTTP 404.
- `GET` with a valid session opens the server-to-client SSE channel.
- `DELETE` with a valid session terminates it and returns 204.
- Requests must send `Accept: text/event-stream` for `events/stream` and
  `events/subscribe` (both keep the POST open).
- No credentials are accepted in URLs or query strings.

### Tools — exactly two

#### `ntfy_publish`

Arguments: `topic` and `message` are required; `title`, `priority` (1–5),
`tags` (string array), `click` (absolute URI), `markdown` (boolean), `delay` are
optional. The message is the request body; the other fields map to `X-Title`,
`X-Priority`, `X-Tags`, `X-Click`, `X-Markdown`, and `X-Delay`.

Result:

```json
{"id": "ntfy-message-id", "topic": "alerts", "time": 1780000000}
```

#### `ntfy_read`

Arguments: `topic`, `since` (default `10m`; accepts `all`, `latest`, a duration,
a unix timestamp, or a message id), and `limit` (default 50, maximum 200).

Result:

```json
{
  "topic": "alerts",
  "cursor": "ntfy-message-id",
  "messages": [
    {"id": "ntfy-message-id", "time": 1780000000, "event": "message", "topic": "alerts", "message": "hello"}
  ]
}
```

`cursor` is the last message id, or `""` when no messages were returned.

### Events extension

One event type is advertised:

```json
{
  "name": "ntfy.message",
  "delivery": ["push", "poll"],
  "inputSchema": {
    "type": "object",
    "properties": {
      "topic": {"type": "string"},
      "tags": {"type": "array", "items": {"type": "string"}},
      "minPriority": {"type": "integer", "minimum": 1, "maximum": 5}
    },
    "required": ["topic"],
    "additionalProperties": false
  },
  "payloadSchema": {
    "type": "object",
    "properties": {
      "id": {"type": "string"},
      "time": {"type": "string", "format": "date-time"},
      "topic": {"type": "string"},
      "title": {"type": "string"},
      "message": {"type": "string"},
      "tags": {"type": "array", "items": {"type": "string"}},
      "priority": {"type": "integer", "minimum": 1, "maximum": 5},
      "click": {"type": "string", "format": "uri"},
      "attachment": {"type": "object"}
    },
    "required": ["id", "time", "topic", "message"]
  }
}
```

- `events/list` returns that descriptor.
- `events/subscribe` validates the event and filter like `events/poll`, then
  replies immediately with a result carrying a stable `subscriptionId` (the
  client's own `subscriptionId` when supplied, otherwise the JSON-RPC request
  id) and keeps the POST open as `text/event-stream`. From then on it pushes
  the same `notifications/events/{active,event,heartbeat}` frames as
  `events/stream` until the subscription is cancelled with
  `events/unsubscribe`, the client disconnects, or `NTFY_STREAM_MAX` elapses.
  `cursor` and `maxAgeMs` replay semantics are shared with `events/stream`.
- `events/unsubscribe` cancels by `subscriptionId` (also accepted as
  `_meta.subscriptionId`) or, when no id is sent, by the
  `{"name":"ntfy.message","arguments":{…}}` selector; every matching
  subscription is cancelled. Unknown ids and selectors return `-32023` with
  `data.kind: "subscription"`.
- At most 32 push subscriptions are active at once: a duplicate id returns
  `-32602`, a full registry returns `-32025`.
- `subscriptions/listen` returns `-32601`; `listChanged` is `false`, so the
  subscription-list method is not offered.
- Unknown event names return `-32023` with `data.kind: "event"`.
- `events/poll` reads `maxEvents+1` upstream messages to compute `hasMore`,
  filters tags/priority, places `cursor` at the response level, and defaults
  `nextPollMs` to `NTFY_POLL_MS`.
- `events/stream` keeps the POST open as `text/event-stream`, flushes after every
  frame with `http.NewResponseController(w).Flush()`, and emits JSON-RPC
  notifications as SSE `data:` frames:
  - `notifications/events/active` — watermark or replay state,
  - `notifications/events/event` — one `ntfy.message` event, carrying
    `eventId`, `name`, `timestamp` and `data` (`payload` and `event` remain as
    compatibility aliases),
  - `notifications/events/heartbeat` — advances the cursor on quiet topics.
- `eventId` is the ntfy message id, so poll and push consumers can deduplicate.
- Every notification carries
  `_meta["io.modelcontextprotocol/subscriptionId"]`, the SEP-3415 demux key.
  Its value defaults to the JSON-RPC id of the request that opened the stream,
  which is what clients such as `pi-mcp-events` route on. The legacy
  `params.subscriptionId` and `_meta.subscriptionId` are still echoed for
  compatibility.

#### Cursor semantics

- **Absent and `null` cursors are identical: start now.** Upstream is opened
  with `since=<unix now>`, no cached history is delivered, and the first
  `notifications/events/active` carries a time-watermark cursor `t<unix>`.
- Cached history is read only when the client passes a cursor it previously
  received (`t<unix>` or an ntfy message id).
- `maxAgeMs` raises the replay floor. If a time cursor is older than the floor,
  the gap is reported as `truncated: true`.
- Heartbeats are at most every 30 seconds (25 s default), so the cursor advances
  even when a topic is quiet.
- If a reconnect's cursor has aged out, ntfy replays its cache. A replayed
  message older than the last delivered `id → time` (tracked in-process), or an
  `X-Messages-Truncated: 1` response header, triggers a fresh
  `notifications/events/active` with `truncated: true`.
- A `null` cursor on `events/poll` returns `events: []` without an upstream call
  and a fresh watermark cursor.

#### Copy-paste `curl -N` recipe

The first request creates a session; every later request reuses it. The stream
delivers only messages published after it opens.

```bash
export MCP_URL='http://127.0.0.1:8080/mcp'
export MCP_AUTH_TOKEN='replace-me'

# 1. Discover and capture the session id.
curl -sS -D /tmp/mcp.headers -o /tmp/mcp.discover.json \
  -X POST "$MCP_URL" \
  -H "Authorization: Bearer $MCP_AUTH_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"server/discover"}'

SESSION=$(awk 'BEGIN{IGNORECASE=1} /^Mcp-Session-Id:/{gsub("\r",""); print $2}' /tmp/mcp.headers)
echo "session: $SESSION"

# 2. Stream events (leave this running).
curl -sS -N -X POST "$MCP_URL" \
  -H "Authorization: Bearer $MCP_AUTH_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  -H "Mcp-Session-Id: $SESSION" \
  --data '{"jsonrpc":"2.0","id":2,"method":"events/stream","params":{"event":"ntfy.message","filter":{"topic":"demo"},"_meta":{"subscriptionId":"demo-1"}}}'

# 3. In another shell, publish to the same topic.
curl -sS -X POST "$MCP_URL" \
  -H "Authorization: Bearer $MCP_AUTH_TOKEN" \
  -H 'Content-Type: application/json' \
  -H "Mcp-Session-Id: $SESSION" \
  --data '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"ntfy_publish","arguments":{"topic":"demo","message":"hello from curl"}}}'
```

A fresh stream emits an `active` frame immediately and does **not** replay the
message from step 3 if it was published before step 2. A message published
after the stream opens arrives as a `notifications/events/event` frame, and a
`heartbeat` follows within the configured interval.

### Error codes

| Code | Meaning in this server |
| --- | --- |
| `-32700` | JSON parse error |
| `-32600` | invalid request / invalid batch |
| `-32601` | absent method (`subscriptions/listen`, unknown methods) |
| `-32602` | invalid params, invalid topic, reserved topic, duplicate subscription id |
| `-32603` | internal/upstream failure |
| `-32023` | not found; unknown event/subscription (`data.kind: "event"`/`"subscription"`) or upstream 404 (`data.kind: "topic"`) |
| `-32025` | resource exhausted; upstream 429 / replay-cache overrun / subscription cap |
| `-32026` | unsupported; older protocol revision or explicit client capability mismatch (`data: {feature, value}`) |

## Upstream contract used

- Publish: `POST {base}/{topic}` with the message as the body and the `X-*`
  headers above.
- Poll/replay: `GET {base}/{topic}/json?poll=1&since=<cursor>` reads cached
  NDJSON and closes.
- Subscribe: `GET {base}/{topic}/json?since=<cursor>` streams NDJSON with
  `event` values including `open`, `keepalive`, and `message`.
- `since=` accepts durations, unix timestamps, message ids, `all`, and `latest`.
- ntfy caps replay at 10 MB per topic and reports
  `X-Messages-Truncated: 1`; overrun can answer HTTP 429 with code `42905`.
  Those map to `-32025`.

## Layout

```
main.go                       flags, http.Server (WriteTimeout 0), /healthz, graceful shutdown
internal/config/config.go     env parsing + startup validation
internal/ntfy/client.go       Publish, Poll, Stream (NDJSON), error mapping
internal/ntfy/topics.go       reserved-topic and topic-name validation
internal/mcpx/jsonrpc.go      JSON-RPC types, batch decode, extension error codes
internal/mcpx/server.go       method dispatch, capability negotiation, resultType
internal/mcpx/stream.go       Streamable HTTP: POST/GET/DELETE, sessions, SSE writer
internal/tools/ntfy.go        ntfy_publish, ntfy_read
internal/events/events.go     events/list, events/poll, events/stream, cursors, gaps
Dockerfile  Makefile  .github/workflows/ci.yml  scripts/smoke.sh
README.md  AGENTS.md  CHANGELOG.md  .gitignore
```

The server intentionally does not use `modelcontextprotocol/go-sdk@v1.8.0`:
its default sending handler refuses unknown methods, custom sending methods are
client-only, and the connection is unexported, so the extension notification
cannot be written on the originating stream through the SDK.

## Tests and CI

```bash
make vet       # gofmt -l must be empty, go vet, go tool staticcheck
make test      # go test -race ./...
make build     # CGO_ENABLED=0, trimpath, -ldflags "-s -w -X main.VERSION=..."
make build-linux
make docker
```

Package tests cover JSON-RPC framing/batching/parse errors, dispatch, session
create/reuse/reject, SSE encoding, auth rejection, cursor mint/parse and
from-now/null behaviour, `hasMore`, truncated detection (header and stale id),
tag/priority filtering, heartbeat interval, `_meta.subscriptionId`, query
building for every `since` form, publish header mapping, 429→`-32025` and
404→`-32023`, NDJSON decoding against `httptest`, tool/config validation, and
the required-environment failure paths.

`scripts/smoke.sh` is the reference consumer. It runs end to end against a real
`binwiederhier/ntfy` container and checks: 401 without a bearer token,
`server/discover` with `listChanged:false`, exactly two tools, publish→read,
`events/list`, a fresh stream with `active` and no cached history, a
post-subscribe pushed message, a heartbeat, unknown event → `-32023`,
`events/subscribe` including its immediate result and a pushed event,
`events/unsubscribe`, reserved topics, non-root container user, and refusal to
start without `MCP_AUTH_TOKEN`/`NTFY_BASE_URL`.

CI runs `vet` → `test` → `build` (linux/amd64 + linux/arm64) → `docker` (image,
ntfy service container, smoke) and publishes multi-arch images to
`ghcr.io/naqerl/ntfy-mcp` on `v*` tags with `latest`, semver, and `sha-…` tags.

## Risks

- **SEP-3415 is a Draft and revision `2026-07-28` is newer than any released
  client.** `scripts/smoke.sh` is therefore the reference consumer. The frames
  follow the SEP text verbatim at the draft date pinned above; expect draft
  movement to require small wire changes.
- **ntfy replay is cache-bound (10 MB per topic).** A client that resumes from a
  cursor whose history has rolled off cannot recover the gap. The
  `truncated` heuristic (`X-Messages-Truncated` or a replayed message older than
  the last delivered `id → time`) is the only defence, and it is covered by
  tests for both poll and stream.
- Message ids are server-generated opaque values; this gateway treats any
  non-`t<unix>` cursor as an ntfy message id.
- There is no topic enumeration or OpenAPI document upstream; topics are
  implicit and must be known by the caller.
- The in-process `id → time` map is bounded and is not shared across replicas.
  Running multiple gateway replicas can therefore miss the stale-id half of the
  truncation heuristic; the upstream truncation header still applies.

## Non-goals

Webhook delivery, `subscriptions/listen` / `list_changed`, stdio transport,
pre-2026-07-28 revisions, OAuth, per-user authorisation, message deletion,
action buttons, attachments upload, topic listing, and UI.
