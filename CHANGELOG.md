# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project uses
[semantic versioning](https://semver.org/).

## [0.1.4] - 2026-11-18

### Fixed

- Stream notifications now carry the SEP-3415 demux key
  `_meta["io.modelcontextprotocol/subscriptionId"]`, set to the JSON-RPC id of
  the request that opened the stream. Clients route push frames by that key
  (`pi-mcp-events` on top of `pi-mcp-adapter` looks up its stream entry with
  it), so `events/stream` and `events/subscribe` no longer time out during
  activation. A client-supplied `subscriptionId` still wins; otherwise the
  request id is the subscription id.
- `notifications/events/event` frames now expose the SEP-3415 payload fields
  `name`, `timestamp` and `data` alongside the existing `event`, `eventId`,
  `cursor` and `payload` fields, so consumers that validate the draft shape
  accept the event instead of dropping it.
- `events/subscribe` results carry the same
  `_meta["io.modelcontextprotocol/subscriptionId"]` stamp.

## [0.1.3] - 2026-11-18

### Added

- `events/subscribe` implements the SEP-3415 subscription control method: it
  validates the event and filter like `events/poll`, registers the
  subscription, replies immediately with a stable `subscriptionId` (echoing
  the client's own id when supplied), and then pushes the existing
  `notifications/events/{active,event,heartbeat}` frames on the same
  `text/event-stream` response. Delivery ends on `events/unsubscribe`, client
  disconnect, or `NTFY_STREAM_MAX`.
- `events/unsubscribe` cancels by `subscriptionId` (or `_meta.subscriptionId`)
  and also accepts the `{"name":…,"arguments":{…}}` selector. Unknown ids and
  selectors return `-32023` with `data.kind: "subscription"`.
- Push subscriptions reuse the existing `cursor`/`maxAgeMs` replay window, so a
  resumed subscription gets the same watermark, replay floor and
  `truncated` reporting as `events/stream`.

### Changed

- Concurrent push subscriptions are capped at 32. A duplicate id returns
  `-32602`; a full registry returns `-32025` with `data.kind: "subscription"`.
- `events/subscribe` requires `Accept: text/event-stream` and is rejected in a
  batch, like `events/stream`.
- `subscriptions/listen` remains `-32601` (`listChanged` is still `false`).

### Tests

- Subscribe returns an id before any event frame; a published message arrives
  as `notifications/events/event`; unsubscribe ends delivery and releases the
  registry; two subscriptions do not receive each other's events; unknown
  events open no upstream; the replay floor and the subscription cap are
  covered.

## [0.1.2] - 2026-11-17

### Fixed

- `events/stream` accepts the SEP-3415 stream envelope
  `{"name":"<event>","arguments":{…}}` in addition to the flat parameter
  object: the event name is read from `name` and `arguments` is merged with
  the inline `cursor`/`maxAgeMs`/`maxEvents`. A client that subscribes by
  event name (`ntfy.message` + `{"topic":…}`) no longer gets
  `unknown event ""`.

## [0.1.1] - 2026-11-17

### Added

- Legacy `initialize` handshake compatibility path so clients pinned to the
  released MCP revision (`2025-11-25`) can attach. The server echoes the
  client's top-level `params.protocolVersion` for `2025-11-25`, `2025-06-18`,
  `2025-03-26` and `2024-11-05`, answers `2026-07-28` for anything else, and
  returns the same `capabilities` and `serverInfo` objects as
  `server/discover`.
- `notifications/initialized` is accepted silently: HTTP 202 with no response
  body and no JSON-RPC error.
- `.github/workflows/release.yml` publishes `ghcr.io/naqerl/ntfy-mcp` on `v*`
  tags using the workflow token.

### Changed

- `_meta.protocolVersion` older than `2026-07-28` stays rejected (`-32026`) on
  the native 2026 methods, but no longer applies to `initialize`.
- `server/discover`, the events extension, bearer auth, the `resultType:
  "complete"` shape and the two tools are unchanged; legacy clients send no
  `_meta`, so `tools/list` and `tools/call` work for them as before.

## [v0.1.0] - 2026-11-15

### Added

- Standalone Streamable-HTTP MCP server for ntfy with stdlib-only JSON-RPC,
  `server/discover`, tools, sessions (`Mcp-Session-Id`), and bearer auth.
- `ntfy_publish` and `ntfy_read` tools with ntfy header mapping, reserved-topic
  and argument validation, and upstream error mapping.
- SEP-3415 events extension (`io.modelcontextprotocol/events`) at revision
  `2026-07-28`, advertising `ntfy.message` with `["push","poll"]` delivery.
- `events/list`, `events/poll`, and `events/stream` with opaque cursors,
  from-now semantics, watermark active frames, heartbeats, filtered pushes, gap
  detection, and `truncated` reporting.
- SSE push over the originating POST with flush after every frame.
- Required environment validation, `/healthz`, graceful shutdown, non-root
  distroless image, Makefile, CI (vet/test/build/docker/smoke/publish), and the
  `scripts/smoke.sh` reference consumer.
- Package tests covering JSON-RPC framing, sessions, auth, SSE, cursors,
  truncation, filtering, ntfy request mapping, tools, and config failures.

### Known limitations

- SEP-3415 is a Draft and the target revision is newer than released clients;
  `scripts/smoke.sh` is the reference consumer.
- ntfy replay is cache-bound (10 MB/topic); only the truncation heuristic can
  detect a rolled-off gap.
- No webhook delivery, `subscriptions/listen`, stdio, pre-2026 revisions,
  OAuth, per-user authorisation, topic enumeration, or UI.
