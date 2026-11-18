# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project uses
[semantic versioning](https://semver.org/).

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
