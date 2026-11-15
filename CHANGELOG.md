# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project uses
[semantic versioning](https://semver.org/).

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
