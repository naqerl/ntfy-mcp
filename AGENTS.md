# AGENTS.md

Guidance for agents working on `github.com/naqerl/ntfy-mcp`.

## What this repository is

A standalone Go implementation of an MCP Streamable-HTTP server for ntfy. It
owns the JSON-RPC/HTTP core because SEP-3415 requires extension notifications to
be written on the originating request's SSE stream, which the pinned Go SDK does
not expose. Do not reintroduce the SDK just for transport convenience.

## Invariants

- **No baked-in ntfy URL.** `NTFY_BASE_URL` is required and validated at
  startup. `MCP_AUTH_TOKEN` is also required; both must exit 1 when absent.
- **Exactly two tools:** `ntfy_publish` and `ntfy_read`. Do not add a third.
- **One event type:** `ntfy.message`, delivery `["push","poll"]`.
- **SEP-3415 revision is `2026-07-28`.** No `initialize` handshake, no stdio,
  no pre-2026 compatibility layer. Successful results carry
  `resultType: "complete"`.
- **From-now semantics:** absent and `null` cursors mean "start now". Never
  deliver cached history for a fresh subscription. The first `active` frame
  carries a `t<unix>` watermark.
- **Opaque cursor:** `t<unix>` before the first message, ntfy message id
  afterwards. `eventId` is the ntfy message id so poll/push dedupe works.
- **Heartbeat ≤ 30s** (default 25s). Flush after every SSE frame using
  `http.NewResponseController(w).Flush()`.
- **Reserved topics** (`docs`, `static`, `file`, `app`, `metrics`, `account`,
  `settings`, `signup`, `login`, `v1`) must be rejected with `-32602` before any
  upstream request.
- **Error codes:** `-32602` invalid params, `-32023` not found
  (`data.kind:"event"` for events), `-32025` resource exhausted (upstream 429),
  `-32026` unsupported, `-32601` absent methods.
- **Auth:** every `/mcp` request needs `Authorization: Bearer <token>`
  compared with `crypto/subtle.ConstantTimeCompare`; reject before parsing.
  `/healthz` stays open.
- **WriteTimeout stays 0** for long-lived streams.
- Keep `gofmt`, `go vet`, and `go tool staticcheck` clean. CI treats output from
  `gofmt -l` as failure.

## Build and test

```bash
go test -race ./...
make vet
make build
make build-linux
scripts/smoke.sh   # needs a running gateway + real ntfy
```

Dependencies are standard library only for the runtime. `honnef.co/go/tools`
is a `tool` directive dependency used by `make vet`; lockfile changes must be
committed. Do not add an ntfy or MCP client library without a strong reason.

## When changing protocol payloads

Update all of:

1. the implementation (`internal/events`, `internal/mcpx`),
2. package tests beside the code,
3. `scripts/smoke.sh` (the reference consumer),
4. the README protocol/cursor/error sections,
5. `CHANGELOG.md`.

Keep the draft date pinned in the README unless intentionally moving the target.
Do not weaken or delete tests to make CI pass; fix the behaviour instead.
