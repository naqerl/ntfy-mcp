#!/usr/bin/env bash
# scripts/smoke.sh is the reference SEP-3415 consumer. It exercises the
# gateway end to end against a real ntfy instance and is used by CI's docker
# job. It intentionally uses the raw JSON-RPC frames (not an SDK), because the
# extension is a draft newer than any released client.
set -euo pipefail

MCP_URL="${MCP_URL:-http://127.0.0.1:8080/mcp}"
MCP_AUTH_TOKEN="${MCP_AUTH_TOKEN:-smoke-token}"
HEALTH_URL="${HEALTH_URL:-${MCP_URL%/mcp}/healthz}"
MCP_IMAGE="${MCP_IMAGE:-}"

command -v curl >/dev/null || { echo "smoke: curl is required" >&2; exit 1; }
command -v jq >/dev/null || { echo "smoke: jq is required" >&2; exit 1; }

SESSION=""
SESSION_FILE="$(mktemp)"
FRAMES="$(mktemp)"
SUB_FRAMES="$(mktemp)"
STREAM_PID=""
SUB_PID=""
cleanup() {
  if [[ -n "$STREAM_PID" ]]; then
    kill "$STREAM_PID" 2>/dev/null || true
    wait "$STREAM_PID" 2>/dev/null || true
  fi
  if [[ -n "$SUB_PID" ]]; then
    kill "$SUB_PID" 2>/dev/null || true
    wait "$SUB_PID" 2>/dev/null || true
  fi
  rm -f "$FRAMES" "$SUB_FRAMES" "$SESSION_FILE"
}
trap cleanup EXIT

fail() {
  echo "smoke FAILED: $*" >&2
  if [[ -s "$FRAMES" ]]; then
    echo "--- last stream frames ---" >&2
    tail -n 20 "$FRAMES" >&2 || true
  fi
  exit 1
}

wait_for() {
  local pattern="$1" timeout="${2:-10}"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if grep -q -- "$pattern" "$FRAMES"; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

wait_for_file() {
  local file="$1" pattern="$2" timeout="${3:-10}"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if grep -q -- "$pattern" "$file"; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

# rpc posts one JSON-RPC request/response payload, reusing/creating the session.
rpc() {
  local payload="$1" accept="${2:-application/json}"
  local hdr body session
  session="$(cat "$SESSION_FILE" 2>/dev/null || true)"
  hdr="$(mktemp)"
  body="$(mktemp)"
  local args=(
    -sS -X POST "$MCP_URL"
    -H "Authorization: Bearer $MCP_AUTH_TOKEN"
    -H "Content-Type: application/json"
    -H "Accept: $accept"
  )
  if [[ -n "$session" ]]; then
    args+=(-H "Mcp-Session-Id: $session")
  fi
  curl "${args[@]}" -D "$hdr" -o "$body" --data-binary "$payload"
  if [[ -z "$session" ]]; then
    session="$(awk 'BEGIN{IGNORECASE=1} /^Mcp-Session-Id:/{gsub("\r",""); print $2}' "$hdr" | tail -n 1)"
    printf '%s' "$session" >"$SESSION_FILE"
  fi
  cat "$body"
  rm -f "$hdr" "$body"
}

echo "smoke: waiting for $HEALTH_URL"
healthy=0
for _ in $(seq 1 100); do
  if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 0.2
done
[[ "$healthy" == 1 ]] || fail "gateway health check did not become ready"

echo "smoke: 401 without bearer"
status="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$MCP_URL" \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"server/discover"}')"
[[ "$status" == "401" ]] || fail "unauthenticated request returned $status"

echo "smoke: server/discover advertises the events extension"
discover="$(rpc '{"jsonrpc":"2.0","id":1,"method":"server/discover"}')"
SESSION="$(cat "$SESSION_FILE" 2>/dev/null || true)"
jq -e '.result.protocolVersion == "2026-07-28"' <<<"$discover" >/dev/null ||
  fail "unexpected protocolVersion: $discover"
jq -e '.result.capabilities.extensions["io.modelcontextprotocol/events"].listChanged == false' <<<"$discover" >/dev/null ||
  fail "events extension is not advertised with listChanged:false: $discover"
[[ -n "$SESSION" ]] || fail "server did not issue Mcp-Session-Id"

echo "smoke: tools/list contains exactly ntfy_publish and ntfy_read"
tools="$(rpc '{"jsonrpc":"2.0","id":2,"method":"tools/list"}')"
jq -e '.result.tools | length == 2' <<<"$tools" >/dev/null ||
  fail "tools/list length: $tools"
jq -e '[.result.tools[].name] | sort == ["ntfy_publish","ntfy_read"]' <<<"$tools" >/dev/null ||
  fail "unexpected tools: $tools"

TOPIC="smoke-$(date +%s)-$$"
CACHED="cached-${RANDOM}-${RANDOM}"
AFTER="after-${RANDOM}-${RANDOM}"

echo "smoke: publish/read round-trip on topic $TOPIC"
publish_payload="$(jq -nc --arg t "$TOPIC" --arg m "$CACHED" \
  '{jsonrpc:"2.0",id:3,method:"tools/call",params:{name:"ntfy_publish",arguments:{topic:$t,message:$m,title:"smoke"}}}')"
published="$(rpc "$publish_payload")"
jq -e --arg topic "$TOPIC" '.result.structuredContent.id != null and .result.structuredContent.topic == $topic' \
  <<<"$published" >/dev/null ||
  fail "publish result: $published"

read_payload="$(jq -nc --arg t "$TOPIC" \
  '{jsonrpc:"2.0",id:4,method:"tools/call",params:{name:"ntfy_read",arguments:{topic:$t,since:"all",limit:50}}}')"
read_result="$(rpc "$read_payload")"
jq -e --arg m "$CACHED" '[.result.structuredContent.messages[].message] | index($m) != null' \
  <<<"$read_result" >/dev/null ||
  fail "read did not return published message: $read_result"

echo "smoke: reserved topics are rejected before upstream"
reserved_payload="$(jq -nc \
  '{jsonrpc:"2.0",id:5,method:"tools/call",params:{name:"ntfy_publish",arguments:{topic:"docs",message:"nope"}}}')"
reserved="$(rpc "$reserved_payload")"
jq -e '.error.code == -32602' <<<"$reserved" >/dev/null ||
  fail "reserved topic was not rejected with -32602: $reserved"

echo "smoke: events/list advertises ntfy.message for push+poll"
events="$(rpc '{"jsonrpc":"2.0","id":6,"method":"events/list"}')"
jq -e '.result.events | length == 1' <<<"$events" >/dev/null ||
  fail "events/list length: $events"
jq -e '.result.events[0].name == "ntfy.message" and .result.events[0].delivery == ["push","poll"]' \
  <<<"$events" >/dev/null ||
  fail "events/list descriptor: $events"

echo "smoke: fresh events/stream emits active and no cached history"
# Make the cached message unambiguously older than the subscription watermark.
sleep 1.1
stream_payload="$(jq -nc --arg t "$TOPIC" \
  '{jsonrpc:"2.0",id:7,method:"events/stream",params:{event:"ntfy.message",filter:{topic:$t}}}')"
: >"$FRAMES"
curl -sS -N -X POST "$MCP_URL" \
  -H "Authorization: Bearer $MCP_AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -H "Mcp-Session-Id: $SESSION" \
  --data-binary "$stream_payload" >"$FRAMES" 2>/dev/null &
STREAM_PID=$!

wait_for 'notifications/events/active' 10 || fail "no active notification"
grep -q '"io.modelcontextprotocol/subscriptionId":"7"' "$FRAMES" ||
  fail "active frame is missing the SEP-3415 request-id subscription key"
sleep 0.3
if grep -q -- "$CACHED" "$FRAMES"; then
  fail "cached message was replayed to a from-now subscription"
fi

echo "smoke: post-subscribe publish arrives as a push event"
after_payload="$(jq -nc --arg t "$TOPIC" --arg m "$AFTER" \
  '{jsonrpc:"2.0",id:8,method:"tools/call",params:{name:"ntfy_publish",arguments:{topic:$t,message:$m}}}')"
after_result="$(rpc "$after_payload")"
jq -e '.result.structuredContent.id != null' <<<"$after_result" >/dev/null ||
  fail "post-subscribe publish failed: $after_result"
wait_for 'notifications/events/event' 10 || fail "no message notification after subscribe"
wait_for "$AFTER" 10 || fail "published message was not pushed"
grep -q 'notifications/events/active' "$FRAMES" || fail "active frame disappeared"

echo "smoke: heartbeat arrives within the configured interval"
# CI sets NTFY_HEARTBEAT=2s. The default is 25s, still below the 30s ceiling.
wait_for 'notifications/events/heartbeat' 35 || fail "no heartbeat notification"

echo "smoke: unknown event -> -32023"
unknown="$(rpc "$(jq -nc --arg t "$TOPIC" \
  '{jsonrpc:"2.0",id:9,method:"events/poll",params:{event:"other",filter:{topic:$t}}}')")"
jq -e '.error.code == -32023 and .error.data.kind == "event"' <<<"$unknown" >/dev/null ||
  fail "unknown event response: $unknown"

echo "smoke: events/subscribe replies with an id and pushes an event"
SUB_AFTER="subscribe-${RANDOM}-${RANDOM}"
sub_payload="$(jq -nc --arg t "$TOPIC" \
  '{jsonrpc:"2.0",id:10,method:"events/subscribe",params:{name:"ntfy.message",arguments:{topic:$t},subscriptionId:"smoke-subscribe"}}')"
: >"$SUB_FRAMES"
curl -sS -N -X POST "$MCP_URL" \
  -H "Authorization: Bearer $MCP_AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -H "Mcp-Session-Id: $SESSION" \
  --data-binary "$sub_payload" >"$SUB_FRAMES" 2>/dev/null &
SUB_PID=$!

wait_for_file "$SUB_FRAMES" '"subscriptionId":"smoke-subscribe"' 10 ||
  fail "no subscribe result with the echoed id"
wait_for_file "$SUB_FRAMES" 'notifications/events/active' 10 ||
  fail "no active notification for events/subscribe"
wait_for_file "$SUB_FRAMES" '"io.modelcontextprotocol/subscriptionId":"smoke-subscribe"' 10 ||
  fail "subscribe frames are missing the SEP-3415 subscription key"

sub_after_payload="$(jq -nc --arg t "$TOPIC" --arg m "$SUB_AFTER" \
  '{jsonrpc:"2.0",id:11,method:"tools/call",params:{name:"ntfy_publish",arguments:{topic:$t,message:$m}}}')"
sub_after_result="$(rpc "$sub_after_payload")"
jq -e '.result.structuredContent.id != null' <<<"$sub_after_result" >/dev/null ||
  fail "publish for the subscription failed: $sub_after_result"
wait_for_file "$SUB_FRAMES" "$SUB_AFTER" 10 ||
  fail "subscribed message was not pushed"
wait_for_file "$SUB_FRAMES" 'notifications/events/event' 10 ||
  fail "no event frame for events/subscribe"

echo "smoke: events/unsubscribe stops the subscription"
unsubscribe="$(rpc '{"jsonrpc":"2.0","id":12,"method":"events/unsubscribe","params":{"subscriptionId":"smoke-subscribe"}}')"
jq -e '.result.unsubscribed == true and (.result.subscriptionIds | index("smoke-subscribe"))' \
  <<<"$unsubscribe" >/dev/null ||
  fail "events/unsubscribe response: $unsubscribe"
# The server closes the SSE response when the subscription is cancelled.
for _ in $(seq 1 50); do
  kill -0 "$SUB_PID" 2>/dev/null || break
  sleep 0.1
done
if kill -0 "$SUB_PID" 2>/dev/null; then
  fail "subscription stream did not close after events/unsubscribe"
fi
wait "$SUB_PID" 2>/dev/null || true
SUB_PID=""
grep -q 'notifications/events/event' "$SUB_FRAMES" || fail "subscription event frame disappeared"

if [[ -n "$MCP_IMAGE" ]]; then
  command -v docker >/dev/null || fail "MCP_IMAGE set but docker is unavailable"
  echo "smoke: container image is non-root and refuses incomplete configuration"
  image_user="$(docker image inspect "$MCP_IMAGE" --format '{{.Config.User}}')"
  case "$image_user" in
    nonroot*|65532*) ;;
    *) fail "container runs as ${image_user:-root}, expected non-root" ;;
  esac

  set +e
  docker run --rm -e NTFY_BASE_URL=http://127.0.0.1:2586 "$MCP_IMAGE" >/dev/null 2>&1
  missing_auth=$?
  docker run --rm -e MCP_AUTH_TOKEN=x "$MCP_IMAGE" >/dev/null 2>&1
  missing_base=$?
  set -e
  [[ "$missing_auth" -eq 1 ]] || fail "container did not exit 1 without MCP_AUTH_TOKEN (got $missing_auth)"
  [[ "$missing_base" -eq 1 ]] || fail "container did not exit 1 without NTFY_BASE_URL (got $missing_base)"
fi

echo "smoke: OK"
