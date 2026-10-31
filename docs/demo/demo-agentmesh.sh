#!/usr/bin/env bash
#
# End-to-end demo of `a2a server` running on Agent Mesh (`agentmesh-one` +
# `agentmesh-node`) instead of Tailscale (`tsnet` + Funnel):
#
#   1. Starts `agentmesh-one` as a single-binary relay + control-plane hub.
#   2. Starts two `a2a server` agents (`coder` and `reviewer`) with
#      `--private agentmesh --public agentmesh`.
#   3. Discovers `reviewer` from `coder` over the mesh (`a2a server peers`),
#      pairs them (`a2a server contact add`), and delegates an A2A task over
#      authenticated P2P libp2p streams.
#   4. Calls `coder` from an external client outside the mesh (no mesh node,
#      fresh HOME) via its rewritten public `/ingress/<peer_id>/a2a/coder` URL
#      after pairing (`a2a pair add`).
#   5. Starts a third agent (`auditor`) with `--agentmesh-ingress-auth edge`
#      and calls its public `/ingress/` URL using an RFC 8693 task-attenuated
#      Agent Mesh Biscuit (zero pairing code required).
#
# Usage:
#   AGENTMESH_ONE_BIN=/path/to/agentmesh-one \
#   AGENTMESH_NODE_BIN=/path/to/agentmesh-node \
#   bash docs/demo/demo-agentmesh.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

A2A_BIN="${A2A_BIN:-$REPO_ROOT/bin/a2a}"
FAKEACP_BIN="${FAKEACP_BIN:-$REPO_ROOT/bin/fakeacp}"
AGENTMESH_ONE_BIN="${AGENTMESH_ONE_BIN:-$(command -v agentmesh-one || true)}"
AGENTMESH_NODE_BIN="${AGENTMESH_NODE_BIN:-$(command -v agentmesh-node || true)}"
TUNNEL_PROVIDER="${TUNNEL_PROVIDER:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --agentmesh-one-bin)
      AGENTMESH_ONE_BIN="$2"
      shift 2
      ;;
    --agentmesh-node-bin)
      AGENTMESH_NODE_BIN="$2"
      shift 2
      ;;
    --tunnel)
      TUNNEL_PROVIDER="$2"
      shift 2
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 1
      ;;
  esac
done
export AGENTMESH_NODE_BIN

if [[ ! -x "$A2A_BIN" ]]; then
  echo "Building a2a CLI..."
  mkdir -p "$REPO_ROOT/bin"
  go build -o "$A2A_BIN" .
fi
if [[ ! -x "$FAKEACP_BIN" ]]; then
  echo "Building fakeacp ACP harness..."
  mkdir -p "$REPO_ROOT/bin"
  go build -o "$FAKEACP_BIN" ./internal/agentsrv/testdata/fakeacp
fi
if [[ -z "$AGENTMESH_ONE_BIN" || ! -x "$AGENTMESH_ONE_BIN" ]]; then
  echo "error: set AGENTMESH_ONE_BIN to the agentmesh-one binary" >&2
  exit 1
fi
if [[ -z "$AGENTMESH_NODE_BIN" || ! -x "$AGENTMESH_NODE_BIN" ]]; then
  echo "error: set AGENTMESH_NODE_BIN to the agentmesh-node binary" >&2
  exit 1
fi

WORK="$(mktemp -d /tmp/a2a-am-demo.XXXXXX)"
OWNER_HOME="$WORK/owner-home"
EXTERNAL_HOME="$WORK/external-home"
mkdir -p "$OWNER_HOME" "$EXTERNAL_HOME" "$WORK/hub" "$WORK/coder" "$WORK/reviewer" "$WORK/auditor"
export HOME="$OWNER_HOME"

cleanup() {
  local rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "=== DEMO FAILED (rc=$rc); dumping diagnostic logs ===" >&2
    if [[ -f "$WORK/hub/agentmesh-one.log" ]]; then
      echo "--- hub log ---" >&2
      tail -n 60 "$WORK/hub/agentmesh-one.log" >&2 || true
    fi
    for d in "$WORK/coder" "$WORK/reviewer" "$WORK/auditor"; do
      for f in "$d/.a2a/logs/"* "$d/.a2a/state/agentmesh/"*.log; do
        if [[ -f "$f" ]]; then
          echo "--- $f ---" >&2
          tail -n 50 "$f" >&2 || true
        fi
      done
    done
  fi
  for d in "$WORK/coder" "$WORK/reviewer" "$WORK/auditor"; do
    if [[ -d "$d/.a2a" ]]; then
      "$A2A_BIN" server --dir "$d" stop >/dev/null 2>&1 || true
    fi
  done
  if [[ -n "${HUB_PID:-}" ]]; then
    kill "$HUB_PID" >/dev/null 2>&1 || true
    wait "$HUB_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

banner() {
  printf '\n\033[1;36m==> %s\033[0m\n' "$*"
}

pick_free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

HUB_PORT="$(pick_free_port)"
if [[ -n "$TUNNEL_PROVIDER" ]]; then
  TUNNEL_CACHE_DIR="${TMPDIR:-/tmp}/a2a-am-tunnel-cache"
  mkdir -p "$TUNNEL_CACHE_DIR" "$WORK/hub/bin"
  if [[ -x "$TUNNEL_CACHE_DIR/cloudflared" ]]; then
    cp -f "$TUNNEL_CACHE_DIR/cloudflared" "$WORK/hub/bin/cloudflared"
  fi
  banner "1. Starting agentmesh-one relay hub with public --tunnel ${TUNNEL_PROVIDER} (no Tailscale)"
  "$AGENTMESH_ONE_BIN" \
    --bind-address 127.0.0.1 \
    --port "$HUB_PORT" \
    --data-dir "$WORK/hub" \
    --tunnel "$TUNNEL_PROVIDER" \
    --tunnel-install \
    --router-allow-loopback=false \
    >"$WORK/hub/agentmesh-one.log" 2>&1 &
  HUB_PID=$!

  HUB_URL=""
  for _ in $(seq 1 300); do
    if ! kill -0 "$HUB_PID" 2>/dev/null; then
      echo "error: agentmesh-one exited while opening tunnel" >&2
      exit 1
    fi
    if [[ -f "$WORK/hub/agentmesh-one.log" ]]; then
      HUB_URL="$(sed -n 's/^API URL:[[:space:]]*\(https\{0,1\}:\/\/[^[:space:]]*\).*/\1/p' "$WORK/hub/agentmesh-one.log" | head -n1)"
      if [[ -n "$HUB_URL" && -s "$WORK/hub/join-token" ]]; then
        break
      fi
    fi
    sleep 0.2
  done
  if [[ -z "$HUB_URL" ]]; then
    echo "error: timed out waiting for public tunnel URL from agentmesh-one" >&2
    exit 1
  fi
  if [[ -x "$WORK/hub/bin/cloudflared" && ! -x "$TUNNEL_CACHE_DIR/cloudflared" ]]; then
    cp -f "$WORK/hub/bin/cloudflared" "$TUNNEL_CACHE_DIR/cloudflared" || true
  fi
  for _ in $(seq 1 60); do
    if curl -fsS "${HUB_URL}/readyz" >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
  done
else
  HUB_URL="http://127.0.0.1:${HUB_PORT}"
  banner "1. Starting agentmesh-one relay hub at ${HUB_URL} (no Tailscale)"
  "$AGENTMESH_ONE_BIN" \
    --bind-address 127.0.0.1 \
    --port "$HUB_PORT" \
    --data-dir "$WORK/hub" \
    >"$WORK/hub/agentmesh-one.log" 2>&1 &
  HUB_PID=$!

  for _ in $(seq 1 100); do
    if curl -fsS "${HUB_URL}/readyz" >/dev/null 2>&1 && [[ -s "$WORK/hub/join-token" ]]; then
      break
    fi
    sleep 0.1
  done
fi
curl -fsS "${HUB_URL}/readyz" >/dev/null
export AGENTMESH_JOIN_TOKEN="$(tr -d '[:space:]' <"$WORK/hub/join-token")"
echo "Hub ready: ${HUB_URL} (ingress base: ${HUB_URL}/ingress)"

banner "2. Starting two a2a-serve agents (coder & reviewer) on Agent Mesh"
"$A2A_BIN" server --dir "$WORK/coder" acp \
  --name coder \
  --description "Coding agent on Agent Mesh" \
  --sandbox local \
  --command "$FAKEACP_BIN" \
  --private agentmesh \
  --public agentmesh \
  --agentmesh-hub "$HUB_URL" \
  --agentmesh-token env:AGENTMESH_JOIN_TOKEN \
  --detach

"$A2A_BIN" server --dir "$WORK/reviewer" acp \
  --name reviewer \
  --description "Code review agent on Agent Mesh" \
  --sandbox local \
  --command "$FAKEACP_BIN" \
  --private agentmesh \
  --public agentmesh \
  --agentmesh-hub "$HUB_URL" \
  --agentmesh-token env:AGENTMESH_JOIN_TOKEN \
  --detach

echo "--- coder status ---"
"$A2A_BIN" server --dir "$WORK/coder" status
echo "--- reviewer status ---"
"$A2A_BIN" server --dir "$WORK/reviewer" status

CODER_STATUS_JSON="$("$A2A_BIN" --output json server --dir "$WORK/coder" status)"
CODER_LOCAL="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["local"])' <<<"$CODER_STATUS_JSON")"
CODER_PUBLIC="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["public"])' <<<"$CODER_STATUS_JSON")"

banner "3. Discovering peers over the private Agent Mesh DHT (a2a server peers)"
for _ in $(seq 1 30); do
  PEERS_OUT="$("$A2A_BIN" server --dir "$WORK/coder" peers)"
  if grep -q "reviewer" <<<"$PEERS_OUT"; then
    break
  fi
  sleep 0.5
done
echo "$PEERS_OUT"
grep -q "reviewer" <<<"$PEERS_OUT"

banner "4. Pairing coder -> reviewer over the private mesh & delegating a task"
INVITE_OUT="$("$A2A_BIN" --output json server --dir "$WORK/reviewer" pair invite coder)"
REVIEWER_CODE="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["code"])' <<<"$INVITE_OUT")"

"$A2A_BIN" server --dir "$WORK/coder" contact add reviewer --code "$REVIEWER_CODE"
"$A2A_BIN" server --dir "$WORK/coder" contact list

DELEGATED_REPLY="$("$A2A_BIN" send -a "$CODER_LOCAL" "call reviewer tool")"
echo "$DELEGATED_REPLY"
grep -q "Listed the files." <<<"$DELEGATED_REPLY"

banner "5. Calling coder from outside the mesh via public /ingress/ URL (passthrough pairing)"
echo "Public A2A URL: $CODER_PUBLIC"
echo "--- Public AgentCard (rewritten to /ingress/<peer_id>/a2a/coder) ---"
CARD_OUT="$(HOME="$EXTERNAL_HOME" "$A2A_BIN" card get -a "$CODER_PUBLIC")"
echo "$CARD_OUT"
grep -q "$CODER_PUBLIC" <<<"$CARD_OUT"

echo "--- Verifying unauthenticated external request is rejected (401) ---"
if HOME="$EXTERNAL_HOME" "$A2A_BIN" send -a "$CODER_PUBLIC" "hello" >/dev/null 2>&1; then
  echo "error: unauthenticated external call unexpectedly succeeded" >&2
  exit 1
fi
echo "Unauthenticated external call rejected as expected."

EXT_INVITE="$("$A2A_BIN" --output json server --dir "$WORK/coder" pair invite hosted-assistant)"
EXT_CODE="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["code"])' <<<"$EXT_INVITE")"

HOME="$EXTERNAL_HOME" "$A2A_BIN" pair add "$CODER_PUBLIC" "$EXT_CODE"
EXT_REPLY="$(HOME="$EXTERNAL_HOME" "$A2A_BIN" send -a "$CODER_PUBLIC" "call reviewer tool")"
echo "$EXT_REPLY"
grep -q "Listed the files." <<<"$EXT_REPLY"

banner "6. Zero-Trust Edge Auth (--agentmesh-ingress-auth edge) with attenuated Biscuit"
"$A2A_BIN" server --dir "$WORK/auditor" acp \
  --name auditor \
  --description "Edge-authenticated audit agent" \
  --sandbox local \
  --command "$FAKEACP_BIN" \
  --private agentmesh \
  --public agentmesh \
  --agentmesh-hub "$HUB_URL" \
  --agentmesh-token env:AGENTMESH_JOIN_TOKEN \
  --agentmesh-ingress-auth edge \
  --detach

AUDITOR_STATUS_JSON="$("$A2A_BIN" --output json server --dir "$WORK/auditor" status)"
AUDITOR_PUBLIC="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["public"])' <<<"$AUDITOR_STATUS_JSON")"
echo "Auditor public edge-authenticated URL: $AUDITOR_PUBLIC"

# Wait until auditor's ingress route is published on the router DHT.
for _ in $(seq 1 30); do
  STATUS_CODE="$(curl -s -o /dev/null -w '%{http_code}' "${AUDITOR_PUBLIC}/.well-known/agent-card.json")"
  if [[ "$STATUS_CODE" == "200" ]]; then
    break
  fi
  sleep 0.5
done

# Mint an RFC 8693 task-attenuated Biscuit from coder's local agentmesh-node socket
# scoped strictly to `a2a://auditor`, then call auditor's public /ingress/ URL
# from outside the mesh with zero pairing codes!
CODER_SOCK="$(python3 -c '
import hashlib, os, sys
coder_dir = os.path.abspath(os.path.join(sys.argv[1], "coder"))
direct = os.path.join(coder_dir, ".a2a/state/agentmesh/agentmesh-node.sock")
if len(direct) <= 100 and os.path.exists(direct):
    print(direct)
else:
    uid = os.getuid()
    base = os.environ.get("XDG_RUNTIME_DIR") or f"/tmp/a2a-{uid}"
    digest = hashlib.sha256(coder_dir.encode()).hexdigest()[:12]
    print(os.path.join(base, f"a2a-{digest}-am.sock"))
' "$WORK")"

WRONG_SCOPE_JSON="$(curl -fsS --unix-socket "$CODER_SOCK" \
  -X POST http://localhost/oauth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=urn:ietf:params:oauth:grant-type:token-exchange' \
  --data-urlencode 'resource=a2a://coder')"
WRONG_BISCUIT="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])' <<<"$WRONG_SCOPE_JSON")"
if HOME="$EXTERNAL_HOME" "$A2A_BIN" send -a "$AUDITOR_PUBLIC" --auth "Bearer ${WRONG_BISCUIT}" "hello" >/dev/null 2>&1; then
  echo "error: Biscuit attenuated to a2a://coder unexpectedly reached a2a://auditor" >&2
  exit 1
fi
echo "Task-attenuated Biscuit for a2a://coder rightly refused on a2a://auditor (403)."

TOKEN_JSON="$(curl -fsS --unix-socket "$CODER_SOCK" \
  -X POST http://localhost/oauth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=urn:ietf:params:oauth:grant-type:token-exchange' \
  --data-urlencode 'resource=a2a://auditor')"
EDGE_BISCUIT="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])' <<<"$TOKEN_JSON")"

EDGE_REPLY="$(HOME="$EXTERNAL_HOME" "$A2A_BIN" send -a "$AUDITOR_PUBLIC" --auth "Bearer ${EDGE_BISCUIT}" "hello")"
echo "$EDGE_REPLY"
grep -q "Hello from fakeacp." <<<"$EDGE_REPLY"

banner "ALL DEMO CHECKS PASSED (Private P2P Discovery + Delegation + Public Passthrough Pairing + Public Edge Biscuit Auth)"
