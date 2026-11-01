# Lesson 7 — Agent Mesh relay without Tailscale

Lesson 6 put an agent on a tailnet and published it with Tailscale Funnel.
This lesson runs the exact same workflow on [Agent Mesh](https://github.com/google/agentmesh)
using `agentmesh-one` as a self-hosted or Cloud Run relay hub and
`agentmesh-node` as the companion connector — no Tailscale account, no Funnel
ACL edits, and no DNS propagation wait.

## What you'll learn

- How to stand up an `agentmesh-one` hub on a laptop, VM, or Cloud Run
- How `a2a server` joins the mesh with `--private agentmesh --public agentmesh`
- How agents discover each other (`a2a server peers`) and delegate tasks over
  authenticated libp2p streams without credentials entering the sandbox
- How callers outside the mesh reach an agent's rewritten public URL
  (`<hub>/ingress/<peer_id>/a2a/<name>`), using either `a2a-serve` pairing
  codes (`passthrough`) or Agent Mesh task-attenuated Biscuits (`edge`)

## Prerequisites

- `a2a` built statically from this branch (`CGO_ENABLED=0 go build -trimpath -o ./bin/a2a .`)
- Docker running locally (for `opencode` container sandboxes)
- `agentmesh-one` and `agentmesh-node` on `PATH`

To run the entire lesson end-to-end with real Docker-sandboxed `opencode`
agents and live LLM inference (`opencode/big-pickle` by default, or pass
`--model <provider/model>` with `OPENROUTER_API_KEY` / `OPENAI_API_KEY` /
`ANTHROPIC_API_KEY` / `GEMINI_API_KEY` exported, `--openai-base-url <url>`, or
`--vertex-project <gcp-project>`):

```bash
# Local loopback relay hub + real Docker opencode agents:
bash docs/demo/demo-agentmesh.sh \
  --agentmesh-one-bin /path/to/agentmesh-one \
  --agentmesh-node-bin /path/to/agentmesh-node

# Full internet E2E over a live public HTTPS Cloudflare Quick Tunnel (*.trycloudflare.com):
bash docs/demo/demo-agentmesh.sh \
  --tunnel cloudflare \
  --agentmesh-one-bin /path/to/agentmesh-one \
  --agentmesh-node-bin /path/to/agentmesh-node
```

## 1. Start the relay hub (`agentmesh-one`)

On a laptop (exposed publicly with a zero-account Cloudflare Quick Tunnel), a
VM, or Cloud Run:

```bash
mkdir -p ~/a2a-lesson7/hub
cat > ~/a2a-lesson7/hub/policy.json <<'EOF'
{
  "roles": [
    {
      "name": "mesh-admin",
      "allowed_services": ["*"],
      "allowed_targets": ["*"],
      "allowed_ingress": [{"services": ["*"]}]
    },
    {
      "name": "mesh:role:router",
      "allowed_services": ["*"],
      "allowed_targets": ["*"]
    },
    {
      "name": "mesh:role:node",
      "allowed_services": ["*"],
      "allowed_targets": ["*"],
      "allowed_publish": ["*"],
      "allowed_labels": ["*"],
      "allowed_ingress": [
        {
          "services": ["a2a://*"],
          "allowed_auth_modes": [
            "INGRESS_AUTH_MODE_EDGE",
            "INGRESS_AUTH_MODE_PASSTHROUGH"
          ]
        }
      ]
    }
  ]
}
EOF
agentmesh-one --bind-address 127.0.0.1 --port 8443 --data-dir ~/a2a-lesson7/hub \
  --policy-file ~/a2a-lesson7/hub/policy.json \
  --tunnel cloudflare --tunnel-install --router-allow-loopback=false
```

By default, `agentmesh-one` grants only zero-trust `INGRESS_AUTH_MODE_EDGE`;
`policy.json` explicitly opts `mesh:role:node` into `INGRESS_AUTH_MODE_PASSTHROUGH`
for `a2a://*` so `a2a-serve` pairing codes work across `/ingress/`. On first boot, `agentmesh-one` persists its join token in
`~/a2a-lesson7/hub/join-token` and serves both the control plane, the WebSocket
libp2p relay, and external HTTP ingress (`http://127.0.0.1:8443/ingress/...`)
on a single port.

```bash
export AGENTMESH_HUB=http://127.0.0.1:8443
export AGENTMESH_JOIN_TOKEN="$(cat ~/a2a-lesson7/hub/join-token)"
```

## 2. Start two agents on the mesh

```bash
mkdir -p ~/a2a-lesson7/coder && cd ~/a2a-lesson7/coder
a2a server opencode --name coder --secret OPENROUTER_API_KEY \
  --private agentmesh --public agentmesh \
  --agentmesh-hub "$AGENTMESH_HUB" \
  --agentmesh-token env:AGENTMESH_JOIN_TOKEN \
  --detach

mkdir -p ~/a2a-lesson7/reviewer && cd ~/a2a-lesson7/reviewer
a2a server opencode --name reviewer --secret OPENROUTER_API_KEY \
  --private agentmesh --public agentmesh \
  --agentmesh-hub "$AGENTMESH_HUB" \
  --agentmesh-token env:AGENTMESH_JOIN_TOKEN \
  --detach
```

Check `coder`'s status:

```bash
cd ~/a2a-lesson7/coder
a2a server status
```

```
coder (opencode) running for 4s, pid 51200
  dir      /home/you/a2a-lesson7/coder
  local    http://127.0.0.1:41823
  private  http://agentmesh.local/mesh/12D3KooW.../a2a/coder
  public   http://127.0.0.1:8443/ingress/12D3KooW.../a2a/coder
```

## 3. Discover and pair agents privately over the mesh

From `coder`'s directory, discover other A2A agents registered on the mesh:

```bash
a2a server peers
```

Mint a pairing code on `reviewer` and add `reviewer` by name on `coder`:

```bash
CODE="$(a2a --output json server --dir ~/a2a-lesson7/reviewer pair invite coder | jq -r .code)"
a2a server --dir ~/a2a-lesson7/coder contact add reviewer --code "$CODE"
```

When `coder` calls `reviewer`, `a2a server` routes the request through its
local `agentmesh-node` Unix socket over end-to-end encrypted libp2p streams;
no credentials enter `coder`'s sandbox.

## 4. Call the agent from outside the mesh

External callers (hosted assistants, webhooks, chat bots, or teammates outside
the mesh) use the `public` URL printed by `a2a server status`:

```bash
# Inspect the public AgentCard (interface URLs are automatically rewritten
# to the public /ingress/<peer_id>/a2a/coder endpoint):
a2a card get -a "$PUBLIC_URL"

# Pair with a one-time invite code from the owner:
a2a server --dir ~/a2a-lesson7/coder pair invite assistant
a2a pair add "$PUBLIC_URL" <CODE>

# Send messages over HTTPS -> agentmesh-router -> /mesh/ingress/1.0.0 -> coder:
a2a send -a "$PUBLIC_URL" "Ask reviewer to check the latest commit."
```

For zero-trust federated callers that hold an Agent Mesh delegated or
task-attenuated Session Biscuit (minted via RFC 8693 `POST /oauth/token`), pass
`--agentmesh-ingress-auth edge` (and optional `--agentmesh-ingress-allow <principal>`)
so `agentmesh-router` and `agentmesh-node` enforce authorization at the mesh
border without pairing codes:

```bash
a2a send -a "$AUDITOR_PUBLIC_URL" --auth "Bearer $ATTENUATED_BISCUIT" "Run compliance check."
```
