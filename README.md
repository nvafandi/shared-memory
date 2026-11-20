# Shared-Memory MCP — Documentation (MCP Native Architecture)

A shared memory system for AI agents based on **MCP (Model Context Protocol)**.
- **MCP Server** runs on a VPS inside Docker, exposing tools `commit_session`, `get_session`, `list_sessions`, etc.
- **Memory pool** = Redis (Docker internal, not exposed to the internet).
- **Client (Python)** connects to the MCP server via HTTP/SSE or stdio over SSH tunnel.

---

## 1. Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
VPS (memory.example.com)
│  ┌─────────────┐     ┌──────────────────────┐     ┌────────────────────┐  │
│  │   Nginx     │────▶│  MCP Shared-Memory   │────▶│  Redis (Docker)    │  │
│  │  :80/:443   │     │  Server (Python)     │     │  :6379 (AOF)       │  │
│  │  SSL, Rate  │     │  FastMCP / mcp[cli]  │     │  Volume: redis_data│  │
│  └─────────────┘     └──────────────────────┘     └────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
           ▲
           │ HTTPS (MCP over HTTP/SSE)
           │
┌──────────┴──────────┐
│  Client Python      │
│  (agents: orchestrator,│
│   analyst, developer,│
│   tester, reviewer)  │
│  Uses: mcp[cli] /  │
│  fastmcp.Client     │
└─────────────────────┘
```

---

## 2. Folder Structure (VPS)

```
~/shared-memory/
├── .env
├── .env.example
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── src/
│   └── mcp_server.py      # MCP Server (FastMCP) - exposes tools to clients
├── nginx/
│   └── shared-memory.conf
├── ops/
│   ├── sm_watchdog.sh
│   ├── sm_watchdog.service
│   └── sm_watchdog.timer
├── memory_client.py       # Shared MCP client (used by orchestrator + agents)
├── orchestrator.py        # Multi-agent pipeline coordinator
├── agent_base.py          # BaseAgent + analyst/developer/tester/reviewer
├── client_example.py      # Minimal client example
├── test_remote_client.py  # Live server smoke test
├── test_orchestrator_offline.py  # Offline pipeline test (no server)
├── deploy_vps.sh
├── run_local.ps1
└── README.md
```

---

## 3. Configuration Files (VPS)

### `.env.example`
```env
# Redis (internal Docker network)
REDIS_URL=redis://redis:6379

# MCP Server
MCP_HOST=0.0.0.0
MCP_PORT=8000
MCP_TRANSPORT=sse          # or "stdio" if using SSH tunnel

# Auth (shared secret between MCP server & clients)
MCP_API_KEY=REPLACE_WITH_32_CHAR_RANDOM_KEY

# Admin (optional, for non-MCP management endpoints)
ADMIN_API_KEY=REPLACE_WITH_DIFFERENT_ADMIN_KEY
```

> **Generate secure keys:**
> ```bash
> openssl rand -hex 32
> ```

### `docker-compose.yml`
```yaml
services:
  redis:
    image: redis:7-alpine
    container_name: mcp-redis
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
    networks:
      - mcp-net

  mcp-server:
    build: .
    container_name: mcp-shared-memory
    ports:
      - "127.0.0.1:8000:8000"   # localhost only, Nginx proxies
    environment:
      - REDIS_URL=redis://redis:6379
      - MCP_HOST=0.0.0.0
      - MCP_PORT=8000
      - MCP_TRANSPORT=sse
      - MCP_API_KEY=${MCP_API_KEY}
      - ADMIN_API_KEY=${ADMIN_API_KEY}
    depends_on:
      redis:
        condition: service_healthy
    networks:
      - mcp-net
    restart: unless-stopped

networks:
  mcp-net:

volumes:
  redis_data:
```

### `Dockerfile`
```dockerfile
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
EXPOSE 8000
CMD ["python", "-m", "src.mcp_server"]
```

### `requirements.txt`
```
fastmcp>=0.2.0
redis>=5.0.0
pydantic>=2.0
python-dotenv>=1.0
httpx>=0.27.0
uvicorn>=0.30.0
```

---

## 4. MCP Server Implementation (`src/mcp_server.py`)

```python
"""
MCP Shared-Memory Server
Expose tools: commit_session, get_session, list_sessions, set_key, get_key, del_key
Plus admin tools: health, stats, keys, flush, export, import
"""
import os
import json
from typing import Any, Optional
from datetime import datetime

from fastmcp import FastMCP
from pydantic import BaseModel, Field
import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
MCP_API_KEY = os.getenv("MCP_API_KEY")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")

# ---- Models ----
class SessionRecord(BaseModel):
    sessionId: str
    agentId: str
    taskSummary: str
    outcome: str = "success"
    decisions: list[dict] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    userCorrections: list[str] = Field(default_factory=list)
    beliefUpdates: list[str] = Field(default_factory=list)
    updatedAt: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")

class CommitSessionParams(BaseModel):
    sessionId: str
    agentId: str
    taskSummary: str
    outcome: str = "success"
    decisions: list[dict] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    userCorrections: list[str] = Field(default_factory=list)
    beliefUpdates: list[str] = Field(default_factory=list)

class SetKeyParams(BaseModel):
    key: str
    value: Any
    ttlSeconds: Optional[int] = None

# ---- Redis Client ----
class RedisClient:
    def __init__(self, url: str):
        self._url = url
        self._client: Optional[redis.Redis] = None

    async def connect(self):
        if self._client is None:
            self._client = redis.from_url(self._url, decode_responses=True)
        return self._client

    async def close(self):
        if self._client:
            await self._client.close()
            self._client = None

    def session_key(self, session_id: str) -> str:
        return f"session:{session_id}"

rc = RedisClient(REDIS_URL)

# ---- Auth Helper ----
def verify_api_key(api_key: str, require_admin: bool = False) -> bool:
    if require_admin:
        return api_key == ADMIN_API_KEY
    return api_key == MCP_API_KEY or api_key == ADMIN_API_KEY

# ---- MCP Server ----
mcp = FastMCP("shared-memory")

# ===== CLIENT TOOLS (API_KEY or ADMIN_API_KEY) =====

@mcp.tool()
async def commit_session(params: CommitSessionParams, api_key: str = "") -> dict:
    """Save/update agent session record."""
    if not verify_api_key(api_key):
        return {"error": "unauthorized"}
    client = await rc.connect()
    record = SessionRecord(**params.model_dump())
    await client.set(rc.session_key(params.sessionId), record.model_dump_json())
    return {"ok": True, "sessionId": params.sessionId}

@mcp.tool()
async def get_session(session_id: str, api_key: str = "") -> dict:
    """Get session record by ID."""
    if not verify_api_key(api_key):
        return {"error": "unauthorized"}
    client = await rc.connect()
    data = await client.get(rc.session_key(session_id))
    if not data:
        return {"error": "not found", "sessionId": session_id}
    return json.loads(data)

@mcp.tool()
async def list_sessions(api_key: str = "") -> dict:
    """List all session IDs."""
    if not verify_api_key(api_key):
        return {"error": "unauthorized"}
    client = await rc.connect()
    keys = await client.keys("session:*")
    sessions = [k.replace("session:", "") for k in keys]
    return {"sessions": sessions, "count": len(sessions)}

@mcp.tool()
async def set_key(params: SetKeyParams, api_key: str = "") -> dict:
    """Set generic key-value (supports TTL)."""
    if not verify_api_key(api_key):
        return {"error": "unauthorized"}
    client = await rc.connect()
    value = params.value if isinstance(params.value, str) else json.dumps(params.value)
    if params.ttlSeconds:
        await client.setex(params.key, params.ttlSeconds, value)
    else:
        await client.set(params.key, value)
    return {"ok": True}

@mcp.tool()
async def get_key(key: str, api_key: str = "") -> dict:
    """Get value by key."""
    if not verify_api_key(api_key):
        return {"error": "unauthorized"}
    client = await rc.connect()
    data = await client.get(key)
    if data is None:
        return {"error": "not found", "key": key}
    try:
        return {"key": key, "value": json.loads(data)}
    except json.JSONDecodeError:
        return {"key": key, "value": data}

@mcp.tool()
async def del_key(key: str, api_key: str = "") -> dict:
    """Delete key."""
    if not verify_api_key(api_key):
        return {"error": "unauthorized"}
    client = await rc.connect()
    await client.delete(key)
    return {"ok": True}

# ===== ADMIN TOOLS (ADMIN_API_KEY only) =====

@mcp.tool()
async def admin_health(api_key: str = "") -> dict:
    """Health check Redis."""
    if not verify_api_key(api_key, require_admin=True):
        return {"error": "admin unauthorized"}
    client = await rc.connect()
    pong = await client.ping()
    return {"status": "ok", "redis": pong}

@mcp.tool()
async def admin_stats(api_key: str = "") -> dict:
    """Summary statistics."""
    if not verify_api_key(api_key, require_admin=True):
        return {"error": "admin unauthorized"}
    client = await rc.connect()
    info = await client.info("memory")
    # redis-py returns dict in newer versions; older returns "key: value" string
    used_mem = (
        info.get("used_memory_human")
        if isinstance(info, dict)
        else next(
            (line.split(":", 1)[1] for line in info.split("\r\n") if line.startswith("used_memory_human:")),
            None,
        )
    )
    total_keys = await client.dbsize()
    sessions = await list_sessions(api_key)  # reuse
    return {
        "usedMemoryHuman": used_mem,
        "totalKeys": total_keys,
        "sessionCount": sessions.get("count", 0),
        "sessions": sessions.get("sessions", [])
    }

@mcp.tool()
async def admin_keys(pattern: str = "session:*", api_key: str = "") -> dict:
    """List keys by pattern."""
    if not verify_api_key(api_key, require_admin=True):
        return {"error": "admin unauthorized"}
    client = await rc.connect()
    keys = await client.keys(pattern)
    return {"pattern": pattern, "count": len(keys), "keys": keys}

@mcp.tool()
async def admin_del_keys(pattern: str = "session:*", api_key: str = "") -> dict:
    """Delete keys by pattern."""
    if not verify_api_key(api_key, require_admin=True):
        return {"error": "admin unauthorized"}
    client = await rc.connect()
    keys = await client.keys(pattern)
    if keys:
        await client.delete(*keys)
    return {"deleted": len(keys), "keys": keys}

@mcp.tool()
async def admin_flush(confirm: bool = False, api_key: str = "") -> dict:
    """Delete ALL data (requires confirm=true)."""
    if not verify_api_key(api_key, require_admin=True):
        return {"error": "admin unauthorized"}
    if not confirm:
        return {"error": "confirm=true required"}
    client = await rc.connect()
    await client.flushdb()
    return {"ok": True, "message": "all keys deleted"}

@mcp.tool()
async def admin_export(api_key: str = "") -> dict:
    """Export all sessions to JSON."""
    if not verify_api_key(api_key, require_admin=True):
        return {"error": "admin unauthorized"}
    client = await rc.connect()
    sessions = await list_sessions(api_key)
    data = {}
    for sid in sessions.get("sessions", []):
        rec = await get_session(sid, api_key)
        if "error" not in rec:
            data[sid] = rec
    return {"exported": len(data), "data": data}

@mcp.tool()
async def admin_import(data: dict, api_key: str = "") -> dict:
    """Import sessions from JSON {sessionId: {...}}."""
    if not verify_api_key(api_key, require_admin=True):
        return {"error": "admin unauthorized"}
    client = await rc.connect()
    imported = 0
    for sid, val in data.items():
        await client.set(rc.session_key(sid), json.dumps(val))
        imported += 1
    return {"ok": True, "imported": imported}

# ===== ENTRYPOINT =====

class XApiKeyMiddleware:
    """Inject api_key argument from x-api-key header if client doesn't send it.

    Without this, every tool call from an agent must include api_key manually.
    Key is still verified per tool by verify_api_key().
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST":
            await self.app(scope, receive, send)
            return

        key = None
        for name, value in scope.get("headers", []):
            if name.lower() == b"x-api-key":
                key = value.decode("latin-1")
                break

        if not key:
            await self.app(scope, receive, send)
            return

        body = b""
        more_body = True
        while more_body:
            message = await receive()
            if message["type"] != "http.request":
                break
            body += message.get("body", b"")
            more_body = message.get("more_body", False)

        try:
            msg = json.loads(body)
        except (ValueError, TypeError):
            msg = None

        if isinstance(msg, dict) and msg.get("method") == "tools/call":
            params = msg.get("params")
            arguments = params.get("arguments") if isinstance(params, dict) else None
            if isinstance(arguments, dict) and not arguments.get("api_key"):
                arguments["api_key"] = key
                body = json.dumps(msg).encode("utf-8")

        replayed = []

        async def replay():
            if replayed:
                return {"type": "http.disconnect"}
            replayed.append(True)
            return {"type": "http.request", "body": body, "more_body": False}

        await self.app(scope, replay, send)


if __name__ == "__main__":
    import uvicorn
    from starlette.responses import JSONResponse
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8000"))
    transport = os.getenv("MCP_TRANSPORT", "sse")
    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        from starlette.middleware import Middleware
        app = mcp.http_app(transport="sse", middleware=[Middleware(XApiKeyMiddleware)])
        app.add_route("/health", lambda req: JSONResponse({"status": "ok"}), methods=["GET"])
        uvicorn.run(app, host=host, port=port)
```

---

## 5. Reverse Proxy & TLS (VPS) — `/etc/nginx/sites-available/shared-memory`

The MCP container listens on `127.0.0.1:8000`. Nginx fronts it on port 80 and a TLS terminator (Cloudflare Tunnel or certbot) publishes it as HTTPS.

### 5.1 Nginx site config (HTTP, upstream = container)

```nginx
server {
    listen 80;
    server_name YOUR_DOMAIN;

    # SSE endpoint for MCP (GET, streaming)
    location /sse {
        proxy_pass http://127.0.0.1:8000/sse;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Connection "";
        proxy_cache off;
        proxy_buffering off;
        # SSE needs a long timeout for streaming
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    }

    # Message endpoint (POST) for MCP over HTTP
    location /messages/ {
        proxy_pass http://127.0.0.1:8000/messages/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Connection "";
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # Health check
    location /health {
        proxy_pass http://127.0.0.1:8000/health;
        access_log off;
    }
}
```

Install and reload:

```bash
sudo cp nginx/shared-memory.conf /etc/nginx/sites-available/shared-memory.conf
sudo ln -sf /etc/nginx/sites-available/shared-memory.conf /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

### 5.2 Option A — Cloudflare Tunnel (no open inbound ports)

Preferred when the VPS sits behind NAT or you don't want to expose 80/443.

```bash
# One-time setup
cloudflared tunnel login
cloudflared tunnel create shared-memory
cloudflared tunnel route dns shared-memory YOUR_DOMAIN

# /etc/cloudflared/config.yml
# tunnel: <TUNNEL_UUID>
# credentials-file: /root/.cloudflared/<TUNNEL_UUID>.json
# ingress:
#   - hostname: YOUR_DOMAIN
#     service: http://127.0.0.1:80
#   - service: http_status:404

sudo cloudflared service install
sudo systemctl enable --now cloudflared
```

### 5.3 Option B — certbot / Let's Encrypt (public VPS with open ports)

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d YOUR_DOMAIN --redirect
sudo certbot renew --dry-run     # verify auto-renewal
```

Certbot rewrites the Nginx site to add the 443 server block and HTTP→HTTPS redirect automatically. If you manage SSL manually, use:

```nginx
server {
    listen 443 ssl http2;
    server_name YOUR_DOMAIN;

    ssl_certificate     /etc/letsencrypt/live/YOUR_DOMAIN/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/YOUR_DOMAIN/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;

    # ... same location blocks as 5.1 ...
}
```

### 5.4 Rate limiting (optional, add to `http {}` in `nginx.conf`)

```nginx
limit_req_zone $binary_remote_addr zone=mcp_api:10m rate=30r/s;
```

Then inside `location /sse` and `location /messages/`:

```nginx
limit_req zone=mcp_api burst=50 nodelay;
```

---

## 6. Python Client — Accessing MCP Shared-Memory

### `client_example.py`
```python
"""
Example: Connect to MCP Shared-Memory server from local machine.
Requires: pip install "mcp[cli]" httpx python-dotenv
"""
import os
import asyncio
import json
from mcp import Client
from mcp.client.sse import sse_client
from dotenv import load_dotenv

load_dotenv()

MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "https://memory.example.com")
MCP_API_KEY = os.getenv("MCP_API_KEY")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")

async def main():
    # SSE transport (HTTP over SSE)
    async with sse_client(f"{MCP_SERVER_URL}/sse") as (read, write):
        client = Client(read, write)
        await client.initialize()

        # List tools
        tools = await client.list_tools()
        print("Available tools:", [t.name for t in tools])

        # --- Client Tools (require MCP_API_KEY) ---

        # Set key (note: params wrapper!)
        result = await client.call_tool("set_key", {
            "params": {"key": "config:timeout", "value": 30, "ttlSeconds": 3600},
            "api_key": MCP_API_KEY
        })
        print("set_key:", result)

        # Get key (flat args)
        result = await client.call_tool("get_key", {
            "key": "config:timeout",
            "api_key": MCP_API_KEY
        })
        print("get_key:", result)

        # Commit session (params wrapper)
        result = await client.call_tool("commit_session", {
            "params": {
                "sessionId": "ses_001",
                "agentId": "analyst",
                "taskSummary": "Analyze login bug",
                "decisions": [{"summary": "root cause: pool exhausted", "rationale": "metrics show 100% used"}],
                "errors": ["timeout after 30s"]
            },
            "api_key": MCP_API_KEY
        })
        print("commit_session:", result)

        # Get session (flat args)
        result = await client.call_tool("get_session", {
            "session_id": "ses_001",
            "api_key": MCP_API_KEY
        })
        print("get_session:", result)

        # List sessions (flat args)
        result = await client.call_tool("list_sessions", {
            "api_key": MCP_API_KEY
        })
        print("list_sessions:", result)

        # --- Admin Tools (require ADMIN_API_KEY) ---
        result = await client.call_tool("admin_stats", {
            "api_key": ADMIN_API_KEY
        })
        print("admin_stats:", result)

        # Delete key
        result = await client.call_tool("del_key", {
            "key": "config:timeout",
            "api_key": MCP_API_KEY
        })
        print("del_key:", result)

        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 7. Running on VPS (Deploy)

```bash
# 1. Clone repo to your VPS
cd ~/shared-memory

# 2. Copy and edit .env
cp .env.example .env
# Edit .env: set MCP_API_KEY, ADMIN_API_KEY (use: openssl rand -hex 32)

# 3. Build & start
docker compose up -d --build

# 4. Check logs
docker compose logs -f mcp-server

# 5. Verify health
curl https://memory.example.com/health
# {"status":"ok"}

# 6. SSL via certbot (first time only)
sudo certbot --nginx -d memory.example.com
# Auto-renewal: sudo certbot renew --dry-run
```

---

## 8. Client Installation Guide (For New Users)

This section is written for **anyone who wants to connect to an existing Shared-Memory server**. You do **not** need to deploy the server — you only need three things from the server owner:

| What you need | Example value | Where to get it |
|---------------|---------------|-----------------|
| **Server URL** | `https://memory.example.com` | From the server owner |
| **API Key** | `<your-api-key>` | From the server owner (32-char hex) |
| **Admin Key** (optional) | `<your-admin-key>` | Only if you need admin tools |

> ⚠️ **Never commit your API key to git.** Store it in `.env`, a secrets manager, or your MCP client config (gitignored).

### 8.1 Prerequisites

```bash
# Python 3.10+ and pip
python --version

# Node.js 18+ (only if you use the npx mcp-remote bridge)
node --version
```

### 8.2 Option A — Python Client (recommended for scripts/agents)

```bash
# 1. Create a working folder
mkdir -p ~/mcp-client && cd ~/mcp-client

# 2. Create a virtual environment (recommended)
python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows (PowerShell):
# .venv\Scripts\Activate.ps1

# 3. Install dependencies
pip install "mcp[cli]" httpx python-dotenv

# 4. Create .env with YOUR credentials
cat > .env << 'EOF'
MCP_SERVER_URL=https://memory.example.com
MCP_API_KEY=<your-api-key>
ADMIN_API_KEY=<your-admin-key>
EOF

# 5. Download client_example.py from this repo, then run it
python client_example.py
```

Expected output:
```
Available tools: ['commit_session', 'get_session', 'list_sessions', 'set_key', 'get_key', 'del_key', 'admin_health', ...]
set_key: {...}
get_key: {...}
```

### 8.3 Option B — MCP Client Config (opencode / Claude Desktop / Cursor / VS Code)

If your AI agent supports MCP servers via config file, point it at the SSE endpoint. Two variants:

**B1. Using `npx mcp-remote` bridge (works with any MCP client, no Python needed):**

```json
{
  "mcpServers": {
    "shared-memory": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "https://memory.example.com/sse",
        "--header",
        "x-api-key: ${MCP_API_KEY}"
      ],
      "env": {
        "MCP_API_KEY": "<your-api-key>"
      }
    }
  }
}
```

**B2. Local server over SSH tunnel (if the server is not publicly exposed):**

```bash
# Terminal 1: open tunnel (keep running)
ssh -N -L 8000:127.0.0.1:8000 <user>@<your-vps-host>
```

```json
{
  "mcpServers": {
    "shared-memory": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "http://localhost:8000/sse",
        "--header",
        "x-api-key: ${MCP_API_KEY}"
      ],
      "env": {
        "MCP_API_KEY": "<your-api-key>"
      }
    }
  }
}
```

**Where to put this config:**

| Client | Config file location |
|--------|----------------------|
| opencode | `~/.config/opencode/opencode.json` (or project `.opencode.json`) |
| Claude Desktop (macOS) | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Claude Desktop (Windows) | `%APPDATA%\Claude\claude_desktop_config.json` |
| Cursor | `.cursor/mcp.json` in project root |
| VS Code (Copilot MCP) | `.vscode/mcp.json` in project root |

Restart the client after editing. The `shared-memory` tools should appear in the tool list.

### 8.4 Option C — Raw HTTP (curl / any language)

Useful for debugging or non-MCP integrations:

```bash
# 1. Health check (no auth)
curl -s https://memory.example.com/health
# {"status":"ok"}

# 2. Open SSE stream (keep running in one terminal)
curl -N -H "x-api-key: <your-api-key>" \
  https://memory.example.com/sse

# 3. In another terminal, POST a JSON-RPC tool call
curl -s -X POST https://memory.example.com/messages/?session_id=<session-id-from-step-2> \
  -H "Content-Type: application/json" \
  -H "x-api-key: <your-api-key>" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "list_sessions",
      "arguments": {}
    }
  }'
```

> The `session_id` comes from the SSE `endpoint` event in step 2. Raw HTTP is mainly for diagnostics — prefer the Python client or MCP config.

### 8.5 Verify Your Setup

Run this checklist in order. Stop at the first failure.

```bash
# 1. Network reachable?
curl -sI https://memory.example.com/health | head -1
# expect: HTTP/2 200

# 2. API key valid? (unauthenticated call must fail)
mcp call-tool list_sessions '{}'
# expect: {"error":"unauthorized"}

# 3. API key works?
mcp call-tool list_sessions '{"api_key": "'$MCP_API_KEY'"}'
# expect: {"sessions":[...],"count":N}

# 4. Write + read round-trip
mcp call-tool set_key '{"params": {"key": "onboarding:test", "value": "ok", "ttlSeconds": 60}, "api_key": "'$MCP_API_KEY'"}'
mcp call-tool get_key '{"key": "onboarding:test", "api_key": "'$MCP_API_KEY'"}'
# expect: {"key":"onboarding:test","value":"ok"}

# 5. Cleanup
mcp call-tool del_key '{"key": "onboarding:test", "api_key": "'$MCP_API_KEY'"}'
```

### 8.6 Troubleshooting Onboarding

| Problem | Likely cause | Fix |
|---------|--------------|-----|
| `Connection refused` | Server down or wrong URL/port | Check `curl .../health`; confirm URL from owner |
| `SSL certificate error` | Self-signed cert / wrong domain | Use the exact domain issued by certbot |
| `{"error":"unauthorized"}` | Wrong or missing API key | Re-copy key from `.env`; check for trailing whitespace |
| `{"error":"admin unauthorized"}` | Using client key for admin tool | Ask owner for `ADMIN_API_KEY` |
| `Invalid arguments: Missing key` | Wrong arg shape | Use `params` wrapper for `set_key`/`commit_session` |
| Tools not showing in agent | Client not restarted / bad JSON | Validate JSON; fully restart the client app |
| `npx mcp-remote` not found | Node.js missing | Install Node.js 18+ |
| SSE drops after ~60s | Proxy timeout | Server-side: `proxy_read_timeout 3600s` |

---

## 9. MCP CLI (Quick Test Without Code)

```bash
# Configure server
mcp config set server.sse.url https://memory.example.com/sse

# Or use env
export MCP_SERVER_URL=https://memory.example.com/sse
export MCP_API_KEY=your-key

# List tools
mcp list-tools

# Call tools (note argument format!)
# set_key / commit_session → USE "params" wrapper
mcp call-tool set_key '{"params": {"key": "test:k", "value": "hello", "ttlSeconds": 60}, "api_key": "'$MCP_API_KEY'"}'

mcp call-tool commit_session '{"params": {"sessionId": "cli-test", "agentId": "cli", "taskSummary": "CLI test", "decisions": [{"summary": "d", "rationale": "r"}]}, "api_key": "'$MCP_API_KEY'"}'

# get_key / del_key / get_session / list_sessions → FLAT args
mcp call-tool get_key '{"key": "test:k", "api_key": "'$MCP_API_KEY'"}'

mcp call-tool get_session '{"session_id": "cli-test", "api_key": "'$MCP_API_KEY'"}'

mcp call-tool list_sessions '{"api_key": "'$MCP_API_KEY'"}'

# Admin tools → ADMIN_API_KEY
mcp call-tool admin_stats '{"api_key": "'$ADMIN_API_KEY'"}'

mcp call-tool admin_export '{"api_key": "'$ADMIN_API_KEY'"}'
```

---

## 10. Agent Integration (Orchestrator/Analyst/Developer/Tester/Reviewer)

Agents use shared memory for:
- **Session log** — `commit_session` per task: decisions, errors, corrections, beliefs.
- **Global KV** — `set_key`/`get_key` for config, feature flags, cached data.
- **TTL cache** — Short-lived values (e.g., rate-limit tokens, API responses).

### 10.1 Orchestrator (`orchestrator.py`)

High-level workflow coordinator. Runs a multi-agent pipeline and persists a full audit trail to shared memory.

```bash
# Run a task through the full agent pipeline
export MCP_SERVER_URL=https://memory.example.com
export MCP_API_KEY=<your-key>
python orchestrator.py run "Deploy v2.3 to prod"

# Resume / pin a session ID
python orchestrator.py run "Fix login bug" --session ses_login_fix

# Log the plan without executing agents
python orchestrator.py run "Deploy v2.3" --dry-run

# Inspect
python orchestrator.py list
python orchestrator.py show ses_login_fix

# Export everything (requires ADMIN_API_KEY)
python orchestrator.py export
```

**Pipeline:** `analyst → developer → tester → reviewer`, stopping at the first failure.

#### How history survives `commit_session` overwrite

The server's `commit_session` **replaces** the whole session record, so naively calling it per agent would erase earlier agents' work. The orchestrator solves this:

| Layer | Key | Purpose |
|-------|-----|---------|
| Orchestrator trail | `orch:<session>:log` | Every start / delegate / complete / error / finish event |
| Per-agent trail | `agent:<agent>:<session>:log` | Each agent's own append-only entries |
| Consolidated view | `orch:<session>:agents` | Merged, time-sorted trail of all agents |
| Session record | `session:<session>` | Single record with **all** decisions/errors/beliefs merged |

`write_consolidated()` merges every agent log and writes one session record, so `get_session` returns the complete picture. The final `outcome` is taken from the orchestrator's own `finish` event — never from a stale agent entry.

### 10.2 Shared Client (`memory_client.py`)

Single `MemoryClient` used by both the orchestrator and all agents:

```python
from memory_client import MemoryClient

async with MemoryClient() as mem:
    await mem.set_key("config:timeout", 30, ttl_seconds=3600)
    print(await mem.get_key("config:timeout"))

    await mem.commit_session(
        session_id="ses_001",
        agent_id="analyst",
        task_summary="Analyze auth bug",
        decisions=[{"summary": "no rate limit", "rationale": "brute force risk"}],
    )
    print(await mem.get_session("ses_001"))
    print(await mem.list_sessions())
```

It handles the argument-format quirks for you (`params` wrapper for `set_key`/`commit_session`, flat args for the rest) and normalizes results into dicts.

### 10.3 Agent Base (`agent_base.py`)

Reusable base class for all agents. Provides:
- `log()` — appends to the agent's rolling log **and** mirrors into the session record
- `save_state()` / `load_state()` — per-agent, per-session KV (`agent:<id>:<session>:<key>`)
- Built-in subclasses: `AnalystAgent`, `DeveloperAgent`, `TesterAgent`, `ReviewerAgent`

```python
# Run a single agent directly
python agent_base.py analyst ses_001 "Analyze auth bug"
```

Add your own agent:

```python
from agent_base import BaseAgent, AgentResult, AGENT_MAP

class DeployAgent(BaseAgent):
    agent_id = "deployer"

    async def run(self, task: str) -> AgentResult:
        await self.log(f"Deploying: {task}")
        await self.save_state("status", "deployed")
        await self.log("Deploy complete", beliefs=["Rollback tested"])
        return AgentResult(True, f"Deployed: {task}")

AGENT_MAP["deployer"] = DeployAgent
```

### 10.4 Offline Test

`test_orchestrator_offline.py` runs the whole pipeline against a fake in-memory client — no server needed:

```bash
python test_orchestrator_offline.py
```

It asserts the rolling logs exist, all four agents run in order, the pipeline halts on failure, dry-run does not delegate, and — importantly — **no agent decisions are lost to `commit_session` overwrites**.

### 10.5 Example: Orchestrator saves decision

```python
await client.call_tool("commit_session", {
    "params": {
        "sessionId": "deploy-001",
        "agentId": "orchestrator",
        "taskSummary": "Deploy v2.3 to prod",
        "outcome": "success",
        "decisions": [
            {"summary": "blue-green strategy", "rationale": "zero-downtime required"}
        ],
        "errors": [],
        "userCorrections": ["user asked for canary instead"],
        "beliefUpdates": ["prod DB can handle 2x load"]
    },
    "api_key": MCP_API_KEY
})
```

### 10.6 Example: Developer reads config

```python
timeout = await client.call_tool("get_key", {
    "key": "config:api_timeout",
    "api_key": MCP_API_KEY
})
# timeout["value"] → 30
```

---

## 11. Admin Operations (via MCP Tools)

| Tool | Purpose | Auth |
|------|---------|------|
| `admin_health` | Redis ping | ADMIN_API_KEY |
| `admin_stats` | Memory, key count, sessions | ADMIN_API_KEY |
| `admin_keys` | List keys by pattern | ADMIN_API_KEY |
| `admin_del_keys` | Bulk delete by pattern | ADMIN_API_KEY |
| `admin_flush` | Delete ALL (confirm=true) | ADMIN_API_KEY |
| `admin_export` | Export all sessions to JSON | ADMIN_API_KEY |
| `admin_import` | Import sessions from JSON | ADMIN_API_KEY |

---

## 11.1 New Features (v2.1)

| Tool / Endpoint | Purpose | Auth / Access |
|-----------------|---------|---------------|
| `mget` | Batch get multiple keys: `{ "keys": ["k1", "k2"] }` | MCP_API_KEY |
| `mset` | Batch set multiple keys atomically: `{ "items": {"k1": "v1"}, "ttlSeconds": 300 }` | MCP_API_KEY |
| `commit_session` `ttlSeconds` | Optional auto-expiry for session records | MCP_API_KEY |
| `GET /metrics` | Prometheus metrics (`mcp_tool_calls_total`, `mcp_sessions_total`, `mcp_kv_keys_total`, `mcp_redis_used_bytes`) | Public (no auth) |

---

## 12. Security Checklist

- [ ] `MCP_API_KEY` and `ADMIN_API_KEY` are 32+ char random strings.
- [ ] `ADMIN_API_KEY` != `MCP_API_KEY`.
- [ ] Nginx rate limiting enabled (`limit_req_zone`).
- [ ] Redis only on Docker internal network (no exposed port).
- [ ] TLS via Let's Encrypt (certbot).
- [ ] `.env` not committed to git (in `.gitignore`).
- [ ] Regular `admin_export` backups stored offline.

---

## 13. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `{"error":"unauthorized"}` | Missing/wrong `api_key` | Send `x-api-key` header or `api_key` arg |
| `{"error":"admin unauthorized"}` | Using `MCP_API_KEY` for admin tool | Use `ADMIN_API_KEY` |
| `{"error":"not found"}` | Key/session doesn't exist | Check spelling; `list_sessions` / `admin_keys` |
| `Invalid arguments: Missing key` | Wrong arg format (params vs flat) | See **Real-World Behavior** below |
| SSE connection drops | Nginx `proxy_read_timeout` too low | Set `3600s` in Nginx config |
| `docker compose up` fails | Port 8000 in use / Redis not healthy | `docker compose down -v` then retry |

---

## 14. Ponytail Notes (Intentionally Omitted)

- No Redis Cluster / Sentinel (single instance, AOF persistence).
- No built-in encryption at rest (VPS disk encryption recommended).
- No multi-tenancy / namespaces (keys are global — use prefixes like `proj1:session:`).
- No Lua scripts / complex transactions (use `mset` pipeline for atomic multi-key writes).

---

## 15. Real-World Behavior (Live Testing Notes)

### Argument Format Quirks (Critical!)

| Tool | Correct Format | Incorrect Format |
|------|----------------|------------------|
| `set_key` | `{ "params": { "key": "k", "value": "v", "ttlSeconds": 60 } }` | `{ "key": "k", "value": "v" }` |
| `commit_session` | `{ "params": { "sessionId": "s1", "agentId": "a", "taskSummary": "t", "decisions": [...], "errors": [...] } }` | flat arguments |
| `get_key` | `{ "key": "k" }` | `{ "params": { "key": "k" } }` |
| `del_key` | `{ "key": "k" }` | `{ "params": { "key": "k" } }` |
| `get_session` | `{ "session_id": "s1" }` | `{ "params": { "session_id": "s1" } }` |
| `list_sessions` | `{}` | `{ "params": {} }` |
| `admin_*` | `{ "api_key": "ADMIN_KEY" }` | without api_key |

> **Pattern:** Tools accepting complex objects (`set_key`, `commit_session`) require a `params` wrapper. Simple tools (`get_key`, `del_key`, `get_session`, `list_sessions`) use flat arguments.

### Actual Runtime Behavior

1. **Session Overwrite** — `commit_session` **overwrites** the existing session record (not append). Fields `decisions`, `errors`, `userCorrections`, `beliefUpdates` are replaced entirely.
2. **Global Namespace** — KV (`set_key`/`get_key`) is **global**, not scoped per session. The `session_id` parameter in `set_key` is ignored (metadata only).
3. **TTL Supported** — `set_key` accepts `ttlSeconds` (seconds), auto-expires via Redis `SETEX`.
4. **JSON Values** — Value can be string, number, boolean, array, nested object. Stored as JSON string in Redis.
5. **Auth Middleware** — `XApiKeyMiddleware` injects `api_key` from header `x-api-key` into MCP request body. Clients **do not need** to send `api_key` manually per call if the header is set.
6. **Admin Lockdown** — All `admin_*` tools require `ADMIN_API_KEY`. Regular `MCP_API_KEY` **cannot** access admin tools.
7. **Error Format** — Always returns an object: `{ "error": "unauthorized" }`, `{ "error": "not found", "key": "..." }`, or `{ "error": "admin unauthorized" }`.

### Correct MCP CLI Call Examples

```bash
# set_key (uses params wrapper)
mcp call-tool set_key --server sse:${MCP_SERVER_URL}/sse \
  '{"params": {"key": "cfg:timeout", "value": 30, "ttlSeconds": 3600}, "api_key": "'$MCP_API_KEY'"}'

# commit_session (uses params wrapper)
mcp call-tool commit_session --server sse:${MCP_SERVER_URL}/sse \
  '{"params": {"sessionId": "ses_001", "agentId": "analyst", "taskSummary": "Analyze login bug", "decisions": [{"summary": "root cause: pool exhausted", "rationale": "metrics show 100% used"}], "errors": ["timeout after 30s"]}, "api_key": "'$MCP_API_KEY'"}'

# get_key / del_key / get_session (FLAT)
mcp call-tool get_key --server sse:${MCP_SERVER_URL}/sse \
  '{"key": "cfg:timeout", "api_key": "'$MCP_API_KEY'"}'
mcp call-tool get_session --server sse:${MCP_SERVER_URL}/sse \
  '{"session_id": "ses_001", "api_key": "'$MCP_API_KEY'"}'
mcp call-tool list_sessions --server sse:${MCP_SERVER_URL}/sse \
  '{"api_key": "'$MCP_API_KEY'"}'

# admin tools (use ADMIN_API_KEY)
mcp call-tool admin_stats --server sse:${MCP_SERVER_URL}/sse \
  '{"api_key": "'$ADMIN_API_KEY'"}'
```

### Data Models (per implementation)

```python
# SessionRecord (commit_session)
{
  "sessionId": "string",
  "agentId": "string",
  "taskSummary": "string",
  "outcome": "success" | "failure" | "partial",
  "decisions": [{"summary": "string", "rationale": "string"}],
  "errors": ["string"],
  "userCorrections": ["string"],
  "beliefUpdates": ["string"],
  "updatedAt": "ISO8601 UTC"
}

# SetKeyParams
{
  "key": "string",
  "value": "any",           # string|number|bool|array|object
  "ttlSeconds": "integer?"  # optional
}

# CommitSessionParams (extends SessionRecord)
{
  "ttlSeconds": "integer?"  # optional — auto-expire session record
}
```

### New Server Features (v2.1)

| Feature | Tool / Endpoint | Description |
|---------|-----------------|-------------|
| **Session TTL** | `commit_session` `ttlSeconds` | Optional auto-expiry for session records (Redis `EXPIRE`). |
| **Batch KV read** | `mget` | Fetch multiple keys in one round-trip: `{ "keys": ["k1", "k2"] }` → `{ "values": {"k1": ..., "k2": ...} }`. |
| **Batch KV write** | `mset` | Atomic multi-key set with optional shared TTL: `{ "items": {"k1": "v1", "k2": 42}, "ttlSeconds": 300 }`. |
| **Argument normalization** | Middleware | Accepts **both** `params` wrapper AND flat args for all tools. Clients can send `{ "params": {...} }` or flat `{ "key": "k", "value": "v" }` — server normalizes automatically. |
| **Prometheus metrics** | `GET /metrics` | Exposes `mcp_tool_calls_total`, `mcp_sessions_total`, `mcp_kv_keys_total`, `mcp_redis_used_bytes` in text format. |
| **Admin stats enriched** | `admin_stats` | Now includes memory usage and key count from Redis INFO. |

---

## 16. Quick Command Cheatsheet

```bash
# Deploy
cd ~/shared-memory && docker compose up -d --build

# Logs
docker compose logs -f mcp-server

# Health
curl https://memory.example.com/health

# Redis CLI (inside container)
docker exec -it mcp-redis redis-cli

# Backup sessions
mcp call-tool admin_export '{"api_key": "'$ADMIN_API_KEY'"}' > backup.json

# Restore sessions
mcp call-tool admin_import '{"data": '$(cat backup.json)', "api_key": "'$ADMIN_API_KEY'"}'

# Cleanup old keys
mcp call-tool admin_del_keys '{"pattern": "session:*", "api_key": "'$ADMIN_API_KEY'"}'

# Full flush (dangerous)
mcp call-tool admin_flush '{"confirm": true, "api_key": "'$ADMIN_API_KEY'"}'
```

---

## 17. Placeholder Reference

All values in this document are placeholders. Replace them with your own:

| Placeholder | Meaning | How to set |
|-------------|---------|------------|
| `YOUR_DOMAIN` | Public hostname of your MCP server | Your DNS record, e.g. `memory.example.com` |
| `memory.example.com` | Example public URL used throughout | Replace with `YOUR_DOMAIN` |
| `<your-api-key>` | Client API key (`MCP_API_KEY`) | `openssl rand -hex 32` on the server; copy to client `.env` |
| `<your-admin-key>` | Admin API key (`ADMIN_API_KEY`) | `openssl rand -hex 32` on the server; **never** ship to normal clients |
| `<your-vps-host>` | SSH host of your VPS | e.g. `root@203.0.113.10` or your SSH alias |
| `~/shared-memory` | Project directory on the VPS | Any path you choose; set `PROJECT_DIR` when using `deploy_vps.sh` |
| `C:\path\to\shared-memory` | Project directory on Windows | Your local clone location |
| `<user>` | Your OS username on the VPS | `whoami` |

### Environment variables used by the project

| Variable | Used by | Required | Description |
|----------|---------|----------|-------------|
| `REDIS_URL` | server | yes | Redis DSN (default `redis://redis:6379` in Docker) |
| `MCP_HOST` | server | no | Bind address (default `0.0.0.0`) |
| `MCP_PORT` | server | no | Bind port (default `8000`) |
| `MCP_TRANSPORT` | server | no | `sse` (default) or `stdio` |
| `MCP_API_KEY` | server + client | yes | Client authentication key |
| `ADMIN_API_KEY` | server + client | optional | Admin-only authentication key |
| `MCP_SERVER_URL` | client | yes | Base URL, e.g. `https://YOUR_DOMAIN` |
| `PROJECT_DIR` | `deploy_vps.sh`, watchdog | optional | Absolute path to project on VPS |
| `DOMAIN` | `deploy_vps.sh` | optional | Domain for Nginx + certbot |
| `ADMIN_EMAIL` | `deploy_vps.sh` | optional | Email for Let's Encrypt registration |
| `HEALTH_URL` | watchdog | optional | Health endpoint override |

### What you must NOT commit

```
.env                 # real secrets
*.pem  *.key  *.crt  # TLS material
backup*.json         # session dumps may contain sensitive agent data
```

Add these to `.gitignore` before pushing to a shared repository.

---

**Version:** 2.1 (MCP Native, public-safe docs)
**License:** internal / adjust as needed
