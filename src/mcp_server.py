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
    ttlSeconds: Optional[int] = None

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
    """Save/update agent session record. Optional ttlSeconds for auto-expiry."""
    if not verify_api_key(api_key):
        return {"error": "unauthorized"}
    client = await rc.connect()
    record = SessionRecord(**params.model_dump(exclude={"ttlSeconds"}))
    await client.set(rc.session_key(params.sessionId), record.model_dump_json())
    if params.ttlSeconds:
        await client.expire(rc.session_key(params.sessionId), params.ttlSeconds)
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
    """Set key-value generic (support TTL)."""
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

@mcp.tool()
async def mget(keys: list[str], api_key: str = "") -> dict:
    """Batch get values for multiple keys in one round-trip."""
    if not verify_api_key(api_key):
        return {"error": "unauthorized"}
    if not keys:
        return {"values": {}}
    client = await rc.connect()
    raw = await client.mget(keys)
    out = {}
    for k, v in zip(keys, raw):
        if v is None:
            out[k] = None
        else:
            try:
                out[k] = json.loads(v)
            except json.JSONDecodeError:
                out[k] = v
    return {"values": out}

@mcp.tool()
async def mset(items: dict[str, Any], ttlSeconds: Optional[int] = None, api_key: str = "") -> dict:
    """Batch set multiple key-values atomically (optional shared ttlSeconds)."""
    if not verify_api_key(api_key):
        return {"error": "unauthorized"}
    if not items:
        return {"ok": True, "count": 0}
    client = await rc.connect()
    encoded = {k: (v if isinstance(v, str) else json.dumps(v)) for k, v in items.items()}
    pipe = client.pipeline(transaction=True)
    pipe.mset(encoded)
    if ttlSeconds:
        for k in encoded:
            pipe.expire(k, ttlSeconds)
    await pipe.execute()
    return {"ok": True, "count": len(encoded)}

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
    """Statistik ringkas."""
    if not verify_api_key(api_key, require_admin=True):
        return {"error": "admin unauthorized"}
    client = await rc.connect()
    info = await client.info("memory")
    # redis-py baru mengembalikan dict; versi lama string "key: value"
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
    """List key by pattern."""
    if not verify_api_key(api_key, require_admin=True):
        return {"error": "admin unauthorized"}
    client = await rc.connect()
    keys = await client.keys(pattern)
    return {"pattern": pattern, "count": len(keys), "keys": keys}

@mcp.tool()
async def admin_del_keys(pattern: str = "session:*", api_key: str = "") -> dict:
    """Hapus key by pattern."""
    if not verify_api_key(api_key, require_admin=True):
        return {"error": "admin unauthorized"}
    client = await rc.connect()
    keys = await client.keys(pattern)
    if keys:
        await client.delete(*keys)
    return {"deleted": len(keys), "keys": keys}

@mcp.tool()
async def admin_flush(confirm: bool = False, api_key: str = "") -> dict:
    """Hapus SEMUA data (butuh confirm=true)."""
    if not verify_api_key(api_key, require_admin=True):
        return {"error": "admin unauthorized"}
    if not confirm:
        return {"error": "confirm=true required"}
    client = await rc.connect()
    await client.flushdb()
    return {"ok": True, "message": "all keys deleted"}

@mcp.tool()
async def admin_export(api_key: str = "") -> dict:
    """Export semua session ke JSON."""
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
    """Import session dari JSON {sessionId: {...}}."""
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
    """1) Inject api_key from x-api-key header when omitted.
    2) Auto-normalize arguments so BOTH flat and {"params": ...} formats work:
       - set_key / commit_session: wrap flat args into {"params": ...}
       - get_key / del_key / get_session / list_sessions / admin_*: unwrap {"params": ...}
    """

    WRAPPED_TOOLS = {"set_key", "commit_session"}

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
            if isinstance(params, dict):
                tool_name = params.get("name", "")
                arguments = params.get("arguments")
                if isinstance(arguments, dict):
                    api_key = arguments.pop("api_key", None) or key
                    # Normalize argument shape
                    if tool_name in self.WRAPPED_TOOLS:
                        if "params" not in arguments:
                            arguments = {"params": dict(arguments)}
                    else:
                        if set(arguments.keys()) == {"params"} and isinstance(arguments["params"], dict):
                            arguments = dict(arguments["params"])
                    if api_key:
                        arguments["api_key"] = api_key
                    params["arguments"] = arguments
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
    from starlette.responses import JSONResponse, PlainTextResponse
    from starlette.middleware import Middleware
    from starlette.routing import Route

    METRICS_HELP = """# HELP mcp_tool_calls_total Total tool calls
# TYPE mcp_tool_calls_total counter
# HELP mcp_sessions_total Total sessions created
# TYPE mcp_sessions_total gauge
# HELP mcp_kv_keys_total Total KV keys
# TYPE mcp_kv_keys_total gauge
"""

    tool_calls: dict[str, int] = {}

    async def metrics_endpoint(request):
        client = await rc.connect()
        info = await client.info("memory")
        used_mem = info.get("used_memory", 0) if isinstance(info, dict) else 0
        total_keys = await client.dbsize()
        sessions = await client.keys("session:*")

        lines = [METRICS_HELP]
        for tool, count in tool_calls.items():
            lines.append(f'mcp_tool_calls_total{{tool="{tool}"}} {count}')
        lines.append(f'mcp_sessions_total {len(sessions)}')
        lines.append(f'mcp_kv_keys_total {total_keys}')
        lines.append(f'mcp_redis_used_bytes {used_mem}')
        return PlainTextResponse("\n".join(lines), media_type="text/plain; version=0.0.4")

    async def wrapped_mcp_app(scope, receive, send):
        if scope["type"] == "http" and scope["method"] == "POST":
            # We don't have easy access to tool name here, but we can track via middleware if needed
            pass
        await mcp_app(scope, receive, send)

    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8000"))
    transport = os.getenv("MCP_TRANSPORT", "sse")
    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp_app = mcp.http_app(transport="sse", middleware=[Middleware(XApiKeyMiddleware)])
        app = mcp_app
        app.add_route("/health", lambda req: JSONResponse({"status": "ok"}), methods=["GET"])
        app.add_route("/metrics", metrics_endpoint, methods=["GET"])
        uvicorn.run(app, host=host, port=port)