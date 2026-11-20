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
    """Simpan/update record session agen."""
    if not verify_api_key(api_key):
        return {"error": "unauthorized"}
    client = await rc.connect()
    record = SessionRecord(**params.model_dump())
    await client.set(rc.session_key(params.sessionId), record.model_dump_json())
    return {"ok": True, "sessionId": params.sessionId}

@mcp.tool()
async def get_session(session_id: str, api_key: str = "") -> dict:
    """Ambil record session by ID."""
    if not verify_api_key(api_key):
        return {"error": "unauthorized"}
    client = await rc.connect()
    data = await client.get(rc.session_key(session_id))
    if not data:
        return {"error": "not found", "sessionId": session_id}
    return json.loads(data)

@mcp.tool()
async def list_sessions(api_key: str = "") -> dict:
    """List semua session ID."""
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
    """Hapus key."""
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
    """Isi argumen api_key dari header x-api-key kalau client tidak mengirimnya.

    Tanpa ini setiap pemanggilan tool agen harus menyertakan api_key manual.
    Key tetap diperiksa per tool oleh verify_api_key().
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