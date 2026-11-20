"""Remote test client for MCP shared-memory (run from a client device).

Usage:  python test_remote_client.py
Env:    MCP_SERVER_URL (default https://memory.example.com)
        MCP_API_KEY, ADMIN_API_KEY
"""
import asyncio
import json
import os
import sys

from mcp import ClientSession
from mcp.client.sse import sse_client

URL = os.getenv("MCP_SERVER_URL", "https://memory.example.com")
API_KEY = os.getenv("MCP_API_KEY")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")

results = []


def check(name, ok, detail=""):
    results.append((name, ok))
    print(("PASS " if ok else "FAIL ") + name + ((" -> " + str(detail)) if detail else ""))


def payload(result):
    """Isi TextContent pertama dari CallToolResult sebagai objek Python."""
    if not result.content:
        return None
    text = getattr(result.content[0], "text", None)
    if text is None:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


async def call(s, tool, **kwargs):
    return await asyncio.wait_for(s.call_tool(tool, kwargs), timeout=30)


async def main():
    if not API_KEY:
        print("MCP_API_KEY belum diset")
        return 1

    async with sse_client(f"{URL}/sse") as (read, write), ClientSession(read, write) as s:
        await asyncio.wait_for(s.initialize(), timeout=30)
        check("initialize/handshake", True, s.server_info)

        # auth boundary: key salah harus ditolak
        r = payload(await call(s, "list_sessions", api_key="kunci-salah"))
        check("key salah ditolak", r == {"error": "unauthorized"}, r)

        r = payload(await call(s, "list_sessions", api_key=API_KEY))
        check("list_sessions", isinstance(r, dict) and "sessions" in r, r)

        r = payload(await call(s, "set_key", params={
            "key": "device_test", "value": "opencode-win", "ttlSeconds": 3600,
        }, api_key=API_KEY))
        check("set_key", r == {"ok": True}, r)

        r = payload(await call(s, "get_key", key="device_test", api_key=API_KEY))
        check("get_key", r == {"key": "device_test", "value": "opencode-win"}, r)

        r = payload(await call(s, "commit_session", params={
            "sessionId": "device-test-001",
            "agentId": "opencode-win",
            "taskSummary": "install & test shared-memory dari device",
            "decisions": [{"key": "endpoint", "value": URL}],
        }, api_key=API_KEY))
        check("commit_session", r == {"ok": True, "sessionId": "device-test-001"}, r)

        r = payload(await call(s, "get_session", session_id="device-test-001", api_key=API_KEY))
        check("get_session", r and r.get("sessionId") == "device-test-001", r)

        r = payload(await call(s, "del_key", key="device_test", api_key=API_KEY))
        check("del_key", r == {"ok": True}, r)

        if ADMIN_API_KEY:
            r = payload(await call(s, "admin_health", api_key=ADMIN_API_KEY))
            check("admin_health (admin key)", r and r.get("status") == "ok", r)
            r = payload(await call(s, "admin_stats", api_key=ADMIN_API_KEY))
            check(
                "admin_stats (admin key)",
                isinstance(r, dict) and isinstance(r.get("usedMemoryHuman"), str) and "sessionCount" in r,
                r,
            )
            r = payload(await call(s, "admin_health", api_key=API_KEY))
            check("admin_health (mcp key) ditolak", r == {"error": "admin unauthorized"}, r)

    # ---- auth lewat header x-api-key (api_key tidak dikirim di argumen) ----
    async with sse_client(f"{URL}/sse", headers={"x-api-key": API_KEY}) as (read, write), ClientSession(read, write) as s:
        await asyncio.wait_for(s.initialize(), timeout=30)
        r = payload(await call(s, "list_sessions"))
        check("header auth: list_sessions", isinstance(r, dict) and "sessions" in r, r)
        r = payload(await call(s, "set_key", params={"key": "header_test", "value": 1}))
        check("header auth: set_key", r == {"ok": True}, r)
        r = payload(await call(s, "del_key", key="header_test"))
        check("header auth: del_key", r == {"ok": True}, r)

    async with sse_client(f"{URL}/sse", headers={"x-api-key": "salah"}) as (read, write), ClientSession(read, write) as s:
        await asyncio.wait_for(s.initialize(), timeout=30)
        r = payload(await call(s, "list_sessions"))
        check("header salah ditolak", r == {"error": "unauthorized"}, r)

    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} lulus" + (f", gagal: {failed}" if failed else ""))
    return 0 if not failed else 1


sys.exit(asyncio.run(main()))