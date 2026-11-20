"""
Example Python client accessing MCP Shared-Memory via FastMCP client (SSE over HTTPS).
Install: pip install "mcp[cli]" httpx python-dotenv
"""
import asyncio
import json
import os

from dotenv import load_dotenv
from mcp import ClientSession
from mcp.client.sse import sse_client

load_dotenv()

MCP_URL = os.getenv("MCP_SERVER_URL")  # https://memory.example.com
API_KEY = os.getenv("MCP_API_KEY")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")  # optional, for admin tools

if not MCP_URL or not API_KEY:
    raise RuntimeError("Set MCP_SERVER_URL and MCP_API_KEY in .env")

async def call_tool(tool_name: str, use_admin_key: bool = False, **kwargs):
    """Helper call tool MCP via SSE."""
    key = ADMIN_API_KEY if use_admin_key and ADMIN_API_KEY else API_KEY
    if not key:
        raise RuntimeError("API key not available")
    kwargs["api_key"] = key
    async with sse_client(f"{MCP_URL}/sse") as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        result = await session.call_tool(tool_name, kwargs)
        # result is list of TextContent, extract text
        if result and hasattr(result[0], "text"):
            return json.loads(result[0].text)
        return {"raw": str(result)}

# ===== Wrapper Functions (Client Tools) =====

async def commit_session(session_id: str, agent_id: str, summary: str, outcome: str = "success", **extras):
    return await call_tool("commit_session", params={
        "sessionId": session_id,
        "agentId": agent_id,
        "taskSummary": summary,
        "outcome": outcome,
        **extras
    })

async def get_session(session_id: str):
    return await call_tool("get_session", session_id=session_id)

async def list_sessions():
    return await call_tool("list_sessions")

async def set_key(key: str, value, ttl_seconds: int | None = None):
    return await call_tool("set_key", params={"key": key, "value": value, "ttlSeconds": ttl_seconds})

async def get_key(key: str):
    return await call_tool("get_key", key=key)

async def del_key(key: str):
    return await call_tool("del_key", key=key)

# ===== Admin Functions (butuh ADMIN_API_KEY di .env) =====

async def admin_health():
    return await call_tool("admin_health", use_admin_key=True)

async def admin_stats():
    return await call_tool("admin_stats", use_admin_key=True)

async def admin_keys(pattern: str = "session:*"):
    return await call_tool("admin_keys", pattern=pattern, use_admin_key=True)

async def admin_del_keys(pattern: str = "session:*"):
    return await call_tool("admin_del_keys", pattern=pattern, use_admin_key=True)

async def admin_export():
    return await call_tool("admin_export", use_admin_key=True)

async def admin_import(data: dict):
    return await call_tool("admin_import", data=data, use_admin_key=True)

# ===== Demo Orchestrator Flow =====
async def main():
    SESSION_ID = "ses_login_fix_001"

    print("=== 1. Orchestrator: cek session existing ===")
    existing = await get_session(SESSION_ID)
    print(json.dumps(existing, indent=2, ensure_ascii=False))

    print("\n=== 2. Analyst: simpan analisis ===")
    await commit_session(SESSION_ID, "analyst", "Analisis root cause login timeout", "success",
        decisions=[{"key": "rootCause", "value": "DB pool exhausted", "alasan": "metrics 100% used"}],
        beliefUpdates=["Perlu increase pool size"]
    )

    print("\n=== 3. Orchestrator: baca hasil analyst ===")
    analyst_result = await get_session(SESSION_ID)
    print(json.dumps(analyst_result, indent=2, ensure_ascii=False))

    print("\n=== 4. Developer: implement fix ===")
    await commit_session(SESSION_ID, "developer", "Implement fix connection pool", "success",
        decisions=[{"key": "fix", "value": "pool 50 + leak detection", "alasan": "analyst rec"}]
    )

    print("\n=== 5. Tester: regression test ===")
    await commit_session(SESSION_ID, "tester", "Regression test login flow", "success",
        userCorrections=["test case 3 butuh delay 2s"]
    )

    print("\n=== 6. Reviewer: code review ===")
    await commit_session(SESSION_ID, "reviewer", "Code review PR #123", "success",
        beliefUpdates=["Fix memadai, no regression"]
    )

    print("\n=== 7. Orchestrator: final report ===")
    final = await get_session(SESSION_ID)
    print(json.dumps(final, indent=2, ensure_ascii=False))

    print("\n=== 8. List all sessions ===")
    sessions = await list_sessions()
    print(json.dumps(sessions, indent=2, ensure_ascii=False))

    # Admin demo (jika ADMIN_API_KEY tersedia)
    if ADMIN_API_KEY:
        print("\n=== Admin: stats ===")
        stats = await admin_stats()
        print(json.dumps(stats, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())