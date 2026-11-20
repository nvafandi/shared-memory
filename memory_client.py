#!/usr/bin/env python3
"""
Shared MCP client wrapper for Shared-Memory.

Single source of truth for connecting to the MCP server and calling tools.
Used by orchestrator.py, agent_base.py, and any custom agent.

Install:
    pip install "mcp[cli]" httpx python-dotenv
"""
import os
import json
from contextlib import AsyncExitStack
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from mcp import ClientSession
from mcp.client.sse import sse_client

load_dotenv()

DEFAULT_URL = "https://memory.example.com"


def _parse_result(result: Any) -> Dict[str, Any]:
    """Normalize an MCP CallToolResult into a Python dict."""
    content = getattr(result, "content", None)
    if not content:
        return {"raw": str(result)}
    text = getattr(content[0], "text", None)
    if text is None:
        return {"raw": str(content[0])}
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return {"value": text}


class MemoryClient:
    """Async client for MCP Shared-Memory.

    Usage:
        async with MemoryClient(url, api_key) as mem:
            await mem.set_key("k", "v")
            print(await mem.get_key("k"))
    """

    def __init__(self, server_url: Optional[str] = None, api_key: Optional[str] = None):
        self.server_url = server_url or os.getenv("MCP_SERVER_URL", DEFAULT_URL)
        self.api_key = api_key or os.getenv("MCP_API_KEY")
        self.admin_key = os.getenv("ADMIN_API_KEY")
        if not self.api_key:
            raise RuntimeError("MCP_API_KEY is required")
        self._session: Optional[ClientSession] = None
        self._stack: Optional[AsyncExitStack] = None

    async def __aenter__(self) -> "MemoryClient":
        self._stack = AsyncExitStack()
        read, write = await self._stack.enter_async_context(
            sse_client(f"{self.server_url}/sse")
        )
        self._session = await self._stack.enter_async_context(ClientSession(read, write))
        await self._session.initialize()
        return self

    async def __aexit__(self, *exc_info) -> None:
        if self._stack:
            await self._stack.aclose()
            self._stack = None
        self._session = None

    async def _call(self, tool: str, arguments: Dict[str, Any],
                    admin: bool = False) -> Dict[str, Any]:
        if not self._session:
            raise RuntimeError("MemoryClient not initialized (use 'async with')")
        key = self.admin_key if admin else self.api_key
        if admin and not key:
            raise RuntimeError("ADMIN_API_KEY is required for admin tools")
        result = await self._session.call_tool(tool, {**arguments, "api_key": key})
        return _parse_result(result)

    # ---------- Session tools ----------

    async def commit_session(
        self,
        session_id: str,
        agent_id: str,
        task_summary: str,
        outcome: str = "success",
        decisions: Optional[List[Dict]] = None,
        errors: Optional[List[str]] = None,
        user_corrections: Optional[List[str]] = None,
        belief_updates: Optional[List[str]] = None,
        ttl_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Overwrites the session record (server-side behavior). Optional ttl_seconds for auto-expiry."""
        args = {
            "params": {
                "sessionId": session_id,
                "agentId": agent_id,
                "taskSummary": task_summary,
                "outcome": outcome,
                "decisions": decisions or [],
                "errors": errors or [],
                "userCorrections": user_corrections or [],
                "beliefUpdates": belief_updates or [],
            }
        }
        if ttl_seconds:
            args["params"]["ttlSeconds"] = ttl_seconds
        return await self._call("commit_session", args)

    async def get_session(self, session_id: str) -> Dict[str, Any]:
        return await self._call("get_session", {"session_id": session_id})

    async def list_sessions(self) -> Dict[str, Any]:
        return await self._call("list_sessions", {})

    # ---------- KV tools ----------

    async def set_key(self, key: str, value: Any,
                      ttl_seconds: Optional[int] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"key": key, "value": value}
        if ttl_seconds:
            params["ttlSeconds"] = ttl_seconds
        return await self._call("set_key", {"params": params})

    async def get_key(self, key: str) -> Dict[str, Any]:
        return await self._call("get_key", {"key": key})

    async def del_key(self, key: str) -> Dict[str, Any]:
        return await self._call("del_key", {"key": key})

    async def mget(self, keys: List[str]) -> Dict[str, Any]:
        """Batch get values for multiple keys."""
        return await self._call("mget", {"keys": keys})

    async def mset(self, items: Dict[str, Any], ttl_seconds: Optional[int] = None) -> Dict[str, Any]:
        """Batch set multiple key-values atomically with optional shared TTL."""
        args = {"items": items}
        if ttl_seconds:
            args["ttlSeconds"] = ttl_seconds
        return await self._call("mset", args)

    # ---------- Admin tools ----------

    async def admin_health(self) -> Dict[str, Any]:
        return await self._call("admin_health", {}, admin=True)

    async def admin_stats(self) -> Dict[str, Any]:
        return await self._call("admin_stats", {}, admin=True)

    async def admin_keys(self, pattern: str = "session:*") -> Dict[str, Any]:
        return await self._call("admin_keys", {"pattern": pattern}, admin=True)

    async def admin_export(self) -> Dict[str, Any]:
        return await self._call("admin_export", {}, admin=True)

    async def admin_import(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return await self._call("admin_import", {"data": data}, admin=True)

    async def admin_del_keys(self, pattern: str = "session:*") -> Dict[str, Any]:
        return await self._call("admin_del_keys", {"pattern": pattern}, admin=True)

    async def admin_flush(self, confirm: bool = False) -> Dict[str, Any]:
        return await self._call("admin_flush", {"confirm": confirm}, admin=True)