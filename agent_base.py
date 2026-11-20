#!/usr/bin/env python3
"""
Agent implementations backed by MCP Shared-Memory.

All agents inherit BaseAgent and record their work via MemoryClient.
Agents never talk to Redis directly — only through MCP tools.

Usage (single agent):
    python agent_base.py analyst ses_001 "Analyze auth bug"

Environment:
    MCP_SERVER_URL  (required)
    MCP_API_KEY     (required)
"""
import sys
import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from memory_client import MemoryClient


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class AgentResult:
    success: bool
    output: str
    decisions: List[Dict[str, str]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    corrections: List[str] = field(default_factory=list)
    beliefs: List[str] = field(default_factory=list)


class BaseAgent:
    """Base class for agents.

    Subclasses implement `run(task)` and use `self.log()` / `self.save_state()`
    to persist to shared memory.
    """

    agent_id: str = "base"

    def __init__(self, memory: MemoryClient, session_id: str):
        self.memory = memory
        self.session_id = session_id

    # ---------- memory helpers ----------

    def _state_key(self, key: str) -> str:
        return f"agent:{self.agent_id}:{self.session_id}:{key}"

    def _log_key(self) -> str:
        """Rolling per-agent log.

        Needed because `commit_session` OVERWRITES the session record, so
        sequential agents would otherwise erase each other's history.
        """
        return f"agent:{self.agent_id}:{self.session_id}:log"

    async def _append_log(self, entry: Dict[str, Any]) -> None:
        res = await self.memory.get_key(self._log_key())
        history = res.get("value") if "error" not in res else []
        if not isinstance(history, list):
            history = []
        entry = {**entry, "agent": self.agent_id, "at": _now_iso()}
        history.append(entry)
        await self.memory.set_key(self._log_key(), history)

    async def log(
        self,
        summary: str,
        outcome: str = "success",
        decisions: Optional[List[Dict]] = None,
        errors: Optional[List[str]] = None,
        corrections: Optional[List[str]] = None,
        beliefs: Optional[List[str]] = None,
    ) -> None:
        # 1. Append to our own rolling log so nothing is lost.
        await self._append_log({
            "summary": summary,
            "outcome": outcome,
            "decisions": decisions or [],
            "errors": errors or [],
            "userCorrections": corrections or [],
            "beliefUpdates": beliefs or [],
        })
        # 2. Mirror into the shared session record (last writer wins).
        await self.memory.commit_session(
            session_id=self.session_id,
            agent_id=self.agent_id,
            task_summary=summary,
            outcome=outcome,
            decisions=decisions,
            errors=errors,
            user_corrections=corrections,
            belief_updates=beliefs,
        )

    async def save_state(self, key: str, value: Any,
                         ttl_seconds: Optional[int] = None) -> None:
        await self.memory.set_key(self._state_key(key), value, ttl_seconds)

    async def load_state(self, key: str) -> Any:
        res = await self.memory.get_key(self._state_key(key))
        return None if "error" in res else res.get("value")

    # ---------- batch KV helpers ----------

    async def mget(self, keys: List[str]) -> Dict[str, Any]:
        """Batch get values for multiple keys."""
        return await self.memory.mget(keys)

    async def mset(self, items: Dict[str, Any], ttl_seconds: Optional[int] = None) -> Dict[str, Any]:
        """Batch set multiple key-values atomically with optional shared TTL."""
        return await self.memory.mset(items, ttl_seconds)

    # ---------- contract ----------

    async def run(self, task: str) -> AgentResult:
        raise NotImplementedError


# ---------- concrete agents ----------

class AnalystAgent(BaseAgent):
    agent_id = "analyst"

    async def run(self, task: str) -> AgentResult:
        await self.log(f"Analyzing: {task}")

        findings = f"Analysis complete for: {task}. 3 issues found, 1 critical."
        decision = {
            "summary": "Critical path identified",
            "rationale": "Auth flow has no rate limiting",
        }

        await self.save_state("findings", findings)
        await self.log(
            f"Analysis done: {task}",
            decisions=[decision],
            beliefs=["Auth service needs a circuit breaker"],
        )
        return AgentResult(
            success=True,
            output=findings,
            decisions=[decision],
            beliefs=["Auth service needs a circuit breaker"],
        )


class DeveloperAgent(BaseAgent):
    agent_id = "developer"

    async def run(self, task: str) -> AgentResult:
        await self.log(f"Implementing: {task}")

        output = f"Code changes pushed for: {task}"
        decision = {
            "summary": "Added rate limiter middleware",
            "rationale": "Prevents brute-force attempts",
        }

        await self.save_state("changes", output)
        await self.log(
            f"Implementation done: {task}",
            decisions=[decision],
            beliefs=["Middleware pattern works for auth"],
        )
        return AgentResult(
            success=True,
            output=output,
            decisions=[decision],
            beliefs=["Middleware pattern works for auth"],
        )


class TesterAgent(BaseAgent):
    agent_id = "tester"

    async def run(self, task: str) -> AgentResult:
        await self.log(f"Testing: {task}")

        output = f"Tests pass for: {task}"
        decision = {
            "summary": "Added integration test for rate limiting",
            "rationale": "Coverage gap on auth endpoints",
        }

        await self.save_state("test_result", output)
        await self.log(f"Tests done: {task}", decisions=[decision])
        return AgentResult(success=True, output=output, decisions=[decision])


class ReviewerAgent(BaseAgent):
    agent_id = "reviewer"

    async def run(self, task: str) -> AgentResult:
        await self.log(f"Reviewing: {task}")

        output = f"Code review approved: {task}"
        decision = {
            "summary": "Approved with minor comments",
            "rationale": "Follows existing team patterns",
        }

        await self.save_state("review", output)
        await self.log(
            f"Review done: {task}",
            decisions=[decision],
            beliefs=["Team conventions are consistent"],
        )
        return AgentResult(
            success=True,
            output=output,
            decisions=[decision],
            beliefs=["Team conventions are consistent"],
        )


AGENT_MAP: Dict[str, type[BaseAgent]] = {
    "analyst": AnalystAgent,
    "developer": DeveloperAgent,
    "tester": TesterAgent,
    "reviewer": ReviewerAgent,
}


async def run_agent(agent_name: str, session_id: str, task: str) -> AgentResult:
    """Run a single agent inside its own MCP connection."""
    cls = AGENT_MAP.get(agent_name)
    if cls is None:
        raise ValueError(f"Unknown agent: {agent_name}. Known: {list(AGENT_MAP)}")
    async with MemoryClient() as mem:
        return await cls(mem, session_id).run(task)


def main(argv: Optional[List[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if len(argv) < 3:
        print("Usage: python agent_base.py <agent> <session_id> <task>")
        print(f"Agents: {list(AGENT_MAP)}")
        return 1
    agent_name, session_id = argv[0], argv[1]
    task = " ".join(argv[2:])
    result = asyncio.run(run_agent(agent_name, session_id, task))
    print(json.dumps(
        {"success": result.success, "output": result.output, "decisions": result.decisions},
        indent=2,
    ))
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())