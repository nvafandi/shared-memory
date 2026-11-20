"""
Orchestrator — coordinates a multi-agent workflow and records everything
in MCP Shared-Memory.

Usage:
    export MCP_SERVER_URL=https://memory.example.com
    export MCP_API_KEY=<your-key>

    python orchestrator.py run "Deploy v2.3 to prod"
    python orchestrator.py run "Fix login bug" --session ses_login_fix
    python orchestrator.py list
    python orchestrator.py show ses_login_fix
    python orchestrator.py export            # admin only

Environment:
    MCP_SERVER_URL  (required)
    MCP_API_KEY     (required)
    ADMIN_API_KEY   (optional, for `export`)
"""
import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime
from typing import Any

from dotenv import load_dotenv

from memory_client import MemoryClient

load_dotenv()

AGENT_ORDER = ["analyst", "developer", "tester", "reviewer"]


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def new_session_id(prefix: str = "orch") -> str:
    return f"{prefix}-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"


class Orchestrator:
    """Coordinates agents and persists a shared, append-like session trail.

    Note: the MCP server's `commit_session` OVERWRITES a session record.
    To preserve history we keep a rolling log in KV under
    `orch:<session>:log` and mirror the latest state into the session record.
    """

    def __init__(self, memory: MemoryClient, session_id: str):
        self.memory = memory
        self.session_id = session_id
        self.log_key = f"orch:{session_id}:log"
        self.history: list[dict[str, Any]] = []

    # ---------- internal ----------

    async def _append(self, entry: dict[str, Any]) -> None:
        entry["at"] = now_iso()
        self.history.append(entry)
        # Persist rolling history (KV only). Session record is synced once at finish().
        await self.memory.set_key(self.log_key, self.history)

    async def _sync_session_record(self) -> None:
        """Sync consolidated session record from accumulated history."""
        decisions = [
            {"summary": e["summary"], "rationale": e.get("rationale", "")}
            for e in self.history if e.get("kind") == "decision"
        ]
        errors = [e["summary"] for e in self.history if e.get("kind") == "error"]
        last = self.history[-1] if self.history else {}
        await self.memory.commit_session(
            session_id=self.session_id,
            agent_id=last.get("agent", "orchestrator"),
            task_summary=last.get("summary", "orchestrator run"),
            outcome=last.get("outcome", "success"),
            decisions=decisions,
            errors=errors,
        )

    # ---------- public API ----------

    async def start(self, task: str) -> None:
        await self._append({
            "kind": "start",
            "agent": "orchestrator",
            "summary": f"Task started: {task}",
            "rationale": "User request",
            "outcome": "success",
        })

    async def decide(self, summary: str, rationale: str,
                     agent: str = "orchestrator") -> None:
        await self._append({
            "kind": "decision",
            "agent": agent,
            "summary": summary,
            "rationale": rationale,
            "outcome": "success",
        })

    async def record_error(self, error: str, agent: str = "orchestrator") -> None:
        await self._append({
            "kind": "error",
            "agent": agent,
            "summary": error,
            "outcome": "failure",
        })

    async def delegate(self, agent: str, task: str) -> None:
        await self._append({
            "kind": "delegate",
            "agent": agent,
            "summary": f"Delegated to {agent}: {task}",
            "rationale": "Orchestrator delegation",
            "outcome": "success",
        })

    async def complete(self, agent: str, result: str) -> None:
        await self._append({
            "kind": "complete",
            "agent": agent,
            "summary": f"{agent} completed: {result}",
            "rationale": "Agent reported success",
            "outcome": "success",
        })

    async def fail(self, agent: str, error: str) -> None:
        await self._append({
            "kind": "error",
            "agent": agent,
            "summary": f"{agent} failed: {error}",
            "outcome": "failure",
        })

    async def finish(self, outcome: str, summary: str) -> None:
        await self._append({
            "kind": "finish",
            "agent": "orchestrator",
            "summary": summary,
            "rationale": f"Final outcome: {outcome}",
            "outcome": outcome,
        })

    async def restore(self) -> list[dict[str, Any]]:
        """Reload history if the session is resumed later."""
        res = await self.memory.get_key(self.log_key)
        if "error" in res:
            return []
        value = res.get("value")
        if isinstance(value, list):
            self.history = value
        return self.history

    async def collect_agent_logs(self, agents: list[str]) -> list[dict[str, Any]]:
        """Merge every agent's rolling log into one chronological trail.

        Agents log to `agent:<id>:<session>:log` because `commit_session`
        overwrites the shared record; merging here reconstructs full history.
        """
        merged: list[dict[str, Any]] = []
        for agent in agents:
            res = await self.memory.get_key(f"agent:{agent}:{self.session_id}:log")
            entries = res.get("value") if "error" not in res else None
            if isinstance(entries, list):
                merged.extend(entries)
        merged.sort(key=lambda e: e.get("at", ""))
        return merged

    async def write_consolidated(self, agents: list[str]) -> None:
        """Persist the merged trail so a single session record holds everything.

        The final `outcome` comes from the orchestrator's own finish entry —
        never from the last agent entry, which may be an earlier success.
        """
        merged = await self.collect_agent_logs(agents)
        decisions: list[dict[str, str]] = []
        errors: list[str] = []
        corrections: list[str] = []
        beliefs: list[str] = []
        for entry in merged:
            for d in entry.get("decisions", []):
                if isinstance(d, dict) and d not in decisions:
                    decisions.append(d)
            errors.extend(e for e in entry.get("errors", []) if e not in errors)
            corrections.extend(c for c in entry.get("userCorrections", []) if c not in corrections)
            beliefs.extend(b for b in entry.get("beliefUpdates", []) if b not in beliefs)

        # Authoritative outcome: the orchestrator's finish entry.
        finish = next(
            (e for e in reversed(self.history) if e.get("kind") == "finish"),
            None,
        )
        outcome = finish["outcome"] if finish else "success"
        summary = finish["summary"] if finish else "consolidated"
        for e in self.history:
            if e.get("kind") == "error":
                msg = e.get("summary", "")
                if msg and msg not in errors:
                    errors.append(msg)

        await self.memory.commit_session(
            session_id=self.session_id,
            agent_id="orchestrator",
            task_summary=summary,
            outcome=outcome,
            decisions=decisions,
            errors=errors,
            user_corrections=corrections,
            belief_updates=beliefs,
        )
        await self.memory.set_key(f"orch:{self.session_id}:agents", merged)


# ---------- workflow ----------

async def run_workflow(task: str, session_id: str, dry_run: bool = False) -> str:
    async with MemoryClient() as mem:
        orch = Orchestrator(mem, session_id)
        await orch.start(task)
        print(f"[orchestrator] session={session_id}")
        print(f"[orchestrator] task={task}")

        if dry_run:
            print("[orchestrator] dry-run: no agent execution")
            await orch.finish("success", "Dry run only")
            return session_id

        # Lazy import to avoid a hard dependency when only using `list`/`show`.
        try:
            from agent_base import run_agent
        except ImportError as exc:
            print(f"ERROR: cannot import agents: {exc}", file=sys.stderr)
            await orch.fail("orchestrator", str(exc))
            return session_id

        failed = False
        for agent in AGENT_ORDER:
            await orch.delegate(agent, task)
            print(f"[orchestrator] -> {agent}")
            try:
                result = await run_agent(agent, session_id, task)
            except Exception as exc:  # noqa: BLE001 - report, don't crash pipeline
                failed = True
                await orch.fail(agent, str(exc))
                print(f"[orchestrator] {agent} FAILED: {exc}", file=sys.stderr)
                break

            if result.success:
                await orch.complete(agent, result.output)
                print(f"[orchestrator] <- {agent} ok")
            else:
                failed = True
                await orch.fail(agent, result.output)
                print(f"[orchestrator] <- {agent} reported failure", file=sys.stderr)
                break

        outcome = "failure" if failed else "success"
        await orch.finish(outcome, f"Pipeline {outcome} for: {task}")
        await orch.write_consolidated(AGENT_ORDER)
        print(f"[orchestrator] finished: {outcome}")

        final = await mem.get_session(session_id)
        print("\n--- Session record (consolidated) ---")
        print(json.dumps(final, indent=2))
        return session_id


# ---------- CLI ----------

async def cmd_list() -> None:
    async with MemoryClient() as mem:
        print(json.dumps(await mem.list_sessions(), indent=2))


async def cmd_show(session_id: str) -> None:
    async with MemoryClient() as mem:
        print(json.dumps(await mem.get_session(session_id), indent=2))


async def cmd_export() -> None:
    async with MemoryClient() as mem:
        print(json.dumps(await mem.admin_export(), indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Orchestrator backed by MCP Shared-Memory"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run the multi-agent pipeline")
    p_run.add_argument("task", help="Task description")
    p_run.add_argument("--session", default=None, help="Session ID (auto-generated if omitted)")
    p_run.add_argument("--dry-run", action="store_true", help="Log the plan without running agents")

    sub.add_parser("list", help="List all sessions")

    p_show = sub.add_parser("show", help="Show one session record")
    p_show.add_argument("session_id")

    sub.add_parser("export", help="Export all sessions (requires ADMIN_API_KEY)")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.cmd == "run":
        session_id = args.session or new_session_id()
        asyncio.run(run_workflow(args.task, session_id, dry_run=args.dry_run))
    elif args.cmd == "list":
        asyncio.run(cmd_list())
    elif args.cmd == "show":
        asyncio.run(cmd_show(args.session_id))
    elif args.cmd == "export":
        asyncio.run(cmd_export())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())