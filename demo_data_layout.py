"""Visual demo of how shared-memory stores data during an orchestration.

Runs offline (fake client), then dumps every key and record so you can see
exactly what the server would hold in Redis.

Run: python demo_data_layout.py
"""
import asyncio
import json
from typing import Any

import agent_base
import memory_client
import orchestrator


class FakeMemoryClient:
    """Mirrors real server semantics: commit_session OVERWRITES, KV is global."""

    def __init__(self, *a, **kw):
        self.store: dict[str, Any] = {}
        self.sessions: dict[str, dict] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def commit_session(self, session_id, agent_id, task_summary, outcome="success",
                             decisions=None, errors=None, user_corrections=None,
                             belief_updates=None):
        self.sessions[session_id] = {
            "sessionId": session_id, "agentId": agent_id,
            "taskSummary": task_summary, "outcome": outcome,
            "decisions": decisions or [], "errors": errors or [],
            "userCorrections": user_corrections or [],
            "beliefUpdates": belief_updates or [],
        }
        return {"ok": True, "sessionId": session_id}

    async def get_session(self, session_id):
        return self.sessions.get(session_id, {"error": "not found", "sessionId": session_id})

    async def list_sessions(self):
        return {"sessions": list(self.sessions), "count": len(self.sessions)}

    async def set_key(self, key, value, ttl_seconds=None):
        self.store[key] = value
        return {"ok": True}

    async def get_key(self, key):
        if key not in self.store:
            return {"error": "not found", "key": key}
        return {"key": key, "value": self.store[key]}

    async def del_key(self, key):
        self.store.pop(key, None)
        return {"ok": True}


async def main():
    fake = FakeMemoryClient()
    memory_client.MemoryClient = lambda *a, **kw: fake
    orchestrator.MemoryClient = lambda *a, **kw: fake
    agent_base.MemoryClient = lambda *a, **kw: fake

    sid = "demo-001"
    print("=" * 70)
    print("STEP 1: Orchestrator runs pipeline for session", sid)
    print("=" * 70)
    await orchestrator.run_workflow("Fix auth bug", sid)

    print("\n" + "=" * 70)
    print("STEP 2: What actually lives in Redis after the run")
    print("=" * 70)

    print("\n--- SESSION RECORDS (key: session:<id>) ---")
    for k, v in fake.sessions.items():
        print(f"\n  session:{k}")
        print("  " + json.dumps(v, indent=2).replace("\n", "\n  "))

    print("\n--- KV KEYS (key: <anything>) ---")
    for k in sorted(fake.store):
        v = fake.store[k]
        if isinstance(v, list):
            print(f"\n  {k}   ({len(v)} entries)")
            for e in v:
                kind = e.get("kind") or "log"
                agent = e.get("agent", "?")
                print(f"    [{kind:9}] {agent:12} {e.get('summary', '')[:45]}")
        else:
            print(f"\n  {k} = {json.dumps(v)[:60]}")

    print("\n" + "=" * 70)
    print("STEP 3: What a consumer sees")
    print("=" * 70)
    print("\n  get_session('demo-001') -> full merged picture:")
    rec = fake.sessions[sid]
    print(f"    outcome   : {rec['outcome']}")
    print(f"    decisions : {len(rec['decisions'])} (one per agent)")
    for d in rec["decisions"]:
        print(f"      - {d['summary']}")
    print(f"    beliefs   : {len(rec['beliefUpdates'])}")
    for b in rec["beliefUpdates"]:
        print(f"      - {b}")


if __name__ == "__main__":
    asyncio.run(main())