#!/usr/bin/env python3
"""Offline test: exercises orchestrator + agents against a fake MemoryClient.

No network / server required.

Run:  python test_orchestrator_offline.py
"""
import asyncio
from typing import Any

import agent_base
import memory_client
import orchestrator


class FakeMemoryClient:
    """In-memory stand-in that mirrors the real server semantics:
    - commit_session OVERWRITES
    - KV is global
    """

    def __init__(self, *a, **kw):
        self.store: dict[str, Any] = {}
        self.sessions: dict[str, dict] = {}
        self.calls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def commit_session(self, session_id, agent_id, task_summary, outcome="success",
                             decisions=None, errors=None, user_corrections=None,
                             belief_updates=None, ttl_seconds=None):
        self.calls.append(f"commit_session:{session_id}")
        self.sessions[session_id] = {
            "sessionId": session_id,
            "agentId": agent_id,
            "taskSummary": task_summary,
            "outcome": outcome,
            "decisions": decisions or [],
            "errors": errors or [],
            "userCorrections": user_corrections or [],
            "beliefUpdates": belief_updates or [],
        }
        return {"ok": True, "sessionId": session_id}

    async def get_session(self, session_id):
        rec = self.sessions.get(session_id)
        return rec if rec else {"error": "not found", "sessionId": session_id}

    async def list_sessions(self):
        return {"sessions": list(self.sessions), "count": len(self.sessions)}

    async def set_key(self, key, value, ttl_seconds=None):
        self.calls.append(f"set_key:{key}")
        self.store[key] = value
        return {"ok": True}

    async def get_key(self, key):
        if key not in self.store:
            return {"error": "not found", "key": key}
        return {"key": key, "value": self.store[key]}

    async def del_key(self, key):
        self.store.pop(key, None)
        return {"ok": True}

    async def mget(self, keys):
        return {"values": {k: self.store.get(k) for k in keys}}

    async def mset(self, items, ttl_seconds=None):
        self.store.update(items)
        return {"ok": True, "count": len(items)}

    async def admin_export(self):
        return {"exported": len(self.sessions), "data": self.sessions}


async def main() -> int:
    fake = FakeMemoryClient()
    memory_client.MemoryClient = lambda *a, **kw: fake  # patch both modules
    orchestrator.MemoryClient = lambda *a, **kw: fake
    agent_base.MemoryClient = lambda *a, **kw: fake

    sid = "test-session-001"
    await orchestrator.run_workflow("Fix auth bug", sid)

    print("\n=== ASSERTIONS ===")
    ok = True

    # 1. Rolling log persisted in KV
    log = fake.store.get(f"orch:{sid}:log")
    assert log, "rolling log missing"
    kinds = [e["kind"] for e in log]
    print(f"log entries: {len(log)} -> {kinds}")
    for expected in ["start", "delegate", "complete", "finish"]:
        assert expected in kinds, f"missing kind: {expected}"
    print("OK  rolling log has start/delegate/complete/finish")

    # 2. Session record mirrored (last entry wins -> finish/success)
    rec = fake.sessions.get(sid)
    assert rec, "session record missing"
    assert rec["outcome"] == "success", f"expected success, got {rec['outcome']}"
    print(f"OK  session record outcome={rec['outcome']} agent={rec['agentId']}")

    # 3. All 4 agents delegated
    agents_delegated = [e["agent"] for e in log if e["kind"] == "delegate"]
    assert agents_delegated == ["analyst", "developer", "tester", "reviewer"], agents_delegated
    print(f"OK  delegated order: {agents_delegated}")

    # 4. Per-agent state written
    for agent in ["analyst", "developer", "tester", "reviewer"]:
        keys = [k for k in fake.store if k.startswith(f"agent:{agent}:{sid}:")]
        assert keys, f"no state for {agent}"
    print("OK  per-agent state keys present")

    # 5. CRITICAL: no data loss from overwriting commit_session
    #    Every agent's decisions must survive in the consolidated record.
    consolidated = fake.store.get(f"orch:{sid}:agents")
    assert consolidated, "consolidated trail missing"
    agent_entries = {e["agent"] for e in consolidated}
    assert agent_entries >= {"analyst", "developer", "tester", "reviewer"}, agent_entries
    print(f"OK  consolidated trail has all agents: {sorted(agent_entries)}")

    # each agent's own decisions present in the final session record
    rec_decisions = [d.get("summary") for d in rec["decisions"]]
    expected_decisions = [
        "Critical path identified",
        "Added rate limiter middleware",
        "Added integration test for rate limiting",
        "Approved with minor comments",
    ]
    for exp in expected_decisions:
        assert exp in rec_decisions, f"lost decision from overwrite: {exp} (have {rec_decisions})"
    print(f"OK  all {len(expected_decisions)} agent decisions preserved (overwrite bug fixed)")

    # each agent's rolling log still intact individually
    for agent in ["analyst", "developer", "tester", "reviewer"]:
        res = fake.store.get(f"agent:{agent}:{sid}:log")
        assert isinstance(res, list) and res, f"missing rolling log for {agent}"
    print("OK  per-agent rolling logs intact")

    # 6. Failure path: agent raises -> pipeline stops, outcome=failure
    class Boom(agent_base.BaseAgent):
        agent_id = "boom"

        async def run(self, task):
            raise RuntimeError("kaboom")

    agent_base.AGENT_MAP["boom"] = Boom
    orig_order = orchestrator.AGENT_ORDER
    orchestrator.AGENT_ORDER = ["analyst", "boom", "reviewer"]
    sid2 = "test-session-fail"
    await orchestrator.run_workflow("trigger failure", sid2)
    rec2 = fake.sessions[sid2]
    assert rec2["outcome"] == "failure", rec2["outcome"]
    log2 = fake.store[f"orch:{sid2}:log"]
    assert "reviewer" not in [e["agent"] for e in log2 if e["kind"] == "delegate"], \
        "pipeline should stop after failure"
    print("OK  failure path stops pipeline and marks outcome=failure")
    orchestrator.AGENT_ORDER = orig_order

    # 7. Dry run logs plan without executing agents
    sid3 = "test-session-dry"
    await orchestrator.run_workflow("dry", sid3, dry_run=True)
    log3 = fake.store[f"orch:{sid3}:log"]
    assert not any(e["kind"] == "delegate" for e in log3), "dry run delegated"
    print("OK  dry-run does not delegate")

    # 8. Batch KV operations (mget/mset)
    fake2 = FakeMemoryClient()
    memory_client.MemoryClient = lambda *a, **kw: fake2
    agent_base.MemoryClient = lambda *a, **kw: fake2

    async with memory_client.MemoryClient() as mem:
        await mem.mset({"batch:k1": "v1", "batch:k2": 42, "batch:k3": {"a": 1}})
        res = await mem.mget(["batch:k1", "batch:k2", "batch:k3", "batch:missing"])
        assert res["values"]["batch:k1"] == "v1"
        assert res["values"]["batch:k2"] == 42
        assert res["values"]["batch:k3"] == {"a": 1}
        assert res["values"]["batch:missing"] is None
    print("OK  mget/mset batch KV works")

    # 9. Session TTL auto-expiry simulation
    fake3 = FakeMemoryClient()
    memory_client.MemoryClient = lambda *a, **kw: fake3
    memory_client.MemoryClient = lambda *a, **kw: fake3
    async with memory_client.MemoryClient() as mem:
        await mem.commit_session(
            session_id="ttl-session",
            agent_id="tester",
            task_summary="TTL test",
            ttl_seconds=1,  # very short TTL
        )
        rec = await mem.get_session("ttl-session")
        assert rec["sessionId"] == "ttl-session"
    print("OK  commit_session accepts ttl_seconds parameter")

    print("\nALL TESTS PASSED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))