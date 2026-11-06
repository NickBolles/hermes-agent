"""Saturated HTTP runs defer canonical admission, not just observation."""
import asyncio
from types import SimpleNamespace

import pytest
from gateway.config import GatewayConfig
from gateway.session import SessionStore
from gateway.session_authority import initialize_session_authority
from hermes_state import SessionDB
from tests.gateway.test_api_unknown_resolution import _AUTH_HEADERS, _adapter, _runs_client
from tests.waiting import async_wait_until


@pytest.mark.asyncio
async def test_authority_queue_waits_for_capacity_and_cancels_before_admission(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    store = SessionStore(tmp_path / "sessions", GatewayConfig())
    db = SessionDB(tmp_path / "state.db")
    executed = []

    async def answer(event):
        from gateway.session_results import execution_result
        executed.append(event.text)
        execution_result.get()["result"] = {"final_response": "ACK", "messages": []}
        return "ACK"

    runner = SimpleNamespace(_session_db=db, session_store=store, _draining=False, _handle_message=answer)
    authority = await initialize_session_authority(runner, profile_id="default", instance_id="queue-owner")
    adapter = _adapter(runner, db)
    runner._adapter_for_source = lambda source: adapter
    adapter._inflight_agent_runs = 10
    client = _runs_client(adapter)
    await client.start_server()
    try:
        async def submit(text):
            response = await client.post("/v1/runs", json={"input": text, "session_id": f"api-{text}"},
                                         headers={**_AUTH_HEADERS, "Idempotency-Key": text})
            assert response.status == 202
            return await response.json()
        first, second = await asyncio.gather(submit("one"), submit("two"))
        assert first["status"] == second["status"] == "queued"
        assert executed == []
        from gateway.platforms.api_server_authority_runs import run_admission
        assert run_admission(adapter, first["run_id"]) is None
        assert run_admission(adapter, second["run_id"]) is None
        replay = await submit("one")
        assert replay["run_id"] == first["run_id"] and replay["replayed"]
        response = await client.post(f"/v1/runs/{second['run_id']}/stop", headers=_AUTH_HEADERS)
        assert response.status == 200
        await async_wait_until(lambda: adapter._run_statuses[second["run_id"]]["status"] == "cancelled")
        adapter._inflight_agent_runs = 0
        await async_wait_until(lambda: adapter._run_statuses[first["run_id"]]["status"] == "completed", timeout=10)
        assert executed == ["one"]
        assert run_admission(adapter, second["run_id"]) is None
        assert not adapter._run_queue.waiting
    finally:
        await client.close()
        adapter._response_store.close()
        adapter._run_idempotency_store.close()
        authority.db.close()
        store.close_all_db_handles()
