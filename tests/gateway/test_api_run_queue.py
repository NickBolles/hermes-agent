"""Real HTTP admission tests: no provider calls, bounded cooperative workers."""
import asyncio
from unittest.mock import patch

import pytest
from aiohttp.test_utils import TestClient, TestServer
from gateway.platforms import api_server_runs as runs
from tests.gateway.test_api_server_runs import _make_adapter, _create_runs_app, _use_idempotency_db
from tests.waiting import async_wait_until


@pytest.mark.asyncio
async def test_ten_slots_fifo_cancel_replay_overflow_and_disconnect(tmp_path):
    adapter = _make_adapter()
    _use_idempotency_db(adapter, tmp_path / "queue.db")
    assert adapter._max_concurrent_runs == 10

    started, releases = [], {}
    peak = 0
    active = 0

    async def execute(owner, launch, **kwargs):
        nonlocal peak, active
        active += 1
        peak = max(peak, active)
        started.append(launch.user_message)
        releases[launch.user_message] = asyncio.Event()
        owner._set_run_status(launch.run_id, "running")
        try:
            await releases[launch.user_message].wait()
            owner._set_run_status(launch.run_id, "completed")
        finally:
            active -= 1
            runs._retire_live_run(owner, launch.run_id)
            launch.put_event(None)

    async with TestClient(TestServer(_create_runs_app(adapter))) as client:
        async def post(text):
            response = await client.post("/v1/runs", json={"input": text}, headers={"Idempotency-Key": text})
            return response.status, await response.json()
        with patch.object(runs, "_execute_run", side_effect=execute):
            responses = await asyncio.gather(*(post(str(i)) for i in range(10)))
            assert all(status == 202 for status, _ in responses)
            await async_wait_until(lambda: len(started) == 10)
            adapter._run_queue.capacity = 3
            a, b, c = [await post(x) for x in ("a", "b", "c")]
            assert all(status == 202 and body["status"] == "queued" for status, body in (a, b, c))
            assert (await post("overflow"))[0] == 429
            assert (await post("a"))[1]["run_id"] == a[1]["run_id"]
            conflict = await client.post("/v1/runs", json={"input": "changed"}, headers={"Idempotency-Key": "a"})
            assert conflict.status == 409
            assert len(started) == 10
            status = await (await client.get(f"/v1/runs/{a[1]['run_id']}")).json()
            assert status["status"] == "queued"
            stream = await client.get(f"/v1/runs/{a[1]['run_id']}/events")
            stream.close()  # Navigation does not cancel server-owned work.
            await client.post(f"/v1/runs/{b[1]['run_id']}/stop")
            await async_wait_until(lambda: adapter._run_statuses[b[1]['run_id']]["status"] == "cancelled")
            assert len(adapter._run_queue.waiting) == 2
            releases["0"].set()
            await async_wait_until(lambda: "a" in started)
            assert "c" not in started and "b" not in started
            releases["1"].set()
            await async_wait_until(lambda: "c" in started)
            assert started[-2:] == ["a", "c"]
            assert peak == 10
            for event in releases.values():
                event.set()
            await async_wait_until(lambda: not adapter._active_run_tasks)
            assert not adapter._run_queue.waiting
    adapter._run_idempotency_store.close()


@pytest.mark.asyncio
async def test_shutdown_waiting_and_failed_dispatch_release_capacity(tmp_path):
    adapter = _make_adapter()
    _use_idempotency_db(adapter, tmp_path / "queue.db")
    adapter._inflight_agent_runs = 10
    async with TestClient(TestServer(_create_runs_app(adapter))) as client:
        response = await client.post("/v1/runs", json={"input": "wait"}, headers={"Idempotency-Key": "wait"})
        run_id = (await response.json())["run_id"]
        adapter._shutdown_interrupted_run_ids.add(run_id)
        await async_wait_until(lambda: adapter._run_statuses[run_id]["status"] == "interrupted")
        assert not adapter._run_queue.waiting
        assert not adapter._active_run_tasks
        adapter._inflight_agent_runs = 0
        with patch.object(runs, "_execute_run", side_effect=RuntimeError("dispatch failed")):
            response = await client.post("/v1/runs", json={"input": "fail"}, headers={"Idempotency-Key": "fail"})
            failed_id = (await response.json())["run_id"]
            await async_wait_until(lambda: adapter._run_statuses[failed_id]["status"] == "failed")
        assert not adapter._run_queue.waiting
        assert not adapter._active_run_tasks
    adapter._run_idempotency_store.close()
