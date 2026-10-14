import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from plexus_flow import ActivityFailure, PermanentFailure, TaskContext, Worker
from plexus_flow._proto import worker_pb2 as pb
from tests.conftest import ServerHandle, task


@asynccontextmanager
async def running(worker: Worker) -> AsyncIterator[None]:
    runner = asyncio.create_task(worker.run())
    try:
        yield
    finally:
        worker.stop()
        await asyncio.wait_for(runner, 3)


def make_worker(fake: ServerHandle, **kwargs: Any) -> Worker:
    return Worker(fake.address, worker_id="py-test", reconnect_backoff_s=0.05, **kwargs)


async def test_registers_activities_and_concurrency(fake: ServerHandle) -> None:
    worker = make_worker(fake, max_concurrency=4)
    worker.register("b-act", lambda ctx, payload: None)
    worker.register("a-act", lambda ctx, payload: None)
    async with running(worker):
        session = await fake.server.session()
    assert session.register.worker_id == "py-test"
    assert list(session.register.activities) == ["a-act", "b-act"]
    assert session.register.max_concurrency == 4


async def test_run_needs_an_activity(fake: ServerHandle) -> None:
    with pytest.raises(ValueError):
        await make_worker(fake).run()


async def test_success_returns_json_output(fake: ServerHandle) -> None:
    worker = make_worker(fake)

    @worker.activity("double")
    async def double(ctx: TaskContext, payload: dict[str, int]) -> dict[str, int]:
        return {"value": payload["value"] * 2}

    async with running(worker):
        session = await fake.server.session()
        session.send_task(task("double", payload=b'{"value": 21}'))
        result = await session.next_result()
    assert result.task_id == "t1"
    assert json.loads(result.success.output) == {"value": 42}


async def test_context_carries_the_task(fake: ServerHandle) -> None:
    seen: list[TaskContext] = []
    worker = make_worker(fake)

    @worker.activity("inspect")
    async def inspect_task(ctx: TaskContext, payload: Any) -> None:
        seen.append(ctx)

    async with running(worker):
        session = await fake.server.session()
        session.send_task(
            task(
                "inspect",
                timeout_ms=1500,
                kind=pb.TASK_KIND_COMPENSATION,
                deps={"first": b'{"x": 1}', "empty": b""},
                metadata={"tenant": "acme"},
                attempt=3,
            )
        )
        await session.next_result()
    (ctx,) = seen
    assert (ctx.workflow_id, ctx.step_name, ctx.attempt) == ("wf-1", "step", 3)
    assert ctx.compensation is True
    assert ctx.timeout_s == 1.5
    assert ctx.dependency_outputs == {"first": {"x": 1}, "empty": None}
    assert ctx.metadata == {"tenant": "acme"}


async def test_failures_map_to_code_and_retryability(fake: ServerHandle) -> None:
    worker = make_worker(fake)

    @worker.activity("retryable")
    async def retryable(ctx: TaskContext, payload: Any) -> None:
        raise ActivityFailure("try again", code="BUSY")

    @worker.activity("permanent")
    async def permanent(ctx: TaskContext, payload: Any) -> None:
        raise PermanentFailure("no such study", code="STUDY_NOT_FOUND")

    @worker.activity("crash")
    async def crash(ctx: TaskContext, payload: Any) -> None:
        raise RuntimeError("boom")

    async with running(worker):
        session = await fake.server.session()
        for name in ("retryable", "permanent", "crash"):
            session.send_task(task(name, task_id=name))
        results = {r.task_id: r.failure for r in [await session.next_result() for _ in range(3)]}
    assert (results["retryable"].code, results["retryable"].retryable) == ("BUSY", True)
    assert (results["permanent"].code, results["permanent"].retryable) == ("STUDY_NOT_FOUND", False)
    assert results["crash"].retryable is True
    assert "RuntimeError: boom" in results["crash"].message


async def test_unknown_activity_is_a_permanent_failure(fake: ServerHandle) -> None:
    worker = make_worker(fake)
    worker.register("known", lambda ctx, payload: None)
    async with running(worker):
        session = await fake.server.session()
        session.send_task(task("unknown"))
        result = await session.next_result()
    assert result.failure.code == "ACTIVITY_NOT_REGISTERED"
    assert result.failure.retryable is False


async def test_server_timeout_becomes_a_retryable_failure(fake: ServerHandle) -> None:
    worker = make_worker(fake)

    @worker.activity("slow")
    async def slow(ctx: TaskContext, payload: Any) -> None:
        await asyncio.sleep(5)

    async with running(worker):
        session = await fake.server.session()
        session.send_task(task("slow", timeout_ms=100))
        result = await session.next_result()
    assert (result.failure.code, result.failure.retryable) == ("TIMEOUT", True)


async def test_cancel_interrupts_the_activity_and_reports_nothing(fake: ServerHandle) -> None:
    started, interrupted = asyncio.Event(), asyncio.Event()
    worker = make_worker(fake)

    @worker.activity("long")
    async def long_running(ctx: TaskContext, payload: Any) -> None:
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            interrupted.set()
            raise

    async with running(worker):
        session = await fake.server.session()
        session.send_task(task("long"))
        await asyncio.wait_for(started.wait(), 2)
        session.send_cancel("t1")
        await asyncio.wait_for(interrupted.wait(), 2)
        await asyncio.sleep(0.1)
        assert session.results.empty()


async def test_sync_handlers_do_not_block_the_loop(fake: ServerHandle) -> None:
    worker = make_worker(fake, max_concurrency=2)

    def blocking(ctx: TaskContext, payload: Any) -> str:
        time.sleep(0.3)
        return "slept"

    async def quick(ctx: TaskContext, payload: Any) -> str:
        return "quick"

    worker.register("blocking", blocking)
    worker.register("quick", quick)
    async with running(worker):
        session = await fake.server.session()
        session.send_task(task("blocking", task_id="slow"))
        session.send_task(task("quick", task_id="fast"))
        first = await session.next_result()
    assert first.task_id == "fast"


async def test_heartbeats_follow_the_server_interval(fake: ServerHandle) -> None:
    worker = make_worker(fake)
    worker.register("noop", lambda ctx, payload: None)
    async with running(worker):
        session = await fake.server.session()
        await asyncio.sleep(0.5)
    assert session.heartbeats >= 3


async def test_reconnects_after_the_server_hangs_up(fake: ServerHandle) -> None:
    worker = make_worker(fake)
    worker.register("noop", lambda ctx, payload: None)
    async with running(worker):
        first = await fake.server.session()
        first.hang_up()
        second = await fake.server.session(1)
    assert second is not first
    assert second.register.worker_id == "py-test"
