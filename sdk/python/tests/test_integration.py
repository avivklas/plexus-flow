"""Drive a real plexus-flow server (built from this repo) with the Python worker."""

import asyncio
import shutil
import socket
import subprocess
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest

from plexus_flow import ActivityFailure, PermanentFailure, TaskContext, Worker

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[3]
TERMINAL = {"COMPLETED", "FAILED", "COMPENSATED", "CANCELLED", "COMPENSATION_FAILED"}


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class Server:
    def __init__(self, http_port: int, grpc_port: int) -> None:
        self.grpc_address = f"127.0.0.1:{grpc_port}"
        self.http = httpx.Client(base_url=f"http://127.0.0.1:{http_port}", timeout=5)

    def start(self, workflow_id: str, steps: list[dict[str, Any]], input: Any = None) -> None:
        body = {
            "workflow_id": workflow_id,
            "definition": {"name": "py-test", "steps": steps},
            "input": input,
        }
        self.http.post("/api/v1/workflows/start", json=body).raise_for_status()

    def wait_terminal(self, workflow_id: str, timeout_s: float = 15) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            wf: dict[str, Any] = self.http.get(f"/api/v1/workflows/{workflow_id}").json()
            if wf["status"] in TERMINAL:
                return wf
            time.sleep(0.05)
        raise TimeoutError(f"workflow {workflow_id} did not finish")


@pytest.fixture(scope="session")
def binary(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if shutil.which("go") is None:
        pytest.skip("go toolchain not found")
    out = tmp_path_factory.mktemp("bin") / "plexus-flow"
    subprocess.run(["go", "build", "-o", str(out), "./cmd/plexus-flow"], cwd=REPO_ROOT, check=True)
    return out


@pytest.fixture
def server(binary: Path, tmp_path: Path) -> Iterator[Server]:
    http_port, grpc_port = free_port(), free_port()
    proc = subprocess.Popen(
        [
            str(binary),
            f"--http-addr=127.0.0.1:{http_port}",
            f"--grpc-addr=127.0.0.1:{grpc_port}",
            f"--raft-addr=127.0.0.1:{free_port()}",
            f"--act-raft-addr=127.0.0.1:{free_port()}",
            f"--data-dir={tmp_path}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    srv = Server(http_port, grpc_port)
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                if srv.http.get("/api/v1/cluster/status").json().get("is_leader"):
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        else:
            raise TimeoutError("plexus-flow did not become leader")
        yield srv
    finally:
        srv.http.close()
        proc.terminate()
        proc.wait(timeout=10)


@asynccontextmanager
async def serving(worker: Worker) -> AsyncIterator[None]:
    runner = asyncio.create_task(worker.run())
    await asyncio.sleep(0.3)  # let it register before work is queued
    try:
        yield
    finally:
        worker.stop()
        await asyncio.wait_for(runner, 5)


def step(name: str, activity: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "activity": activity, **extra}


async def finished(server: Server, workflow_id: str) -> dict[str, Any]:
    return await asyncio.to_thread(server.wait_terminal, workflow_id)


async def test_workflow_passes_outputs_between_python_activities(server: Server) -> None:
    worker = Worker(server.grpc_address, worker_id="py-it")

    @worker.activity("produce")
    async def produce(ctx: TaskContext, payload: dict[str, int]) -> dict[str, int]:
        return {"n": payload["n"] + 1}

    @worker.activity("consume")
    async def consume(ctx: TaskContext, payload: Any) -> dict[str, Any]:
        return {"got": ctx.dependency_outputs["first"], "workflow": ctx.workflow_id}

    async with serving(worker):
        await asyncio.to_thread(
            server.start,
            "wf-ok",
            [step("first", "produce"), step("second", "consume", depends_on=["first"])],
            {"n": 1},
        )
        wf = await finished(server, "wf-ok")

    assert wf["status"] == "COMPLETED"
    assert wf["steps"]["second"]["output"] == {"got": {"n": 2}, "workflow": "wf-ok"}


async def test_permanent_failure_runs_compensation_and_keeps_the_code(server: Server) -> None:
    undone: list[str] = []
    worker = Worker(server.grpc_address, worker_id="py-it")
    worker.register("reserve", lambda ctx, payload: {"reservation": 7})

    @worker.activity("release")
    async def release(ctx: TaskContext, payload: Any) -> None:
        undone.append(ctx.step_name)

    @worker.activity("charge")
    async def charge(ctx: TaskContext, payload: Any) -> None:
        raise PermanentFailure("card declined", code="CARD_DECLINED")

    async with serving(worker):
        await asyncio.to_thread(
            server.start,
            "wf-saga",
            [
                step("reserve", "reserve", compensating_action="release"),
                step("charge", "charge", depends_on=["reserve"], retries=3),
            ],
        )
        wf = await finished(server, "wf-saga")

    assert wf["status"] == "COMPENSATED"
    assert wf["error_code"] == "CARD_DECLINED"
    assert wf["steps"]["charge"]["attempt"] == 1  # permanent: no retries burned
    assert undone == ["reserve"]


async def test_retryable_failure_is_retried_by_the_engine(server: Server) -> None:
    attempts: list[int] = []
    worker = Worker(server.grpc_address, worker_id="py-it")

    @worker.activity("flaky")
    async def flaky(ctx: TaskContext, payload: Any) -> str:
        attempts.append(ctx.attempt)
        if ctx.attempt < 3:
            raise ActivityFailure("busy", code="BUSY")
        return "done"

    async with serving(worker):
        await asyncio.to_thread(server.start, "wf-retry", [step("only", "flaky", retries=3)])
        wf = await finished(server, "wf-retry")

    assert wf["status"] == "COMPLETED"
    assert attempts == [1, 2, 3]


async def test_cancel_interrupts_a_running_python_activity(server: Server) -> None:
    started, interrupted = asyncio.Event(), asyncio.Event()
    worker = Worker(server.grpc_address, worker_id="py-it")

    @worker.activity("hang")
    async def hang(ctx: TaskContext, payload: Any) -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            interrupted.set()
            raise

    async with serving(worker):
        await asyncio.to_thread(server.start, "wf-cancel", [step("only", "hang")])
        async with asyncio.timeout(5):
            await started.wait()
        await asyncio.to_thread(
            lambda: server.http.post("/api/v1/workflows/wf-cancel/cancel", json={"reason": "t"})
        )
        wf = await finished(server, "wf-cancel")
        async with asyncio.timeout(5):
            await interrupted.wait()

    assert wf["status"] == "CANCELLED"
