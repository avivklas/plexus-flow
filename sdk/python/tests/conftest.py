"""A fake plexus-flow server speaking the worker protocol, for fast unit tests."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import grpc
import grpc.aio
import pytest

from plexus_flow._proto import worker_pb2 as pb
from plexus_flow._proto import worker_pb2_grpc as pb_grpc


@dataclass
class FakeSession:
    """One connected worker as seen by the fake server."""

    register: pb.Register
    results: asyncio.Queue[pb.TaskResult] = field(default_factory=asyncio.Queue)
    heartbeats: int = 0
    outgoing: asyncio.Queue[pb.ServerMessage | None] = field(default_factory=asyncio.Queue)

    def send_task(self, task: pb.Task) -> None:
        self.outgoing.put_nowait(pb.ServerMessage(task=task))

    def send_cancel(self, task_id: str) -> None:
        self.outgoing.put_nowait(
            pb.ServerMessage(cancel=pb.CancelTask(task_id=task_id, reason="test"))
        )

    def hang_up(self) -> None:
        self.outgoing.put_nowait(None)

    async def next_result(self) -> pb.TaskResult:
        async with asyncio.timeout(3):
            return await self.results.get()


class FakeServer(pb_grpc.WorkerServiceServicer):
    def __init__(self, heartbeat_interval_ms: int = 100) -> None:
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.sessions: list[FakeSession] = []
        self._arrived = asyncio.Condition()

    async def Connect(
        self,
        request_iterator: AsyncIterator[pb.WorkerMessage],
        context: grpc.aio.ServicerContext[pb.WorkerMessage, pb.ServerMessage],
    ) -> AsyncIterator[pb.ServerMessage]:
        first = await anext(request_iterator)
        session = FakeSession(register=first.register)
        self.sessions.append(session)
        async with self._arrived:
            self._arrived.notify_all()
        yield pb.ServerMessage(
            registered=pb.Registered(heartbeat_interval_ms=self.heartbeat_interval_ms)
        )

        async def read() -> None:
            async for message in request_iterator:
                if message.WhichOneof("message") == "heartbeat":
                    session.heartbeats += 1
                elif message.WhichOneof("message") == "result":
                    session.results.put_nowait(message.result)
            session.outgoing.put_nowait(None)

        reader = asyncio.create_task(read())
        try:
            while (message := await session.outgoing.get()) is not None:
                yield message
        finally:
            reader.cancel()

    async def session(self, index: int = 0) -> FakeSession:
        async with asyncio.timeout(3), self._arrived:
            await self._arrived.wait_for(lambda: len(self.sessions) > index)
        return self.sessions[index]


@dataclass
class ServerHandle:
    server: FakeServer
    address: str


@pytest.fixture
async def fake() -> AsyncIterator[ServerHandle]:
    server = grpc.aio.server()
    servicer = FakeServer()
    pb_grpc.add_WorkerServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    yield ServerHandle(servicer, f"127.0.0.1:{port}")
    await server.stop(grace=0)


def task(
    activity: str,
    *,
    task_id: str = "t1",
    payload: bytes = b"{}",
    timeout_ms: int = 0,
    kind: pb.TaskKind.ValueType = pb.TASK_KIND_ACTIVITY,
    deps: dict[str, bytes] | None = None,
    metadata: dict[str, str] | None = None,
    attempt: int = 1,
) -> pb.Task:
    return pb.Task(
        task_id=task_id,
        kind=kind,
        workflow_id="wf-1",
        run_id="run-1",
        step_name="step",
        activity=activity,
        input=payload,
        dependency_outputs=deps or {},
        metadata=metadata or {},
        attempt=attempt,
        timeout_ms=timeout_ms,
    )
