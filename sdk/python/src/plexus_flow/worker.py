"""Connect to plexus-flow over gRPC and execute the tasks it hands out."""

import asyncio
import functools
import inspect
import json
import logging
import os
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, TypeAlias

import grpc
import grpc.aio

from plexus_flow._proto import worker_pb2 as pb
from plexus_flow._proto import worker_pb2_grpc as pb_grpc
from plexus_flow.context import TaskContext
from plexus_flow.errors import ActivityFailure

logger = logging.getLogger("plexus_flow")

Handler = Callable[[TaskContext, Any], Awaitable[Any] | Any]
"""An activity: receives the parsed JSON input, returns a JSON-serialisable value."""

Outbox: TypeAlias = asyncio.Queue[pb.WorkerMessage | None]

_CODE_TIMEOUT = "TIMEOUT"
_CODE_NOT_REGISTERED = "ACTIVITY_NOT_REGISTERED"


class Worker:
    """Runs registered activities for a plexus-flow server.

    Delivery is at-least-once, so activities must be idempotent. The server
    cancels a task by cancelling its asyncio task; sync handlers run in a
    thread and cannot be interrupted.
    """

    def __init__(
        self,
        address: str,
        *,
        worker_id: str | None = None,
        max_concurrency: int = 1,
        credentials: grpc.ChannelCredentials | None = None,
        reconnect_backoff_s: float = 0.5,
    ) -> None:
        self._address = address
        self._worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
        self._max_concurrency = max(1, max_concurrency)
        self._credentials = credentials
        self._backoff_s = reconnect_backoff_s
        self._handlers: dict[str, Handler] = {}
        self._stop = asyncio.Event()

    @property
    def worker_id(self) -> str:
        """Identifier the server knows this worker by."""
        return self._worker_id

    def register(self, name: str, handler: Handler) -> None:
        """Serve the activity `name`; compensating actions are activities too."""
        self._handlers[name] = handler

    def activity(self, name: str) -> Callable[[Handler], Handler]:
        """Register the decorated function as the activity `name`."""

        def decorator(handler: Handler) -> Handler:
            self.register(name, handler)
            return handler

        return decorator

    def stop(self) -> None:
        """Ask `run` to return; tasks in flight are cancelled and redelivered."""
        self._stop.set()

    async def run(self) -> None:
        """Serve tasks until `stop` is called or the task is cancelled."""
        if not self._handlers:
            raise ValueError("register at least one activity before running the worker")
        if self._credentials is not None:
            channel = grpc.aio.secure_channel(self._address, self._credentials)
        else:
            channel = grpc.aio.insecure_channel(self._address)
        backoff = self._backoff_s
        try:
            stub = pb_grpc.WorkerServiceStub(channel)
            loop = asyncio.get_running_loop()
            while not self._stop.is_set():
                started = loop.time()
                try:
                    await self._session(stub)
                except grpc.aio.AioRpcError as err:
                    logger.warning("worker %s disconnected: %s", self._worker_id, err.details())
                if self._stop.is_set():
                    break
                if loop.time() - started > 10:
                    backoff = self._backoff_s
                await self._sleep_or_stop(backoff)
                backoff = min(backoff * 2, 5.0)
        finally:
            await channel.close()

    async def _sleep_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            pass

    async def _session(self, stub: "pb_grpc.WorkerServiceAsyncStub") -> None:
        outbox: Outbox = asyncio.Queue()

        async def requests() -> AsyncIterator[pb.WorkerMessage]:
            while (message := await outbox.get()) is not None:
                yield message

        outbox.put_nowait(
            pb.WorkerMessage(
                register=pb.Register(
                    worker_id=self._worker_id,
                    activities=sorted(self._handlers),
                    max_concurrency=self._max_concurrency,
                )
            )
        )
        call = stub.Connect(requests())
        running: dict[str, asyncio.Task[None]] = {}
        heartbeat: asyncio.Task[None] | None = None
        stopper = asyncio.create_task(self._stop.wait())
        try:
            reader = call.__aiter__()
            while True:
                next_message = asyncio.ensure_future(reader.__anext__())
                done, _ = await asyncio.wait(
                    {next_message, stopper}, return_when=asyncio.FIRST_COMPLETED
                )
                if next_message not in done:
                    next_message.cancel()
                    return
                try:
                    message = next_message.result()
                except StopAsyncIteration:
                    return
                which = message.WhichOneof("message")
                if which == "registered":
                    interval = max(message.registered.heartbeat_interval_ms, 100) / 1000
                    heartbeat = asyncio.create_task(self._heartbeat(outbox, interval / 2))
                elif which == "task":
                    self._spawn(message.task, outbox, running)
                elif which == "cancel":
                    if (victim := running.get(message.cancel.task_id)) is not None:
                        victim.cancel()
        finally:
            stopper.cancel()
            if heartbeat is not None:
                heartbeat.cancel()
            for t in list(running.values()):
                t.cancel()
            await asyncio.gather(*running.values(), return_exceptions=True)
            outbox.put_nowait(None)
            call.cancel()

    def _spawn(self, task: pb.Task, outbox: Outbox, running: dict[str, asyncio.Task[None]]) -> None:
        running[task.task_id] = asyncio.create_task(self._run_task(task, outbox))
        running[task.task_id].add_done_callback(functools.partial(_forget, running, task.task_id))

    async def _heartbeat(self, outbox: Outbox, every_s: float) -> None:
        while True:
            await asyncio.sleep(every_s)
            outbox.put_nowait(pb.WorkerMessage(heartbeat=pb.Heartbeat()))

    async def _run_task(self, task: pb.Task, outbox: Outbox) -> None:
        result = await self._execute(task)
        outbox.put_nowait(pb.WorkerMessage(result=result))

    async def _execute(self, task: pb.Task) -> pb.TaskResult:
        """Run one task; a cancelled task raises CancelledError and reports nothing."""

        def failure(code: str, message: str, retryable: bool) -> pb.TaskResult:
            return pb.TaskResult(
                task_id=task.task_id,
                failure=pb.Failure(code=code, message=message, retryable=retryable),
            )

        handler = self._handlers.get(task.activity)
        if handler is None:
            msg = f"activity {task.activity!r} is not registered on worker {self._worker_id}"
            return failure(_CODE_NOT_REGISTERED, msg, False)

        try:
            ctx = _context_of(task)
            payload = _loads(task.input)
            async with asyncio.timeout(ctx.timeout_s):
                output = await _invoke(handler, ctx, payload)
            return pb.TaskResult(
                task_id=task.task_id, success=pb.Success(output=json.dumps(output).encode())
            )
        except ActivityFailure as err:
            return failure(err.code, str(err), err.retryable)
        except TimeoutError:
            return failure(_CODE_TIMEOUT, f"activity {task.activity} timed out", True)
        except Exception as err:
            logger.exception(
                "activity %s failed for %s/%s", task.activity, task.workflow_id, task.step_name
            )
            return failure("", f"{type(err).__name__}: {err}", True)


def _forget(
    running: dict[str, asyncio.Task[None]], task_id: str, _done: asyncio.Future[None]
) -> None:
    running.pop(task_id, None)


async def _invoke(handler: Handler, ctx: TaskContext, payload: Any) -> Any:
    if inspect.iscoroutinefunction(handler):
        return await handler(ctx, payload)
    result = await asyncio.to_thread(handler, ctx, payload)
    if inspect.isawaitable(result):
        return await result
    return result


def _loads(raw: bytes) -> Any:
    return json.loads(raw) if raw else None


def _context_of(task: pb.Task) -> TaskContext:
    return TaskContext(
        task_id=task.task_id,
        workflow_id=task.workflow_id,
        run_id=task.run_id,
        step_name=task.step_name,
        activity=task.activity,
        compensation=task.kind == pb.TASK_KIND_COMPENSATION,
        attempt=task.attempt,
        timeout_s=task.timeout_ms / 1000 if task.timeout_ms > 0 else None,
        dependency_outputs={name: _loads(raw) for name, raw in task.dependency_outputs.items()},
        metadata=dict(task.metadata),
    )
