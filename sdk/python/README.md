# plexus-flow Python worker SDK

Run plexus-flow activities (and compensations) in a Python process. The worker
connects to the plexus-flow gRPC address, announces its activities and executes
the tasks the server hands it.

```python
import asyncio
from plexus_flow import PermanentFailure, TaskContext, Worker

worker = Worker("127.0.0.1:9090", worker_id="pacs-1", max_concurrency=16)


@worker.activity("fetch-study")
async def fetch_study(ctx: TaskContext, payload: dict) -> dict:
    if payload["study"] == "missing":
        raise PermanentFailure("no such study", code="STUDY_NOT_FOUND")
    return {"bytes": 1024}


asyncio.run(worker.run())
```

* Delivery is **at-least-once**: make activities idempotent.
* `PermanentFailure` fails the step at once; any other exception is retried
  while the step has retries left. A `code` travels to `workflow.error_code`.
* The server cancels a task by cancelling the asyncio task: handle
  `asyncio.CancelledError` (or just let it propagate).
* Sync functions are supported and run in a thread (they cannot be cancelled mid-flight).

The protocol is `pkg/remote/workerpb/worker.proto`; `scripts/generate_proto.sh` regenerates the stubs.
