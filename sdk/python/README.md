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

## Watching workflows

Clients don't need to poll. The server pushes every change over server-sent events, and these helpers read them using only the standard library:

```python
from plexus_flow import wait_for, watch_flow, watch_workflow

for wf in watch_workflow("http://127.0.0.1:8080", "order-42"):
    print(wf["version"], wf["status"])          # current state, then each change until terminal

final = wait_for("http://127.0.0.1:8080", "order-42")

for run in watch_flow("http://127.0.0.1:8080", "order-saga", terminal_only=True):
    print(run["workflow_id"], run["status"], run["duration_ms"])
```

A dropped connection is resumed, and changes already seen are not repeated (each change carries a `version`).

The protocol is `pkg/remote/workerpb/worker.proto`; `scripts/generate_proto.sh` regenerates the stubs.
