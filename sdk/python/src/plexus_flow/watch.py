"""Watch workflows over plexus-flow's HTTP API without polling.

The server pushes changes as server-sent events; these helpers read them with
the standard library only, so they add no dependencies.

    from plexus_flow import wait_for, watch_workflow

    for wf in watch_workflow("http://127.0.0.1:8080", "order-42"):
        print(wf["version"], wf["status"])      # current state, then every change

    final = wait_for("http://127.0.0.1:8080", "order-42")   # blocks until terminal
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from typing import Any

TERMINAL = frozenset({"COMPLETED", "FAILED", "COMPENSATED", "CANCELLED", "COMPENSATION_FAILED"})


class WorkflowNotFound(LookupError):
    """The server has no workflow with that ID."""


def _events(url: str, timeout: float | None) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield (event, data) pairs from a text/event-stream endpoint."""
    req = urllib.request.Request(url, headers={"Accept": "text/event-stream"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (caller-provided server URL)
        event, data = "", list[str]()
        for raw in resp:
            line = raw.decode("utf-8").rstrip("\r\n")
            if not line:
                if data:
                    yield event, json.loads("\n".join(data))
                event, data = "", list[str]()
            elif line.startswith(":"):
                continue  # keep-alive
            elif line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].removeprefix(" "))


def watch_workflow(
    base_url: str, workflow_id: str, *, timeout: float | None = 60.0
) -> Iterator[dict[str, Any]]:
    """Yield the workflow's current state, then every change, until it reaches a terminal status.

    A dropped connection is resumed transparently; changes already yielded are not repeated.
    ``timeout`` bounds how long a single read may wait (the server sends a keep-alive
    every 15 s).
    """
    wf_id = urllib.parse.quote(workflow_id, safe="")
    url = f"{base_url.rstrip('/')}/api/v1/workflows/{wf_id}/watch"
    last = -1
    backoff = 0.1
    while True:
        try:
            for event, wf in _events(url, timeout):
                if event != "workflow" or wf.get("version", 0) <= last:
                    continue
                last = wf.get("version", 0)
                yield wf
                if wf.get("status") in TERMINAL:
                    return
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise WorkflowNotFound(workflow_id) from e
            raise
        except (urllib.error.URLError, OSError, TimeoutError):
            pass  # reconnect below
        time.sleep(backoff)
        backoff = min(backoff * 2, 2.0)


def wait_for(base_url: str, workflow_id: str, *, timeout: float | None = 60.0) -> dict[str, Any]:
    """Block until the workflow reaches a terminal status and return its final state."""
    final: dict[str, Any] = {}
    for wf in watch_workflow(base_url, workflow_id, timeout=timeout):
        final = wf
    return final


def watch_flow(
    base_url: str, flow: str, *, terminal_only: bool = False, timeout: float | None = 60.0
) -> Iterator[dict[str, Any]]:
    """Yield a summary (workflow_id, status, started_at, ended_at, duration_ms, version)
    for every change to any run of ``flow``; only runs reaching a terminal status with
    ``terminal_only``. Runs until the caller stops iterating."""
    q = "?terminal=1" if terminal_only else ""
    url = f"{base_url.rstrip('/')}/api/v1/flows/{urllib.parse.quote(flow, safe='')}/watch{q}"
    while True:
        try:
            for event, summary in _events(url, timeout):
                if event == "trace":
                    yield summary
        except (urllib.error.URLError, OSError, TimeoutError):
            time.sleep(0.2)
