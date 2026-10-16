import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from plexus_flow import WorkflowNotFound, wait_for, watch_workflow


def sse(version: int, status: str) -> bytes:
    data = json.dumps({"workflow_id": "wf", "version": version, "status": status})
    return f"id: {version}\nevent: workflow\ndata: {data}\n\n".encode()


@pytest.fixture
def server() -> Iterator[str]:
    """First stream drops after version 2; the reconnect replays from the start
    and ends at a terminal status."""
    connections = {"n": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if "/missing/" in self.path:
                self.send_response(404)
                self.end_headers()
                return
            connections["n"] += 1
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(b": ping\n\n")
            if connections["n"] == 1:
                self.wfile.write(sse(1, "RUNNING") + sse(2, "RUNNING"))
            else:
                self.wfile.write(sse(1, "RUNNING") + sse(2, "RUNNING") + sse(3, "COMPLETED"))
            self.wfile.flush()

        def log_message(self, *args: object) -> None:
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def test_watch_resumes_without_repeating_changes(server: str) -> None:
    seen = [(wf["version"], wf["status"]) for wf in watch_workflow(server, "wf", timeout=5)]
    assert seen == [(1, "RUNNING"), (2, "RUNNING"), (3, "COMPLETED")]


def test_wait_for_returns_final_state(server: str) -> None:
    assert wait_for(server, "wf", timeout=5)["status"] == "COMPLETED"


def test_unknown_workflow(server: str) -> None:
    with pytest.raises(WorkflowNotFound):
        wait_for(server, "missing", timeout=5)
