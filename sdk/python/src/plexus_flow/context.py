"""What an activity knows about the task it is running."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class TaskContext:
    """Identity and inputs of one task delivery."""

    task_id: str
    workflow_id: str
    run_id: str
    step_name: str
    activity: str
    compensation: bool
    attempt: int
    """1-based attempt number of the step."""
    timeout_s: float | None
    last_progress: Any = None
    """Parsed JSON progress checkpoint from a previous attempt of this step, if any."""
    dependency_outputs: dict[str, Any] = field(default_factory=dict)
    """Parsed JSON outputs of the steps this step depends on, by step name."""
    metadata: dict[str, str] = field(default_factory=dict)
    _report_progress: Callable[[Any], None] | None = field(
        default=None, repr=False, compare=False
    )

    async def report_progress(self, progress: Any) -> None:
        """Stream a JSON progress checkpoint to plexus-flow inside a Heartbeat frame.

        If the worker crashes or the task is retried, the next delivery receives
        this value in ``ctx.last_progress``.
        """
        if self._report_progress is not None:
            self._report_progress(progress)
            await asyncio.sleep(0)
