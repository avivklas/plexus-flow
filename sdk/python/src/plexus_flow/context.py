"""What an activity knows about the task it is running."""

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
    dependency_outputs: dict[str, Any] = field(default_factory=dict)
    """Parsed JSON outputs of the steps this step depends on, by step name."""
    metadata: dict[str, str] = field(default_factory=dict)
