"""Failures an activity can raise to control how plexus-flow reacts."""


class ActivityFailure(Exception):
    """Fail the step; it is retried while the step has retries left."""

    def __init__(self, message: str, *, code: str = "", retryable: bool = True) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class PermanentFailure(ActivityFailure):
    """Fail the step at once, without consuming its retries."""

    def __init__(self, message: str, *, code: str = "") -> None:
        super().__init__(message, code=code, retryable=False)
