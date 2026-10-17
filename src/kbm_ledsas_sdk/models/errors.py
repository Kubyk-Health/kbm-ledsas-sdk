"""
Error types for the KeborMed LEDSAS SDK.

These error types control retry behavior and DLQ routing:
- Retryable: Transient errors that will be retried with exponential backoff
- Permanent: Non-recoverable errors that go to DLQ (no retry)
- DeadlineExceeded: Handler exceeded deadline (NOT retried — the deadline
  is in the past, so a retry would just exceed it again; goes to DLQ)
"""


def _checked_user_message(user_message: object) -> "str | None":
    """``user_message`` as given, or a TypeError: a caller-facing message
    is text (``str``) or None (not set). An empty string is a message too:
    the caller then sees an empty message, never the internal one."""
    if user_message is None or isinstance(user_message, str):
        return user_message
    err = TypeError(f"user_message must be a str or None, got {type(user_message).__name__}")
    # A bug in the handler, the same on every attempt: replied Permanent
    # (with this text, or the generic message when generic errors are on;
    # never the internal message) and not retried.
    from ..blob.operations import _mark_non_retryable

    _mark_non_retryable(err, str(err))
    raise err


class SDKError(Exception):
    """Base exception for all KeborMed LEDSAS SDK errors."""


class Retryable(SDKError):
    """
    Transient error - will be retried with exponential backoff.

    Use this for:
    - Network hiccups
    - Rate limits
    - Temporary service unavailability
    - Blob storage busy/throttling
    - Transient database connection errors

    The SDK will retry the handler with exponential backoff + jitter.

    Args:
        message: Internal error message — always logged; sent to the caller only
            when ``user_message`` is unset AND ``generic_errors`` is disabled.
        user_message: Caller-facing message. **When set, the caller always sees
            this** (regardless of ``generic_errors``). Use it whenever you want
            to surface a friendly message to upstream callers.
            A ``str`` (an empty string is a message too) or None (not set);
            any other type raises TypeError here.

    Example:
        raise Retryable("Internal: connection pool exhausted",
                        user_message="Service is busy, please try again")
    """

    def __init__(self, message: str, user_message: str | None = None):
        super().__init__(message)
        self.user_message = _checked_user_message(user_message)


class Permanent(SDKError):
    """
    Permanent error - goes to DLQ, no retry.

    Use this for:
    - Bad input (missing required field, invalid format)
    - Schema mismatch
    - Missing required blob
    - Unsupported command version
    - Business logic validation failures

    The message will be sent to the dead-letter queue for triage.

    Args:
        message: Internal error message — always logged; sent to the caller only
            when ``user_message`` is unset AND ``generic_errors`` is disabled.
        user_message: Caller-facing message. **When set, the caller always sees
            this** (regardless of ``generic_errors``).
            A ``str`` (an empty string is a message too) or None (not set);
            any other type raises TypeError here.

    Example:
        raise Permanent("Invalid field 'foo': expected int, got str",
                        user_message="Invalid input data")
    """

    def __init__(self, message: str, user_message: str | None = None):
        super().__init__(message)
        self.user_message = _checked_user_message(user_message)


class DeadlineExceeded(SDKError):
    """
    Handler exceeded deadline.

    Raised when:
    - Handler takes longer than the deadline specified in envelope
    - Handler checks ctx.deadline and determines insufficient time to proceed

    Treated as terminal — the message goes to DLQ without retry. The
    deadline is already in the past, so a fresh attempt would just fail
    the same way; the orchestrator is expected to resend with a new
    deadline if appropriate.

    Args:
        message: Internal error message — always logged; sent to the caller only
            when ``user_message`` is unset AND ``generic_errors`` is disabled.
        user_message: Caller-facing message. **When set, the caller always sees
            this** (regardless of ``generic_errors``). Symmetric with
            ``Retryable`` and ``Permanent``.
            A ``str`` (an empty string is a message too) or None (not set);
            any other type raises TypeError here.

    Example:
        if ctx.deadline and remaining < 5:
            raise DeadlineExceeded(
                f"Only {remaining:.1f}s left; need >= 5s",
                user_message="Request arrived too close to deadline",
            )
    """

    def __init__(self, message: str = "", user_message: str | None = None):
        super().__init__(message)
        self.user_message = _checked_user_message(user_message)
