"""
Error types for the KeborMed LEDSAS SDK.

These error types control retry behavior and DLQ routing:
- Retryable: Transient errors that will be retried with exponential backoff
- Permanent: Non-recoverable errors that go to DLQ (no retry)
- DeadlineExceeded: Handler exceeded deadline (NOT retried — the deadline
  is in the past, so a retry would just exceed it again; goes to DLQ)
"""


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

    Example:
        raise Retryable("Internal: connection pool exhausted",
                        user_message="Service is busy, please try again")
    """

    def __init__(self, message: str, user_message: str | None = None):
        super().__init__(message)
        self.user_message = user_message


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

    Example:
        raise Permanent("Invalid field 'foo': expected int, got str",
                        user_message="Invalid input data")
    """

    def __init__(self, message: str, user_message: str | None = None):
        super().__init__(message)
        self.user_message = user_message


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

    Example:
        if ctx.deadline and remaining < 5:
            raise DeadlineExceeded(
                f"Only {remaining:.1f}s left; need >= 5s",
                user_message="Request arrived too close to deadline",
            )
    """

    def __init__(self, message: str = "", user_message: str | None = None):
        super().__init__(message)
        self.user_message = user_message
