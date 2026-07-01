"""
Handler registry and execution for LEDSAS SDK.

The HandlerRegistry manages command handlers and executes them with proper
error handling, response generation, and error classification.

Handlers are async functions with the signature:
    async def handler(ctx: ExecutionContext, payload: dict) -> dict

The registry:
- Stores handlers keyed by (command_name, version)
- Executes handlers with ExecutionContext
- Wraps results in Response messages
- Classifies errors (Retryable, Permanent, DeadlineExceeded)
- Handles uncaught exceptions
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from ..models.envelope import Envelope
from ..models.errors import DeadlineExceeded, Permanent, Retryable
from ..models.messages import Command, Response
from .context import ExecutionContext

# Type alias for handler functions
HandlerFunc = Callable[[ExecutionContext, dict], Awaitable[dict]]

logger = logging.getLogger(__name__)

# Default generic error messages for customer-facing responses
# Used when generic_errors=True to hide internal error details from callers
# DeadlineExceeded and Timeout map to different strings because they have
# different retry semantics. A caller who sees "Processing timed out (will
# retry)" knows the SDK is going to try again; a caller who sees
# "Request arrived too close to deadline" knows the SDK has dead-lettered
# the message and a retry must be issued by the orchestrator with a fresh
# deadline.
GENERIC_ERROR_MESSAGES = {
    "Retryable": "Processing failed temporarily",
    "Permanent": "Processing failed",
    "DeadlineExceeded": "Request arrived too close to deadline",
    "Timeout": "Processing timed out (will retry)",
    "UnexpectedError": "An unexpected error occurred",
    "HandlerNotFound": "Handler not available",
}


class HandlerRegistry:
    """
    Registry for command handlers.

    Handlers are registered by command name and version, then executed when
    matching commands are received.

    Example:
        registry = HandlerRegistry()

        async def process_dataset(ctx: ExecutionContext, payload: dict) -> dict:
            # Handler logic here
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", process_dataset)

        # Execute handler
        command = Command(...)
        ctx = ExecutionContext(...)
        response = await registry.execute(ctx, command)
    """

    def __init__(self, generic_errors: bool = False):
        """
        Initialize handler registry.

        Args:
            generic_errors: If True, return generic error messages to callers
                           instead of specific error details. Defaults to False.
        """
        self._handlers: dict[tuple[str, str], HandlerFunc] = {}
        self._generic_errors = generic_errors

    def register(self, command_name: str, version: str, handler: HandlerFunc) -> None:
        """
        Register a handler for a command.

        Args:
            command_name: Command name (e.g., "ProcessDataset")
            version: Message version (e.g., "1.0")
            handler: Async handler function

        Raises:
            ValueError: If handler already registered for this (name, version)

        Example:
            registry.register("ProcessDataset", "1.0", my_handler)
        """
        key = (command_name, version)
        if key in self._handlers:
            raise ValueError(f"Handler already registered for {command_name} v{version}")
        self._handlers[key] = handler
        logger.info(f"Registered handler for {command_name} v{version}")

    def get(self, command_name: str, version: str) -> HandlerFunc | None:
        """
        Get handler for a command.

        Args:
            command_name: Command name
            version: Message version

        Returns:
            Handler function or None if not found
        """
        return self._handlers.get((command_name, version))

    def list_handlers(self) -> list[tuple[str, str]]:
        """
        List all registered handlers.

        Returns:
            List of (command_name, version) tuples
        """
        return list(self._handlers.keys())

    def _get_error_message(
        self, code: str, specific_message: str, user_message: str | None = None
    ) -> str:
        """
        Resolve the message the caller will see.

        Precedence:
        1. If the handler supplied ``user_message``, the caller always sees that.
        2. Otherwise, if ``generic_errors`` is enabled, the caller sees the
           generic fallback (e.g. "Processing failed").
        3. Otherwise, the caller sees the raw internal ``specific_message``.

        The internal ``specific_message`` is always available in logs regardless
        of these settings.
        """
        if user_message:
            return user_message
        if self._generic_errors:
            return GENERIC_ERROR_MESSAGES.get(code, "An error occurred")
        return specific_message

    def _calculate_timeout(self, envelope: Envelope, config_timeout: int) -> float | None:
        """
        Calculate effective timeout (minimum of deadline remaining and config).

        Priority:
        1. If deadline is set and config_timeout > 0: use minimum
        2. If only deadline is set: use remaining time
        3. If only config_timeout > 0: use config_timeout
        4. If neither: return None (no timeout)

        Args:
            envelope: Command envelope (may contain deadline)
            config_timeout: Configured timeout in seconds (0=disabled)

        Returns:
            Effective timeout in seconds, or None if no timeout should be applied

        Raises:
            DeadlineExceeded: When ``envelope.deadline`` is in the past at
                computation time. Callers should invoke this from inside
                the same try-block that catches DeadlineExceeded so the
                response is classified terminal (not retryable Timeout).
        """
        if envelope.deadline is not None:
            remaining = (envelope.deadline - datetime.now(UTC)).total_seconds()
            if remaining <= 0:
                # Deadline already in the past (e.g. raced past the
                # explicit pre-flight check). Raise so the response is
                # classified DeadlineExceeded (terminal) — using a tiny
                # positive timeout would misclassify this as Timeout
                # (retryable), which would have the orchestrator retry
                # against the same expired deadline.
                raise DeadlineExceeded(f"Deadline {envelope.deadline.isoformat()} already passed")
            if config_timeout > 0:
                return min(remaining, config_timeout)
            return remaining
        return config_timeout if config_timeout > 0 else None

    async def execute(
        self, ctx: ExecutionContext, command: Command, timeout_seconds: int = 0
    ) -> Response:
        """
        Execute handler for a command and return response.

        This method:
        1. Looks up the handler for the command
        2. Checks deadline before starting
        3. Executes the handler with error handling and timeout enforcement
        4. Wraps result in Response message
        5. Classifies errors (Retryable/Permanent/Deadline/Timeout)

        Args:
            ctx: Execution context
            command: Command to execute
            timeout_seconds: Handler timeout in seconds (0=disabled, uses deadline if set)

        Returns:
            Response message (success or error)

        Raises:
            Retryable: For transient errors (will be retried)
            Permanent: For permanent errors (goes to DLQ)
            DeadlineExceeded: For deadline violations

        Example:
            response = await registry.execute(ctx, command, timeout_seconds=30)
            if "error" in response.payload:
                # Handle error
                pass
        """
        envelope = command.envelope
        payload = command.payload

        # Look up handler
        handler = self.get(envelope.name, envelope.message_version)
        if handler is None:
            # No handler registered - permanent error
            logger.error(
                f"No handler for {envelope.name} v{envelope.message_version}",
                extra={"correlation_id": envelope.correlation_id},
            )
            response_envelope = self._build_response_envelope(envelope)
            specific_msg = f"No handler registered for {envelope.name} v{envelope.message_version}"
            error_payload = {
                "error": {
                    "code": "HandlerNotFound",
                    "message": self._get_error_message("HandlerNotFound", specific_msg),
                    "retryable": False,
                }
            }
            return Response(envelope=response_envelope, payload=error_payload)

        # Execute handler. Deadline-pre-flight and timeout-calculation both
        # live inside this try block so a deadline-already-past finding
        # lands in the DeadlineExceeded branch (terminal classification)
        # rather than escaping execute() as an uncaught exception.
        effective_timeout: float | None = None
        try:
            # Deadline pre-flight: raises DeadlineExceeded if already in
            # the past. Caught below to produce a terminal Response.
            if envelope.deadline is not None:
                now = datetime.now(UTC)
                if now >= envelope.deadline:
                    logger.warning(
                        "Deadline already exceeded",
                        extra={
                            "correlation_id": envelope.correlation_id,
                            "command_name": envelope.name,
                            "deadline": envelope.deadline.isoformat(),
                            "now": now.isoformat(),
                        },
                    )
                    raise DeadlineExceeded(
                        f"Deadline {envelope.deadline.isoformat()} already passed"
                    )

            # Calculate effective timeout (may also raise DeadlineExceeded
            # for a tiny race window between the pre-flight check and now).
            effective_timeout = self._calculate_timeout(envelope, timeout_seconds)

            logger.info(
                "Executing handler",
                extra={
                    "correlation_id": envelope.correlation_id,
                    "command_name": envelope.name,
                    "timeout": effective_timeout,
                },
            )

            # Apply timeout if configured
            if effective_timeout is not None and effective_timeout > 0:
                result = await asyncio.wait_for(handler(ctx, payload), timeout=effective_timeout)
            else:
                result = await handler(ctx, payload)

            # Build success response
            response_envelope = self._build_response_envelope(envelope)
            return Response(envelope=response_envelope, payload=result)

        except TimeoutError:
            # Handler exceeded timeout - retryable error
            logger.warning(
                "Handler timeout",
                extra={
                    "correlation_id": envelope.correlation_id,
                    "command_name": envelope.name,
                    "timeout_seconds": effective_timeout,
                },
            )
            response_envelope = self._build_response_envelope(envelope)
            error_payload = {
                "error": {
                    "code": "Timeout",
                    "message": self._get_error_message(
                        "Timeout", f"Handler timed out after {effective_timeout}s"
                    ),
                    "retryable": True,
                }
            }
            return Response(envelope=response_envelope, payload=error_payload)

        except Retryable as e:
            # Retryable error - will be retried with backoff. Caller-controlled
            # text travels via extra={"error_message": ...} so the JSON
            # formatter escapes any control characters; text-format viewers
            # don't render extra values into the line at all.
            logger.warning(
                "Retryable error in handler",
                extra={
                    "correlation_id": envelope.correlation_id,
                    "command_name": envelope.name,
                    "error_message": str(e),
                },
            )
            response_envelope = self._build_response_envelope(envelope)
            error_payload = {
                "error": {
                    "code": "Retryable",
                    "message": self._get_error_message(
                        "Retryable", str(e), getattr(e, "user_message", None)
                    ),
                    "retryable": True,
                }
            }
            return Response(envelope=response_envelope, payload=error_payload)

        except Permanent as e:
            # Permanent error - goes to DLQ. See Retryable branch above for
            # why ``error_message`` rides in ``extra`` rather than the
            # message string.
            logger.error(
                "Permanent error in handler",
                extra={
                    "correlation_id": envelope.correlation_id,
                    "command_name": envelope.name,
                    "error_message": str(e),
                },
            )
            response_envelope = self._build_response_envelope(envelope)
            error_payload = {
                "error": {
                    "code": "Permanent",
                    "message": self._get_error_message(
                        "Permanent", str(e), getattr(e, "user_message", None)
                    ),
                    "retryable": False,
                }
            }
            return Response(envelope=response_envelope, payload=error_payload)

        except DeadlineExceeded as e:
            # Deadline exceeded - NOT retryable (deadline already passed).
            # Caller-controlled text via extra; see Retryable branch.
            logger.warning(
                "Deadline exceeded in handler",
                extra={
                    "correlation_id": envelope.correlation_id,
                    "command_name": envelope.name,
                    "error_message": str(e),
                },
            )
            response_envelope = self._build_response_envelope(envelope)
            error_payload = {
                "error": {
                    "code": "DeadlineExceeded",
                    "message": self._get_error_message(
                        "DeadlineExceeded", str(e), getattr(e, "user_message", None)
                    ),
                    "retryable": False,  # Deadlines are terminal — see errors.py
                }
            }
            return Response(envelope=response_envelope, payload=error_payload)

        except Exception as e:
            # Uncaught exception - treat as retryable. Do NOT pass str(e)
            # to the customer-facing message path: the exception's str
            # repr often leaks internal field names
            # (KeyError("authorization_token") becomes
            # "'authorization_token'") that disclose handler state. The
            # full detail still goes to logs via logger.exception below
            # and via ``internal_message`` in the extra fields. Callers
            # always see the generic message; handlers that want richer
            # caller-visible text should raise errors.Permanent /
            # Retryable with user_message= explicitly.
            internal_msg = f"Unexpected error ({type(e).__name__}): {e}"
            logger.exception(
                "Uncaught exception in handler",
                extra={
                    "correlation_id": envelope.correlation_id,
                    "command_name": envelope.name,
                    "internal_message": internal_msg,
                },
            )
            response_envelope = self._build_response_envelope(envelope)
            error_payload = {
                "error": {
                    "code": "UnexpectedError",
                    "message": GENERIC_ERROR_MESSAGES["UnexpectedError"],
                    "retryable": True,
                }
            }
            return Response(envelope=response_envelope, payload=error_payload)

    def _build_response_envelope(self, command_envelope: Envelope) -> Envelope:
        """
        Build response envelope from command envelope.

        Copies metadata from command envelope and sets type to "response".
        Preserves job_id for business-level tracking.
        Note: Deadline is not copied to response (deadlines apply to commands).

        Args:
            command_envelope: Original command envelope

        Returns:
            Response envelope
        """
        return Envelope(
            schema_version=command_envelope.schema_version,
            type="response",
            name=command_envelope.name,
            message_version=command_envelope.message_version,
            message_id=command_envelope.message_id,
            correlation_id=command_envelope.correlation_id,
            idempotency_key=command_envelope.idempotency_key,
            sent_at=datetime.now(UTC),
            deadline=None,  # Deadlines apply to commands, not responses
            trace_id=command_envelope.trace_id,
            reply_to=command_envelope.reply_to,
            priority=command_envelope.priority,
            job_id=command_envelope.job_id,
        )

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"HandlerRegistry(handlers={len(self._handlers)})"
