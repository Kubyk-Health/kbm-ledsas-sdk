"""
ExecutionContext for LEDSAS SDK handlers.

The ExecutionContext provides all the APIs a handler needs to:
- Access envelope metadata (correlation_id, trace_id, deadline, etc.)
- Download/upload blobs
- Emit status updates
- Access structured logging

The context is scoped to a single command execution and is immutable after creation.
"""

import enum
import logging
from datetime import UTC, datetime
from typing import Any

from ..blob.operations import (
    AllowedContainersBlobOperations,
    BlobOperations,
    _find_lone_surrogate,
    _mark_non_retryable,
)
from ..models.envelope import Envelope
from ..models.messages import Status
from ..transport.base import Transport
from ..utils.logging import masked_logger


def _handler_logger(command_name: str) -> logging.Logger:
    """``ctx.logger``: the stdlib logger ``handler.<command_name>``, as in
    earlier releases (outside the ``kbm_ledsas_sdk`` hierarchy, so its
    records propagate to the application's root handlers and follow the
    application's levels), with the SDK's secret mask attached. Created on
    the first ``ctx.logger`` access only: the handler code that accesses it
    runs only for registered command names, so a sender choosing new names
    creates no loggers."""
    return masked_logger(f"handler.{command_name}")


def _status_text(field: str, value: object, allowed: str) -> str | None:
    """``value`` as the text ``emit_status`` sends for ``field``: a ``str``
    as it is (a ``str`` Enum included), ``None`` for ``note``, an ``Enum``
    member whose value is a ``str`` as that value. Anything else raises
    ``TypeError``, marked non-retryable."""
    if isinstance(value, str) or (field == "note" and value is None):
        return value
    if isinstance(value, enum.Enum) and isinstance(value.value, str):
        return value.value
    err = TypeError(f"emit_status() takes {field} as {allowed}, got {type(value).__name__}")
    _mark_non_retryable(err, str(err))
    raise err


class ExecutionContext:
    """
    Execution context passed to handlers.

    Provides access to:
    - envelope: Current command envelope (metadata)
    - payload: Command payload (dict — same dict as the ``req``
      argument the handler receives; exposed here for convenience
      when the payload also needs to be referenced via ``ctx``)
    - blob: the blob storage interface for download/upload
      (:class:`kbm_ledsas_sdk.BlobOperations`, for type annotations)
    - logger: the stdlib :class:`logging.Logger` ``handler.<command name>``
      (configured by the application, not the SDK: its records propagate
      to the root logger; secrets in them are masked). Structured fields
      go in ``extra={...}``, not as keyword arguments.
    - message_id: Unique message identifier (UUID, equivalent to
      ``ctx.envelope.message_id``)
    - correlation_id: Correlation ID for distributed tracing
    - trace_id: opaque distributed-tracing id (URL-safe;
      ``[A-Za-z0-9_.:-]+``)
    - idempotency_key: Idempotency key for replay safety
    - deadline: Handler deadline (or None if no deadline)
    - job_id: Optional business-level job identifier (or None)

    Example usage in a handler:
        @app.handler("ProcessDataset")
        async def process_dataset(ctx: ExecutionContext, payload: dict) -> dict:
            # Download input data
            input_data = await ctx.blob.download_bytes(BlobRef.from_uri(payload["input_uri"]))

            # Emit progress update
            await ctx.emit_status(stage="processing", progress=0.5)

            # Upload result
            result_ref = await ctx.blob.upload_json("results", {"data": processed})

            return {"result_uri": result_ref.uri}
    """

    def __init__(
        self,
        transport: Transport,
        envelope: Envelope,
        payload: "dict[str, Any]",
        *,
        allowed_containers: "frozenset[str] | None" = None,
    ) -> None:
        """
        Initialize execution context.

        Internal: the SDK builds the context for each command and passes it
        to the handler; handler code does not construct one (the signature
        may change). Its type hints are for reading only.

        Args:
            transport: Transport instance (for blob ops and status emission)
            envelope: Command envelope with metadata
            payload: Command payload (dict)
            allowed_containers: The containers ``blob`` may use
                (``KBM_LEDSAS_ALLOWED_CONTAINERS``); None for no restriction.
        """
        self._transport = transport
        self._envelope = envelope
        self._payload = payload
        blob_ops = transport.get_blob_operations()
        if isinstance(allowed_containers, (set, frozenset)) and allowed_containers:
            blob_ops = AllowedContainersBlobOperations(blob_ops, frozenset(allowed_containers))
        self._blob_ops = blob_ops
        self._logger: logging.Logger | None = None

    @property
    def envelope(self) -> Envelope:
        """Get the current command envelope (read-only: the envelope is
        frozen, so assigning one of its fields raises
        ``pydantic.ValidationError``)."""
        return self._envelope

    @property
    def payload(self) -> "dict[str, Any]":
        """Get the command payload (read-only)."""
        return self._payload

    @property
    def blob(self) -> BlobOperations:
        """Get the blob storage interface (``kbm_ledsas_sdk.BlobOperations``)
        for download/upload."""
        return self._blob_ops

    @property
    def logger(self) -> logging.Logger:
        """Get the handler logger, ``handler.<command name>`` (a plain
        stdlib logger that propagates to the application's root handlers)."""
        if self._logger is None:
            self._logger = _handler_logger(self._envelope.name)
        return self._logger

    @property
    def message_id(self) -> str:
        """
        Get the unique message identifier (UUID).

        Equivalent to ``ctx.envelope.message_id``; provided for symmetry
        with ``correlation_id`` / ``trace_id`` / ``idempotency_key``,
        all of which are also exposed both ways.
        """
        return self._envelope.message_id

    @property
    def correlation_id(self) -> str:
        """Get correlation ID for distributed tracing."""
        return self._envelope.correlation_id

    @property
    def trace_id(self) -> str:
        """Get the opaque distributed-tracing id (URL-safe;
        ``[A-Za-z0-9_.:-]+``)."""
        return self._envelope.trace_id

    @property
    def idempotency_key(self) -> str:
        """Get idempotency key for replay safety."""
        return self._envelope.idempotency_key

    @property
    def deadline(self) -> datetime | None:
        """Get handler deadline (or None if no deadline)."""
        return self._envelope.deadline

    @property
    def job_id(self) -> str | None:
        """Get optional job ID for business-level tracking (or None if not set)."""
        return self._envelope.job_id

    async def emit_status(
        self,
        stage: str,
        progress: float,
        note: str | None = None,
    ) -> None:
        """
        Emit a status update during handler execution.

        Args:
            stage: Current stage (e.g., "downloading", "processing", "uploading")
            progress: Progress as a float between 0.0 and 1.0
            note: Optional human-readable note (e.g., "Downloading 15 MB dataset")

        An ``Enum`` member whose value is a ``str`` is accepted for
        ``stage`` and ``note`` and sent as that value (annotated ``str``: a
        ``StrEnum`` member is a ``str``; an ``Enum`` whose values are not
        text, such as an ``IntEnum``, is refused).

        Raises:
            TypeError: If ``stage`` is not a ``str``, ``note`` is neither
                ``None`` nor a ``str`` (an ``Enum`` with a ``str`` value
                counts as one), or progress is not a number (``True`` /
                ``False`` are not numbers here)
            ValueError: If progress is not between 0.0 and 1.0, or
                ``stage`` / ``note`` holds a lone surrogate (text UTF-8
                cannot encode, e.g. ``"\\ud800"`` from a JSON request)
            Both are raised before anything is published. Uncaught, they
            are replied as Permanent (not retried): the same call fails
            the same way on every attempt.

        Example:
            await ctx.emit_status("downloading", 0.25, "Downloading input dataset")
        """
        # stage and note are text: any other value (a number, a list, an
        # object with no JSON form) is refused before the status is built,
        # like an invalid progress: the same call fails the same way on
        # every attempt, so it is one Permanent reply, not a retry. An
        # Enum member whose value is text (class Stage(Enum): DOWNLOADING =
        # "downloading") is sent as that value, as it was before this check.
        stage_text = _status_text("stage", stage, "a str")
        note = _status_text("note", note, "None or a str")

        # bool is an int subclass: True / False are refused, as the
        # envelope's priority refuses them.
        if isinstance(progress, bool) or not isinstance(progress, (int, float)):
            type_error = TypeError(
                f"Progress must be a number between 0.0 and 1.0, got " f"{type(progress).__name__}"
            )
            _mark_non_retryable(type_error, str(type_error))
            raise type_error
        if not 0.0 <= progress <= 1.0:
            range_error = ValueError(f"Progress must be between 0.0 and 1.0, got {progress}")
            _mark_non_retryable(range_error, str(range_error))
            raise range_error

        # A lone surrogate would be written as an escape that strict JSON
        # readers refuse (as in replies and upload_json).
        for field, text in (("stage", stage_text), ("note", note)):
            problem = _find_lone_surrogate(text, field) if text is not None else None
            if problem is not None:
                surrogate_error = ValueError(f"emit_status() refused: {problem}")
                _mark_non_retryable(surrogate_error, str(surrogate_error))
                raise surrogate_error

        payload: dict[str, Any] = {"stage": stage_text, "progress": progress}
        if note is not None:
            payload["note"] = note

        # Build a fresh envelope with type="status" rather than
        # reusing the command's envelope verbatim. The command envelope
        # has type="command"; consumers that discriminate by
        # envelope.type (the documented contract) would otherwise
        # mis-classify status updates as commands. Mirrors the symmetric
        # _build_response_envelope path in runtime/handler.py.
        status_envelope = Envelope(
            schema_version=self._envelope.schema_version,
            type="status",
            name=self._envelope.name,
            message_version=self._envelope.message_version,
            message_id=self._envelope.message_id,
            correlation_id=self._envelope.correlation_id,
            idempotency_key=self._envelope.idempotency_key,
            sent_at=datetime.now(UTC),
            deadline=None,
            trace_id=self._envelope.trace_id,
            reply_to=self._envelope.reply_to,
            priority=self._envelope.priority,
            job_id=self._envelope.job_id,
        )
        status = Status(envelope=status_envelope, payload=payload)
        await self._transport.send_status(status)

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        job_part = f", job_id={self.job_id!r}" if self.job_id else ""
        return (
            f"ExecutionContext(name={self.envelope.name!r}, "
            f"correlation_id={self.correlation_id!r}, "
            f"deadline={self.deadline}{job_part})"
        )
