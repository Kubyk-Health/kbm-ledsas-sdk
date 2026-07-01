"""
ExecutionContext for LEDSAS SDK handlers.

The ExecutionContext provides all the APIs a handler needs to:
- Access envelope metadata (correlation_id, trace_id, deadline, etc.)
- Download/upload blobs
- Emit status updates
- Access structured logging
- Access SDK configuration

The context is scoped to a single command execution and is immutable after creation.
"""

import logging
from datetime import UTC, datetime

from ..blob.operations import BlobOperations
from ..models.envelope import Envelope
from ..models.messages import Status
from ..transport.base import Transport


class ExecutionContext:
    """
    Execution context passed to handlers.

    Provides access to:
    - envelope: Current command envelope (metadata)
    - payload: Command payload (dict — same dict as the ``req``
      argument the handler receives; exposed here for convenience
      when the payload also needs to be referenced via ``ctx``)
    - blob: BlobOperations interface for download/upload
    - logger: stdlib logger pre-configured with command name
      (``handler.<envelope.name>``). NOTE: this is a vanilla
      :class:`logging.Logger`; structured fields go in ``extra={...}``,
      not as keyword arguments.
    - message_id: Unique message identifier (UUID, equivalent to
      ``ctx.envelope.message_id``)
    - correlation_id: Correlation ID for distributed tracing
    - trace_id: W3C traceparent for distributed tracing
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
        payload: dict,
    ):
        """
        Initialize execution context.

        Args:
            transport: Transport instance (for blob ops and status emission)
            envelope: Command envelope with metadata
            payload: Command payload (dict)
        """
        self._transport = transport
        self._envelope = envelope
        self._payload = payload
        self._blob_ops = transport.get_blob_operations()
        self._logger = logging.getLogger(f"handler.{envelope.name}")

    @property
    def envelope(self) -> Envelope:
        """Get the current command envelope (read-only)."""
        return self._envelope

    @property
    def payload(self) -> dict:
        """Get the command payload (read-only)."""
        return self._payload

    @property
    def blob(self) -> BlobOperations:
        """Get blob operations interface for download/upload."""
        return self._blob_ops

    @property
    def logger(self) -> logging.Logger:
        """Get structured logger pre-configured with command name."""
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
        """Get W3C traceparent for distributed tracing."""
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

    async def emit_status(self, stage: str, progress: float, note: str | None = None) -> None:
        """
        Emit a status update during handler execution.

        Args:
            stage: Current stage (e.g., "downloading", "processing", "uploading")
            progress: Progress as a float between 0.0 and 1.0
            note: Optional human-readable note (e.g., "Downloading 15 MB dataset")

        Raises:
            ValueError: If progress is not between 0.0 and 1.0

        Example:
            await ctx.emit_status("downloading", 0.25, "Downloading input dataset")
        """
        if not 0.0 <= progress <= 1.0:
            raise ValueError(f"Progress must be between 0.0 and 1.0, got {progress}")

        payload = {"stage": stage, "progress": progress}
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
