"""
Envelope model for LEDSAS messages.

The envelope is a fixed wrapper around all message types (command, response, status, error)
containing metadata for routing, correlation, tracing, and deadlines.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator


class Envelope(BaseModel):
    """
    Fixed envelope for all LEDSAS messages.

    The envelope wraps all message types and provides:
    - Message metadata (ID, correlation, timestamps)
    - Routing information (reply_to, priority)
    - Distributed tracing (trace_id)
    - Deadline enforcement
    - Schema versioning

    All fields are required unless marked optional.
    """

    schema_version: str = Field(
        ...,
        description="Envelope schema version (not payload version)",
        pattern=r"^\d+\.\d+$",
    )

    # Drop the unused ``"error"`` value. The SDK
    # emits only command / response / status; an ``"error"`` envelope was
    # never produced and is rejected on the command exchange by the envelope-type guard.
    type: Literal["command", "response", "status"] = Field(..., description="Message category")

    name: str = Field(
        ...,
        description="Message name in PascalCase (e.g., ProcessDataset, ValidateImage)",
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z][A-Za-z0-9_]*$",
    )

    message_version: str = Field(
        ...,
        description="Version of the payload schema for this message name",
        pattern=r"^\d+\.\d+$",
    )

    # Accept mixed-case hex in UUID fields. RFC 4122
    # §3 specifies lowercase as the canonical *output* form but says inputs
    # are case-insensitive — and producers in other ecosystems (.NET,
    # operator-pasted IDs from a UI) routinely emit uppercase. The
    # ``before`` validator below normalizes to lowercase so internal
    # storage, logs, and downstream comparisons stay in a single form.
    message_id: str = Field(
        ...,
        description="Unique message identifier (UUID v4 format, case-insensitive)",
        pattern=r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    )

    correlation_id: str = Field(
        ...,
        description="Correlation ID linking command to responses/status (UUID v4 format, case-insensitive)",
        pattern=r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    )

    idempotency_key: str = Field(
        ...,
        description=(
            "Idempotency key for replay safety. Must be URL-safe: "
            "alphanumerics, dot, underscore, hyphen, colon. Length "
            "1..128. The bundled examples interpolate this key into "
            "blob paths, so the character set is restricted to avoid "
            "path-injection footguns in customer code."
        ),
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9_\-:.]+$",
    )

    sent_at: datetime = Field(
        ..., description="When the message was published (RFC3339 UTC timestamp)"
    )

    deadline: datetime | None = Field(
        None,
        description="Handler should finish before this time (RFC3339 UTC timestamp)",
    )

    trace_id: str = Field(
        ...,
        description=(
            "W3C traceparent for distributed tracing. Must be URL-safe: "
            "alphanumerics, dot, underscore, hyphen, colon. Length 1..255. "
            "Same character-set restriction as idempotency_key — handlers "
            "and downstream tooling commonly interpolate trace_id into "
            "paths, log lines, and search queries, so the SDK constrains "
            "the field to close the path-traversal / control-char / "
            "log-injection footgun."
        ),
        min_length=1,
        max_length=255,
        pattern=r"^[A-Za-z0-9_\-:.]+$",
    )

    reply_to: str = Field(
        default="",
        description=(
            "Exchange to send response/status to. Must be URL-safe — "
            "alphanumerics, dot, underscore, hyphen, colon — and at most "
            "127 chars. The SDK logs this value on every reply-publish "
            "(success and failure), so the character set is locked down "
            "to the same shape as trace_id / idempotency_key / job_id to "
            "close the log-injection footgun. The 127-char cap matches "
            "AMQP's protocol-level limit on exchange-name length; a "
            "longer name would be accepted by the schema but later "
            "rejected by the broker with 'Max length exceeded for "
            "exchange' AFTER the handler had already run. Empty string "
            "is the explicit fire-and-forget value (no response sent)."
        ),
        max_length=127,
        pattern=r"^([A-Za-z0-9_\-:.]+)?$",
    )

    priority: int | None = Field(
        None, description="Message priority (0-9, higher is more urgent)", ge=0, le=9
    )

    job_id: str | None = Field(
        None,
        description=(
            "Optional orchestrator-controlled job identifier for "
            "business-level tracking. Unlike correlation_id "
            "(SDK-controlled), job_id is set by the orchestrator and "
            "echoed in all responses for workflow tracking. Same "
            "URL-safe character set as idempotency_key / trace_id "
            "(`^[A-Za-z0-9_\\-:.]+$`, length 1..128) so handlers can "
            "safely interpolate it into paths."
        ),
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9_\-:.]+$",
    )

    @field_validator("message_id", "correlation_id", mode="before")
    @classmethod
    def _normalize_uuid_case(cls, v):
        """Lowercase UUID-shaped IDs.

        The accepted pattern is case-insensitive hex (RFC 4122 allows
        either case on input), but internal storage and logs use the
        canonical lowercase form so equality comparisons downstream
        don't have to think about case.
        """
        if isinstance(v, str):
            return v.lower()
        return v

    @field_validator("sent_at", "deadline")
    @classmethod
    def validate_timezone_aware(cls, v: datetime | None) -> datetime | None:
        """Ensure datetime fields are timezone-aware (preferably UTC)."""
        if v is not None and v.tzinfo is None:
            raise ValueError("Datetime must be timezone-aware (use datetime.timezone.utc)")
        return v

    @field_validator("deadline")
    @classmethod
    def validate_deadline_after_sent_at(cls, v: datetime | None, info) -> datetime | None:
        """Ensure deadline is after sent_at if both are present."""
        if v is not None and info.data.get("sent_at") is not None:
            sent_at = info.data["sent_at"]
            if v <= sent_at:
                raise ValueError(f"Deadline ({v}) must be after sent_at ({sent_at})")
        return v

    model_config = ConfigDict(
        validate_assignment=True,
    )

    @field_serializer("sent_at", "deadline", when_used="json")
    def _serialize_datetime(self, v: datetime | None) -> str | None:
        return v.isoformat() if v is not None else None

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        job_part = f", job_id={self.job_id!r}" if self.job_id else ""
        return (
            f"Envelope(type={self.type!r}, name={self.name!r}, "
            f"message_id={self.message_id!r}, correlation_id={self.correlation_id!r}{job_part})"
        )
