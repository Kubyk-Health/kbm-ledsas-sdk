"""
W3C Trace Context utilities for LEDSAS SDK.

Provides helpers for parsing and generating W3C traceparent headers
for distributed tracing.

W3C Traceparent format:
    version-trace_id-parent_id-trace_flags

Example:
    00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
    │  │                                │                  │
    │  │                                │                  └─ flags (01 = sampled)
    │  │                                └─ parent/span ID (16 hex chars)
    │  └─ trace ID (32 hex chars)
    └─ version (00)

Reference: https://www.w3.org/TR/trace-context/
"""

import re
import secrets
from typing import NamedTuple


class TraceContext(NamedTuple):
    """
    Parsed W3C trace context.

    Attributes:
        version: Traceparent version (usually "00")
        trace_id: Trace ID (32 hex characters)
        parent_id: Parent/span ID (16 hex characters)
        trace_flags: Trace flags (2 hex characters, e.g., "01" for sampled)
    """

    version: str
    trace_id: str
    parent_id: str
    trace_flags: str

    def to_traceparent(self) -> str:
        """
        Convert to traceparent header string.

        Returns:
            Traceparent header value

        Example:
            >>> ctx = TraceContext("00", "abc...", "def...", "01")
            >>> ctx.to_traceparent()
            "00-abc...-def...-01"
        """
        return f"{self.version}-{self.trace_id}-{self.parent_id}-{self.trace_flags}"


# Regex pattern for traceparent validation
TRACEPARENT_PATTERN = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")


def parse_traceparent(traceparent: str) -> TraceContext:
    """
    Parse W3C traceparent header.

    Args:
        traceparent: Traceparent header value

    Returns:
        TraceContext with parsed fields

    Raises:
        ValueError: If traceparent format is invalid

    Example:
        >>> ctx = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        >>> ctx.trace_id
        "4bf92f3577b34da6a3ce929d0e0e4736"
    """
    match = TRACEPARENT_PATTERN.match(traceparent.strip().lower())
    if not match:
        raise ValueError(
            f"Invalid traceparent format: {traceparent}. "
            f"Expected: version-trace_id-parent_id-flags "
            f"(e.g., 00-{generate_trace_id()}-{generate_span_id()}-01)"
        )

    version, trace_id, parent_id, trace_flags = match.groups()

    # Validate trace_id is not all zeros
    if trace_id == "00000000000000000000000000000000":
        raise ValueError("Trace ID cannot be all zeros")

    # Validate parent_id is not all zeros
    if parent_id == "0000000000000000":
        raise ValueError("Parent ID cannot be all zeros")

    return TraceContext(
        version=version,
        trace_id=trace_id,
        parent_id=parent_id,
        trace_flags=trace_flags,
    )


def generate_trace_id() -> str:
    """
    Generate new W3C trace ID (32 hex characters).

    Returns:
        Random trace ID

    Example:
        >>> trace_id = generate_trace_id()
        >>> len(trace_id)
        32
        >>> all(c in '0123456789abcdef' for c in trace_id)
        True
    """
    return secrets.token_hex(16)


def generate_span_id() -> str:
    """
    Generate new W3C span/parent ID (16 hex characters).

    Returns:
        Random span ID

    Example:
        >>> span_id = generate_span_id()
        >>> len(span_id)
        16
        >>> all(c in '0123456789abcdef' for c in span_id)
        True
    """
    return secrets.token_hex(8)


def generate_traceparent(sampled: bool = True) -> str:
    """
    Generate new W3C traceparent header.

    Args:
        sampled: Whether trace is sampled (default: True)

    Returns:
        Traceparent header value

    Example:
        >>> traceparent = generate_traceparent(sampled=True)
        >>> traceparent.startswith("00-")
        True
        >>> traceparent.endswith("-01")  # sampled flag
        True
    """
    version = "00"
    trace_id = generate_trace_id()
    span_id = generate_span_id()
    flags = "01" if sampled else "00"

    return f"{version}-{trace_id}-{span_id}-{flags}"


def create_child_span(parent_traceparent: str, sampled: bool = True) -> str:
    """
    Create child span from parent traceparent.

    Preserves the trace_id but generates a new span_id.

    Args:
        parent_traceparent: Parent traceparent header
        sampled: Whether child span is sampled (default: True)

    Returns:
        New traceparent for child span

    Raises:
        ValueError: If parent traceparent is invalid

    Example:
        >>> parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        >>> child = create_child_span(parent)
        >>> parse_traceparent(child).trace_id
        "4bf92f3577b34da6a3ce929d0e0e4736"  # Same trace_id
    """
    parent_ctx = parse_traceparent(parent_traceparent)

    version = parent_ctx.version
    trace_id = parent_ctx.trace_id
    new_span_id = generate_span_id()
    flags = "01" if sampled else "00"

    return f"{version}-{trace_id}-{new_span_id}-{flags}"


def is_sampled(traceparent: str) -> bool:
    """
    Check if trace is sampled (trace_flags & 0x01 == 1).

    Args:
        traceparent: Traceparent header value

    Returns:
        True if sampled, False otherwise

    Example:
        >>> is_sampled("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        True
        >>> is_sampled("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
        False
    """
    ctx = parse_traceparent(traceparent)
    flags_int = int(ctx.trace_flags, 16)
    return (flags_int & 0x01) == 1
