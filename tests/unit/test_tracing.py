"""
Unit tests for W3C trace context utilities.

Tests include:
- Traceparent parsing
- Trace/span ID generation
- Child span creation
- Sampling flag handling
- Validation
"""

import re

import pytest

from kbm_ledsas_sdk.utils.tracing import (
    TraceContext,
    create_child_span,
    generate_span_id,
    generate_trace_id,
    generate_traceparent,
    is_sampled,
    parse_traceparent,
)


class TestTraceIDGeneration:
    """Test trace and span ID generation."""

    def test_generate_trace_id_length(self):
        """Trace ID is 32 hex characters."""
        trace_id = generate_trace_id()
        assert len(trace_id) == 32

    def test_generate_trace_id_hex(self):
        """Trace ID contains only hex characters."""
        trace_id = generate_trace_id()
        assert all(c in "0123456789abcdef" for c in trace_id)

    def test_generate_trace_id_unique(self):
        """Generated trace IDs are unique."""
        ids = [generate_trace_id() for _ in range(100)]
        assert len(set(ids)) == 100

    def test_generate_span_id_length(self):
        """Span ID is 16 hex characters."""
        span_id = generate_span_id()
        assert len(span_id) == 16

    def test_generate_span_id_hex(self):
        """Span ID contains only hex characters."""
        span_id = generate_span_id()
        assert all(c in "0123456789abcdef" for c in span_id)

    def test_generate_span_id_unique(self):
        """Generated span IDs are unique."""
        ids = [generate_span_id() for _ in range(100)]
        assert len(set(ids)) == 100


class TestTraceparentGeneration:
    """Test traceparent header generation."""

    def test_generate_traceparent_format(self):
        """Generated traceparent has correct format."""
        traceparent = generate_traceparent()

        # Should match W3C pattern
        pattern = r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$"
        assert re.match(pattern, traceparent)

    def test_generate_traceparent_sampled(self):
        """Generated traceparent with sampled=True has flag 01."""
        traceparent = generate_traceparent(sampled=True)
        assert traceparent.endswith("-01")

    def test_generate_traceparent_not_sampled(self):
        """Generated traceparent with sampled=False has flag 00."""
        traceparent = generate_traceparent(sampled=False)
        assert traceparent.endswith("-00")

    def test_generate_traceparent_version(self):
        """Generated traceparent starts with version 00."""
        traceparent = generate_traceparent()
        assert traceparent.startswith("00-")


class TestTraceparentParsing:
    """Test parsing of traceparent headers."""

    def test_parse_valid_traceparent(self):
        """Parse valid traceparent header."""
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        ctx = parse_traceparent(traceparent)

        assert ctx.version == "00"
        assert ctx.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
        assert ctx.parent_id == "00f067aa0ba902b7"
        assert ctx.trace_flags == "01"

    def test_parse_traceparent_case_insensitive(self):
        """Parse traceparent with uppercase hex."""
        traceparent = "00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01"
        ctx = parse_traceparent(traceparent)

        # Should be normalized to lowercase
        assert ctx.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
        assert ctx.parent_id == "00f067aa0ba902b7"

    def test_parse_traceparent_with_whitespace(self):
        """Parse traceparent with leading/trailing whitespace."""
        traceparent = "  00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01  "
        ctx = parse_traceparent(traceparent)

        assert ctx.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"

    def test_parse_invalid_format(self):
        """Parse invalid traceparent format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid traceparent format"):
            parse_traceparent("invalid-format")

    def test_parse_wrong_version_length(self):
        """Parse traceparent with wrong version length."""
        with pytest.raises(ValueError, match="Invalid traceparent format"):
            parse_traceparent("0-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

    def test_parse_wrong_trace_id_length(self):
        """Parse traceparent with wrong trace ID length."""
        with pytest.raises(ValueError, match="Invalid traceparent format"):
            parse_traceparent("00-4bf92f3577b34da6-00f067aa0ba902b7-01")

    def test_parse_wrong_span_id_length(self):
        """Parse traceparent with wrong span ID length."""
        with pytest.raises(ValueError, match="Invalid traceparent format"):
            parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa-01")

    def test_parse_all_zeros_trace_id(self):
        """Parse traceparent with all-zeros trace ID raises ValueError."""
        with pytest.raises(ValueError, match="Trace ID cannot be all zeros"):
            parse_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01")

    def test_parse_all_zeros_span_id(self):
        """Parse traceparent with all-zeros span ID raises ValueError."""
        with pytest.raises(ValueError, match="Parent ID cannot be all zeros"):
            parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01")


class TestTraceContext:
    """Test TraceContext NamedTuple."""

    def test_trace_context_creation(self):
        """Create TraceContext with all fields."""
        ctx = TraceContext(
            version="00",
            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
            parent_id="00f067aa0ba902b7",
            trace_flags="01",
        )

        assert ctx.version == "00"
        assert ctx.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
        assert ctx.parent_id == "00f067aa0ba902b7"
        assert ctx.trace_flags == "01"

    def test_trace_context_to_traceparent(self):
        """Convert TraceContext back to traceparent string."""
        ctx = TraceContext(
            version="00",
            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
            parent_id="00f067aa0ba902b7",
            trace_flags="01",
        )

        traceparent = ctx.to_traceparent()
        assert traceparent == "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

    def test_parse_and_to_traceparent_roundtrip(self):
        """Parse traceparent and convert back to string."""
        original = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        ctx = parse_traceparent(original)
        result = ctx.to_traceparent()

        assert result == original


class TestChildSpanCreation:
    """Test creating child spans."""

    def test_create_child_span(self):
        """Create child span preserves trace_id."""
        parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        child = create_child_span(parent)

        parent_ctx = parse_traceparent(parent)
        child_ctx = parse_traceparent(child)

        # Trace ID should be the same
        assert child_ctx.trace_id == parent_ctx.trace_id

        # Span ID should be different
        assert child_ctx.parent_id != parent_ctx.parent_id

        # Version should be the same
        assert child_ctx.version == parent_ctx.version

    def test_create_child_span_sampled(self):
        """Create child span with sampled=True."""
        parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
        child = create_child_span(parent, sampled=True)

        assert child.endswith("-01")

    def test_create_child_span_not_sampled(self):
        """Create child span with sampled=False."""
        parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        child = create_child_span(parent, sampled=False)

        assert child.endswith("-00")

    def test_create_child_span_invalid_parent(self):
        """Create child span with invalid parent raises ValueError."""
        with pytest.raises(ValueError):
            create_child_span("invalid-parent")


class TestSamplingFlags:
    """Test sampling flag handling."""

    def test_is_sampled_true(self):
        """is_sampled returns True for flag 01."""
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        assert is_sampled(traceparent) is True

    def test_is_sampled_false(self):
        """is_sampled returns False for flag 00."""
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
        assert is_sampled(traceparent) is False

    def test_is_sampled_invalid(self):
        """is_sampled raises ValueError for invalid traceparent."""
        with pytest.raises(ValueError):
            is_sampled("invalid-traceparent")


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_generated_traceparent_can_be_parsed(self):
        """Generated traceparent can be parsed successfully."""
        traceparent = generate_traceparent()
        ctx = parse_traceparent(traceparent)

        # Should have valid structure
        assert len(ctx.trace_id) == 32
        assert len(ctx.parent_id) == 16
        assert ctx.version == "00"

    def test_child_span_can_be_parsed(self):
        """Child span can be parsed successfully."""
        parent = generate_traceparent()
        child = create_child_span(parent)
        ctx = parse_traceparent(child)

        # Should have valid structure
        assert len(ctx.trace_id) == 32
        assert len(ctx.parent_id) == 16

    def test_multiple_child_spans_unique(self):
        """Multiple child spans have unique span IDs."""
        parent = generate_traceparent()
        children = [create_child_span(parent) for _ in range(10)]

        # All should have same trace_id
        trace_ids = [parse_traceparent(c).trace_id for c in children]
        assert len(set(trace_ids)) == 1

        # All should have unique span_ids
        span_ids = [parse_traceparent(c).parent_id for c in children]
        assert len(set(span_ids)) == 10
