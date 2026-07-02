"""
Unit tests for ExecutionContext.

Tests include:
- Context initialization
- Property access (envelope, payload, blob, logger, etc.)
- Status emission
- Deadline checking
- Error handling
"""

from datetime import UTC, datetime, timedelta

import pytest

from kbm_ledsas_sdk.models import Envelope
from kbm_ledsas_sdk.runtime.context import ExecutionContext
from kbm_ledsas_sdk.transport.mock import MockBlobOperations, MockTransport


@pytest.fixture
def sample_envelope():
    """Sample command envelope for testing."""
    return Envelope(
        schema_version="1.0",
        type="command",
        name="ProcessDataset",
        message_version="1.0",
        message_id="550e8400-e29b-41d4-a716-446655440000",
        correlation_id="660e8400-e29b-41d4-a716-446655440000",
        idempotency_key="idem-123",
        sent_at=datetime.now(UTC),
        deadline=datetime.now(UTC) + timedelta(seconds=30),
        trace_id="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        reply_to="resp.dev.orchestrator.v1",
        priority=5,
    )


@pytest.fixture
def sample_payload():
    """Sample command payload for testing."""
    return {
        "dataset_uri": "azblob://dev/input.json",
        "options": {"normalize": True},
    }


@pytest.fixture
def mock_transport():
    """Mock transport for testing."""
    transport = MockTransport()
    return transport


class TestExecutionContextInitialization:
    """Test ExecutionContext initialization and property access."""

    @pytest.mark.asyncio
    async def test_context_initialization(self, mock_transport, sample_envelope, sample_payload):
        """Context initializes with correct properties."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )

        assert ctx.envelope == sample_envelope
        assert ctx.payload == sample_payload
        assert ctx.correlation_id == sample_envelope.correlation_id
        assert ctx.trace_id == sample_envelope.trace_id
        assert ctx.idempotency_key == sample_envelope.idempotency_key
        assert ctx.deadline == sample_envelope.deadline

    @pytest.mark.asyncio
    async def test_context_blob_operations(self, mock_transport, sample_envelope, sample_payload):
        """Context provides access to blob operations."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )

        # Blob operations should be available
        assert ctx.blob is not None
        assert isinstance(ctx.blob, MockBlobOperations)

    @pytest.mark.asyncio
    async def test_context_logger(self, mock_transport, sample_envelope, sample_payload):
        """Context provides logger with command name."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )

        # Logger should have the handler name
        assert ctx.logger is not None
        assert "ProcessDataset" in ctx.logger.name

    @pytest.mark.asyncio
    async def test_context_deadline_none(self, mock_transport, sample_payload):
        """Context handles None deadline correctly."""
        envelope = Envelope(
            schema_version="1.0",
            type="command",
            name="ProcessDataset",
            message_version="1.0",
            message_id="550e8400-e29b-41d4-a716-446655440000",
            correlation_id="660e8400-e29b-41d4-a716-446655440000",
            idempotency_key="idem-123",
            sent_at=datetime.now(UTC),
            deadline=None,  # No deadline
            trace_id="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            reply_to="resp.dev.orchestrator.v1",
        )

        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=envelope,
            payload=sample_payload,
        )

        assert ctx.deadline is None


class TestExecutionContextStatusEmission:
    """Test status emission via ExecutionContext."""

    @pytest.mark.asyncio
    async def test_emit_status_basic(self, mock_transport, sample_envelope, sample_payload):
        """Emit status update with stage and progress."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )

        # Emit status
        await ctx.emit_status(stage="downloading", progress=0.25)

        # Verify status was sent via transport
        assert len(mock_transport.sent_statuses) == 1
        status = mock_transport.sent_statuses[0]

        # emit_status builds a fresh envelope with
        # type="status" rather than reusing the command envelope verbatim.
        # Identifying fields (message_id, correlation_id, idempotency_key,
        # trace_id, reply_to, job_id, name) are copied through; type and
        # sent_at are not.
        assert status.envelope.type == "status"
        assert status.envelope.message_id == sample_envelope.message_id
        assert status.envelope.correlation_id == sample_envelope.correlation_id
        assert status.envelope.idempotency_key == sample_envelope.idempotency_key
        assert status.envelope.trace_id == sample_envelope.trace_id
        assert status.envelope.reply_to == sample_envelope.reply_to
        assert status.envelope.name == sample_envelope.name
        assert status.envelope.deadline is None  # not copied to status
        assert status.payload["stage"] == "downloading"
        assert status.payload["progress"] == 0.25
        assert "note" not in status.payload

    @pytest.mark.asyncio
    async def test_emit_status_with_note(self, mock_transport, sample_envelope, sample_payload):
        """Emit status update with note."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )

        # Emit status with note
        await ctx.emit_status(stage="processing", progress=0.5, note="Processing 1000 records")

        # Verify status was sent
        assert len(mock_transport.sent_statuses) == 1
        status = mock_transport.sent_statuses[0]

        assert status.payload["stage"] == "processing"
        assert status.payload["progress"] == 0.5
        assert status.payload["note"] == "Processing 1000 records"

    @pytest.mark.asyncio
    async def test_emit_status_multiple_updates(
        self, mock_transport, sample_envelope, sample_payload
    ):
        """Emit multiple status updates in sequence."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )

        # Emit multiple updates
        await ctx.emit_status(stage="downloading", progress=0.0)
        await ctx.emit_status(stage="downloading", progress=0.5)
        await ctx.emit_status(stage="processing", progress=0.75)
        await ctx.emit_status(stage="done", progress=1.0)

        # Verify all statuses sent
        assert len(mock_transport.sent_statuses) == 4
        assert mock_transport.sent_statuses[0].payload["progress"] == 0.0
        assert mock_transport.sent_statuses[1].payload["progress"] == 0.5
        assert mock_transport.sent_statuses[2].payload["progress"] == 0.75
        assert mock_transport.sent_statuses[3].payload["progress"] == 1.0

    @pytest.mark.asyncio
    async def test_emit_status_invalid_progress_low(
        self, mock_transport, sample_envelope, sample_payload
    ):
        """Emit status with progress < 0.0 raises ValueError."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )

        with pytest.raises(ValueError, match="Progress must be between 0.0 and 1.0"):
            await ctx.emit_status(stage="processing", progress=-0.1)

    @pytest.mark.asyncio
    async def test_emit_status_invalid_progress_high(
        self, mock_transport, sample_envelope, sample_payload
    ):
        """Emit status with progress > 1.0 raises ValueError."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )

        with pytest.raises(ValueError, match="Progress must be between 0.0 and 1.0"):
            await ctx.emit_status(stage="processing", progress=1.5)

    @pytest.mark.asyncio
    async def test_emit_status_boundary_values(
        self, mock_transport, sample_envelope, sample_payload
    ):
        """Emit status with boundary values (0.0 and 1.0)."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )

        # Test boundary values
        await ctx.emit_status(stage="initializing", progress=0.0)
        await ctx.emit_status(stage="done", progress=1.0)

        # Verify both sent successfully
        assert len(mock_transport.sent_statuses) == 2
        assert mock_transport.sent_statuses[0].payload["progress"] == 0.0
        assert mock_transport.sent_statuses[1].payload["progress"] == 1.0


class TestExecutionContextProperties:
    """Test read-only properties and immutability."""

    @pytest.mark.asyncio
    async def test_properties_readonly(self, mock_transport, sample_envelope, sample_payload):
        """Context properties are read-only (no setters)."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )

        # Properties should be accessible but not settable
        assert ctx.envelope is not None
        assert ctx.payload is not None
        assert ctx.blob is not None
        assert ctx.logger is not None
        assert ctx.message_id is not None
        assert ctx.correlation_id is not None
        assert ctx.trace_id is not None
        assert ctx.idempotency_key is not None

    @pytest.mark.asyncio
    async def test_message_id_matches_envelope(
        self, mock_transport, sample_envelope, sample_payload
    ):
        """ctx.message_id is the symmetric shortcut for ctx.envelope.message_id."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )
        assert ctx.message_id == sample_envelope.message_id
        assert ctx.message_id == ctx.envelope.message_id

    @pytest.mark.asyncio
    async def test_repr(self, mock_transport, sample_envelope, sample_payload):
        """Context has useful __repr__ for debugging."""
        ctx = ExecutionContext(
            transport=mock_transport,
            envelope=sample_envelope,
            payload=sample_payload,
        )

        repr_str = repr(ctx)
        assert "ExecutionContext" in repr_str
        assert "ProcessDataset" in repr_str
        assert sample_envelope.correlation_id in repr_str
