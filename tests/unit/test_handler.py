"""
Unit tests for HandlerRegistry.

Tests include:
- Handler registration and lookup
- Handler execution with success
- Error handling (Retryable, Permanent, DeadlineExceeded, uncaught)
- Response generation
- Deadline checking
- Edge cases
"""

from datetime import UTC, datetime, timedelta

import pytest

from kbm_ledsas_sdk.models import Command, Envelope
from kbm_ledsas_sdk.models.errors import DeadlineExceeded, Permanent, Retryable
from kbm_ledsas_sdk.runtime.context import ExecutionContext
from kbm_ledsas_sdk.runtime.handler import HandlerRegistry
from kbm_ledsas_sdk.transport.mock import MockTransport


@pytest.fixture
def registry():
    """Empty handler registry."""
    return HandlerRegistry()


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
    )


@pytest.fixture
def sample_command(sample_envelope):
    """Sample command for testing."""
    return Command(
        envelope=sample_envelope,
        payload={"input_uri": "azblob://dev/input.json"},
    )


@pytest.fixture
def mock_transport():
    """Mock transport for testing."""
    return MockTransport()


@pytest.fixture
def sample_context(mock_transport, sample_envelope):
    """Sample execution context for testing."""
    return ExecutionContext(
        transport=mock_transport,
        envelope=sample_envelope,
        payload={"input_uri": "azblob://dev/input.json"},
    )


class TestHandlerRegistration:
    """Test handler registration and lookup."""

    @pytest.mark.asyncio
    async def test_register_handler(self, registry):
        """Register a handler successfully."""

        async def my_handler(ctx, payload):
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", my_handler)

        # Handler should be retrievable
        handler = registry.get("ProcessDataset", "1.0")
        assert handler is my_handler

    @pytest.mark.asyncio
    async def test_register_duplicate_handler(self, registry):
        """Register duplicate handler raises ValueError."""

        async def handler1(ctx, payload):
            return {}

        async def handler2(ctx, payload):
            return {}

        registry.register("ProcessDataset", "1.0", handler1)

        with pytest.raises(ValueError, match="Handler already registered"):
            registry.register("ProcessDataset", "1.0", handler2)

    @pytest.mark.asyncio
    async def test_register_multiple_handlers(self, registry):
        """Register multiple handlers for different commands."""

        async def handler1(ctx, payload):
            return {}

        async def handler2(ctx, payload):
            return {}

        registry.register("ProcessDataset", "1.0", handler1)
        registry.register("ValidateImage", "1.0", handler2)

        assert registry.get("ProcessDataset", "1.0") is handler1
        assert registry.get("ValidateImage", "1.0") is handler2

    @pytest.mark.asyncio
    async def test_register_multiple_versions(self, registry):
        """Register multiple versions of same command."""

        async def handler_v1(ctx, payload):
            return {"version": "1.0"}

        async def handler_v2(ctx, payload):
            return {"version": "2.0"}

        registry.register("ProcessDataset", "1.0", handler_v1)
        registry.register("ProcessDataset", "2.0", handler_v2)

        assert registry.get("ProcessDataset", "1.0") is handler_v1
        assert registry.get("ProcessDataset", "2.0") is handler_v2

    @pytest.mark.asyncio
    async def test_get_nonexistent_handler(self, registry):
        """Get nonexistent handler returns None."""
        handler = registry.get("NonExistent", "1.0")
        assert handler is None

    @pytest.mark.asyncio
    async def test_list_handlers_empty(self, registry):
        """List handlers on empty registry."""
        handlers = registry.list_handlers()
        assert handlers == []

    @pytest.mark.asyncio
    async def test_list_handlers_multiple(self, registry):
        """List all registered handlers."""

        async def handler1(ctx, payload):
            return {}

        async def handler2(ctx, payload):
            return {}

        registry.register("ProcessDataset", "1.0", handler1)
        registry.register("ValidateImage", "1.0", handler2)

        handlers = registry.list_handlers()
        assert len(handlers) == 2
        assert ("ProcessDataset", "1.0") in handlers
        assert ("ValidateImage", "1.0") in handlers


class TestHandlerExecution:
    """Test handler execution with success."""

    @pytest.mark.asyncio
    async def test_execute_handler_success(self, registry, sample_context, sample_command):
        """Execute handler successfully returns Response."""

        async def my_handler(ctx, payload):
            return {"result_uri": "azblob://dev/output.json"}

        registry.register("ProcessDataset", "1.0", my_handler)

        response = await registry.execute(sample_context, sample_command)

        # Response should have correct structure
        assert response.envelope.type == "response"
        assert response.envelope.name == "ProcessDataset"
        assert response.envelope.correlation_id == sample_command.envelope.correlation_id
        assert response.payload == {"result_uri": "azblob://dev/output.json"}

    @pytest.mark.asyncio
    async def test_execute_handler_receives_context(self, registry, sample_context, sample_command):
        """Handler receives correct context and payload."""
        received_ctx = None
        received_payload = None

        async def my_handler(ctx, payload):
            nonlocal received_ctx, received_payload
            received_ctx = ctx
            received_payload = payload
            return {"result": "ok"}

        registry.register("ProcessDataset", "1.0", my_handler)
        await registry.execute(sample_context, sample_command)

        # Handler should receive correct context and payload
        assert received_ctx == sample_context
        assert received_payload == sample_command.payload

    @pytest.mark.asyncio
    async def test_execute_handler_not_found(self, registry, sample_context, sample_command):
        """Execute with no handler returns error response."""
        # No handlers registered
        response = await registry.execute(sample_context, sample_command)

        # Should return error response with retryable=False
        assert "error" in response.payload
        assert response.payload["error"]["code"] == "HandlerNotFound"
        assert response.payload["error"]["retryable"] is False


class TestHandlerErrorHandling:
    """Test error handling in handlers."""

    @pytest.mark.asyncio
    async def test_execute_handler_retryable_error(self, registry, sample_context, sample_command):
        """Handler raises Retryable - returns error response."""

        async def my_handler(ctx, payload):
            raise Retryable("Network timeout")

        registry.register("ProcessDataset", "1.0", my_handler)

        response = await registry.execute(sample_context, sample_command)

        # Response should contain error with retryable=True
        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Retryable"
        assert response.payload["error"]["message"] == "Network timeout"
        assert response.payload["error"]["retryable"] is True

    @pytest.mark.asyncio
    async def test_execute_handler_permanent_error(self, registry, sample_context, sample_command):
        """Handler raises Permanent - returns error response."""

        async def my_handler(ctx, payload):
            raise Permanent("Invalid input format")

        registry.register("ProcessDataset", "1.0", my_handler)

        response = await registry.execute(sample_context, sample_command)

        # Response should contain error with retryable=False
        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Permanent"
        assert response.payload["error"]["message"] == "Invalid input format"
        assert response.payload["error"]["retryable"] is False

    @pytest.mark.asyncio
    async def test_execute_handler_deadline_exceeded_error(
        self, registry, sample_context, sample_command
    ):
        """Handler raises DeadlineExceeded - returns error response (not retryable)."""

        async def my_handler(ctx, payload):
            raise DeadlineExceeded("Processing took too long")

        registry.register("ProcessDataset", "1.0", my_handler)

        response = await registry.execute(sample_context, sample_command)

        # Response should contain error with retryable=False
        # (deadline already passed, retry would fail again)
        assert "error" in response.payload
        assert response.payload["error"]["code"] == "DeadlineExceeded"
        assert response.payload["error"]["message"] == "Processing took too long"
        assert response.payload["error"]["retryable"] is False

    @pytest.mark.asyncio
    async def test_execute_handler_uncaught_exception(
        self, registry, sample_context, sample_command
    ):
        """Handler raises uncaught exception - generic caller msg, retryable.

        The caller-visible message must NOT contain str(e) — the
        exception text often leaks internal field names that disclose
        handler state. The full detail still goes to logs; callers see
        the generic "An unexpected error occurred" fallback.
        """

        async def my_handler(ctx, payload):
            raise ValueError("Something went wrong")

        registry.register("ProcessDataset", "1.0", my_handler)

        response = await registry.execute(sample_context, sample_command)

        # Response should contain error with retryable=True
        assert "error" in response.payload
        assert response.payload["error"]["code"] == "UnexpectedError"
        # Generic message only — no leakage of the underlying exception.
        assert response.payload["error"]["message"] == "An unexpected error occurred"
        assert "Something went wrong" not in response.payload["error"]["message"]
        assert response.payload["error"]["retryable"] is True


class TestHandlerDeadlineChecking:
    """Test deadline checking before handler execution."""

    @pytest.mark.asyncio
    async def test_execute_deadline_not_exceeded(self, registry, mock_transport):
        """Execute with deadline in future succeeds."""

        async def my_handler(ctx, payload):
            return {"result": "ok"}

        registry.register("ProcessDataset", "1.0", my_handler)

        # Create command with deadline in future
        envelope = Envelope(
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
        )
        command = Command(envelope=envelope, payload={})
        ctx = ExecutionContext(transport=mock_transport, envelope=envelope, payload={})

        # Should execute successfully
        response = await registry.execute(ctx, command)
        assert response.payload == {"result": "ok"}

    @pytest.mark.asyncio
    async def test_execute_deadline_already_exceeded(self, registry, mock_transport):
        """Execute with deadline in past returns a terminal DeadlineExceeded response.

        Previously the pre-flight deadline check lived OUTSIDE the
        try block, so ``DeadlineExceeded`` escaped ``execute()`` and
        had to be caught by ``_handle_command``'s generic
        ``except Exception``. That log line ("Uncaught exception in
        _handle_command") was misleading — it suggested a bug rather
        than the documented terminal-error path. The check now lives
        INSIDE the try block, so the caller sees a normal terminal
        response with ``code="DeadlineExceeded"`` and ``retryable=False``.
        """

        async def my_handler(ctx, payload):
            return {"result": "ok"}

        registry.register("ProcessDataset", "1.0", my_handler)

        # Create command with deadline in past
        envelope = Envelope(
            schema_version="1.0",
            type="command",
            name="ProcessDataset",
            message_version="1.0",
            message_id="550e8400-e29b-41d4-a716-446655440000",
            correlation_id="660e8400-e29b-41d4-a716-446655440000",
            idempotency_key="idem-123",
            sent_at=datetime.now(UTC) - timedelta(seconds=60),
            deadline=datetime.now(UTC) - timedelta(seconds=10),
            trace_id="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            reply_to="resp.dev.orchestrator.v1",
        )
        command = Command(envelope=envelope, payload={})
        ctx = ExecutionContext(transport=mock_transport, envelope=envelope, payload={})

        # Should produce a terminal DeadlineExceeded response (not raise).
        response = await registry.execute(ctx, command)
        assert "error" in response.payload
        assert response.payload["error"]["code"] == "DeadlineExceeded"
        assert response.payload["error"]["retryable"] is False
        assert "already passed" in response.payload["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_no_deadline(self, registry, mock_transport):
        """Execute with no deadline succeeds."""

        async def my_handler(ctx, payload):
            return {"result": "ok"}

        registry.register("ProcessDataset", "1.0", my_handler)

        # Create command with no deadline
        envelope = Envelope(
            schema_version="1.0",
            type="command",
            name="ProcessDataset",
            message_version="1.0",
            message_id="550e8400-e29b-41d4-a716-446655440000",
            correlation_id="660e8400-e29b-41d4-a716-446655440000",
            idempotency_key="idem-123",
            sent_at=datetime.now(UTC),
            deadline=None,
            trace_id="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            reply_to="resp.dev.orchestrator.v1",
        )
        command = Command(envelope=envelope, payload={})
        ctx = ExecutionContext(transport=mock_transport, envelope=envelope, payload={})

        # Should execute successfully
        response = await registry.execute(ctx, command)
        assert response.payload == {"result": "ok"}


class TestHandlerRegistryEdgeCases:
    """Test edge cases and special scenarios."""

    @pytest.mark.asyncio
    async def test_repr(self, registry):
        """Registry has useful __repr__."""

        async def handler1(ctx, payload):
            return {}

        registry.register("ProcessDataset", "1.0", handler1)

        repr_str = repr(registry)
        assert "HandlerRegistry" in repr_str
        assert "1" in repr_str  # 1 handler registered
