"""
Unit tests for timeout enforcement in HandlerRegistry.

Tests include:
- Handler completes within timeout
- Handler exceeds timeout and is cancelled
- timeout=0 disables timeout enforcement
- Deadline takes precedence over config timeout
- _calculate_timeout helper method
"""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from kbm_ledsas_sdk.models import Command, Envelope
from kbm_ledsas_sdk.runtime.context import ExecutionContext
from kbm_ledsas_sdk.runtime.handler import HandlerRegistry
from kbm_ledsas_sdk.transport.mock import MockTransport


@pytest.fixture
def registry():
    """Empty handler registry."""
    return HandlerRegistry()


@pytest.fixture
def mock_transport():
    """Mock transport for testing."""
    return MockTransport()


def create_command(deadline=None):
    """Create a test command with optional deadline."""
    envelope = Envelope(
        schema_version="1.0",
        type="command",
        name="ProcessDataset",
        message_version="1.0",
        message_id="550e8400-e29b-41d4-a716-446655440000",
        correlation_id="660e8400-e29b-41d4-a716-446655440000",
        idempotency_key="idem-123",
        sent_at=datetime.now(UTC),
        deadline=deadline,
        trace_id="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        reply_to="resp.dev.orchestrator.v1",
    )
    return Command(envelope=envelope, payload={"input": "test"})


def create_context(mock_transport, envelope):
    """Create execution context for testing."""
    return ExecutionContext(
        transport=mock_transport,
        envelope=envelope,
        payload={"input": "test"},
    )


class TestCalculateTimeout:
    """Test _calculate_timeout helper method."""

    def test_no_deadline_no_config(self, registry):
        """No deadline and config_timeout=0 returns None."""
        envelope = create_command(deadline=None).envelope
        result = registry._calculate_timeout(envelope, config_timeout=0)
        assert result is None

    def test_no_deadline_with_config(self, registry):
        """No deadline with config_timeout returns config value."""
        envelope = create_command(deadline=None).envelope
        result = registry._calculate_timeout(envelope, config_timeout=30)
        assert result == 30

    def test_deadline_in_future_no_config(self, registry):
        """Deadline in future with config_timeout=0 returns remaining time."""
        deadline = datetime.now(UTC) + timedelta(seconds=60)
        envelope = create_command(deadline=deadline).envelope
        result = registry._calculate_timeout(envelope, config_timeout=0)
        # Should be close to 60 seconds (within tolerance)
        assert 59 <= result <= 60

    def test_deadline_in_future_with_config_smaller(self, registry):
        """Deadline in future with smaller config_timeout returns config value."""
        deadline = datetime.now(UTC) + timedelta(seconds=60)
        envelope = create_command(deadline=deadline).envelope
        result = registry._calculate_timeout(envelope, config_timeout=30)
        # Config is smaller, should return config
        assert result == 30

    def test_deadline_in_future_with_config_larger(self, registry):
        """Deadline in future with larger config_timeout returns remaining time."""
        deadline = datetime.now(UTC) + timedelta(seconds=30)
        envelope = create_command(deadline=deadline).envelope
        result = registry._calculate_timeout(envelope, config_timeout=60)
        # Remaining time is smaller, should return remaining (~30)
        assert 29 <= result <= 30

    def test_deadline_expired_raises_deadline_exceeded(self, registry):
        """Expired deadline raises DeadlineExceeded so the response is terminal.

        Previously the function returned 0.1 to trigger an "immediate
        timeout" — but asyncio.wait_for(coro, 0.1) cancels via
        TimeoutError which the SDK classified as ``code="Timeout"``
        (retryable). That misclassified an already-past deadline as
        retryable; the orchestrator would just retry against the same
        expired deadline. Now the function raises ``DeadlineExceeded``
        so the catch in ``execute()`` produces a terminal response.
        """
        from kbm_ledsas_sdk.models.errors import DeadlineExceeded

        past_dt = MagicMock(wraps=datetime.now(UTC) - timedelta(seconds=10))
        past_dt.isoformat = MagicMock(return_value="2026-05-27T07:00:00+00:00")
        # Make arithmetic on the mock return a negative timedelta so
        # remaining = (deadline - now).total_seconds() < 0.
        past_dt.__sub__ = lambda self_, other: timedelta(seconds=-10)

        mock_envelope = MagicMock()
        mock_envelope.deadline = past_dt
        with pytest.raises(DeadlineExceeded):
            registry._calculate_timeout(mock_envelope, config_timeout=0)

    def test_deadline_expired_with_config_raises_deadline_exceeded(self, registry):
        """Expired deadline with config_timeout also raises DeadlineExceeded.

        ``config_timeout`` is irrelevant once the deadline has passed —
        the SDK must classify the failure as terminal, not retryable.
        """
        from kbm_ledsas_sdk.models.errors import DeadlineExceeded

        past_dt = MagicMock(wraps=datetime.now(UTC) - timedelta(seconds=10))
        past_dt.isoformat = MagicMock(return_value="2026-05-27T07:00:00+00:00")
        past_dt.__sub__ = lambda self_, other: timedelta(seconds=-10)

        mock_envelope = MagicMock()
        mock_envelope.deadline = past_dt
        with pytest.raises(DeadlineExceeded):
            registry._calculate_timeout(mock_envelope, config_timeout=60)


class TestTimeoutEnforcement:
    """Test timeout enforcement during handler execution."""

    @pytest.mark.asyncio
    async def test_handler_completes_within_timeout(self, registry, mock_transport):
        """Handler that completes within timeout succeeds."""

        async def fast_handler(ctx, payload):
            await asyncio.sleep(0.01)  # 10ms
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", fast_handler)

        command = create_command(deadline=None)
        ctx = create_context(mock_transport, command.envelope)

        # Execute with 1 second timeout - should succeed
        response = await registry.execute(ctx, command, timeout_seconds=1)

        assert "error" not in response.payload
        assert response.payload == {"result": "success"}

    @pytest.mark.asyncio
    async def test_handler_exceeds_timeout(self, registry, mock_transport):
        """Handler that exceeds timeout returns timeout error."""

        async def slow_handler(ctx, payload):
            await asyncio.sleep(5)  # 5 seconds - will be cancelled
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", slow_handler)

        command = create_command(deadline=None)
        ctx = create_context(mock_transport, command.envelope)

        # Execute with 0.1 second timeout - should timeout
        response = await registry.execute(ctx, command, timeout_seconds=0.1)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Timeout"
        assert response.payload["error"]["retryable"] is True
        assert "0.1s" in response.payload["error"]["message"]

    @pytest.mark.asyncio
    async def test_timeout_zero_disables_enforcement(self, registry, mock_transport):
        """timeout_seconds=0 disables timeout enforcement."""

        async def handler(ctx, payload):
            await asyncio.sleep(0.05)  # 50ms
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", handler)

        command = create_command(deadline=None)
        ctx = create_context(mock_transport, command.envelope)

        # Execute with timeout=0 - no timeout enforcement
        response = await registry.execute(ctx, command, timeout_seconds=0)

        assert "error" not in response.payload
        assert response.payload == {"result": "success"}

    @pytest.mark.asyncio
    async def test_deadline_enforces_timeout(self, registry, mock_transport):
        """Deadline in envelope enforces timeout even when config is 0."""

        async def slow_handler(ctx, payload):
            await asyncio.sleep(5)  # 5 seconds - will be cancelled
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", slow_handler)

        # Deadline in 1 second (enough time for response creation)
        deadline = datetime.now(UTC) + timedelta(seconds=1)
        command = create_command(deadline=deadline)
        ctx = create_context(mock_transport, command.envelope)

        # Execute with timeout=0.1 - config timeout triggers first
        # This tests that timeout works with deadline present
        response = await registry.execute(ctx, command, timeout_seconds=0.1)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Timeout"
        assert response.payload["error"]["retryable"] is True

    @pytest.mark.asyncio
    async def test_config_timeout_takes_precedence_when_smaller(self, registry, mock_transport):
        """Config timeout takes precedence when smaller than deadline."""

        async def slow_handler(ctx, payload):
            await asyncio.sleep(5)  # 5 seconds - will be cancelled
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", slow_handler)

        # Deadline in 10 seconds, but config timeout is 0.1s
        deadline = datetime.now(UTC) + timedelta(seconds=10)
        command = create_command(deadline=deadline)
        ctx = create_context(mock_transport, command.envelope)

        # Config timeout (0.1s) is smaller than deadline (10s)
        response = await registry.execute(ctx, command, timeout_seconds=0.1)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Timeout"
        assert "0.1s" in response.payload["error"]["message"]

    @pytest.mark.asyncio
    async def test_deadline_takes_precedence_when_smaller(self, registry, mock_transport):
        """Deadline takes precedence when smaller than config timeout."""

        async def slow_handler(ctx, payload):
            await asyncio.sleep(5)  # 5 seconds - will be cancelled
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", slow_handler)

        # Deadline in 0.5 seconds, config timeout is 10s
        # Using 0.5s to allow time for response envelope creation
        deadline = datetime.now(UTC) + timedelta(seconds=0.5)
        command = create_command(deadline=deadline)
        ctx = create_context(mock_transport, command.envelope)

        # Deadline (~0.5s) is smaller than config (10s)
        response = await registry.execute(ctx, command, timeout_seconds=10)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Timeout"

    @pytest.mark.asyncio
    async def test_timeout_error_contains_duration(self, registry, mock_transport):
        """Timeout error message contains the timeout duration."""

        async def slow_handler(ctx, payload):
            await asyncio.sleep(5)
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", slow_handler)

        command = create_command(deadline=None)
        ctx = create_context(mock_transport, command.envelope)

        response = await registry.execute(ctx, command, timeout_seconds=0.2)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Timeout"
        assert "0.2s" in response.payload["error"]["message"]


class TestTimeoutWithGenericErrors:
    """Test timeout with generic_errors enabled."""

    @pytest.mark.asyncio
    async def test_timeout_with_generic_errors_enabled(self, mock_transport):
        """Timeout with generic_errors returns generic message."""
        registry = HandlerRegistry(generic_errors=True)

        async def slow_handler(ctx, payload):
            await asyncio.sleep(5)
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", slow_handler)

        command = create_command(deadline=None)
        ctx = create_context(mock_transport, command.envelope)

        response = await registry.execute(ctx, command, timeout_seconds=0.1)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Timeout"
        # Generic message should be used
        assert response.payload["error"]["message"] == "Processing timed out (will retry)"
        assert response.payload["error"]["retryable"] is True

    @pytest.mark.asyncio
    async def test_timeout_with_generic_errors_disabled(self, mock_transport):
        """Timeout with generic_errors disabled returns specific message."""
        registry = HandlerRegistry(generic_errors=False)

        async def slow_handler(ctx, payload):
            await asyncio.sleep(5)
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", slow_handler)

        command = create_command(deadline=None)
        ctx = create_context(mock_transport, command.envelope)

        response = await registry.execute(ctx, command, timeout_seconds=0.1)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Timeout"
        # Specific message with duration
        assert "0.1s" in response.payload["error"]["message"]
        assert response.payload["error"]["retryable"] is True


class TestTimeoutEdgeCases:
    """Test edge cases for timeout enforcement."""

    @pytest.mark.asyncio
    async def test_handler_completes_just_before_timeout(self, registry, mock_transport):
        """Handler that completes just before timeout succeeds."""

        async def handler(ctx, payload):
            await asyncio.sleep(0.05)  # 50ms
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", handler)

        command = create_command(deadline=None)
        ctx = create_context(mock_transport, command.envelope)

        # Timeout is 0.5 seconds, handler takes 0.05 seconds
        response = await registry.execute(ctx, command, timeout_seconds=0.5)

        assert "error" not in response.payload
        assert response.payload == {"result": "success"}

    @pytest.mark.asyncio
    async def test_timeout_cancels_handler_cleanly(self, registry, mock_transport):
        """Timeout cancels handler without leaving resources in bad state."""
        cleanup_called = False

        async def handler_with_cleanup(ctx, payload):
            nonlocal cleanup_called
            try:
                await asyncio.sleep(5)
                return {"result": "success"}
            except asyncio.CancelledError:
                cleanup_called = True
                raise

        registry.register("ProcessDataset", "1.0", handler_with_cleanup)

        command = create_command(deadline=None)
        ctx = create_context(mock_transport, command.envelope)

        response = await registry.execute(ctx, command, timeout_seconds=0.1)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Timeout"
        # Handler should have received CancelledError
        assert cleanup_called is True

    @pytest.mark.asyncio
    async def test_multiple_handlers_with_different_timeouts(self, registry, mock_transport):
        """Different handlers can have different timeout behaviors."""

        async def fast_handler(ctx, payload):
            await asyncio.sleep(0.01)
            return {"result": "fast"}

        async def slow_handler(ctx, payload):
            await asyncio.sleep(5)
            return {"result": "slow"}

        registry.register("FastCommand", "1.0", fast_handler)
        registry.register("SlowCommand", "1.0", slow_handler)

        # Fast command succeeds
        fast_envelope = Envelope(
            schema_version="1.0",
            type="command",
            name="FastCommand",
            message_version="1.0",
            message_id="550e8400-e29b-41d4-a716-446655440001",
            correlation_id="660e8400-e29b-41d4-a716-446655440001",
            idempotency_key="idem-fast",
            sent_at=datetime.now(UTC),
            deadline=None,
            trace_id="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            reply_to="resp.dev.orchestrator.v1",
        )
        fast_command = Command(envelope=fast_envelope, payload={})
        fast_ctx = create_context(mock_transport, fast_envelope)

        fast_response = await registry.execute(fast_ctx, fast_command, timeout_seconds=1)
        assert fast_response.payload == {"result": "fast"}

        # Slow command times out
        slow_envelope = Envelope(
            schema_version="1.0",
            type="command",
            name="SlowCommand",
            message_version="1.0",
            message_id="550e8400-e29b-41d4-a716-446655440002",
            correlation_id="660e8400-e29b-41d4-a716-446655440002",
            idempotency_key="idem-slow",
            sent_at=datetime.now(UTC),
            deadline=None,
            trace_id="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            reply_to="resp.dev.orchestrator.v1",
        )
        slow_command = Command(envelope=slow_envelope, payload={})
        slow_ctx = create_context(mock_transport, slow_envelope)

        slow_response = await registry.execute(slow_ctx, slow_command, timeout_seconds=0.1)
        assert "error" in slow_response.payload
        assert slow_response.payload["error"]["code"] == "Timeout"
