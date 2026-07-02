"""
Unit tests for retry mechanism and exponential backoff.

Tests include:
- _calculate_backoff method
- Retry logic in _handle_command
- DLQ after max retries
- Exponential backoff with jitter
"""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from kbm_ledsas_sdk.app import ServiceApp
from kbm_ledsas_sdk.models import Command, Envelope
from kbm_ledsas_sdk.models.errors import Permanent, Retryable
from kbm_ledsas_sdk.transport.mock import MockTransport


@pytest.fixture
def app():
    """ServiceApp instance for testing."""
    return ServiceApp("test_service")


@pytest.fixture
def sample_envelope():
    """Sample command envelope."""
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
    """Sample command."""
    return Command(
        envelope=sample_envelope,
        payload={"input_uri": "azblob://dev/input.json"},
    )


def create_mock_config(
    handler_timeout=1800,
    max_retries=3,
    generic_errors=False,
    concurrency=10,
):
    """Create a mock config object."""
    config = Mock()
    config.handler_timeout = handler_timeout
    config.max_retries = max_retries
    config.generic_errors = generic_errors
    config.concurrency = concurrency
    return config


class TestCalculateBackoff:
    """Test _calculate_backoff method."""

    def test_backoff_retry_0(self, app):
        """First retry (retry_count=0) has base delay of ~1-2 seconds."""
        # Retry 0: base_delay * 2^0 + jitter = 1 + [0,1) = [1, 2)
        delay = app._calculate_backoff(0)
        assert 1 <= delay < 2

    def test_backoff_retry_1(self, app):
        """Second retry (retry_count=1) has delay of ~2-3 seconds."""
        # Retry 1: base_delay * 2^1 + jitter = 2 + [0,1) = [2, 3)
        delay = app._calculate_backoff(1)
        assert 2 <= delay < 3

    def test_backoff_retry_2(self, app):
        """Third retry (retry_count=2) has delay of ~4-5 seconds."""
        # Retry 2: base_delay * 2^2 + jitter = 4 + [0,1) = [4, 5)
        delay = app._calculate_backoff(2)
        assert 4 <= delay < 5

    def test_backoff_retry_3(self, app):
        """Fourth retry (retry_count=3) has delay of ~8-9 seconds."""
        # Retry 3: base_delay * 2^3 + jitter = 8 + [0,1) = [8, 9)
        delay = app._calculate_backoff(3)
        assert 8 <= delay < 9

    def test_backoff_max_delay(self, app):
        """Backoff is capped at max_delay (60 seconds)."""
        # Retry 10: base_delay * 2^10 = 1024, but capped at 60
        delay = app._calculate_backoff(10)
        assert delay <= 61  # 60 + up to 1 second jitter

    def test_backoff_includes_jitter(self, app):
        """Backoff includes random jitter (varies between calls)."""
        delays = [app._calculate_backoff(0) for _ in range(10)]
        # Not all delays should be exactly the same (jitter)
        unique_delays = set(delays)
        assert len(unique_delays) > 1

    def test_backoff_is_positive(self, app):
        """Backoff is always positive."""
        for retry_count in range(20):
            delay = app._calculate_backoff(retry_count)
            assert delay > 0


class TestRetryLogic:
    """Test retry logic in _handle_command."""

    @pytest.mark.asyncio
    async def test_retryable_error_requeues_with_backoff(self, app, sample_command):
        """Retryable error applies backoff before requeue."""

        # Register handler that raises Retryable
        @app.handler("ProcessDataset", "1.0")
        async def my_handler(ctx, payload):
            raise Retryable("Network timeout")

        # Mock transport
        mock_transport = MockTransport()
        app.transport = mock_transport
        app.config = create_mock_config(max_retries=3)

        # Create semaphore
        semaphore = asyncio.Semaphore(1)

        # Handle command - should apply backoff before NACK
        start_time = asyncio.get_event_loop().time()
        await app._handle_command(sample_command, semaphore)
        elapsed = asyncio.get_event_loop().time() - start_time

        # Should have waited at least 1 second (base backoff)
        assert elapsed >= 1.0

        # Message should be NACKed with requeue
        assert len(mock_transport.nacked_messages) == 1
        message_id, requeue = mock_transport.nacked_messages[0]
        assert requeue is True

    @pytest.mark.asyncio
    async def test_max_retries_sends_to_dlq(self, app, sample_command):
        """After max retries, message is sent to DLQ (requeue=False)."""

        # Register handler that raises Retryable
        @app.handler("ProcessDataset", "1.0")
        async def my_handler(ctx, payload):
            raise Retryable("Network timeout")

        # Mock transport with retry count at max
        mock_transport = MockTransport()
        mock_transport.get_retry_count = Mock(return_value=3)  # At max retries
        app.transport = mock_transport
        app.config = create_mock_config(max_retries=3)

        # Create semaphore
        semaphore = asyncio.Semaphore(1)

        # Handle command
        await app._handle_command(sample_command, semaphore)

        # Message should be NACKed WITHOUT requeue (DLQ)
        assert len(mock_transport.nacked_messages) == 1
        message_id, requeue = mock_transport.nacked_messages[0]
        assert requeue is False

    @pytest.mark.asyncio
    async def test_retry_count_below_max_requeues(self, app, sample_command):
        """Retry count below max still requeues."""

        # Register handler that raises Retryable
        @app.handler("ProcessDataset", "1.0")
        async def my_handler(ctx, payload):
            raise Retryable("Network timeout")

        # Mock transport with retry count below max
        mock_transport = MockTransport()
        mock_transport.get_retry_count = Mock(return_value=1)  # Below max
        app.transport = mock_transport
        app.config = create_mock_config(max_retries=3)

        # Create semaphore
        semaphore = asyncio.Semaphore(1)

        # Handle command
        await app._handle_command(sample_command, semaphore)

        # Message should be NACKed WITH requeue
        assert len(mock_transport.nacked_messages) == 1
        message_id, requeue = mock_transport.nacked_messages[0]
        assert requeue is True

    @pytest.mark.asyncio
    async def test_permanent_error_goes_to_dlq_immediately(self, app, sample_command):
        """Permanent error goes to DLQ without retrying."""

        # Register handler that raises Permanent
        @app.handler("ProcessDataset", "1.0")
        async def my_handler(ctx, payload):
            raise Permanent("Invalid input format")

        # Mock transport
        mock_transport = MockTransport()
        app.transport = mock_transport
        app.config = create_mock_config(max_retries=3)

        # Create semaphore
        semaphore = asyncio.Semaphore(1)

        # Handle command
        await app._handle_command(sample_command, semaphore)

        # Message should be NACKed WITHOUT requeue (DLQ) - no retry
        assert len(mock_transport.nacked_messages) == 1
        message_id, requeue = mock_transport.nacked_messages[0]
        assert requeue is False

    @pytest.mark.asyncio
    async def test_success_does_not_trigger_retry(self, app, sample_command):
        """Successful handler execution does not trigger retry logic."""

        # Register handler that succeeds
        @app.handler("ProcessDataset", "1.0")
        async def my_handler(ctx, payload):
            return {"result": "success"}

        # Mock transport
        mock_transport = MockTransport()
        app.transport = mock_transport
        app.config = create_mock_config(max_retries=3)

        # Create semaphore
        semaphore = asyncio.Semaphore(1)

        # Handle command
        await app._handle_command(sample_command, semaphore)

        # Message should be ACKed (not NACKed)
        assert len(mock_transport.acked_messages) == 1
        assert len(mock_transport.nacked_messages) == 0


class TestTimeoutIntegration:
    """Test timeout integration with retry logic."""

    @pytest.mark.asyncio
    async def test_timeout_triggers_retry(self, app, sample_command):
        """Handler timeout triggers retry logic."""

        # Register handler that takes too long
        @app.handler("ProcessDataset", "1.0")
        async def slow_handler(ctx, payload):
            await asyncio.sleep(5)  # Takes 5 seconds
            return {"result": "success"}

        # Mock transport
        mock_transport = MockTransport()
        app.transport = mock_transport
        app.config = create_mock_config(handler_timeout=1, max_retries=3)  # 1 second timeout

        # Create semaphore
        semaphore = asyncio.Semaphore(1)

        # Handle command - should timeout
        await app._handle_command(sample_command, semaphore)

        # Error response should indicate timeout
        assert len(mock_transport.sent_responses) == 1
        response = mock_transport.sent_responses[0]
        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Timeout"
        assert response.payload["error"]["retryable"] is True

        # Should be NACKed with requeue (retryable)
        assert len(mock_transport.nacked_messages) == 1
        message_id, requeue = mock_transport.nacked_messages[0]
        assert requeue is True


class TestGenericErrorsIntegration:
    """Test generic_errors config integration."""

    @pytest.mark.asyncio
    async def test_generic_errors_config_applied(self, app, sample_command):
        """generic_errors config is applied to registry."""

        # Register handler that raises error
        @app.handler("ProcessDataset", "1.0")
        async def my_handler(ctx, payload):
            raise Retryable("Detailed internal error message")

        # Mock transport
        mock_transport = MockTransport()
        app.transport = mock_transport
        app.config = create_mock_config(generic_errors=True, max_retries=3)

        # Apply config to registry (normally done in _run_async)
        app.registry._generic_errors = app.config.generic_errors

        # Create semaphore
        semaphore = asyncio.Semaphore(1)

        # Handle command
        await app._handle_command(sample_command, semaphore)

        # Error response should have generic message
        assert len(mock_transport.sent_responses) == 1
        response = mock_transport.sent_responses[0]
        assert "error" in response.payload
        # Generic message instead of specific
        assert response.payload["error"]["message"] == "Processing failed temporarily"


class TestRetryWithNoTransportSupport:
    """Test retry logic when transport doesn't support retry counting."""

    @pytest.mark.asyncio
    async def test_no_get_retry_count_defaults_to_zero(self, app, sample_command):
        """If transport has no get_retry_count, retry_count defaults to 0."""

        # Register handler that raises Retryable
        @app.handler("ProcessDataset", "1.0")
        async def my_handler(ctx, payload):
            raise Retryable("Network timeout")

        # Mock transport WITHOUT get_retry_count method
        mock_transport = MockTransport()
        # MockTransport doesn't have get_retry_count by default
        assert not hasattr(mock_transport, "get_retry_count")

        app.transport = mock_transport
        app.config = create_mock_config(max_retries=3)

        # Create semaphore
        semaphore = asyncio.Semaphore(1)

        # Handle command
        await app._handle_command(sample_command, semaphore)

        # Should still requeue (retry_count=0 < max_retries=3)
        assert len(mock_transport.nacked_messages) == 1
        message_id, requeue = mock_transport.nacked_messages[0]
        assert requeue is True


class TestConfigDefaults:
    """Test default config values for retry."""

    @pytest.mark.asyncio
    async def test_no_config_uses_defaults(self, app, sample_command):
        """When config is None, defaults are used."""

        # Register handler that raises Retryable
        @app.handler("ProcessDataset", "1.0")
        async def my_handler(ctx, payload):
            raise Retryable("Network timeout")

        # Mock transport
        mock_transport = MockTransport()
        app.transport = mock_transport
        app.config = None  # No config

        # Create semaphore
        semaphore = asyncio.Semaphore(1)

        # Handle command - should use default max_retries=3
        await app._handle_command(sample_command, semaphore)

        # Should still work with defaults
        assert len(mock_transport.nacked_messages) == 1
