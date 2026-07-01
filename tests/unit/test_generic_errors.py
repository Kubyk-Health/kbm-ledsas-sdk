"""
Unit tests for generic error messages feature.

Tests include:
- Generic error messages when enabled
- Specific error messages when disabled (default)
- Handler override with user_message
- All error codes have generic mappings
"""

from datetime import UTC, datetime

import pytest

from kbm_ledsas_sdk.models.envelope import Envelope
from kbm_ledsas_sdk.models.errors import DeadlineExceeded, Permanent, Retryable
from kbm_ledsas_sdk.models.messages import Command
from kbm_ledsas_sdk.runtime.context import ExecutionContext
from kbm_ledsas_sdk.runtime.handler import (
    GENERIC_ERROR_MESSAGES,
    HandlerRegistry,
)
from kbm_ledsas_sdk.transport.mock import MockTransport


def create_test_envelope(name: str = "TestCommand", version: str = "1.0") -> Envelope:
    """Create a test envelope."""
    return Envelope(
        schema_version="1.0",
        type="command",
        name=name,
        message_version=version,
        message_id="550e8400-e29b-41d4-a716-446655440000",
        correlation_id="660e8400-e29b-41d4-a716-446655440000",
        idempotency_key="idem-123",
        sent_at=datetime.now(UTC),
        trace_id="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    )


def create_test_command(name: str = "TestCommand", version: str = "1.0") -> Command:
    """Create a test command."""
    envelope = create_test_envelope(name, version)
    return Command(
        envelope=envelope,
        payload={"test": "data"},
    )


def create_test_context(envelope: Envelope = None) -> ExecutionContext:
    """Create a test execution context."""
    if envelope is None:
        envelope = create_test_envelope()
    transport = MockTransport()
    return ExecutionContext(
        transport=transport,
        envelope=envelope,
        payload={"test": "data"},
    )


class TestGenericErrorsDisabled:
    """Test error messages when generic_errors=False (default)."""

    @pytest.mark.asyncio
    async def test_retryable_error_shows_specific_message(self):
        """Retryable error returns specific message when generic_errors=False."""
        registry = HandlerRegistry(generic_errors=False)

        async def failing_handler(ctx, payload):
            raise Retryable("Database connection timeout after 30s")

        registry.register("TestCommand", "1.0", failing_handler)

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Retryable"
        assert response.payload["error"]["message"] == "Database connection timeout after 30s"

    @pytest.mark.asyncio
    async def test_permanent_error_shows_specific_message(self):
        """Permanent error returns specific message when generic_errors=False."""
        registry = HandlerRegistry(generic_errors=False)

        async def failing_handler(ctx, payload):
            raise Permanent("Invalid field 'foo': expected int, got str")

        registry.register("TestCommand", "1.0", failing_handler)

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Permanent"
        assert response.payload["error"]["message"] == "Invalid field 'foo': expected int, got str"

    @pytest.mark.asyncio
    async def test_handler_not_found_shows_specific_message(self):
        """HandlerNotFound returns specific message when generic_errors=False."""
        registry = HandlerRegistry(generic_errors=False)
        # Don't register any handler

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "HandlerNotFound"
        assert "No handler registered" in response.payload["error"]["message"]
        assert "TestCommand" in response.payload["error"]["message"]


class TestGenericErrorsEnabled:
    """Test error messages when generic_errors=True."""

    @pytest.mark.asyncio
    async def test_retryable_error_shows_generic_message(self):
        """Retryable error returns generic message when generic_errors=True."""
        registry = HandlerRegistry(generic_errors=True)

        async def failing_handler(ctx, payload):
            raise Retryable("Internal: connection pool exhausted, 47 active connections")

        registry.register("TestCommand", "1.0", failing_handler)

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Retryable"
        assert response.payload["error"]["message"] == "Processing failed temporarily"
        # Internal details should NOT be in the message
        assert "connection pool" not in response.payload["error"]["message"]

    @pytest.mark.asyncio
    async def test_permanent_error_shows_generic_message(self):
        """Permanent error returns generic message when generic_errors=True."""
        registry = HandlerRegistry(generic_errors=True)

        async def failing_handler(ctx, payload):
            raise Permanent("Field validation failed: 'patient_id' is required")

        registry.register("TestCommand", "1.0", failing_handler)

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Permanent"
        assert response.payload["error"]["message"] == "Processing failed"
        # Internal details should NOT be in the message
        assert "patient_id" not in response.payload["error"]["message"]

    @pytest.mark.asyncio
    async def test_deadline_exceeded_shows_generic_message(self):
        """DeadlineExceeded error returns generic message when generic_errors=True."""
        registry = HandlerRegistry(generic_errors=True)

        async def failing_handler(ctx, payload):
            raise DeadlineExceeded("Handler took 45s but deadline was 30s")

        registry.register("TestCommand", "1.0", failing_handler)

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "DeadlineExceeded"
        # DeadlineExceeded and Timeout now have distinct generic strings —
        # they have distinct retry semantics (Timeout is retryable;
        # DeadlineExceeded is terminal) and the caller-facing strings
        # now reflect that.
        assert response.payload["error"]["message"] == "Request arrived too close to deadline"

    @pytest.mark.asyncio
    async def test_handler_not_found_shows_generic_message(self):
        """HandlerNotFound returns generic message when generic_errors=True."""
        registry = HandlerRegistry(generic_errors=True)
        # Don't register any handler

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "HandlerNotFound"
        assert response.payload["error"]["message"] == "Handler not available"

    @pytest.mark.asyncio
    async def test_unexpected_error_shows_generic_message(self):
        """Unexpected error returns generic message when generic_errors=True."""
        registry = HandlerRegistry(generic_errors=True)

        async def failing_handler(ctx, payload):
            raise ValueError("some internal detail about ValueError")

        registry.register("TestCommand", "1.0", failing_handler)

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "UnexpectedError"
        assert response.payload["error"]["message"] == "An unexpected error occurred"
        # Internal details should NOT be in the message
        assert "ValueError" not in response.payload["error"]["message"]


class TestUserMessageOverride:
    """Test handler override with user_message parameter."""

    @pytest.mark.asyncio
    async def test_retryable_user_message_overrides_generic(self):
        """Retryable with user_message uses user_message when generic_errors=True."""
        registry = HandlerRegistry(generic_errors=True)

        async def failing_handler(ctx, payload):
            raise Retryable(
                "Internal: rate limit exceeded, quota=100, used=150",
                user_message="Service is busy, please try again in 1 minute",
            )

        registry.register("TestCommand", "1.0", failing_handler)

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Retryable"
        # User message should override generic
        assert (
            response.payload["error"]["message"] == "Service is busy, please try again in 1 minute"
        )

    @pytest.mark.asyncio
    async def test_permanent_user_message_overrides_generic(self):
        """Permanent with user_message uses user_message when generic_errors=True."""
        registry = HandlerRegistry(generic_errors=True)

        async def failing_handler(ctx, payload):
            raise Permanent(
                "Field 'age' must be positive, got -5",
                user_message="Invalid input: age must be a positive number",
            )

        registry.register("TestCommand", "1.0", failing_handler)

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert "error" in response.payload
        assert response.payload["error"]["code"] == "Permanent"
        # User message should override generic
        assert (
            response.payload["error"]["message"] == "Invalid input: age must be a positive number"
        )

    @pytest.mark.asyncio
    async def test_user_message_wins_even_when_generic_errors_disabled(self):
        """v0.1.7+: user_message always takes precedence, regardless of generic_errors."""
        registry = HandlerRegistry(generic_errors=False)

        async def failing_handler(ctx, payload):
            raise Retryable(
                "Internal: connection pool exhausted",
                user_message="Please try again later",
            )

        registry.register("TestCommand", "1.0", failing_handler)

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert "error" in response.payload
        assert response.payload["error"]["message"] == "Please try again later"

    @pytest.mark.asyncio
    async def test_internal_message_used_when_no_user_message_and_generic_disabled(
        self,
    ):
        """Without user_message and generic_errors=False, the caller sees the internal message."""
        registry = HandlerRegistry(generic_errors=False)

        async def failing_handler(ctx, payload):
            raise Retryable("Internal: connection pool exhausted")

        registry.register("TestCommand", "1.0", failing_handler)

        command = create_test_command()
        ctx = create_test_context(command.envelope)
        response = await registry.execute(ctx, command)

        assert response.payload["error"]["message"] == "Internal: connection pool exhausted"


class TestGenericErrorMessages:
    """Test the GENERIC_ERROR_MESSAGES mapping."""

    def test_all_error_codes_have_mappings(self):
        """All expected error codes have generic message mappings."""
        expected_codes = [
            "Retryable",
            "Permanent",
            "DeadlineExceeded",
            "Timeout",
            "UnexpectedError",
            "HandlerNotFound",
        ]

        for code in expected_codes:
            assert code in GENERIC_ERROR_MESSAGES, f"Missing mapping for {code}"
            assert isinstance(GENERIC_ERROR_MESSAGES[code], str)
            assert len(GENERIC_ERROR_MESSAGES[code]) > 0

    def test_generic_messages_are_user_friendly(self):
        """Generic messages don't contain technical jargon."""
        technical_terms = [
            "exception",
            "stack",
            "traceback",
            "internal",
            "database",
            "connection",
            "socket",
            "timeout",  # "timed out" is OK, but not "timeout" as a noun
        ]

        for code, message in GENERIC_ERROR_MESSAGES.items():
            message_lower = message.lower()
            for term in technical_terms:
                # Allow "timed out" but not "timeout"
                if term == "timeout" and "timed out" in message_lower:
                    continue
                assert (
                    term not in message_lower
                ), f"Generic message for {code} contains technical term '{term}': {message}"


class TestErrorClassesUserMessage:
    """Test error classes support user_message parameter."""

    def test_retryable_with_user_message(self):
        """Retryable error stores user_message."""
        error = Retryable("Internal error", user_message="Please try again")

        assert str(error) == "Internal error"
        assert error.user_message == "Please try again"

    def test_retryable_without_user_message(self):
        """Retryable error without user_message has None."""
        error = Retryable("Internal error")

        assert str(error) == "Internal error"
        assert error.user_message is None

    def test_permanent_with_user_message(self):
        """Permanent error stores user_message."""
        error = Permanent("Validation failed", user_message="Invalid input")

        assert str(error) == "Validation failed"
        assert error.user_message == "Invalid input"

    def test_permanent_without_user_message(self):
        """Permanent error without user_message has None."""
        error = Permanent("Validation failed")

        assert str(error) == "Validation failed"
        assert error.user_message is None


class TestGetErrorMessage:
    """Test _get_error_message helper method."""

    def test_generic_errors_false_returns_specific(self):
        """When generic_errors=False, returns specific message."""
        registry = HandlerRegistry(generic_errors=False)

        result = registry._get_error_message("Retryable", "Specific error details")

        assert result == "Specific error details"

    def test_generic_errors_true_returns_generic(self):
        """When generic_errors=True, returns generic message."""
        registry = HandlerRegistry(generic_errors=True)

        result = registry._get_error_message("Retryable", "Specific error details")

        assert result == "Processing failed temporarily"

    def test_generic_errors_true_with_user_message(self):
        """When generic_errors=True and user_message provided, returns user_message."""
        registry = HandlerRegistry(generic_errors=True)

        result = registry._get_error_message(
            "Retryable", "Specific error details", user_message="Custom user message"
        )

        assert result == "Custom user message"

    def test_generic_errors_true_unknown_code_fallback(self):
        """When generic_errors=True and unknown code, returns fallback."""
        registry = HandlerRegistry(generic_errors=True)

        result = registry._get_error_message("UnknownCode", "Specific error details")

        assert result == "An error occurred"
