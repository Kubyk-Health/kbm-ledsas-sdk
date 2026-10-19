"""Envelope rules: read-only message models, reserved reply exchanges, version fields."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pydantic
import pytest

import kbm_ledsas_sdk
from kbm_ledsas_sdk.models.envelope import Envelope
from kbm_ledsas_sdk.models.messages import Command
from kbm_ledsas_sdk.transport.mock import MockTransport


def _envelope(**over) -> Envelope:
    fields = dict(
        schema_version="1.0",
        type="command",
        name="Work",
        message_version="1.0",
        message_id="11111111-2222-4333-8444-555555555555",
        correlation_id="66666666-7777-4888-8999-aaaaaaaaaaaa",
        idempotency_key="idem-1",
        sent_at=datetime.now(UTC),
        trace_id="trace-1",
        reply_to="resp.dev.example_orchestrator.v1",
    )
    fields.update(over)
    return Envelope(**fields)


def _app_with_mock(max_retries: int = 3, generic_errors: bool = False):
    app = kbm_ledsas_sdk.ServiceApp("svc-1")
    app.transport = MockTransport()
    app.config = SimpleNamespace(
        handler_timeout=0, max_retries=max_retries, generic_errors=generic_errors, concurrency=1
    )
    app.registry._generic_errors = generic_errors
    return app


class TestReadOnlyMessages:
    def test_assignment_raises(self):
        from kbm_ledsas_sdk.models.messages import Response, Status

        envelope = _envelope()
        for target, field, value in (
            (envelope, "message_id", "99999999-2222-4333-8444-555555555555"),
            (envelope, "reply_to", "resp.other.v1"),
            (Command(envelope=envelope, payload={}), "payload", {}),
            (
                Response(envelope=envelope.model_copy(update={"type": "response"}), payload={}),
                "envelope",
                envelope,
            ),
            (
                Status(envelope=envelope.model_copy(update={"type": "status"}), payload={}),
                "payload",
                {},
            ),
        ):
            with pytest.raises(pydantic.ValidationError):
                setattr(target, field, value)
        assert envelope.model_copy(update={"reply_to": "resp.x.v1"}).reply_to == "resp.x.v1"

    async def test_a_mutating_handler_settles_the_original_delivery(self):
        app = _app_with_mock()
        command = Command(envelope=_envelope(), payload={})

        @app.handler("Work")
        async def work(ctx, req):
            ctx.envelope.message_id = "99999999-2222-4333-8444-555555555555"

        app.config.max_retries = 0  # settle on this attempt
        await app._handle_command(command, asyncio.Semaphore(1))
        assert app.transport.nacked_messages == [(command.envelope.message_id, False)]
        assert command.envelope.message_id == "11111111-2222-4333-8444-555555555555"
        [reply] = app.transport.sent_responses
        assert reply.envelope.message_id == command.envelope.message_id
        assert reply.envelope.correlation_id == command.envelope.correlation_id


class TestReservedReplyExchange:
    @pytest.mark.parametrize(
        "reply_to", ["amq.topic", "amq.fanout", "amq.direct", "amq.rabbitmq.reply-to"]
    )
    def test_refused(self, reply_to):
        with pytest.raises(pydantic.ValidationError, match="reserved broker exchange"):
            _envelope(reply_to=reply_to)

    @pytest.mark.parametrize("reply_to", ["resp.dev.example_orchestrator.v1", "amqx.reply", ""])
    def test_accepted(self, reply_to):
        assert _envelope(reply_to=reply_to).reply_to == reply_to


class TestEnvelopeVersions:
    @pytest.mark.parametrize("field", ["schema_version", "message_version"])
    @pytest.mark.parametrize("value", ["1" * 100_000 + ".0", "١.٠", "２.０", "12345.0", "1.0\n"])
    def test_refused(self, field, value):
        with pytest.raises(pydantic.ValidationError):
            _envelope(**{field: value})

    @pytest.mark.parametrize("value", ["1.0", "9999.9999", "0.1"])
    def test_accepted(self, value):
        assert _envelope(schema_version=value, message_version=value).message_version == value

    def test_handler_versions_use_the_same_shape(self):
        app = kbm_ledsas_sdk.ServiceApp("svc")
        with pytest.raises(ValueError, match="Invalid message version"):

            @app.handler("Work", "١.٠")
            async def bad(ctx, req):
                return {}

        @app.handler("Work", "9999.9999")
        async def good(ctx, req):
            return {}
