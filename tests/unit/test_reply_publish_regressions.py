"""Regression tests for direct-mode reply-publish parity.

Replies (and statuses) now publish with ``mandatory=True`` so an
exists-but-unbound reply exchange no longer swallows the response: the
broker returns the message (Basic.Return) and aio-pika raises
``DeliveryError``, which drives the EXISTING failure chain
(``send_response()`` -> False -> command NACK to DLQ).

Covers:
- ``_classify_publish_error`` recognizes both ``DeliveryError`` shapes as
  expected operator conditions (zero-traceback discipline), with DISTINCT
  reasons: ``PublishError`` = unroutable return (Basic.Return), bare
  ``DeliveryError`` = broker refused to confirm (Basic.Nack). Only the
  unroutable case bumps ``reply_unroutable_failures``.
- ``AMQPPublisher`` publishes both responses and statuses with
  ``mandatory=True`` and logs DeliveryError at DEBUG only (no ERROR, no
  traceback dump at the publisher layer).
- ``DirectTransport.send_response`` on DeliveryError: returns False,
  bumps BOTH ``reply_publish_failures`` and the new
  ``reply_unroutable_failures`` sub-counter, logs one operator ERROR
  with the bound-queue hint, and never dumps the returned message body.
- ``DirectTransport.send_status`` on DeliveryError: swallowed (statuses
  stay best-effort), WARNING logged, ``status_publish_failures`` bumped,
  reply counters untouched.
- Missing-exchange path unchanged: no unroutable bump, original hint.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from aio_pika.exceptions import DeliveryError, PublishError
from aiormq.abc import DeliveredMessage
from pamqp.commands import Basic

from kbm_ledsas_sdk.amqp.publisher import (
    AMQPPublisher,
    _classify_publish_error,
    _is_unroutable_return,
)
from kbm_ledsas_sdk.models.envelope import Envelope
from kbm_ledsas_sdk.models.messages import Response, Status
from kbm_ledsas_sdk.transport.direct import DirectTransport

AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Fak3K3yForT3sts999==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)

# Sentinel that must NEVER appear in any log output: it stands in for
# the returned message's body bytes, which repr(DeliveryError) includes.
BODY_SENTINEL = "SENTINEL-BODY-BYTES-DO-NOT-LOG"


class _FakeReturnedMessage:
    """Stands in for aiormq's DeliveredMessage in the raised error.

    Precision note: the REAL unroutable-return exception is
    ``PublishError``, whose ``__init__`` replaces ``args`` with the short
    ``(reply_text, routing_key)`` strings — its ``str()`` never contains
    the body. A *bare* ``DeliveryError``, however, passes the message
    object into ``args``, so formatting it CAN dump the body. The SDK
    defends both cases with a static description; these helpers exercise
    the worst case.
    """

    def __repr__(self) -> str:  # pragma: no cover - repr only
        return f"DeliveredMessage(body=b'{BODY_SENTINEL}')"


def _delivery_error() -> DeliveryError:
    """A bare DeliveryError: broker refused to confirm (Basic.Nack)."""
    return DeliveryError(_FakeReturnedMessage(), None)


def _publish_error() -> PublishError:
    """The real unroutable-return shape: PublishError wrapping a
    DeliveredMessage whose delivery frame is Basic.Return."""
    returned = DeliveredMessage(
        delivery=Basic.Return(
            reply_code=312,
            reply_text="NO_ROUTE",
            exchange="test.reply.exchange.v1",
            routing_key="response",
        ),
        header=None,
        body=BODY_SENTINEL.encode(),
        channel=None,
    )
    return PublishError(returned, None)


def _envelope(type_: str, reply_to: str = "test.reply.exchange.v1") -> Envelope:
    return Envelope(
        schema_version="1.0",
        type=type_,
        name="TestCommand",
        message_version="1.0",
        message_id="550e8400-e29b-41d4-a716-446655440046",
        correlation_id="660e8400-e29b-41d4-a716-446655440046",
        idempotency_key="idem-reply-pub",
        sent_at=datetime.now(UTC),
        trace_id="00-test-trace-id",
        reply_to=reply_to,
    )


def _response() -> Response:
    return Response(envelope=_envelope("response"), payload={"result": "ok"})


def _status() -> Status:
    return Status(envelope=_envelope("status"), payload={"state": "running"})


def _transport() -> DirectTransport:
    """A DirectTransport that was never start()ed; publisher injected."""
    return DirectTransport(
        rabbitmq_url="amqp://guest:guest@127.0.0.1:5672/",
        blob_conn_string=AZURITE_CONN,
        service_name="reply-pub-svc",
    )


def _mock_publisher(side_effect: BaseException | None = None) -> MagicMock:
    pub = MagicMock()
    pub.publish_response = AsyncMock(side_effect=side_effect)
    pub.publish_status = AsyncMock(side_effect=side_effect)
    return pub


def _mock_connection() -> tuple[MagicMock, MagicMock]:
    """(connection, exchange) pair for driving AMQPPublisher directly."""
    exchange = MagicMock()
    exchange.publish = AsyncMock()
    channel = MagicMock()
    channel.get_exchange = AsyncMock(return_value=exchange)
    channel.is_closed = False
    channel.close = AsyncMock()
    connection = MagicMock()
    connection.channel = AsyncMock(return_value=channel)
    return connection, exchange


# ---------------------------------------------------------------------------
# _classify_publish_error
# ---------------------------------------------------------------------------


class TestClassifyDeliveryError:
    def test_unroutable_return_is_expected(self) -> None:
        is_expected, reason = _classify_publish_error(_publish_error())
        assert is_expected is True
        assert reason == "reply_to exchange has no bound queue (message unroutable)"

    def test_bare_delivery_error_is_expected_with_nack_reason(self) -> None:
        """Basic.Nack (broker refused to confirm) is a DIFFERENT operator
        condition from unroutable — same noise treatment, distinct reason
        so on-call isn't misdirected to check bindings."""
        is_expected, reason = _classify_publish_error(_delivery_error())
        assert is_expected is True
        assert reason == "broker did not confirm the publish (Basic.Nack)"

    def test_unroutable_predicate_distinguishes_the_two(self) -> None:
        assert _is_unroutable_return(_publish_error()) is True
        assert _is_unroutable_return(_delivery_error()) is False

    def test_existing_classifications_unchanged(self) -> None:
        class ChannelNotFoundEntity(Exception):
            pass

        assert _classify_publish_error(ChannelNotFoundEntity())[0] is True
        assert _classify_publish_error(ValueError("Max length exceeded for exchange"))[0] is True
        assert _classify_publish_error(RuntimeError("boom")) == (False, None)


# ---------------------------------------------------------------------------
# AMQPPublisher — mandatory flag + DEBUG-only classification
# ---------------------------------------------------------------------------


class TestPublisherMandatoryFlag:
    async def test_publish_response_uses_mandatory_true(self) -> None:
        connection, exchange = _mock_connection()
        pub = AMQPPublisher(connection)
        await pub.enable_confirms()

        await pub.publish_response(_response(), "test.reply.exchange.v1")

        kwargs = exchange.publish.await_args.kwargs
        assert kwargs["mandatory"] is True

    async def test_publish_status_uses_mandatory_true(self) -> None:
        connection, exchange = _mock_connection()
        pub = AMQPPublisher(connection)
        await pub.enable_confirms()

        await pub.publish_status(_status(), "test.reply.exchange.v1")

        kwargs = exchange.publish.await_args.kwargs
        assert kwargs["mandatory"] is True

    @pytest.mark.parametrize("method", ["publish_response", "publish_status"])
    async def test_transient_channel_opened_with_on_return_raises(self, method: str) -> None:
        """Without ``on_return_raises=True`` aio-pika resolves a returned
        mandatory publish NORMALLY (no exception) — mandatory=True alone
        is a silent no-op and the unroutable response vanishes again.
        Pin the flag on the transient channel."""
        connection, _ = _mock_connection()
        pub = AMQPPublisher(connection)
        await pub.enable_confirms()

        if method == "publish_response":
            await pub.publish_response(_response(), "test.reply.exchange.v1")
        else:
            await pub.publish_status(_status(), "test.reply.exchange.v1")

        kwargs = connection.channel.await_args.kwargs
        assert kwargs.get("on_return_raises") is True

    async def test_response_delivery_error_logs_debug_only(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """DeliveryError is an expected operator condition: the publisher
        layer must log at DEBUG only (no ERROR, no traceback) and re-raise
        for the transport layer to handle."""
        connection, exchange = _mock_connection()
        exchange.publish = AsyncMock(side_effect=_publish_error())
        pub = AMQPPublisher(connection)
        await pub.enable_confirms()

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk.amqp.publisher"):
            with pytest.raises(DeliveryError):
                await pub.publish_response(_response(), "test.reply.exchange.v1")

        publisher_records = [
            r
            for r in caplog.records
            if r.name == "kbm_ledsas_sdk.amqp.publisher" and r.levelno >= logging.ERROR
        ]
        assert publisher_records == []
        assert "message unroutable" in caplog.text

    async def test_status_delivery_error_logs_debug_only(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connection, exchange = _mock_connection()
        exchange.publish = AsyncMock(side_effect=_delivery_error())
        pub = AMQPPublisher(connection)
        await pub.enable_confirms()

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk.amqp.publisher"):
            with pytest.raises(DeliveryError):
                await pub.publish_status(_status(), "test.reply.exchange.v1")

        publisher_records = [
            r
            for r in caplog.records
            if r.name == "kbm_ledsas_sdk.amqp.publisher" and r.levelno >= logging.ERROR
        ]
        assert publisher_records == []


# ---------------------------------------------------------------------------
# DirectTransport.send_response — DeliveryError drives the failure chain
# ---------------------------------------------------------------------------


class TestSendResponseUnroutable:
    async def test_returns_false_and_bumps_both_counters(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        transport = _transport()
        transport.publisher = _mock_publisher(side_effect=_publish_error())

        with caplog.at_level(logging.ERROR, logger="kbm_ledsas_sdk.transport.direct"):
            result = await transport.send_response(_response())

        assert result is False
        assert transport.reply_publish_failures == 1
        assert transport.reply_unroutable_failures == 1
        assert transport.status_publish_failures == 0

    async def test_bare_delivery_error_bumps_aggregate_only(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Basic.Nack (bare DeliveryError) is NOT the unroutable case:
        aggregate counter only, and the operator hint points at broker
        policies, not at bindings."""
        transport = _transport()
        transport.publisher = _mock_publisher(side_effect=_delivery_error())

        with caplog.at_level(logging.ERROR, logger="kbm_ledsas_sdk.transport.direct"):
            result = await transport.send_response(_response())

        assert result is False
        assert transport.reply_publish_failures == 1
        assert transport.reply_unroutable_failures == 0
        assert "did not confirm" in caplog.text
        assert "no queue bound" not in caplog.text
        assert BODY_SENTINEL not in caplog.text

    async def test_error_log_has_bound_queue_hint_and_no_body(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        transport = _transport()
        transport.publisher = _mock_publisher(side_effect=_publish_error())

        with caplog.at_level(logging.ERROR, logger="kbm_ledsas_sdk.transport.direct"):
            await transport.send_response(_response())

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        text = errors[0].getMessage()
        assert "no queue bound" in text
        assert "NACKed to DLQ" in text
        # A DeliveryError's args can embed the returned message, body
        # bytes included — the log line must never contain them.
        assert BODY_SENTINEL not in caplog.text

    async def test_counter_accumulates_across_failures(self) -> None:
        transport = _transport()
        transport.publisher = _mock_publisher(side_effect=_publish_error())

        await transport.send_response(_response())
        await transport.send_response(_response())

        assert transport.reply_publish_failures == 2
        assert transport.reply_unroutable_failures == 2

    async def test_missing_exchange_path_unchanged(self, caplog: pytest.LogCaptureFixture) -> None:
        """A non-DeliveryError failure (e.g. exchange missing) bumps only
        the aggregate counter and keeps the original operator hint."""

        class ChannelNotFoundEntity(Exception):
            pass

        transport = _transport()
        transport.publisher = _mock_publisher(
            side_effect=ChannelNotFoundEntity("NOT_FOUND - no exchange")
        )

        with caplog.at_level(logging.ERROR, logger="kbm_ledsas_sdk.transport.direct"):
            result = await transport.send_response(_response())

        assert result is False
        assert transport.reply_publish_failures == 1
        assert transport.reply_unroutable_failures == 0
        assert "pre-declared this exchange" in caplog.text


# ---------------------------------------------------------------------------
# DirectTransport.send_status — best-effort, single counter
# ---------------------------------------------------------------------------


class TestSendStatusUnroutable:
    @pytest.mark.parametrize(
        "error_factory",
        [_publish_error, _delivery_error],
        ids=["unroutable-return", "bare-nack"],
    )
    async def test_swallowed_with_warning_and_status_counter_only(
        self, error_factory, caplog: pytest.LogCaptureFixture
    ) -> None:
        transport = _transport()
        transport.publisher = _mock_publisher(side_effect=error_factory())

        with caplog.at_level(logging.WARNING, logger="kbm_ledsas_sdk.transport.direct"):
            # Must NOT raise: statuses are best-effort.
            await transport.send_status(_status())

        assert transport.status_publish_failures == 1
        assert transport.reply_publish_failures == 0
        assert transport.reply_unroutable_failures == 0

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "Continuing." in warnings[0].getMessage()
        assert BODY_SENTINEL not in caplog.text

    async def test_non_delivery_error_swallowed_with_original_text(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Pre-existing branch: a non-DeliveryError status
        failure is still swallowed with a WARNING carrying str(e) and
        the single status counter."""

        class ChannelNotFoundEntity(Exception):
            pass

        transport = _transport()
        transport.publisher = _mock_publisher(
            side_effect=ChannelNotFoundEntity("NOT_FOUND - no exchange")
        )

        with caplog.at_level(logging.WARNING, logger="kbm_ledsas_sdk.transport.direct"):
            await transport.send_status(_status())

        assert transport.status_publish_failures == 1
        assert transport.reply_publish_failures == 0
        assert "NOT_FOUND - no exchange" in caplog.text
        assert "Continuing." in caplog.text
