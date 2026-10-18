"""Bounded shutdown while the broker is down (fixed in 0.3.6).

Before 0.3.6, ``DirectTransport.stop()`` -> ``AMQPConsumer.stop()`` ->
``queue.cancel()`` on aio-pika's robust channel waited for the connection to
come back (``RobustChannel.get_underlay_channel()`` awaits
``connection.ready()``) and so never returned during a broker outage: SIGTERM
logged "Stopping AMQP consumer" and the process sat there until SIGKILL.

Covers:
- ``await_bounded``: result / exception pass-through, the bound, and that a
  timed-out operation is left pending (not cancelled) with its outcome
  consumed (no "exception was never retrieved" record later).
- ``AMQPConsumer.stop``: with REAL aio-pika robust objects (a robust
  connection that is not connected, i.e. an outage mid-reconnect) the stop
  returns within its bound, both when the channel already reports closed
  (cancel skipped) and when it still looks open (cancel bounded);
  connection-level errors are quiet (INFO only); healthy and unexpected-error
  behavior is unchanged.
- ``DirectTransport.stop``: every broker step is bounded; when the broker is
  unreachable the channel close is skipped, the connection is still closed
  (this stops aio-pika's reconnect loop), the blob client is still closed and
  exactly ONE WARNING (no ERROR, no traceback) is logged, including the
  number of unacknowledged messages the broker will redeliver.
- ``ServiceApp._shutdown`` on a transport in that state completes in bound.
- Commands still in flight when shutdown begins (the drain before
  ``transport.stop()``): ``BrokerShutdownGuard`` leaves round trips unbounded
  in normal operation, bounds them (including one already waiting) once
  ``begin_shutdown()`` runs, and fails fast once the broker is known
  unreachable; the publisher and ``DirectTransport.send_response`` /
  ``send_status`` then report an undelivered reply at INFO and leave the
  command unacknowledged (no dead-lettering), while a broker REFUSAL keeps
  its existing ERROR + dead-letter handling; ``ServiceApp`` calls the hook
  before draining, so a stalled broker cannot hold the drain.
- The ACK / NACK / reject of a drained command (e.g. one without
  ``reply_to``, or a handler failure): once shutdown has begun it is bounded,
  and one that cannot reach the broker keeps the message held (unacknowledged,
  redelivered) with one INFO line instead of ERROR + traceback + a second
  NACK; ``ServiceApp`` then leaves the command alone and ``stop()`` counts it
  in its single WARNING. Normal operation (no shutdown) keeps the existing
  ERROR path. A retry backoff is cut short once shutdown is requested.
- ``stop()`` reached from a failed ``start()`` reports an unreachable broker
  at INFO (``start()`` already logged the failure).

The live counterpart (real broker container stopped / frozen, real SIGTERM)
is ``tests/integration/test_sigterm_broker_outage_live.py``.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import time
import uuid
import warnings
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock

import aio_pika
import aiormq.exceptions
import pytest
from yarl import URL

from kbm_ledsas_sdk.amqp.consumer import (
    BROKER_UNREACHABLE_ERRORS,
    AMQPConsumer,
    BrokerNotReachable,
    BrokerShutdownGuard,
    await_bounded,
    is_broker_unreachable,
)
from kbm_ledsas_sdk.amqp.publisher import AMQPPublisher
from kbm_ledsas_sdk.app import ServiceApp
from kbm_ledsas_sdk.models.envelope import Envelope
from kbm_ledsas_sdk.models.errors import Permanent, Retryable
from kbm_ledsas_sdk.models.messages import Command, Response, Status
from kbm_ledsas_sdk.transport.base import DeliveryLost
from kbm_ledsas_sdk.transport.direct import DirectTransport

AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)
# Never connected to: the robust connection below is constructed, not opened.
UNREACHABLE_URL = URL("amqp://guest:guest@127.0.0.1:1/")
STEP = 0.2  # shrunken per-step bound for unit tests
SLACK = 1.0  # scheduling slack on top of the expected bound
NOTICE = "AMQP broker not reachable during shutdown"


async def _hang(*_args: Any, **_kwargs: Any) -> None:
    """An operation the broker never answers."""
    await asyncio.Event().wait()


def _records(caplog: pytest.LogCaptureFixture, level: int) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.levelno == level]


def _assert_no_tracebacks(caplog: pytest.LogCaptureFixture) -> None:
    assert not [r for r in caplog.records if r.exc_info], [
        r.getMessage() for r in caplog.records if r.exc_info
    ]
    assert "Traceback" not in caplog.text


class _OpenLookingRobustChannel(aio_pika.RobustChannel):
    """A real robust channel that still reports open, as it does in the
    moment between the broker going away and the client noticing."""

    @property
    def is_closed(self) -> bool:  # type: ignore[override]
        return False


def _robust_outage_queue(*, channel_looks_open: bool) -> tuple[Any, Any, Any]:
    """Real aio-pika robust connection/channel/queue during an outage: the
    connection is not connected, exactly like the reconnect loop's state."""
    connection = aio_pika.RobustConnection(UNREACHABLE_URL)
    channel_cls = _OpenLookingRobustChannel if channel_looks_open else aio_pika.RobustChannel
    channel = channel_cls(connection)
    queue = aio_pika.RobustQueue(channel, "cmd.test.v1", False, False, False, None)
    return connection, channel, queue


def _consumer(
    queue: Any,
    *,
    tag: str = "ctag-1",
    pending: int = 0,
    shutdown_guard: BrokerShutdownGuard | None = None,
) -> AMQPConsumer:
    consumer = AMQPConsumer(queue=queue, prefetch_count=10, shutdown_guard=shutdown_guard)
    consumer.consumer_tag = tag
    for i in range(pending):
        consumer.pending_messages[f"msg-{i}"] = MagicMock()
    return consumer


def _transport(
    *,
    consumer: Any,
    channel: Any,
    connection: Any,
    step: float = STEP,
) -> DirectTransport:
    transport = DirectTransport(
        rabbitmq_url="amqp://guest:guest@127.0.0.1:5672/",
        blob_conn_string=AZURITE_CONN,
        service_name="shutdown-svc",
    )
    transport.SHUTDOWN_STEP_TIMEOUT_S = step  # instance override
    transport.consumer = consumer
    transport.channel = channel
    transport.connection = connection
    transport.azure_client = MagicMock()
    transport.azure_client.close = AsyncMock()
    return transport


def _mock_channel(*, closed: bool = False, close: Any = None) -> MagicMock:
    channel = MagicMock()
    channel.is_closed = closed
    channel.close = AsyncMock(side_effect=close)
    return channel


def _mock_connection(*, close: Any = None) -> MagicMock:
    connection = MagicMock()
    connection.is_closed = False
    connection.close = AsyncMock(side_effect=close)
    return connection


def _mock_queue(*, cancel: Any = None, channel_closed: Any = False) -> MagicMock:
    queue = MagicMock()
    queue.name = "cmd.test.v1"
    queue.channel.is_closed = channel_closed
    queue.cancel = AsyncMock(side_effect=cancel)
    return queue


# ---------------------------------------------------------------------------
# await_bounded
# ---------------------------------------------------------------------------


class TestAwaitBounded:
    async def test_returns_result_in_time(self) -> None:
        async def quick() -> str:
            return "ok"

        assert await await_bounded(quick(), 1.0) == "ok"

    async def test_none_timeout_awaits_directly(self) -> None:
        async def quick() -> int:
            await asyncio.sleep(0)
            return 7

        assert await await_bounded(quick(), None) == 7

    async def test_operation_error_passes_through(self) -> None:
        async def boom() -> None:
            raise aiormq.exceptions.ChannelInvalidStateError("closed")

        with pytest.raises(aiormq.exceptions.ChannelInvalidStateError):
            await await_bounded(boom(), 1.0)

    async def test_times_out_within_bound_and_leaves_operation_pending(self) -> None:
        started = asyncio.Event()
        cancelled = []

        async def stuck() -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        t0 = time.monotonic()
        with pytest.raises(asyncio.TimeoutError):
            await await_bounded(stuck(), STEP)
        assert time.monotonic() - t0 < STEP + SLACK
        assert started.is_set()
        await asyncio.sleep(0.05)
        # Not cancelled by the helper: the caller closes the connection and
        # the loop's teardown cancels it later, quietly.
        assert cancelled == []

    async def test_abandoned_failure_is_never_reported_as_unretrieved(self) -> None:
        loop = asyncio.get_running_loop()
        reported: list[dict] = []
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, ctx: reported.append(ctx))
        release = asyncio.Event()

        async def fails_later() -> None:
            await release.wait()
            raise aiormq.exceptions.ConnectionClosed(320, "CONNECTION_FORCED")

        try:
            with pytest.raises(asyncio.TimeoutError):
                await await_bounded(fails_later(), 0.05)
            release.set()
            for _ in range(5):
                await asyncio.sleep(0)
            gc.collect()
            await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(previous)
        assert reported == []

    def test_timeout_counts_as_broker_unreachable(self) -> None:
        assert issubclass(asyncio.TimeoutError, BROKER_UNREACHABLE_ERRORS)
        for exc in (
            aiormq.exceptions.ChannelInvalidStateError,
            aiormq.exceptions.ConnectionClosed,
            aiormq.exceptions.AMQPConnectionError,
            ConnectionError,
            RuntimeError,  # aiormq: "<Connection ...> closed"
        ):
            assert issubclass(exc, BROKER_UNREACHABLE_ERRORS), exc


# ---------------------------------------------------------------------------
# AMQPConsumer.stop
# ---------------------------------------------------------------------------


class TestConsumerStopBrokerOutage:
    async def test_real_robust_queue_cancel_never_completes_stop_returns_in_bound(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The bug itself: a real RobustQueue.cancel() waits on
        connection.ready() forever; stop(timeout) must return anyway."""
        connection, _channel, queue = _robust_outage_queue(channel_looks_open=True)
        # Sanity: unbounded, the real aio-pika cancel does hang.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(queue.cancel("probe-tag"), 0.1)

        consumer = _consumer(queue, pending=2)
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            t0 = time.monotonic()
            result = await consumer.stop(timeout=STEP)
            elapsed = time.monotonic() - t0

        assert result is False
        assert elapsed < STEP + SLACK
        assert consumer.consumer_tag is None
        assert consumer.pending_messages == {}
        assert _records(caplog, logging.WARNING) == []
        assert _records(caplog, logging.ERROR) == []
        _assert_no_tracebacks(caplog)
        assert "without broker confirmation" in caplog.text
        await connection.close()

    async def test_real_robust_channel_already_closed_skips_cancel(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connection, channel, _ = _robust_outage_queue(channel_looks_open=False)
        assert channel.is_closed is True  # the real aio-pika answer
        queue = _mock_queue()
        queue.channel = channel
        consumer = _consumer(queue)

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            t0 = time.monotonic()
            result = await consumer.stop(timeout=5.0)  # bound not even needed
            elapsed = time.monotonic() - t0

        assert result is False
        assert elapsed < 0.5
        queue.cancel.assert_not_awaited()
        assert consumer.consumer_tag is None
        assert _records(caplog, logging.WARNING) == []
        assert _records(caplog, logging.ERROR) == []
        await connection.close()

    @pytest.mark.parametrize(
        "error",
        [
            aiormq.exceptions.ChannelInvalidStateError("No active transport"),
            aiormq.exceptions.ConnectionClosed(320, "CONNECTION_FORCED"),
            RuntimeError("<Connection: amqp://127.0.0.1/> closed"),
            ConnectionResetError(54, "Connection reset by peer"),
        ],
        ids=["channel-invalid", "connection-closed", "runtime-closed", "reset"],
    )
    async def test_connection_level_error_is_quiet(
        self, error: BaseException, caplog: pytest.LogCaptureFixture
    ) -> None:
        consumer = _consumer(_mock_queue(cancel=error))
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            result = await consumer.stop(timeout=STEP)
        assert result is False
        assert consumer.consumer_tag is None
        assert _records(caplog, logging.WARNING) == []
        assert _records(caplog, logging.ERROR) == []
        _assert_no_tracebacks(caplog)

    async def test_healthy_cancel_unchanged(self, caplog: pytest.LogCaptureFixture) -> None:
        queue = _mock_queue()
        consumer = _consumer(queue, pending=1)
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            result = await consumer.stop(timeout=STEP)
        assert result is True
        queue.cancel.assert_awaited_once_with("ctag-1")
        assert consumer.consumer_tag is None
        assert consumer.pending_messages == {}
        messages = [r.getMessage() for r in caplog.records]
        assert "AMQP consumer stopped" in messages
        # The pre-existing pending-messages notice is kept on the healthy path.
        assert [r.getMessage() for r in _records(caplog, logging.WARNING)] == [
            "Consumer stopped with 1 pending messages (will be requeued)"
        ]

    async def test_default_timeout_none_still_awaits_the_cancel(self) -> None:
        queue = _mock_queue()
        consumer = _consumer(queue)
        assert await consumer.stop() is True
        queue.cancel.assert_awaited_once_with("ctag-1")

    async def test_mock_channel_attribute_is_not_mistaken_for_closed(self) -> None:
        """A MagicMock ``is_closed`` (truthy, not ``True``) must not skip the cancel."""
        queue = MagicMock()
        queue.name = "cmd.test.v1"
        queue.cancel = AsyncMock()
        consumer = _consumer(queue)
        assert await consumer.stop(timeout=STEP) is True
        queue.cancel.assert_awaited_once_with("ctag-1")

    async def test_unexpected_error_keeps_existing_error_path(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        consumer = _consumer(_mock_queue(cancel=KeyError("bug")))
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            result = await consumer.stop(timeout=STEP)
        assert result is True  # not a broker-availability problem
        errors = _records(caplog, logging.ERROR)
        assert [r.getMessage() for r in errors] == ["Error stopping AMQP consumer"]
        assert errors[0].exc_info is not None
        assert consumer.consumer_tag == "ctag-1"

    async def test_no_consumer_is_a_noop(self) -> None:
        queue = _mock_queue()
        consumer = AMQPConsumer(queue=queue, prefetch_count=10)
        assert await consumer.stop(timeout=STEP) is True
        queue.cancel.assert_not_awaited()


# ---------------------------------------------------------------------------
# DirectTransport.stop
# ---------------------------------------------------------------------------


class TestDirectTransportStopBrokerOutage:
    async def test_outage_with_real_robust_objects_is_fast_with_one_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Broker down, aio-pika reconnecting: no broker round trip is
        attempted, so even the production 5 s bound is not consumed."""
        connection, channel, queue = _robust_outage_queue(channel_looks_open=False)
        transport = _transport(
            consumer=_consumer(queue, pending=3),
            channel=channel,
            connection=connection,
            step=DirectTransport.SHUTDOWN_STEP_TIMEOUT_S,
        )

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            t0 = time.monotonic()
            await transport.stop()
            elapsed = time.monotonic() - t0

        assert elapsed < 1.0
        warnings = _records(caplog, logging.WARNING)
        assert len(warnings) == 1
        assert warnings[0].getMessage().startswith(NOTICE)
        assert "(3 held by this consumer)" in warnings[0].getMessage()
        assert warnings[0].getMessage().endswith("Cause: connection to the broker already lost.")
        assert warnings[0].pending_messages == 3  # structured field
        assert _records(caplog, logging.ERROR) == []
        _assert_no_tracebacks(caplog)
        transport.azure_client.close.assert_awaited_once()

    async def test_stalled_broker_every_step_hangs_stop_is_bounded(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Broker frozen (TCP up, no answers): cancel times out, the channel
        round trip is skipped, the connection close is bounded too."""
        queue = _mock_queue(cancel=_hang)
        channel = _mock_channel(close=_hang)
        connection = _mock_connection(close=_hang)
        transport = _transport(
            consumer=_consumer(queue, pending=1), channel=channel, connection=connection
        )

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            t0 = time.monotonic()
            await transport.stop()
            elapsed = time.monotonic() - t0

        # Two bounded steps: cancel + connection close (channel skipped).
        assert elapsed < 2 * STEP + SLACK
        queue.cancel.assert_awaited_once_with("ctag-1")
        channel.close.assert_not_awaited()
        connection.close.assert_awaited_once()
        transport.azure_client.close.assert_awaited_once()
        warnings = _records(caplog, logging.WARNING)
        assert len(warnings) == 1 and warnings[0].getMessage().startswith(NOTICE)
        assert _records(caplog, logging.ERROR) == []
        _assert_no_tracebacks(caplog)

    async def test_hanging_channel_close_is_bounded(self, caplog: pytest.LogCaptureFixture) -> None:
        channel = _mock_channel(close=_hang)
        connection = _mock_connection()
        transport = _transport(
            consumer=_consumer(_mock_queue()), channel=channel, connection=connection
        )
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            t0 = time.monotonic()
            await transport.stop()
            elapsed = time.monotonic() - t0
        assert elapsed < STEP + SLACK
        channel.close.assert_awaited_once()
        connection.close.assert_awaited_once()
        warnings = _records(caplog, logging.WARNING)
        assert len(warnings) == 1 and warnings[0].getMessage().startswith(NOTICE)
        assert _records(caplog, logging.ERROR) == []
        _assert_no_tracebacks(caplog)

    async def test_hanging_connection_close_is_bounded(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connection = _mock_connection(close=_hang)
        transport = _transport(
            consumer=_consumer(_mock_queue()), channel=_mock_channel(), connection=connection
        )
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            t0 = time.monotonic()
            await transport.stop()
            elapsed = time.monotonic() - t0
        assert elapsed < STEP + SLACK
        transport.azure_client.close.assert_awaited_once()
        warnings = _records(caplog, logging.WARNING)
        assert len(warnings) == 1 and warnings[0].getMessage().startswith(NOTICE)
        assert _records(caplog, logging.ERROR) == []

    async def test_channel_close_connection_error_counts_as_unreachable(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        channel = _mock_channel(
            close=aiormq.exceptions.ChannelInvalidStateError("No active transport")
        )
        transport = _transport(
            consumer=_consumer(_mock_queue()), channel=channel, connection=_mock_connection()
        )
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            await transport.stop()
        warnings = _records(caplog, logging.WARNING)
        assert len(warnings) == 1 and warnings[0].getMessage().startswith(NOTICE)
        assert _records(caplog, logging.ERROR) == []
        _assert_no_tracebacks(caplog)

    async def test_healthy_broker_no_warning_and_every_step_runs(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        queue = _mock_queue()
        channel = _mock_channel()
        connection = _mock_connection()
        transport = _transport(consumer=_consumer(queue), channel=channel, connection=connection)
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            await transport.stop()
        queue.cancel.assert_awaited_once_with("ctag-1")
        channel.close.assert_awaited_once()
        connection.close.assert_awaited_once()
        transport.azure_client.close.assert_awaited_once()
        assert _records(caplog, logging.WARNING) == []
        assert _records(caplog, logging.ERROR) == []
        messages = [r.getMessage() for r in caplog.records]
        for expected in (
            "Stopping DirectTransport",
            "Consumer stopped",
            "Channel closed",
            "Connection closed",
        ):
            assert expected in messages

    async def test_consumer_double_returning_none_is_not_unreachable(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Only an explicit False from consumer.stop() means unreachable."""
        consumer = MagicMock()
        consumer.pending_count = 0
        consumer.stop = AsyncMock(return_value=None)
        channel = _mock_channel()
        transport = _transport(consumer=consumer, channel=channel, connection=_mock_connection())
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            await transport.stop()
        channel.close.assert_awaited_once()
        assert _records(caplog, logging.WARNING) == []

    def test_production_step_bound_fits_the_grace_period(self) -> None:
        """Three bounded steps must fit well inside a 30 s default grace
        period (the docs ask for >= 60 s)."""
        assert 0 < DirectTransport.SHUTDOWN_STEP_TIMEOUT_S
        assert 3 * DirectTransport.SHUTDOWN_STEP_TIMEOUT_S <= 15


# ---------------------------------------------------------------------------
# ServiceApp shutdown path
# ---------------------------------------------------------------------------


class TestServiceAppShutdownBrokerOutage:
    async def test_shutdown_completes_while_broker_down(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connection, channel, queue = _robust_outage_queue(channel_looks_open=True)
        transport = _transport(consumer=_consumer(queue), channel=channel, connection=connection)
        app = ServiceApp("shutdown-svc")
        app.transport = transport
        app._running = True

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            # Generous outer guard only so a regression fails instead of hanging.
            await asyncio.wait_for(app._shutdown(), 3 * STEP + SLACK + 2)

        messages = [r.getMessage() for r in caplog.records]
        assert "Transport stopped successfully" in messages
        assert "ServiceApp shutdown complete" in messages
        warnings = _records(caplog, logging.WARNING)
        assert len(warnings) == 1 and warnings[0].getMessage().startswith(NOTICE)
        assert _records(caplog, logging.ERROR) == []
        _assert_no_tracebacks(caplog)


# ---------------------------------------------------------------------------
# Commands in flight when shutdown begins (the drain before transport.stop())
# ---------------------------------------------------------------------------

REPLY_EXCHANGE = "shutdown.reply.v1"
UNDELIVERED = "not confirmed: AMQP broker not reachable during shutdown"


def _envelope(type_: str, *, name: str = "Slow") -> Envelope:
    return Envelope(
        schema_version="1.0",
        type=type_,
        name=name,
        message_version="1.0",
        message_id=str(uuid.uuid4()),
        correlation_id=str(uuid.uuid4()),
        idempotency_key="idem-shutdown",
        sent_at=datetime.now(UTC),
        trace_id="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        reply_to=REPLY_EXCHANGE,
    )


def _response() -> Response:
    return Response(envelope=_envelope("response"), payload={"result": "ok"})


def _status() -> Status:
    return Status(envelope=_envelope("status"), payload={"state": "running"})


def _publish_connection(
    *, open_: Any = None, get_exchange: Any = None, publish: Any = None
) -> tuple[MagicMock, MagicMock, MagicMock]:
    """(connection, channel, exchange) for driving AMQPPublisher.

    ``open_`` replaces the channel open (e.g. ``_hang`` for a stalled
    broker); the other two are side effects of the channel/exchange calls.
    """
    exchange = MagicMock()
    exchange.publish = AsyncMock(side_effect=publish)
    channel = MagicMock()
    channel.get_exchange = AsyncMock(return_value=exchange, side_effect=get_exchange)
    channel.is_closed = False
    channel.close = AsyncMock()
    connection = MagicMock()
    if open_ is None:
        connection.channel = AsyncMock(return_value=channel)
    else:
        connection.channel = MagicMock(side_effect=lambda **_kw: open_())
    connection.is_closed = False
    connection.close = AsyncMock()
    return connection, channel, exchange


def _publishing_transport(connection: Any) -> DirectTransport:
    """A DirectTransport (never start()ed) wired like start() wires it."""
    transport = _transport(
        consumer=_consumer(_mock_queue(), pending=1),
        channel=_mock_channel(),
        connection=connection,
    )
    transport.publisher = AMQPPublisher(connection, shutdown_guard=transport._shutdown_guard)
    transport.publisher._confirms_enabled = True
    return transport


class TestBrokerShutdownGuard:
    def test_classification_refusal_vs_unreachable(self) -> None:
        refusals = [
            aiormq.exceptions.ChannelNotFoundEntity("NOT_FOUND - no exchange"),
            aiormq.exceptions.ChannelPreconditionFailed("PRECONDITION_FAILED"),
            aiormq.exceptions.ChannelClosed(504, "CHANNEL_ERROR"),
            aiormq.exceptions.DeliveryError(None, None),
            ValueError("Max length exceeded for exchange"),
            KeyError("bug"),
        ]
        unreachable = [
            TimeoutError(),
            aiormq.exceptions.ConnectionClosed(320, "CONNECTION_FORCED"),
            aiormq.exceptions.ChannelInvalidStateError("No active transport"),
            aiormq.exceptions.AMQPConnectionError("refused"),
            RuntimeError("Connection was not opened"),
            ConnectionResetError(54, "Connection reset by peer"),
            BrokerNotReachable("skipped"),
        ]
        assert [is_broker_unreachable(e) for e in refusals] == [False] * len(refusals)
        assert [is_broker_unreachable(e) for e in unreachable] == [True] * len(unreachable)

    async def test_before_shutdown_round_trip_is_not_bounded(self) -> None:
        guard = BrokerShutdownGuard(STEP)

        async def slow() -> str:
            await asyncio.sleep(3 * STEP)  # longer than the shutdown bound
            return "ok"

        assert await guard.round_trip(slow()) == "ok"
        assert guard.broker_unreachable is False

    async def test_after_begin_shutdown_round_trip_is_bounded_and_latches(self) -> None:
        guard = BrokerShutdownGuard(STEP)
        guard.begin_shutdown()
        t0 = time.monotonic()
        with pytest.raises(asyncio.TimeoutError):
            await guard.round_trip(_hang())
        assert time.monotonic() - t0 < STEP + SLACK
        assert guard.broker_unreachable is True
        assert guard.fail_fast is True
        assert isinstance(guard.cause, asyncio.TimeoutError)

    async def test_round_trip_already_waiting_is_bounded_once_shutdown_begins(self) -> None:
        """The frozen-broker case: a reply publish was already stuck when
        SIGTERM arrived; it must not keep waiting past the bound."""
        guard = BrokerShutdownGuard(STEP)
        waiting = asyncio.create_task(guard.round_trip(_hang()))
        await asyncio.sleep(2 * STEP)
        assert not waiting.done()  # unbounded in normal operation
        t0 = time.monotonic()
        guard.begin_shutdown()
        with pytest.raises(asyncio.TimeoutError):
            await waiting
        assert time.monotonic() - t0 < STEP + SLACK
        assert guard.broker_unreachable is True

    async def test_fail_fast_once_unreachable_never_starts_the_round_trip(self) -> None:
        guard = BrokerShutdownGuard(STEP)
        guard.begin_shutdown()
        guard.mark_unreachable(aiormq.exceptions.ConnectionClosed(320, "x"))
        started = []

        async def op() -> None:
            started.append(True)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            t0 = time.monotonic()
            with pytest.raises(BrokerNotReachable):
                await guard.round_trip(op())
            assert time.monotonic() - t0 < 0.1
            gc.collect()
        assert started == []
        assert not [w for w in caught if "never awaited" in str(w.message)]

    async def test_refusal_during_shutdown_does_not_latch(self) -> None:
        guard = BrokerShutdownGuard(STEP)
        guard.begin_shutdown()

        async def refused() -> None:
            raise aiormq.exceptions.ChannelNotFoundEntity("NOT_FOUND")

        with pytest.raises(aiormq.exceptions.ChannelNotFoundEntity):
            await guard.round_trip(refused())
        assert guard.broker_unreachable is False

    async def test_connection_error_before_shutdown_does_not_latch(self) -> None:
        guard = BrokerShutdownGuard(STEP)

        async def reset() -> None:
            raise ConnectionResetError(54, "reset")

        with pytest.raises(ConnectionResetError):
            await guard.round_trip(reset())
        assert guard.broker_unreachable is False
        guard.begin_shutdown()
        assert guard.fail_fast is False

    async def test_outer_cancellation_cancels_the_round_trip(self) -> None:
        guard = BrokerShutdownGuard(STEP)
        cancelled = asyncio.Event()

        async def stuck() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        outer = asyncio.create_task(guard.round_trip(stuck()))
        await asyncio.sleep(0.05)
        outer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await outer
        await asyncio.wait_for(cancelled.wait(), 1.0)


class TestPublisherDuringShutdown:
    async def test_stalled_channel_open_is_bounded_and_debug_only(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connection, channel, exchange = _publish_connection(open_=_hang)
        guard = BrokerShutdownGuard(STEP)
        publisher = AMQPPublisher(connection, shutdown_guard=guard)
        await publisher.enable_confirms()
        guard.begin_shutdown()

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            t0 = time.monotonic()
            with pytest.raises(asyncio.TimeoutError):
                await publisher.publish_response(_response(), REPLY_EXCHANGE)
            elapsed = time.monotonic() - t0

        assert elapsed < STEP + SLACK
        assert guard.broker_unreachable is True
        exchange.publish.assert_not_awaited()
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        _assert_no_tracebacks(caplog)

    async def test_confirm_waiting_when_shutdown_begins_is_bounded(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Publish + confirm already waiting (e.g. a broker that blocked
        publishers on a resource alarm) when shutdown begins."""
        connection, channel, _ = _publish_connection(publish=_hang)
        guard = BrokerShutdownGuard(STEP)
        publisher = AMQPPublisher(connection, shutdown_guard=guard)
        await publisher.enable_confirms()

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            pending = asyncio.create_task(publisher.publish_status(_status(), REPLY_EXCHANGE))
            await asyncio.sleep(2 * STEP)
            assert not pending.done()  # normal operation: no bound
            t0 = time.monotonic()
            guard.begin_shutdown()
            with pytest.raises(asyncio.TimeoutError):
                await pending
            elapsed = time.monotonic() - t0

        assert elapsed < STEP + SLACK
        # The broker is known unreachable: no channel-close round trip.
        channel.close.assert_not_awaited()
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        _assert_no_tracebacks(caplog)

    async def test_fails_fast_without_opening_a_channel_once_unreachable(self) -> None:
        connection, _, _ = _publish_connection()
        guard = BrokerShutdownGuard(STEP)
        guard.begin_shutdown()
        guard.mark_unreachable(TimeoutError())
        publisher = AMQPPublisher(connection, shutdown_guard=guard)
        await publisher.enable_confirms()
        with pytest.raises(BrokerNotReachable):
            await publisher.publish_response(_response(), REPLY_EXCHANGE)
        connection.channel.assert_not_called()

    async def test_healthy_publish_with_guard_unchanged(self) -> None:
        connection, channel, exchange = _publish_connection()
        guard = BrokerShutdownGuard(STEP)
        publisher = AMQPPublisher(connection, shutdown_guard=guard)
        await publisher.enable_confirms()
        await publisher.publish_response(_response(), REPLY_EXCHANGE)
        guard.begin_shutdown()  # a healthy broker during shutdown, too
        await publisher.publish_response(_response(), REPLY_EXCHANGE)
        assert exchange.publish.await_count == 2
        assert channel.close.await_count == 2
        assert connection.channel.await_args.kwargs == {"on_return_raises": True}
        assert guard.broker_unreachable is False


class TestTransportPublishDuringShutdown:
    async def test_reply_with_stalled_broker_is_undelivered_info_only(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connection, _, _ = _publish_connection(open_=_hang)
        transport = _publishing_transport(connection)
        transport.begin_shutdown()

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            t0 = time.monotonic()
            ok = await transport.send_response(_response())
            elapsed = time.monotonic() - t0

        assert ok is False
        assert elapsed < STEP + SLACK
        assert transport.broker_unreachable_at_shutdown is True
        assert transport.replies_undelivered_at_shutdown == 1
        assert transport.reply_publish_failures == 0  # not a config failure
        infos = [r for r in caplog.records if UNDELIVERED in r.getMessage()]
        assert len(infos) == 1 and infos[0].levelno == logging.INFO
        assert infos[0].error_class == "TimeoutError"
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        _assert_no_tracebacks(caplog)

    async def test_reply_with_stopped_broker_is_undelivered_info_only(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Broker stopped: aio-pika fails the channel open at once."""

        async def not_opened() -> None:
            raise RuntimeError("Connection was not opened")

        connection, _, _ = _publish_connection(open_=not_opened)
        transport = _publishing_transport(connection)
        transport.begin_shutdown()
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            assert await transport.send_response(_response()) is False
        assert transport.broker_unreachable_at_shutdown is True
        assert transport.replies_undelivered_at_shutdown == 1
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        _assert_no_tracebacks(caplog)

    async def test_broker_refusal_during_shutdown_keeps_dead_letter_path(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A reachable broker REFUSING the reply (missing exchange) is not
        an outage: same ERROR + dead-letter handling as before."""
        connection, _, _ = _publish_connection(
            get_exchange=aiormq.exceptions.ChannelNotFoundEntity("NOT_FOUND")
        )
        transport = _publishing_transport(connection)
        transport.begin_shutdown()
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            assert await transport.send_response(_response()) is False
        assert transport.broker_unreachable_at_shutdown is False
        assert transport.reply_publish_failures == 1
        assert transport.replies_undelivered_at_shutdown == 0
        errors = _records(caplog, logging.ERROR)
        assert len(errors) == 1
        assert errors[0].getMessage().startswith("Failed to send response")

    async def test_outage_outside_shutdown_keeps_existing_handling(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        async def not_opened() -> None:
            raise RuntimeError("Connection was not opened")

        connection, _, _ = _publish_connection(open_=not_opened)
        transport = _publishing_transport(connection)
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            assert await transport.send_response(_response()) is False
        assert transport.broker_unreachable_at_shutdown is False
        assert transport.reply_publish_failures == 1
        assert len(_records(caplog, logging.ERROR)) == 1

    async def test_status_with_stalled_broker_is_info_only(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connection, _, _ = _publish_connection(open_=_hang)
        transport = _publishing_transport(connection)
        transport.begin_shutdown()
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            t0 = time.monotonic()
            await transport.send_status(_status())
            elapsed = time.monotonic() - t0
        assert elapsed < STEP + SLACK
        assert transport.status_publish_failures == 0
        assert transport.broker_unreachable_at_shutdown is True
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        _assert_no_tracebacks(caplog)

    async def test_stop_after_undelivered_reply_skips_broker_and_reports_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connection, _, _ = _publish_connection(open_=_hang)
        transport = _publishing_transport(connection)
        queue = transport.consumer.queue
        transport.begin_shutdown()
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            await transport.send_response(_response())
            t0 = time.monotonic()
            await transport.stop()
            elapsed = time.monotonic() - t0

        assert elapsed < 0.5  # no further bounded wait
        queue.cancel.assert_not_awaited()
        transport.channel.close.assert_not_awaited()
        connection.close.assert_awaited_once()
        warnings_ = _records(caplog, logging.WARNING)
        assert len(warnings_) == 1
        text = warnings_[0].getMessage()
        assert text.startswith(NOTICE)
        assert "(1 held by this consumer)" in text
        assert "1 finished command(s) could not confirm their reply" in text
        # A stalled broker may still deliver it: the caller is told a
        # duplicate reply is possible (at-least-once), not that none came.
        assert "the caller can receive it twice" in text
        assert "could not be acknowledged" not in text
        assert "Cause: TimeoutError (operation did not complete within" in text
        assert warnings_[0].replies_undelivered == 1
        assert warnings_[0].cause_error_class == "TimeoutError"
        assert _records(caplog, logging.ERROR) == []
        _assert_no_tracebacks(caplog)

    async def test_warning_names_the_swallowed_error_without_credentials(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A misclassified bug (e.g. an unexpected RuntimeError) is reported
        with its class and message, so it can be told apart from an outage;
        credentials in an embedded URL are masked."""
        consumer = _consumer(
            _mock_queue(cancel=RuntimeError('<Connection: "amqp://u:s3cret@h:5672/"> closed'))
        )
        transport = _transport(
            consumer=consumer, channel=_mock_channel(), connection=_mock_connection()
        )
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            await transport.stop()
        warnings_ = _records(caplog, logging.WARNING)
        assert len(warnings_) == 1
        assert warnings_[0].cause_error_class == "RuntimeError"
        assert "closed" in warnings_[0].cause_error
        assert 'Cause: RuntimeError (<Connection: "amqp://***@h:5672/"> closed).' in (
            warnings_[0].getMessage()
        )
        assert "s3cret" not in caplog.text
        _assert_no_tracebacks(caplog)


class _Drainable:
    """ServiceApp + a real DirectTransport/AMQPConsumer/AMQPPublisher whose
    broker connection never answers a channel open (a frozen broker).

    ``reply_to=False`` sends a command without a reply exchange; ``outcome``
    is what the handler does after ``handler_seconds`` (``"ok"``, or raise
    ``"permanent"`` / ``"retryable"``); ``settle_error`` makes the message's
    ACK / NACK / reject fail with it (a stopped broker fails them at once).
    """

    def __init__(
        self,
        handler_seconds: float,
        *,
        reply_to: bool = True,
        outcome: str = "ok",
        settle_error: BaseException | None = None,
    ) -> None:
        self.connection, _, _ = _publish_connection(open_=_hang)
        self.queue = _mock_queue()
        self.transport = _transport(
            consumer=None, channel=_mock_channel(), connection=self.connection
        )
        # Wired like DirectTransport.start(): one guard for both.
        self.consumer = _consumer(self.queue, shutdown_guard=self.transport._shutdown_guard)
        self.transport.consumer = self.consumer
        self.transport.publisher = AMQPPublisher(
            self.connection, shutdown_guard=self.transport._shutdown_guard
        )
        self.transport.publisher._confirms_enabled = True

        self.app = ServiceApp("shutdown-svc")
        self.app.transport = self.transport
        self.app.config = Mock(
            concurrency=4, handler_timeout=1800, max_retries=3, generic_errors=False
        )
        self.app._running = True
        self.started = asyncio.Event()

        @self.app.handler("Slow", "1.0")
        async def _slow(ctx: Any, payload: dict) -> dict:
            self.started.set()
            await asyncio.sleep(handler_seconds)
            if outcome == "permanent":
                raise Permanent("bad input")
            if outcome == "retryable":
                raise Retryable("try later")
            return {"ok": True}

        envelope = _envelope("command")
        if not reply_to:
            envelope = envelope.model_copy(update={"reply_to": ""})
        self.message = MagicMock()
        self.message.ack = AsyncMock(side_effect=settle_error)
        self.message.nack = AsyncMock(side_effect=settle_error)
        self.message.reject = AsyncMock(side_effect=settle_error)
        self.consumer.pending_messages[envelope.message_id] = self.message
        self.consumer.command_queue.put_nowait(Command(envelope=envelope, payload={}))


class TestServiceAppDrainDuringShutdown:
    async def test_stalled_broker_with_command_in_flight_exits_in_bound(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The verifier's frozen-broker case: SIGTERM while a command runs;
        the handler finishes after the signal and its reply publish meets a
        broker that never answers. Before the fix the drain waited forever."""
        handler_seconds = 0.3
        d = _Drainable(handler_seconds)
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            loop_task = asyncio.create_task(d.app._execution_loop())
            await asyncio.wait_for(d.started.wait(), 2.0)
            t0 = time.monotonic()
            d.app._shutdown_event.set()  # what the SIGTERM handler does
            # Generous outer guards only so a regression fails instead of hanging.
            await asyncio.wait_for(loop_task, handler_seconds + 2 * STEP + SLACK + 2)
            await asyncio.wait_for(d.app._shutdown(), 2 * STEP + SLACK + 2)
            elapsed = time.monotonic() - t0

        # Handler remainder + one bounded publish wait; stop() adds none.
        assert elapsed < handler_seconds + STEP + SLACK
        # Neither ACKed nor dead-lettered: the broker redelivers it.
        d.message.ack.assert_not_awaited()
        d.message.reject.assert_not_awaited()
        d.message.nack.assert_not_awaited()
        # The consumer is cancelled once, when the drain starts (this mock
        # queue still answers; only channel opens hang), so the broker stops
        # delivering to this instance; stop() sends no second cancel.
        d.queue.cancel.assert_awaited_once_with("ctag-1")
        warnings_ = _records(caplog, logging.WARNING)
        assert len(warnings_) == 1, [r.getMessage() for r in warnings_]
        assert warnings_[0].getMessage().startswith(NOTICE)
        assert "(1 held by this consumer)" in warnings_[0].getMessage()
        assert _records(caplog, logging.ERROR) == []
        _assert_no_tracebacks(caplog)
        messages = [r.getMessage() for r in caplog.records]
        assert "ServiceApp shutdown complete" in messages
        assert not any("dead-letter" in m for m in messages)

    async def test_healthy_broker_drain_unchanged(self, caplog: pytest.LogCaptureFixture) -> None:
        d = _Drainable(0.1)
        d.connection.channel = AsyncMock(return_value=_publish_connection()[1])
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            loop_task = asyncio.create_task(d.app._execution_loop())
            await asyncio.wait_for(d.started.wait(), 2.0)
            d.app._shutdown_event.set()
            await asyncio.wait_for(loop_task, 5.0)
            await asyncio.wait_for(d.app._shutdown(), 5.0)
        d.message.ack.assert_awaited_once()
        d.queue.cancel.assert_awaited_once()
        assert _records(caplog, logging.WARNING) == []
        assert _records(caplog, logging.ERROR) == []
        assert "Sent response for Slow" in [r.getMessage() for r in caplog.records]

    async def test_reply_failure_without_unreachable_flag_still_dead_letters(self) -> None:
        """Transports without the flag (or a False flag) keep the existing
        "reply publish failed -> dead-letter" behavior."""
        app = ServiceApp("shutdown-svc")
        app.config = Mock(concurrency=1, handler_timeout=1800, max_retries=3, generic_errors=False)

        @app.handler("Slow", "1.0")
        async def _h(ctx: Any, payload: dict) -> dict:
            return {}

        for flag in (None, False, MagicMock()):
            transport = MagicMock()
            transport.send_response = AsyncMock(return_value=False)
            transport.nack = AsyncMock()
            transport.ack = AsyncMock()
            if flag is None:
                del transport.broker_unreachable_at_shutdown
            else:
                transport.broker_unreachable_at_shutdown = flag
            app.transport = transport
            command = Command(envelope=_envelope("command"), payload={})
            await app._handle_command(command, asyncio.Semaphore(1))
            transport.nack.assert_awaited_once_with(command.envelope.message_id, requeue=False)

    async def test_reply_undelivered_at_shutdown_is_left_unacknowledged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        app = ServiceApp("shutdown-svc")
        app.config = Mock(concurrency=1, handler_timeout=1800, max_retries=3, generic_errors=False)

        @app.handler("Slow", "1.0")
        async def _h(ctx: Any, payload: dict) -> dict:
            return {}

        transport = MagicMock()
        transport.send_response = AsyncMock(return_value=False)
        transport.nack = AsyncMock()
        transport.ack = AsyncMock()
        transport.broker_unreachable_at_shutdown = True
        app.transport = transport
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            await app._handle_command(
                Command(envelope=_envelope("command"), payload={}), asyncio.Semaphore(1)
            )
        transport.nack.assert_not_awaited()
        transport.ack.assert_not_awaited()
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []

    async def test_begin_shutdown_hook_sync_async_or_absent(self) -> None:
        app = ServiceApp("shutdown-svc")
        sync_transport = MagicMock()
        app.transport = sync_transport
        await app._begin_transport_shutdown()
        sync_transport.begin_shutdown.assert_called_once_with()

        async_transport = AsyncMock()
        app.transport = async_transport
        await app._begin_transport_shutdown()
        async_transport.begin_shutdown.assert_awaited_once_with()

        app.transport = object()  # no hook: no-op
        await app._begin_transport_shutdown()

        failing = MagicMock()
        failing.begin_shutdown.side_effect = RuntimeError("boom")
        app.transport = failing
        await app._begin_transport_shutdown()  # never breaks the shutdown


# ---------------------------------------------------------------------------
# ACK / NACK / reject of commands drained after shutdown began
# ---------------------------------------------------------------------------

SETTLE_NOT_SENT = "not sent: AMQP broker not reachable during shutdown"


def _settling_consumer(
    *, stopping: bool = True, guard: BrokerShutdownGuard | None = None
) -> tuple[AMQPConsumer, BrokerShutdownGuard, MagicMock]:
    """A consumer holding one message ("m-1") whose ack/nack/reject are
    AsyncMocks; its guard has begun shutdown unless ``stopping=False``."""
    guard = guard or BrokerShutdownGuard(STEP)
    if stopping:
        guard.begin_shutdown()
    consumer = _consumer(_mock_queue(), shutdown_guard=guard)
    message = MagicMock()
    message.ack = AsyncMock()
    message.nack = AsyncMock()
    message.reject = AsyncMock()
    consumer.pending_messages["m-1"] = message
    return consumer, guard, message


async def _settle(consumer: AMQPConsumer, op: str) -> None:
    if op == "ack":
        await consumer.ack("m-1")
    else:
        await consumer.nack("m-1", requeue=(op == "requeue"))


SETTLE_OPS = ["ack", "requeue", "dead-letter"]


class TestSettlementDuringShutdown:
    @pytest.mark.parametrize("op", SETTLE_OPS)
    async def test_stopped_broker_keeps_message_held_and_logs_info_only(
        self, op: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Broker stopped: aio-pika fails the frame at once
        (ChannelInvalidStateError). Before the fix: ERROR + traceback, the
        message dropped from the held set, and a second (failing) NACK."""
        consumer, guard, message = _settling_consumer()
        error = aiormq.exceptions.ChannelInvalidStateError()
        for name in ("ack", "nack", "reject"):
            getattr(message, name).side_effect = error

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            with pytest.raises(BrokerNotReachable) as raised:
                await _settle(consumer, op)

        assert isinstance(raised.value, ConnectionError)
        assert raised.value.__cause__ is error
        # Still held: counted in the shutdown report, redelivered by the broker.
        assert consumer.pending_messages == {"m-1": message}
        assert consumer.unsettled_at_shutdown == 1
        assert guard.fail_fast is True and guard.cause is error
        infos = [r for r in caplog.records if SETTLE_NOT_SENT in r.getMessage()]
        assert len(infos) == 1 and infos[0].levelno == logging.INFO
        assert infos[0].error_class == "ChannelInvalidStateError"
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        _assert_no_tracebacks(caplog)

    @pytest.mark.parametrize("op", SETTLE_OPS)
    async def test_frame_the_connection_never_takes_is_bounded(
        self, op: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        consumer, guard, message = _settling_consumer()
        for name in ("ack", "nack", "reject"):
            getattr(message, name).side_effect = _hang

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            t0 = time.monotonic()
            with pytest.raises(BrokerNotReachable):
                await _settle(consumer, op)
            elapsed = time.monotonic() - t0

        assert elapsed < STEP + SLACK
        assert isinstance(guard.cause, asyncio.TimeoutError)
        assert "m-1" in consumer.pending_messages
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        _assert_no_tracebacks(caplog)

    @pytest.mark.parametrize("op", SETTLE_OPS)
    async def test_buffered_settlement_on_frozen_broker_still_settles(self, op: str) -> None:
        """A frozen broker still takes the frame (buffered): the settlement
        goes through, even after another round trip latched the outage, so
        the broker can apply it when it recovers (no needless re-run)."""
        consumer, guard, message = _settling_consumer()
        guard.mark_unreachable(TimeoutError())
        await _settle(consumer, op)
        assert consumer.pending_messages == {}
        assert consumer.unsettled_at_shutdown == 0
        if op == "ack":
            message.ack.assert_awaited_once_with()
        elif op == "requeue":
            message.nack.assert_awaited_once_with(requeue=True)
        else:
            message.reject.assert_awaited_once_with(requeue=False)

    async def test_before_shutdown_settlement_is_not_bounded(self) -> None:
        consumer, guard, message = _settling_consumer(stopping=False)

        async def slow_ack() -> None:
            await asyncio.sleep(3 * STEP)  # longer than the shutdown bound

        message.ack.side_effect = slow_ack
        await consumer.ack("m-1")
        assert consumer.pending_messages == {}
        assert guard.broker_unreachable is False

    @pytest.mark.parametrize("stopping", [False, None], ids=["before-shutdown", "no-guard"])
    async def test_connection_error_outside_shutdown_is_one_warning(
        self, stopping: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Outside shutdown, an ACK that fails because the delivery's channel
        is gone (e.g. a broker restart while the handler ran) is one
        WARNING without a traceback and raises DeliveryLost: the broker
        redelivers the message (before, an
        ERROR with a traceback, then two more in the service). The message
        is no longer held, so its redelivery is not refused as a duplicate,
        and the shutdown guard is not latched."""
        if stopping is None:
            consumer = _consumer(_mock_queue())
            message = MagicMock()
            message.ack = AsyncMock()
            consumer.pending_messages["m-1"] = message
            guard = None
        else:
            consumer, guard, message = _settling_consumer(stopping=False)
        message.ack.side_effect = aiormq.exceptions.ChannelInvalidStateError()

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            with pytest.raises(DeliveryLost):
                await consumer.ack("m-1")

        assert consumer.pending_messages == {}
        assert consumer.unsettled_at_shutdown == 0
        if guard is not None:
            assert guard.broker_unreachable is False
        assert _records(caplog, logging.ERROR) == []
        warnings_ = _records(caplog, logging.WARNING)
        assert len(warnings_) == 1
        assert (
            warnings_[0]
            .getMessage()
            .startswith("ACK not sent: the connection to the AMQP broker was lost")
        )
        assert warnings_[0].exc_info is None

    @pytest.mark.parametrize("stopping", [False, None], ids=["before-shutdown", "no-guard"])
    async def test_non_connection_error_outside_shutdown_keeps_error_path(
        self, stopping: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Normal operation is unchanged for anything that is not a lost
        connection: ERROR with traceback, the original exception re-raised,
        the message no longer held."""
        if stopping is None:
            consumer = _consumer(_mock_queue())
            message = MagicMock()
            message.ack = AsyncMock()
            consumer.pending_messages["m-1"] = message
            guard = None
        else:
            consumer, guard, message = _settling_consumer(stopping=False)
        message.ack.side_effect = KeyError("bug")

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            with pytest.raises(KeyError):
                await consumer.ack("m-1")

        assert consumer.pending_messages == {}
        assert consumer.unsettled_at_shutdown == 0
        if guard is not None:
            assert guard.broker_unreachable is False
        errors = _records(caplog, logging.ERROR)
        assert [r.getMessage() for r in errors] == ["Failed to ACK message"]
        assert errors[0].exc_info is not None

    @pytest.mark.parametrize(
        "error",
        [KeyError("bug"), aio_pika.exceptions.MessageProcessError("already processed")],
        ids=["unexpected", "already-processed"],
    )
    async def test_non_outage_error_during_shutdown_keeps_error_path(
        self, error: BaseException, caplog: pytest.LogCaptureFixture
    ) -> None:
        consumer, guard, message = _settling_consumer()
        message.reject.side_effect = error
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            with pytest.raises(type(error)):
                await consumer.nack("m-1", requeue=False)
        assert guard.broker_unreachable is False
        assert consumer.unsettled_at_shutdown == 0
        assert "Failed to NACK message" in [r.getMessage() for r in _records(caplog, logging.ERROR)]

    def test_already_processed_message_is_not_an_outage(self) -> None:
        assert not is_broker_unreachable(
            aio_pika.exceptions.MessageProcessError("already processed")
        )


class TestStopReportsUnsettledCommands:
    async def test_one_warning_counts_held_and_unsettled(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connection, channel, queue = _robust_outage_queue(channel_looks_open=False)
        transport = _transport(consumer=None, channel=channel, connection=connection)
        consumer = _consumer(queue, shutdown_guard=transport._shutdown_guard)
        transport.consumer = consumer
        message = MagicMock()
        message.ack = AsyncMock(side_effect=aiormq.exceptions.ChannelInvalidStateError())
        consumer.pending_messages["m-1"] = message
        transport.begin_shutdown()

        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            with pytest.raises(BrokerNotReachable):
                await transport.ack("m-1")
            await transport.stop()

        warnings_ = _records(caplog, logging.WARNING)
        assert len(warnings_) == 1
        text = warnings_[0].getMessage()
        assert text.startswith(NOTICE)
        assert "(1 held by this consumer)" in text
        assert (
            "1 finished command(s) could not be acknowledged, requeued or "
            "dead-lettered and run again on redelivery." in text
        )
        assert "could not confirm their reply" not in text
        assert text.endswith("Cause: ChannelInvalidStateError.")
        assert warnings_[0].commands_unsettled == 1
        assert warnings_[0].pending_messages == 1
        assert _records(caplog, logging.ERROR) == []
        _assert_no_tracebacks(caplog)
        await connection.close()

    @pytest.mark.parametrize("consumer_kind", ["none", "never-subscribed"])
    async def test_stop_after_failed_start_reports_at_info(
        self, consumer_kind: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        """stop() also runs when start() fails; start() already logged the
        failure as one line, so an unreachable broker there is INFO detail,
        not a second (WARNING) report."""
        consumer = None if consumer_kind == "none" else _consumer(_mock_queue(), tag=None)
        transport = _transport(
            consumer=consumer,
            channel=_mock_channel(close=ConnectionResetError(54, "reset")),
            connection=_mock_connection(close=ConnectionResetError(54, "reset")),
        )
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            await transport.stop()
        notices = [r for r in caplog.records if r.getMessage().startswith(NOTICE)]
        assert len(notices) == 1 and notices[0].levelno == logging.INFO
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        _assert_no_tracebacks(caplog)


class TestServiceAppSettlementDuringShutdown:
    async def _drain(self, d: _Drainable, caplog: pytest.LogCaptureFixture) -> float:
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            loop_task = asyncio.create_task(d.app._execution_loop())
            await asyncio.wait_for(d.started.wait(), 2.0)
            t0 = time.monotonic()
            d.app._shutdown_event.set()  # what the SIGTERM handler does
            await asyncio.wait_for(loop_task, 5.0)
            await asyncio.wait_for(d.app._shutdown(), 5.0)
            return time.monotonic() - t0

    async def test_no_reply_command_with_stopped_broker_is_left_for_redelivery(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The verifier's residual: a command WITHOUT reply_to finishes after
        SIGTERM while the broker is stopped. Before: 3 ERROR records with
        tracebacks ("Failed to ACK message", "Uncaught exception in
        _handle_command", "Failed to NACK message"), a second WARNING, and
        "0 held by this consumer"."""
        d = _Drainable(
            0.2,
            reply_to=False,
            settle_error=aiormq.exceptions.ChannelInvalidStateError(),
        )
        elapsed = await self._drain(d, caplog)

        assert elapsed < 0.2 + SLACK
        d.message.ack.assert_awaited_once()
        d.message.reject.assert_not_awaited()
        d.message.nack.assert_not_awaited()
        warnings_ = _records(caplog, logging.WARNING)
        assert len(warnings_) == 1, [r.getMessage() for r in warnings_]
        text = warnings_[0].getMessage()
        assert text.startswith(NOTICE)
        assert "(1 held by this consumer)" in text
        assert "1 finished command(s) could not be acknowledged" in text
        assert _records(caplog, logging.ERROR) == []
        _assert_no_tracebacks(caplog)
        messages = [r.getMessage() for r in caplog.records]
        assert "ACK not sent" in " ".join(messages)
        assert not [m for m in messages if m.startswith("ACKed command")]
        assert not [m for m in messages if "Uncaught exception" in m]
        assert "ServiceApp shutdown complete" in messages

    async def test_permanent_failure_with_stopped_broker_is_left_for_redelivery(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        d = _Drainable(
            0.2,
            reply_to=False,
            outcome="permanent",
            settle_error=aiormq.exceptions.ChannelInvalidStateError(),
        )
        await self._drain(d, caplog)

        d.message.reject.assert_awaited_once_with(requeue=False)
        d.message.ack.assert_not_awaited()
        warnings_ = _records(caplog, logging.WARNING)
        assert len(warnings_) == 1 and warnings_[0].getMessage().startswith(NOTICE)
        assert "(1 held by this consumer)" in warnings_[0].getMessage()
        # Only the application's own outcome lines; nothing from the
        # settlement failure itself.
        # One ERROR per failed command:
        # the handler layer's line; the "sending to DLQ" outcome is INFO.
        assert sorted(r.getMessage() for r in _records(caplog, logging.ERROR)) == [
            "Permanent error in handler",
        ]
        _assert_no_tracebacks(caplog)
        assert "Dead-letter not sent" in caplog.text

    async def test_retry_backoff_is_cut_short_by_shutdown(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A retryable failure waits its backoff (up to 60 s) before the
        requeue; once shutdown is requested the requeue happens at once
        instead of holding the drain past the grace period."""
        monkeypatch.setattr(ServiceApp, "_calculate_backoff", lambda self, n: 30.0)
        d = _Drainable(0.0, reply_to=False, outcome="retryable")
        d.connection.channel = AsyncMock(return_value=_publish_connection()[1])
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            loop_task = asyncio.create_task(d.app._execution_loop())
            await asyncio.wait_for(d.started.wait(), 2.0)
            await asyncio.sleep(0.2)  # now inside the 30 s backoff
            d.message.nack.assert_not_awaited()
            t0 = time.monotonic()
            d.app._shutdown_event.set()
            await asyncio.wait_for(loop_task, 5.0)
            elapsed = time.monotonic() - t0
            await asyncio.wait_for(d.app._shutdown(), 5.0)

        assert elapsed < SLACK
        d.message.nack.assert_awaited_once_with(requeue=True)
        assert [r for r in _records(caplog, logging.ERROR)] == []

    async def test_backoff_outside_shutdown_waits_the_full_delay(self) -> None:
        app = ServiceApp("shutdown-svc")
        t0 = time.monotonic()
        await app._backoff_unless_shutdown(3 * STEP)
        assert time.monotonic() - t0 >= 3 * STEP - 0.02
        t0 = time.monotonic()
        await app._backoff_unless_shutdown(0)
        assert time.monotonic() - t0 < 0.1
        app._shutdown_event.set()
        t0 = time.monotonic()
        await app._backoff_unless_shutdown(30.0)
        assert time.monotonic() - t0 < 0.1

    @pytest.mark.parametrize(
        "error, flag",
        [
            (ConnectionError("lost"), False),
            (KeyError("bug"), True),
        ],
        ids=["connection-error-no-flag", "other-error-with-flag"],
    )
    async def test_catch_all_keeps_dead_letter_path_unless_both_hold(
        self, error: BaseException, flag: bool, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Only a ConnectionError (the consumer's BrokerNotReachable) while
        the transport reports the broker unreachable at shutdown skips the
        existing "uncaught exception -> dead-letter" path."""
        app = ServiceApp("shutdown-svc")
        app.config = Mock(concurrency=1, handler_timeout=1800, max_retries=3, generic_errors=False)

        @app.handler("Slow", "1.0")
        async def _h(ctx: Any, payload: dict) -> dict:
            return {}

        transport = MagicMock()
        transport.ack = AsyncMock(side_effect=error)
        transport.nack = AsyncMock()
        transport.broker_unreachable_at_shutdown = flag
        app.transport = transport
        envelope = _envelope("command").model_copy(update={"reply_to": ""})
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            await app._handle_command(Command(envelope=envelope, payload={}), asyncio.Semaphore(1))
        transport.nack.assert_awaited_once_with(envelope.message_id, requeue=False)
        assert "Uncaught exception in _handle_command; dead-lettering" in [
            r.getMessage() for r in _records(caplog, logging.ERROR)
        ]

    async def test_catch_all_leaves_command_when_settlement_hit_the_outage(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        app = ServiceApp("shutdown-svc")
        app.config = Mock(concurrency=1, handler_timeout=1800, max_retries=3, generic_errors=False)

        @app.handler("Slow", "1.0")
        async def _h(ctx: Any, payload: dict) -> dict:
            return {}

        transport = MagicMock()
        transport.ack = AsyncMock(side_effect=BrokerNotReachable("ACK not sent"))
        transport.nack = AsyncMock()
        transport.broker_unreachable_at_shutdown = True
        app.transport = transport
        envelope = _envelope("command").model_copy(update={"reply_to": ""})
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            await app._handle_command(Command(envelope=envelope, payload={}), asyncio.Semaphore(1))
        transport.nack.assert_not_awaited()
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        _assert_no_tracebacks(caplog)
