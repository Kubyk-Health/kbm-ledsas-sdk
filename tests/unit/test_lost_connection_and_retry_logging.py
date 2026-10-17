"""Regression tests: a broker connection lost while a handler runs, a broker
redelivery of a command that is still running, and the per-attempt logging
of retried failures (SDK 0.3.5).

- **Connection lost while a handler runs (no shutdown).** The command's
  ACK / requeue / dead-letter, or its reply, cannot be sent on the lost
  connection. One WARNING for the command, no ERROR, no traceback, no
  second NACK, no "Uncaught exception"; the broker redelivers it. Driven
  end to end through ``ServiceApp`` with the real ``DirectTransport`` and
  ``AMQPConsumer`` and deliveries whose channel closes mid-handler.
- **Redelivery while the original still runs.** After the connection is
  restored the broker delivers the unacknowledged command again, often
  while its handler is still running. That redelivery used to be refused
  by the duplicate-``message_id`` guard and dead-lettered (and the
  original's ACK then failed on the lost connection). It is now handed to
  the running handler, whose outcome settles it: the command runs once.
  A true duplicate (the original's delivery still live, or another body)
  is still dead-lettered.
- **Retried failures log one WARNING per attempt.** A transient storage or
  network error never logs a traceback; any other uncaught exception logs
  its traceback once, on the command's last attempt; the "Retry k/N" line
  is INFO; the failed command has one ERROR (``Max retries (N) exceeded``).
  The blob layer's own line for a transient storage failure is INFO.
- **aiormq 6.x's empty exception** on the broker's close-reason record (a
  bare ``NoneType: None`` line) is dropped; the record is kept.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiormq.exceptions
import pytest
from azure.core.exceptions import HttpResponseError, ServiceRequestError

import kbm_ledsas_sdk.app as app_module
from kbm_ledsas_sdk.amqp.consumer import AMQPConsumer, is_redelivery_of
from kbm_ledsas_sdk.app import ServiceApp
from kbm_ledsas_sdk.models import Command, Envelope
from kbm_ledsas_sdk.models.errors import Retryable
from kbm_ledsas_sdk.runtime.handler import (
    HandlerRegistry,
    _transient_reason,
)
from kbm_ledsas_sdk.transport.mock import MockTransport

AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)
MESSAGE_ID = "11111111-2222-4333-8444-555555555555"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _envelope(reply_to: str = "reply-ex", message_id: str = MESSAGE_ID) -> Envelope:
    return Envelope(
        schema_version="1.0",
        type="command",
        name="Work",
        message_version="1.0",
        message_id=message_id,
        correlation_id="66666666-7777-4888-8999-aaaaaaaaaaaa",
        idempotency_key="idem-lost-connection",
        sent_at=datetime(2026, 10, 14, tzinfo=UTC),
        trace_id="trace-lost-connection",
        reply_to=reply_to,
    )


def _body(reply_to: str = "reply-ex", **payload) -> bytes:
    command = Command(envelope=_envelope(reply_to), payload=payload)
    return json.dumps(command.model_dump(mode="json")).encode()


class _Delivery:
    """Stands in for an aio-pika ``IncomingMessage``: settling it after its
    channel closed raises ``ChannelInvalidStateError``, like aio-pika (the
    ``channel`` property raises once the channel is closed)."""

    def __init__(self, body: bytes, *, redelivered: bool = False) -> None:
        self.body = body
        self.redelivered = redelivered
        self.closed = False
        self.settled: list = []
        self._channel = SimpleNamespace(is_closed=False)

    @property
    def channel(self):
        if self.closed:
            raise aiormq.exceptions.ChannelInvalidStateError("channel closed")
        return self._channel

    def lose_connection(self) -> None:
        self.closed = True
        self._channel.is_closed = True

    async def ack(self, multiple: bool = False) -> None:
        self.channel
        self.settled.append("ack")

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None:
        self.channel
        self.settled.append(("nack", requeue))

    async def reject(self, requeue: bool = False) -> None:
        self.channel
        self.settled.append(("reject", requeue))


def _consumer() -> AMQPConsumer:
    return AMQPConsumer(queue=SimpleNamespace(name="q"), prefetch_count=4)


def _direct_app(*, publish_side_effect=None, max_retries: int = 3):
    """ServiceApp wired to a real DirectTransport + AMQPConsumer; the
    publisher is a stand-in."""
    from kbm_ledsas_sdk.transport.direct import DirectTransport

    app = ServiceApp("svc-lost-connection")
    transport = DirectTransport(
        rabbitmq_url="amqp://guest:guest@127.0.0.1:5672/",
        blob_conn_string=AZURITE_CONN,
        service_name="svc-lost-connection",
    )
    transport.consumer = _consumer()
    transport.publisher = SimpleNamespace(
        publish_response=AsyncMock(side_effect=publish_side_effect),
        publish_status=AsyncMock(),
    )
    app.transport = transport
    app.config = SimpleNamespace(
        handler_timeout=0,
        max_retries=max_retries,
        generic_errors=False,
        concurrency=4,
    )
    return app, transport


def _sdk_records(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name.startswith("kbm_ledsas_sdk")]


async def _start_running(app, transport, delivery, gate: asyncio.Event, runs: list):
    """Deliver ``delivery``, start its handler (which waits for ``gate``)."""

    @app.handler("Work")
    async def work(ctx, payload):
        runs.append(ctx.message_id)
        await gate.wait()
        return {"ok": True}

    await transport.consumer._on_message(delivery)
    command = transport.consumer.command_queue.get_nowait()
    task = asyncio.create_task(app._handle_command(command, asyncio.Semaphore(4)))
    await asyncio.sleep(0)
    return task


# ---------------------------------------------------------------------------
# connection lost while the handler runs: one WARNING, nothing else
# ---------------------------------------------------------------------------


class TestConnectionLostWhileHandlerRuns:
    async def test_ack_on_the_lost_connection_is_one_warning(self, caplog) -> None:
        app, transport = _direct_app()
        original = _Delivery(_body(reply_to=""))
        gate, runs = asyncio.Event(), []
        with caplog.at_level(logging.DEBUG):
            task = await _start_running(app, transport, original, gate, runs)
            original.lose_connection()  # broker dropped mid-handler
            gate.set()
            await task

        records = _sdk_records(caplog)
        assert [r for r in records if r.levelno >= logging.ERROR] == []
        assert [r for r in records if r.exc_info] == []
        warnings = [r for r in records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert warnings[0].getMessage().startswith("ACK not sent: the connection")
        # A broker restart keeps only a persistent message: the line says so.
        assert (
            warnings[0]
            .getMessage()
            .endswith("(after a broker restart, only if it was published persistent).")
        )
        messages = [r.getMessage() for r in records]
        assert not [m for m in messages if "Uncaught exception" in m]
        assert "Rejecting message" not in messages  # no second NACK
        assert original.settled == []
        assert transport.consumer.pending_messages == {}
        # The broker's redelivery is accepted, not refused as a duplicate.
        await transport.consumer._on_message(_Delivery(original.body, redelivered=True))
        assert transport.consumer.command_queue.qsize() == 1

    async def test_reply_on_the_lost_connection_is_one_warning(self, caplog) -> None:
        app, transport = _direct_app(publish_side_effect=RuntimeError("Connection was not opened"))
        original = _Delivery(_body())
        gate, runs = asyncio.Event(), []
        with caplog.at_level(logging.DEBUG):
            task = await _start_running(app, transport, original, gate, runs)
            original.lose_connection()
            transport._broker_connection_lost = True  # the close callback ran
            gate.set()
            await task

        records = _sdk_records(caplog)
        assert [r for r in records if r.levelno >= logging.ERROR] == []
        assert [r for r in records if r.exc_info] == []
        warnings = [r for r in records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert warnings[0].getMessage().startswith("Reply to exchange 'reply-ex' not sent")
        assert (
            warnings[0]
            .getMessage()
            .endswith("(after a broker restart, only if it was published persistent).")
        )
        assert original.settled == []
        assert transport.reply_publish_failures == 0
        assert transport.consumer.pending_messages == {}

    async def test_lost_requeue_keeps_the_attempt_count(self) -> None:
        consumer = _consumer()
        delivery = _Delivery(_body())
        await consumer._on_message(delivery)
        consumer.retry_counts[MESSAGE_ID] = 1
        delivery.lose_connection()
        from kbm_ledsas_sdk.transport.base import DeliveryLost

        with pytest.raises(DeliveryLost):
            await consumer.nack(MESSAGE_ID, requeue=True)
        # The failed attempt counted; the redelivery is the next attempt.
        assert consumer.get_retry_count(MESSAGE_ID) == 2

    async def test_lost_dead_letter_keeps_the_attempt_count(self, caplog) -> None:
        """A terminal dead-letter that cannot be sent on the lost connection
        keeps the count the command reached: the redelivery does not get a
        new retry budget (it would otherwise run max_retries + 1 more
        times, each logged, plus another final traceback)."""
        from kbm_ledsas_sdk.transport.base import DeliveryLost

        consumer = _consumer()
        delivery = _Delivery(_body())
        await consumer._on_message(delivery)
        consumer.retry_counts[MESSAGE_ID] = 3
        delivery.lose_connection()
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            with pytest.raises(DeliveryLost):
                await consumer.nack(MESSAGE_ID, requeue=False)
        assert consumer.get_retry_count(MESSAGE_ID) == 3
        assert consumer.pending_messages == {}
        assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []
        assert [r for r in caplog.records if r.exc_info] == []
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert warnings[0].getMessage().startswith("Dead-letter not sent: the connection")
        # A dead-letter that is sent still clears the count.
        redelivery = _Delivery(delivery.body, redelivered=True)
        await consumer._on_message(redelivery)
        await consumer.nack(MESSAGE_ID, requeue=False)
        assert redelivery.settled == [("reject", False)]
        assert consumer.get_retry_count(MESSAGE_ID) == 0

    async def test_retry_exhausted_redelivery_after_a_lost_dead_letter_is_terminal(
        self, no_backoff
    ) -> None:
        """End to end: retries spent, the dead-letter is lost with the
        connection; the broker's redelivery fails once more and goes
        straight to the dead-letter queue, with no new retries."""
        app, transport = _direct_app(max_retries=2)
        transport.consumer.retry_counts[MESSAGE_ID] = 2  # retries spent
        runs = []

        @app.handler("Work")
        async def work(ctx, payload):
            runs.append(1)
            raise Retryable("still failing")

        first = _Delivery(_body(reply_to=""))
        await transport.consumer._on_message(first)
        first.lose_connection()
        await app._handle_command(
            transport.consumer.command_queue.get_nowait(), asyncio.Semaphore(1)
        )
        assert first.settled == []
        assert transport.consumer.get_retry_count(MESSAGE_ID) == 2

        again = _Delivery(first.body, redelivered=True)
        await transport.consumer._on_message(again)
        await app._handle_command(
            transport.consumer.command_queue.get_nowait(), asyncio.Semaphore(1)
        )
        assert again.settled == [("reject", False)]  # terminal, not requeued
        assert runs == [1, 1]


# ---------------------------------------------------------------------------
# redelivery of a command that is still running here
# ---------------------------------------------------------------------------


class TestRedeliveryWhileRunning:
    async def test_redelivery_is_settled_by_the_running_handler(self, caplog) -> None:
        app, transport = _direct_app()
        original = _Delivery(_body())
        gate, runs = asyncio.Event(), []
        with caplog.at_level(logging.DEBUG):
            task = await _start_running(app, transport, original, gate, runs)
            original.lose_connection()  # lost ...
            redelivery = _Delivery(original.body, redelivered=True)
            await transport.consumer._on_message(redelivery)  # ... and restored
            assert transport.consumer.command_queue.qsize() == 0  # not run twice
            gate.set()
            await task

        assert runs == [MESSAGE_ID]
        assert redelivery.settled == ["ack"]
        assert original.settled == []
        transport.publisher.publish_response.assert_awaited_once()
        assert transport.consumer.redeliveries_adopted == 1
        assert transport.consumer.pending_messages == {}
        records = _sdk_records(caplog)
        assert [r for r in records if r.levelno >= logging.WARNING] == []
        adopted = [
            r
            for r in records
            if r.getMessage().startswith(
                "Redelivered command arrived while it is still in progress here"
            )
        ]
        assert len(adopted) == 1 and adopted[0].levelno == logging.INFO
        assert adopted[0].redelivered is True

    @pytest.mark.parametrize("confirmed", [True, False])
    async def test_stop_record_reports_the_adopted_redeliveries(self, caplog, confirmed) -> None:
        """The adoption counter is visible without a metrics backend: the
        consumer's stop record carries it, whether or not the broker
        confirmed the cancel."""
        consumer = AMQPConsumer(
            queue=SimpleNamespace(
                name="q",
                channel=SimpleNamespace(is_closed=False),
                cancel=AsyncMock(),
            ),
            prefetch_count=4,
        )
        consumer.consumer_tag = "ctag"
        original = _Delivery(_body())
        await consumer._on_message(original)
        original.lose_connection()
        await consumer._on_message(_Delivery(original.body, redelivered=True))
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            assert await consumer.stop(timeout=1, skip_cancel=not confirmed) is confirmed
        expected = (
            "AMQP consumer stopped"
            if confirmed
            else "AMQP consumer stopped without broker confirmation " "(broker not reachable)"
        )
        stopped = [r for r in caplog.records if r.getMessage() == expected]
        assert len(stopped) == 1 and stopped[0].levelno == logging.INFO
        assert stopped[0].redeliveries_adopted == 1

    async def test_requeue_and_dead_letter_apply_to_the_redelivery(self) -> None:
        for requeue in (True, False):
            consumer = _consumer()
            original = _Delivery(_body())
            await consumer._on_message(original)
            original.lose_connection()
            redelivery = _Delivery(original.body, redelivered=True)
            await consumer._on_message(redelivery)
            await consumer.nack(MESSAGE_ID, requeue=requeue)
            expected = ("nack", True) if requeue else ("reject", False)
            assert redelivery.settled == [expected]
            assert original.settled == []

    async def test_redelivered_flag_is_reported_not_required(self) -> None:
        consumer = _consumer()
        original = _Delivery(_body())
        await consumer._on_message(original)
        original.lose_connection()
        # Some queue types do not keep the flag across a broker restart.
        redelivery = _Delivery(original.body, redelivered=False)
        await consumer._on_message(redelivery)
        assert consumer.pending_messages[MESSAGE_ID] is redelivery
        assert redelivery.settled == []

    async def test_true_duplicate_while_the_original_is_live_is_dead_lettered(self, caplog) -> None:
        consumer = _consumer()
        original = _Delivery(_body())
        await consumer._on_message(original)
        duplicate = _Delivery(original.body, redelivered=True)
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            await consumer._on_message(duplicate)
        assert duplicate.settled == [("reject", False)]
        assert consumer.pending_messages[MESSAGE_ID] is original
        assert consumer.redeliveries_adopted == 0
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and warnings[0].getMessage().startswith(
            "Duplicate message_id while original still pending"
        )

    async def test_other_body_with_the_same_id_is_a_duplicate(self) -> None:
        consumer = _consumer()
        original = _Delivery(_body(n=1))
        await consumer._on_message(original)
        original.lose_connection()
        other = _Delivery(_body(n=2), redelivered=True)
        await consumer._on_message(other)
        assert other.settled == [("reject", False)]
        assert consumer.pending_messages[MESSAGE_ID] is original

    def test_is_redelivery_of_needs_a_closed_channel_and_the_same_body(self) -> None:
        live = _Delivery(b"x")
        stale = _Delivery(b"x")
        stale.lose_connection()
        assert is_redelivery_of(stale, _Delivery(b"x"))
        assert not is_redelivery_of(live, _Delivery(b"x"))
        assert not is_redelivery_of(stale, _Delivery(b"y"))
        assert not is_redelivery_of(stale, SimpleNamespace(body=None))
        assert not is_redelivery_of(None, _Delivery(b"x"))


# ---------------------------------------------------------------------------
# aiormq 6.x: the close-reason record's empty exception is dropped
# ---------------------------------------------------------------------------


class TestEmptyLibraryExceptionDropped:
    def test_no_bare_nonetype_line(self) -> None:
        app_module._quiet_noisy_libraries()
        lib = logging.getLogger("aiormq.connection")
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        lib.addHandler(handler)
        try:
            # log.exception() outside an except block, as aiormq 6.x does for
            # "Unexpected connection close from remote ...".
            lib.exception("Unexpected connection close from remote %r", "amqp://x")
        finally:
            lib.removeHandler(handler)
        output = stream.getvalue()
        assert "Unexpected connection close from remote" in output
        assert "NoneType" not in output

    def test_real_exceptions_are_untouched(self) -> None:
        flt = app_module._SuppressExpectedAMQPErrors()
        try:
            raise ValueError("boom")
        except ValueError:
            import sys

            record = logging.LogRecord(
                "aiormq.connection", logging.ERROR, __file__, 1, "other", (), sys.exc_info()
            )
        assert flt.filter(record) is True
        assert record.exc_info is not None and record.exc_info[1] is not None


# ---------------------------------------------------------------------------
# retried failures: one WARNING per attempt, at most one traceback
# ---------------------------------------------------------------------------


class _RetryCountingTransport(MockTransport):
    def __init__(self) -> None:
        super().__init__()
        self.counts: dict[str, int] = {}

    async def nack(self, message_id: str, requeue: bool) -> None:
        await super().nack(message_id, requeue)
        if requeue:
            self.counts[message_id] = self.counts.get(message_id, 0) + 1
        else:
            self.counts.pop(message_id, None)

    async def ack(self, message_id: str) -> None:
        await super().ack(message_id)
        self.counts.pop(message_id, None)

    def get_retry_count(self, message_id: str) -> int:
        return self.counts.get(message_id, 0)


@pytest.fixture
def no_backoff(monkeypatch):
    monkeypatch.setattr(ServiceApp, "_calculate_backoff", lambda self, n: 0.0)


def _retry_app(max_retries: int = 3):
    app = ServiceApp("svc-retry-logging")
    transport = _RetryCountingTransport()
    app.transport = transport
    app.config = SimpleNamespace(
        handler_timeout=0,
        max_retries=max_retries,
        generic_errors=False,
        concurrency=1,
    )
    return app, transport


def _command() -> Command:
    return Command(envelope=_envelope(), payload={})


async def _until_terminal(app, transport, limit: int = 10) -> int:
    command = _command()
    for attempt in range(1, limit + 1):
        await app._handle_command(command, asyncio.Semaphore(1))
        if MESSAGE_ID in transport.acked_messages:
            return attempt
        if transport.nacked_messages and transport.nacked_messages[-1] == (MESSAGE_ID, False):
            return attempt
    raise AssertionError("never terminal")


def _storage_unreachable() -> ServiceRequestError:
    return ServiceRequestError(
        "Cannot connect to host 127.0.0.1:10000 ssl:default [Connect call failed]"
    )


class TestRetriedFailureLogging:
    @pytest.mark.parametrize(
        "make_error",
        [
            _storage_unreachable,
            lambda: HttpResponseError(message="Server busy", response=MagicMock(status_code=503)),
            lambda: ConnectionResetError("connection reset by peer"),
        ],
        ids=["storage-unreachable", "storage-503", "connection-reset"],
    )
    async def test_transient_error_never_logs_a_traceback(
        self, make_error, caplog, no_backoff
    ) -> None:
        app, transport = _retry_app(max_retries=3)

        @app.handler("Work")
        async def work(ctx, payload):
            raise make_error()

        with caplog.at_level(logging.DEBUG):
            attempts = await _until_terminal(app, transport)
        assert attempts == 4
        records = _sdk_records(caplog)
        assert [r for r in records if r.exc_info] == []
        warnings = [r for r in records if r.levelno == logging.WARNING]
        assert len(warnings) == 4  # one per attempt
        assert all(r.getMessage().startswith("Transient error in handler: ") for r in warnings)
        assert [r.will_retry for r in warnings] == [True, True, True, False]
        errors = [r for r in records if r.levelno >= logging.ERROR]
        assert [r.getMessage() for r in errors] == ["Max retries (3) exceeded, sending to DLQ"]
        retry_lines = [r for r in records if r.getMessage().startswith("Retry ")]
        assert len(retry_lines) == 3 and all(r.levelno == logging.INFO for r in retry_lines)

    async def test_other_uncaught_error_logs_its_traceback_once_on_the_last_attempt(
        self, caplog, no_backoff
    ) -> None:
        app, transport = _retry_app(max_retries=3)

        @app.handler("Work")
        async def work(ctx, payload):
            raise KeyError("missing_field")

        with caplog.at_level(logging.DEBUG):
            await _until_terminal(app, transport)
        records = _sdk_records(caplog)
        uncaught = [r for r in records if r.getMessage() == "Uncaught exception in handler"]
        assert len(uncaught) == 4
        assert all(r.levelno == logging.WARNING for r in uncaught)
        assert [r.exc_info is not None for r in uncaught] == [False, False, False, True]
        assert [r for r in records if r.exc_info] == [uncaught[-1]]
        assert all("KeyError" in r.internal_message for r in uncaught)
        assert all(
            r.error_location.startswith("test_lost_connection_and_retry_logging.py:")
            for r in uncaught
        )
        errors = [r for r in records if r.levelno >= logging.ERROR]
        assert [r.getMessage() for r in errors] == ["Max retries (3) exceeded, sending to DLQ"]
        assert [r for r in records if r.levelno == logging.WARNING] == uncaught

    async def test_success_on_retry_logs_no_traceback(self, caplog, no_backoff) -> None:
        app, transport = _retry_app(max_retries=3)
        calls = []

        @app.handler("Work")
        async def work(ctx, payload):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("first attempt only")
            return {"ok": True}

        with caplog.at_level(logging.DEBUG):
            assert await _until_terminal(app, transport) == 2
        records = _sdk_records(caplog)
        assert [r for r in records if r.exc_info] == []
        assert [r for r in records if r.levelno >= logging.ERROR] == []
        assert len([r for r in records if r.levelno == logging.WARNING]) == 1

    async def test_no_retries_configured_is_the_last_attempt(self, caplog, no_backoff) -> None:
        app, transport = _retry_app(max_retries=0)

        @app.handler("Work")
        async def work(ctx, payload):
            raise ValueError("bad state")

        with caplog.at_level(logging.DEBUG):
            assert await _until_terminal(app, transport) == 1
        records = _sdk_records(caplog)
        assert len([r for r in records if r.exc_info]) == 1
        assert len([r for r in records if r.levelno >= logging.ERROR]) == 1

    @pytest.mark.parametrize("failure", ["retryable", "timeout"])
    async def test_retryable_and_timeout_attempts_are_one_warning_each(
        self, failure, caplog, no_backoff
    ) -> None:
        app, transport = _retry_app(max_retries=1)
        if failure == "timeout":
            app.config.handler_timeout = 0.05

        @app.handler("Work")
        async def work(ctx, payload):
            if failure == "timeout":
                await asyncio.sleep(10)  # cut short by the handler timeout
            raise Retryable("db pool exhausted")

        with caplog.at_level(logging.DEBUG):
            assert await _until_terminal(app, transport) == 2
        records = _sdk_records(caplog)
        expected = {"retryable": "Retryable error in handler", "timeout": "Handler timeout"}
        warnings = [r for r in records if r.levelno == logging.WARNING]
        assert [r.getMessage() for r in warnings] == [expected[failure]] * 2
        assert [r for r in records if r.exc_info] == []
        errors = [r for r in records if r.levelno >= logging.ERROR]
        assert [r.getMessage() for r in errors] == ["Max retries (1) exceeded, sending to DLQ"]
        reply = transport.sent_responses[-1].payload["error"]
        assert reply["code"] == ("Timeout" if failure == "timeout" else "Retryable")

    async def test_transport_without_a_retry_count_keeps_the_error_and_traceback(
        self, caplog, no_backoff
    ) -> None:
        """A transport that keeps no retry count gives no retry context:
        an unexpected error is logged at ERROR with its traceback on each
        attempt (never silently without one)."""
        app = ServiceApp("svc-no-retry-count")
        transport = MockTransport()
        assert not hasattr(transport, "get_retry_count")
        app.transport = transport
        app.config = SimpleNamespace(
            handler_timeout=0,
            max_retries=3,
            generic_errors=False,
            concurrency=1,
        )

        @app.handler("Work")
        async def work(ctx, payload):
            raise ValueError("bad state")

        with caplog.at_level(logging.DEBUG):
            await app._handle_command(_command(), asyncio.Semaphore(1))
            await app._handle_command(_command(), asyncio.Semaphore(1))
        uncaught = [
            r for r in _sdk_records(caplog) if r.getMessage() == "Uncaught exception in handler"
        ]
        assert len(uncaught) == 2
        assert all(r.levelno == logging.ERROR and r.exc_info is not None for r in uncaught)

    async def test_registry_without_retry_context_keeps_the_error_and_traceback(
        self, caplog
    ) -> None:
        registry = HandlerRegistry()

        async def work(ctx, payload):
            raise ValueError("boom")

        registry.register("Work", "1.0", work)
        ctx = SimpleNamespace()
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            response = await registry.execute(ctx, _command())
        assert response.payload["error"]["code"] == "UnexpectedError"
        records = [r for r in caplog.records if r.getMessage() == "Uncaught exception in handler"]
        assert len(records) == 1
        assert records[0].levelno == logging.ERROR and records[0].exc_info is not None

    async def test_reply_is_unchanged(self, no_backoff) -> None:
        app, transport = _retry_app(max_retries=0)

        @app.handler("Work")
        async def work(ctx, payload):
            raise _storage_unreachable()

        await _until_terminal(app, transport)
        reply = transport.sent_responses[-1].payload
        assert reply == {
            "error": {
                "code": "UnexpectedError",
                "message": "An unexpected error occurred",
                "retryable": True,
            }
        }


class TestTransientClassification:
    def test_classified(self) -> None:
        assert _transient_reason(_storage_unreachable()).startswith(
            "blob storage not reachable (ServiceRequestError: Cannot connect"
        )
        busy = HttpResponseError(message="busy", response=MagicMock(status_code=503))
        busy.error_code = "ServerBusy"
        assert _transient_reason(busy) == (
            "blob storage temporarily unavailable (HTTP 503, ServerBusy)"
        )
        throttled = HttpResponseError(message="slow down", response=MagicMock(status_code=429))
        assert _transient_reason(throttled).startswith(
            "blob storage temporarily unavailable (HTTP 429"
        )
        assert _transient_reason(ConnectionRefusedError("refused")).startswith(
            "connection error (ConnectionRefusedError: refused"
        )

    def test_not_classified(self) -> None:
        assert _transient_reason(ValueError("x")) is None
        assert _transient_reason(KeyError("x")) is None
        conflict = HttpResponseError(message="conflict", response=MagicMock(status_code=409))
        assert _transient_reason(conflict) is None

    def test_credentials_in_the_text_are_masked(self) -> None:
        reason = _transient_reason(ConnectionError("amqp://user:s3cret@host:5672/ refused"))
        assert "s3cret" not in reason and "amqp://***@host" in reason


class TestBlobLayerTransientLine:
    async def test_transient_storage_failure_is_info_without_traceback(self, caplog) -> None:
        from kbm_ledsas_sdk.blob.azure_client import AzureBlobClient
        from kbm_ledsas_sdk.models import BlobRef

        client = AzureBlobClient(AZURITE_CONN, "dev")
        blob_client = MagicMock()
        blob_client.download_blob = AsyncMock(side_effect=_storage_unreachable())
        client.service_client = MagicMock()
        client.service_client.get_blob_client.return_value = blob_client
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            with pytest.raises(ServiceRequestError):
                await client.download_blob(BlobRef(uri="azblob://dev/in/data.csv"))
        records = [r for r in caplog.records if r.name == "kbm_ledsas_sdk.blob.azure_client"]
        failures = [r for r in records if "Failed to download blob" in r.getMessage()]
        assert len(failures) == 1
        assert failures[0].levelno == logging.INFO and failures[0].exc_info is None
        assert failures[0].transient is True

    async def test_unexpected_storage_failure_keeps_error_and_traceback(self, caplog) -> None:
        from kbm_ledsas_sdk.blob.azure_client import AzureBlobClient
        from kbm_ledsas_sdk.models import BlobRef

        client = AzureBlobClient(AZURITE_CONN, "dev")
        blob_client = MagicMock()
        blob_client.download_blob = AsyncMock(side_effect=RuntimeError("bug"))
        client.service_client = MagicMock()
        client.service_client.get_blob_client.return_value = blob_client
        with caplog.at_level(logging.DEBUG, logger="kbm_ledsas_sdk"):
            with pytest.raises(RuntimeError):
                await client.download_blob(BlobRef(uri="azblob://dev/in/data.csv"))
        failures = [r for r in caplog.records if r.getMessage() == "Failed to download blob"]
        assert len(failures) == 1
        assert failures[0].levelno == logging.ERROR and failures[0].exc_info is not None
