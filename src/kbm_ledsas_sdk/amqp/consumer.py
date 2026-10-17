"""
Consumes LEDSAS commands from RabbitMQ.
"""

import asyncio
import json
import logging
import re
import time
from collections import deque
from collections.abc import Awaitable
from typing import Any, TypeVar

from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aio_pika.exceptions import (
    CONNECTION_EXCEPTIONS,
    AMQPChannelError,
    DeliveryError,
    MessageProcessError,
)
from aiormq.exceptions import AMQPConnectionError, ChannelInvalidStateError
from pydantic import ValidationError

from kbm_ledsas_sdk.models.messages import Command
from kbm_ledsas_sdk.transport.base import DeliveryLost

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Exception classes that mean "the broker could not be reached" when they
# come out of a shutdown round trip: aio-pika's own definition of a
# connection-level failure (AMQP errors, ConnectionError/OSError, the
# RuntimeError aiormq raises for an operation on a closed connection) plus
# a bounded wait running out. These are an expected operational condition
# during shutdown, never a defect, so they are reported without a traceback.
BROKER_UNREACHABLE_ERRORS = (asyncio.TimeoutError, *CONNECTION_EXCEPTIONS)


# Operations abandoned by await_bounded(). The event loop only keeps weak
# references to tasks, so an abandoned task that nothing else references
# could be garbage-collected while pending (asyncio then logs "Task was
# destroyed but it is pending!" at ERROR). Hold them until they finish.
_ABANDONED: "set[asyncio.Future[Any]]" = set()


def _consume_outcome(task: "asyncio.Future[Any]") -> None:
    """Retrieve an abandoned task's outcome so asyncio never logs it."""
    _ABANDONED.discard(task)
    if not task.cancelled():
        task.exception()


def _abandon(task: "asyncio.Future[Any]", *, cancel: bool) -> None:
    """Stop waiting for ``task``: keep it referenced, consume its outcome."""
    if cancel:
        task.cancel()
    _ABANDONED.add(task)
    task.add_done_callback(_consume_outcome)


def is_broker_unreachable(error: BaseException) -> bool:
    """True when ``error`` means the broker could not be reached or did not
    answer, as opposed to a reachable broker refusing the operation.

    A channel-level refusal (``AMQPChannelError``, e.g. a missing exchange)
    and a publish the broker returned or did not confirm (``DeliveryError``)
    are answers from a reachable broker; they keep their own handling. A
    message that was already acknowledged or rejected (``MessageProcessError``)
    is a local state error, not an outage.
    """
    if isinstance(error, (AMQPChannelError, DeliveryError, MessageProcessError)):
        return False
    return isinstance(error, BROKER_UNREACHABLE_ERRORS)


def _channel_closed(message: Any) -> bool:
    """True when ``message`` arrived on a channel that is closed now.

    aio-pika's ``IncomingMessage.channel`` raises ``ChannelInvalidStateError``
    once that channel is closed, so the error itself is the answer.
    """
    if message is None:
        return False
    try:
        channel = message.channel
    except ChannelInvalidStateError:
        return True
    except Exception:
        return False
    return getattr(channel, "is_closed", False) is True


def is_redelivery_of(pending: Any, message: Any) -> bool:
    """True when ``message`` is the broker's redelivery of ``pending``, a
    command with the same ``message_id`` that is still in progress here
    (its handler running, or the command waiting for a worker).

    The broker only delivers a message again once the delivery it holds
    unacknowledged has been released: the channel or the connection it was
    delivered on closed (for example a broker restart, or a dropped
    connection that the client re-established). So the proof is that the
    pending command's own delivery sits on a channel that is closed now,
    and the new delivery carries the same body. The ``redelivered`` flag is
    not required: it is reported, but whether a broker keeps it across its
    own restart depends on the queue type.

    A second command with the same ``message_id`` that arrives while the
    original's delivery is still live is a true duplicate (a sender bug or a
    second copy of the message), and so is one whose body differs.
    """
    if not _channel_closed(pending):
        return False
    old_body = getattr(pending, "body", None)
    new_body = getattr(message, "body", None)
    if not isinstance(old_body, (bytes, bytearray)) or not isinstance(new_body, (bytes, bytearray)):
        return False
    return bytes(old_body) == bytes(new_body)


def delivery_connection_gone(error: BaseException, message: Any = None) -> bool:
    """True when ``error`` (from settling ``message`` or publishing its
    reply) means the broker connection or the message's channel is gone,
    as opposed to a broker refusal or a local defect.

    Connection-level errors count (``is_broker_unreachable``), including
    the client library's error for an operation on a closed channel. A
    plain ``RuntimeError`` only counts when the message's channel is
    actually closed, so an unrelated defect never makes a command look
    redelivered.
    """
    if not is_broker_unreachable(error):
        return False
    if isinstance(error, (ConnectionError, OSError, asyncio.TimeoutError)):
        return True
    if isinstance(error, ChannelInvalidStateError):
        return True
    if isinstance(error, AMQPConnectionError):
        return True
    return _channel_closed(message)


class BrokerNotReachable(ConnectionError):
    """A broker round trip was skipped: shutdown is in progress and the
    broker has already been found unreachable."""


class BrokerShutdownGuard:
    """Bounds the broker round trips made after shutdown has begun.

    Shared by the transport and its publisher. Shutdown begins when the
    service stops taking new work, before the commands still in flight
    are drained, so it also covers the reply and status publishes those
    commands make (v0.3.5).

    - Before shutdown, a round trip is awaited without a time limit: a slow
      or flow-controlled broker is legitimate back-pressure in normal
      operation, and nothing about that changes.
    - Once shutdown has begun, a round trip that is already waiting, or any
      new one, gets at most ``step_timeout`` more seconds.
    - The first round trip during shutdown that fails because the broker is
      unreachable (no answer in time, or a connection-level error) latches
      ``broker_unreachable``. From then on round trips fail fast with
      ``BrokerNotReachable`` instead of waiting again, and the cause is
      kept for the single shutdown report.
    """

    def __init__(self, step_timeout: float = 5.0) -> None:
        self.step_timeout = step_timeout
        self.stopping = False
        self.broker_unreachable = False
        self.cause: BaseException | None = None
        self._stopping_signal: asyncio.Future[None] | None = None

    def begin_shutdown(self) -> None:
        """Start bounding round trips, including those already waiting."""
        self.stopping = True
        signal = self._stopping_signal
        if signal is not None and not signal.done():
            signal.set_result(None)

    def mark_unreachable(self, error: BaseException | None) -> None:
        """Record that the broker is unreachable; the first cause is kept
        (``None``: not known yet, the next error recorded is the cause)."""
        if not self.broker_unreachable:
            self.broker_unreachable = True
            self.cause = error
        elif self.cause is None:
            self.cause = error

    @property
    def fail_fast(self) -> bool:
        """True once shutdown has begun AND the broker is known unreachable."""
        return self.stopping and self.broker_unreachable

    def check(self) -> None:
        """Raise ``BrokerNotReachable`` when round trips must fail fast."""
        if self.fail_fast:
            raise BrokerNotReachable(
                "AMQP broker not reachable during shutdown; round trip skipped"
            )

    def _signal(self) -> "asyncio.Future[None]":
        loop = asyncio.get_running_loop()
        signal = self._stopping_signal
        if signal is None or signal.get_loop() is not loop:
            signal = loop.create_future()
            if self.stopping:
                signal.set_result(None)
            self._stopping_signal = signal
        return signal

    async def round_trip(self, awaitable: Awaitable[T]) -> T:
        """Await one broker round trip under the rules above.

        Raises:
            BrokerNotReachable: Shutdown is in progress and the broker was
                already found unreachable; ``awaitable`` was not awaited.
            asyncio.TimeoutError: Shutdown is in progress and the round trip
                did not finish within ``step_timeout``.
            Exception: Whatever the round trip itself raised.
        """
        if self.fail_fast:
            if asyncio.iscoroutine(awaitable):
                awaitable.close()  # never started; avoid "never awaited"
            self.check()
        try:
            if self.stopping:
                return await await_bounded(awaitable, self.step_timeout)
            task = asyncio.ensure_future(awaitable)
            try:
                await asyncio.wait(
                    {task, self._signal()},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except asyncio.CancelledError:
                _abandon(task, cancel=True)
                raise
            if task.done():
                return task.result()
            # Shutdown began while this round trip was waiting.
            return await await_bounded(task, self.step_timeout)
        except Exception as error:
            if self.stopping and is_broker_unreachable(error):
                self.mark_unreachable(error)
            raise


async def await_bounded(awaitable: Awaitable[T], timeout: float | None) -> T:
    """Await ``awaitable`` for at most ``timeout`` seconds (``None``: no bound).

    Unlike ``asyncio.wait_for``, an operation that runs out of time is
    neither cancelled nor waited for here: it is left pending and the
    caller moves on. Cancelling it at this point would run the AMQP
    client's cancellation cleanup while the channel is still open, which
    can itself need the broker (queueing a channel close behind a stalled
    socket) and makes the client log its own warning for a decision the
    SDK already reports. Once the caller has closed the connection, the
    pending operation is either failed by the client or cancelled with the
    event loop's remaining tasks at interpreter shutdown, quietly, because
    its channel is closed by then. Its outcome is consumed, so it never
    surfaces later as an "exception was never retrieved" log record.

    Raises:
        asyncio.TimeoutError: The operation did not finish within ``timeout``.
        Exception: Whatever the operation itself raised in time.
    """
    if timeout is None:
        return await awaitable
    task = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    except asyncio.CancelledError:
        _abandon(task, cancel=True)
        raise
    if task in done:
        return task.result()
    _abandon(task, cancel=False)
    raise TimeoutError(f"operation did not complete within {timeout:g} s")


# Deepest nesting of arrays/objects a command body may have (the body's
# outermost object is level 1, its "payload" object level 2). A deeper body
# is dead-lettered when it arrives, before its envelope is validated or a
# handler sees it: one ERROR line, no traceback, no retry, no reply.
#
# The limit is explicit so that the outcome does not depend on the Python
# version. json.loads recurses on nested structures and gives up at a depth
# that depends on the interpreter and on the current stack depth: about
# 1000 levels on Python 3.11, about 10000 on 3.13. Without this limit, a
# body nested between those depths was handed to the handler on one
# interpreter and dead-lettered on the other.
#
# Depth is measured on the parsed body: when a key appears twice, only the
# kept (last) value counts. A discarded value is still read by json.loads,
# so one nested past the parser's own limit (above) still makes it give up.
#
# Cost: a body with at most this many "[" and "{" characters cannot nest
# past the limit, so it is not walked (_may_nest_deeper_than: two C-speed
# counts). Any other body is walked once after parsing, on the event loop
# like the parse itself. The walk holds at most one iterator per level and
# is linear in the number of values in the body (array elements and object
# members, numbers and strings included): measured at about 30 ns per
# number or string and 100 to 120 ns per array or object. Per MiB that is
# about 7 to 8 ms for ordinary records, up to about 20 ms for a flat array of
# small numbers, about 40 ms on Python 3.13 and up to 55 ms on 3.11 for a
# body made of empty arrays/objects, and up to about 60 and 85 ms for a
# body of 255-level chains. For a body at the default 16 MiB payload cap
# that is up to about 1 s on 3.13 and 1.3 s on 3.11; for a body made almost
# entirely of empty objects it is several times the parse (about 5x on
# 3.13, 6x to 10x on 3.11). The times vary from run to run and with the
# hardware. Large data belongs in a blob.
MAX_COMMAND_NESTING_DEPTH = 256

# The ``error`` field of the ERROR line for a body nested too deeply. The
# same text whether the limit above or json.loads itself caught the body,
# so the record is identical on every supported interpreter.
_TOO_DEEP = f"JSON nested more than {MAX_COMMAND_NESTING_DEPTH} levels deep"


class _BodyTooDeepError(Exception):
    """A command body nests arrays/objects more than
    MAX_COMMAND_NESTING_DEPTH levels deep. Raised and handled inside
    AMQPConsumer._on_message only, so both ways of detecting such a body
    (json.loads giving up, or the walk after parsing) share one handler:
    one ERROR line, one reject."""


class _RejectOnArrivalError(Exception):
    """A message that is dead-lettered on arrival after its one log line
    (body too large, envelope type not "command", duplicate message_id,
    pending-messages cap). Raised and handled inside
    AMQPConsumer._on_message only, so the reject runs in its own except
    clause, outside the try: a reject that fails (for example because the
    connection is lost at that moment) propagates once, like the reject of
    the parse-failure branches, instead of reaching the generic branch,
    which would log a second record and reject the delivery again."""


def _may_nest_deeper_than(body: str, limit: int) -> bool:
    """False if JSON text ``body`` has too few ``[`` and ``{`` characters
    to nest arrays/objects more than ``limit`` levels deep.

    Every level opens with one of them, and any inside strings only add to
    the count, so a False is exact. Two C-speed counts, so most bodies need
    no walk of the parsed value.
    """
    return body.count("[") + body.count("{") > limit


def _nesting_depth_exceeds(value: Any, limit: int) -> bool:
    """True if parsed JSON ``value`` nests arrays/objects more than
    ``limit`` levels deep (an outermost array or object is level 1).

    Iterative, with one iterator per open level, so it neither recurses nor
    holds more than ``limit`` iterators, whatever the body's size or depth.
    """
    if not isinstance(value, (dict, list)):
        return False
    stack = [iter(value.values() if isinstance(value, dict) else value)]
    while stack:
        for child in stack[-1]:
            if isinstance(child, (dict, list)):
                if len(stack) >= limit:
                    return True
                stack.append(iter(child.values() if isinstance(child, dict) else child))
                break
        else:
            stack.pop()
    return False


def _delivery_fields(message: Any) -> dict[str, Any]:
    """Log fields that identify a delivery whose body cannot be used.

    Taken from the message's AMQP ``message_id`` / ``correlation_id``
    properties (when set; at most 128 characters each) and the body's size
    in bytes, without reading the body, so an operator can find the
    message in the dead-letter queue.
    """
    fields: dict[str, Any] = {}
    for key, attr in (
        ("amqp_message_id", "message_id"),
        ("amqp_correlation_id", "correlation_id"),
    ):
        value = getattr(message, attr, None)
        if isinstance(value, str) and value:
            fields[key] = value[:128]
    body = getattr(message, "body", None)
    if isinstance(body, (bytes, bytearray)):
        fields["body_bytes"] = len(body)
    return fields


# An envelope id quoted in a log record of a command whose envelope failed
# validation: the characters a message_id / correlation_id can hold (UUIDs,
# tokens), at most 128 of them. Anything else is left out of the record.
_LOGGABLE_ID = re.compile(r"^[A-Za-z0-9_\-:.]{1,128}$")


def _envelope_ids(data: Any) -> dict[str, Any]:
    """The body's ``envelope.message_id`` / ``envelope.correlation_id``,
    as log fields, when the body could be parsed and they are readable ids,
    even though another envelope field failed validation (the dead-lettered
    body's envelope then matches its record by ``correlation_id``, as for
    every other dead letter)."""
    envelope = data.get("envelope") if isinstance(data, dict) else None
    if not isinstance(envelope, dict):
        return {}
    fields: dict[str, Any] = {}
    for key in ("message_id", "correlation_id"):
        value = envelope.get(key)
        if isinstance(value, str) and _LOGGABLE_ID.fullmatch(value):
            fields[key] = value
    return fields


class AMQPConsumer:
    """
    Consumes commands from RabbitMQ queue.

    Consumes the LEDSAS commands of this service from its RabbitMQ
    queue.

    Features:
    - Manual message acknowledgment (no auto-ack)
    - Pushes commands to asyncio.Queue for async iteration
    - Tracks pending messages for ACK/NACK correlation
    - JSON parsing with validation

    Attributes:
        queue: RabbitMQ queue to consume from
        command_queue: Internal asyncio.Queue for commands
        pending_messages: Maps message_id -> AMQP IncomingMessage
        consumer_tag: RabbitMQ consumer tag (set when started)
    """

    # Class-level defaults, so a consumer built without ``__init__`` (e.g.
    # ``AMQPConsumer.__new__`` in a test) settles messages as before.
    _shutdown_guard: BrokerShutdownGuard | None = None
    unsettled_at_shutdown: int = 0
    settled_while_stopping: int = 0
    redeliveries_adopted: int = 0
    _finishing: "set[str] | frozenset[str]" = frozenset()
    # Command body reads (decode, parse, nesting check, validation) that
    # held the event loop at least _SLOW_BODY_READ_S, as (seconds, body
    # bytes), for the event-loop watchdog (see take_slow_body_reads). At
    # most _SLOW_BODY_READS_KEPT are kept, the oldest dropped.
    _SLOW_BODY_READ_S = 0.05
    _SLOW_BODY_READS_KEPT = 32
    _slow_body_reads: "deque[tuple[float, int]] | None" = None

    def __init__(
        self,
        queue: AbstractQueue,
        prefetch_count: int = 10,
        max_pending_multiplier: int = 10,
        max_payload_bytes: int = 16 * 1024 * 1024,
        shutdown_guard: BrokerShutdownGuard | None = None,
    ):
        """
        Initialize AMQP consumer.

        Args:
            queue: RabbitMQ queue to consume from
            prefetch_count: Number of messages to prefetch
            max_pending_multiplier: Cap on ``pending_messages`` dict
                size, expressed as a multiple of ``prefetch_count``.
                A consumer with prefetch=10 and multiplier=10 caps at
                100 in-flight entries — well above legitimate steady
                state, low enough that a hostile sender flooding
                distinct message_ids can't grow it without bound.
            max_payload_bytes: Reject messages whose body exceeds this
                size (DLQ them, single WARNING line). 0 disables the
                check. Default 16 MiB — large payloads should be
                shipped via blob storage, not AMQP body.
            shutdown_guard: Optional guard shared with the transport. Once
                the transport begins shutting down, an ACK / NACK / reject
                is bounded by it, and one that cannot reach the broker
                leaves the message unacknowledged (the broker redelivers
                it) instead of failing with an ERROR (see ``ack``). Without
                a guard, or before shutdown, behavior is unchanged.
        """
        self.queue = queue
        self.prefetch_count = prefetch_count
        self.max_pending = max(prefetch_count * max_pending_multiplier, prefetch_count)
        self.max_payload_bytes = max_payload_bytes
        self._shutdown_guard = shutdown_guard

        # Commands whose ACK / requeue / dead-letter could not reach the
        # broker after shutdown began. They stay unacknowledged (and in
        # pending_messages), so the broker redelivers them; the transport
        # reports the count once at stop().
        self.unsettled_at_shutdown: int = 0

        # ACKs / NACKs / rejects written after shutdown began. The broker
        # never answers one, so if it then turns out not to answer at all
        # (frozen rather than disconnected) they may not have reached it:
        # the transport's shutdown report counts them.
        self.settled_while_stopping: int = 0

        # Redeliveries of a command that was still in progress here when
        # they arrived (its delivery's connection had been lost and
        # restored): that command's outcome settles them instead of a
        # second run. Each one logs an INFO line; the total is reported on
        # the consumer's stop record.
        self.redeliveries_adopted: int = 0

        # Internal queue for commands (consumed by Transport.subscribe())
        self.command_queue: asyncio.Queue[Command] = asyncio.Queue()

        # Track pending messages: message_id -> IncomingMessage
        self.pending_messages: dict[str, AbstractIncomingMessage] = {}

        # message_ids of pending commands that have finished: their one
        # reply is being sent, and their ACK / dead-letter follows (see
        # mark_finishing). A command that reuses such a message_id waits
        # for that settlement instead of being dead-lettered as a
        # duplicate.
        self._finishing: set[str] = set()

        # Retry attempt count per message_id. Incremented on every
        # nack(requeue=True) and cleared on ack() / nack(requeue=False).
        # app.py reads this via DirectTransport.get_retry_count() to
        # enforce KBM_LEDSAS_MAX_RETRIES. In-process only — survives
        # requeue cycles in the same consumer but resets across
        # consumer restarts (a restart is a fresh attempt from the
        # SDK's POV; the broker doesn't track per-message retry counts
        # for simple requeue, only for full DLX cycles).
        self.retry_counts: dict[str, int] = {}

        # Consumer tag (set when started)
        self.consumer_tag: str | None = None

        # Why the last stop() could not confirm the cancel with the broker
        # (None: confirmed, or the cancel was skipped). Read by the transport
        # for its single shutdown report.
        self.stop_error: BaseException | None = None

        # True once stop_consuming() had the broker confirm the cancel:
        # stop() then does not send it again.
        self.consuming_stopped = False

    async def start(self) -> None:
        """
        Start consuming messages from the queue.

        Sets up consumer with manual acknowledgment and prefetch.

        Raises:
            Exception: If consumer startup fails
        """
        logger.info(
            "Starting AMQP consumer",
            extra={
                "queue": self.queue.name,
                "prefetch": self.prefetch_count,
            },
        )

        try:
            # Set prefetch count (QoS)
            await self.queue.channel.set_qos(prefetch_count=self.prefetch_count)

            # Start consuming with manual ack
            self.consumer_tag = await self.queue.consume(
                callback=self._on_message,
                no_ack=False,  # Manual acknowledgment
            )

            logger.info("AMQP consumer started", extra={"consumer_tag": self.consumer_tag})

        except Exception as e:
            logger.error("Failed to start AMQP consumer", exc_info=True, extra={"error": str(e)})
            raise

    async def stop_consuming(
        self, timeout: float | None = None, *, skip_cancel: bool = False
    ) -> list[Command]:
        """Stop receiving new messages, keeping the received ones settleable.

        Sends the cancel (Basic.Cancel, bounded by ``timeout``) but keeps the
        channel open and ``pending_messages`` intact, so the commands still
        running can reply and be acknowledged. From then on the broker
        delivers this queue's messages, including any given back below or
        later, to the other consumers only. Then every command received but
        not yet handed to the service (still in ``command_queue``) is given
        back (requeued without counting a retry).

        The cancel is skipped when ``skip_cancel`` is set, the channel is
        closed or the shutdown guard already found the broker unreachable.
        When the broker does not answer in time (or the connection is gone)
        during shutdown, that is latched on the guard, so the rest of the
        shutdown does not wait for the broker again; any other failure is
        logged at DEBUG and ``stop()`` tries the cancel again. Never raises.

        Returns:
            The commands given back.
        """
        guard = self._shutdown_guard
        if self.consumer_tag and not self.consuming_stopped:
            channel = getattr(self.queue, "channel", None)
            if not (
                skip_cancel
                or getattr(channel, "is_closed", False) is True
                or (guard is not None and guard.fail_fast)
            ):
                try:
                    await await_bounded(self.queue.cancel(self.consumer_tag), timeout)
                    self.consuming_stopped = True
                    logger.info(
                        "AMQP consumer no longer receives new commands",
                        extra={"pending_messages": len(self.pending_messages)},
                    )
                except Exception as e:
                    if guard is not None and guard.stopping and is_broker_unreachable(e):
                        guard.mark_unreachable(e)
                    logger.debug(
                        "Cancel of the AMQP consumer not confirmed",
                        exc_info=True,
                    )
        given_back: list[Command] = []
        while True:
            try:
                command = self.command_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            await self.release(command.envelope.message_id)
            given_back.append(command)
        return given_back

    async def stop(self, timeout: float | None = None, *, skip_cancel: bool = False) -> bool:
        """
        Stop consuming messages.

        Cancels the consumer and clears pending messages.

        The cancel is a broker round trip (Basic.Cancel -> Basic.CancelOk).
        On a robust aio-pika channel it first waits for the connection to
        be up, so during a broker outage it would wait for a reconnection
        that may never come. Hence (v0.3.5):

        - if the consumer's channel is already closed (the connection to
          the broker was lost), or the caller already knows the broker is
          unreachable (``skip_cancel``), the cancel is skipped: the broker
          drops this consumer together with the connection;
        - otherwise the cancel is bounded by ``timeout`` seconds
          (``None`` waits indefinitely).

        Args:
            timeout: Upper bound, in seconds, for the cancel round trip.
            skip_cancel: Do not contact the broker at all; abandon the
                consumer locally.

        Returns:
            True when the broker confirmed the cancel, or there was no
            consumer to cancel. False when the broker could not be reached
            (cancel skipped, cancel not answered within ``timeout``, or a
            connection-level error): the consumer is then abandoned locally
            and logged at INFO only, because the caller reports the
            unreachable broker once for the whole shutdown. The reason is
            kept in ``stop_error`` (None when the cancel was skipped).
            Unacknowledged messages are redelivered by the broker once the
            connection is gone.
        """
        self.stop_error = None
        if not self.consumer_tag:
            return True

        logger.info("Stopping AMQP consumer")

        # ``is True``: only a real aio-pika channel answers with a bool;
        # anything else (e.g. a test double) falls through to the cancel.
        channel = getattr(self.queue, "channel", None)
        if skip_cancel or getattr(channel, "is_closed", False) is True:
            confirmed = False
        elif self.consuming_stopped:
            confirmed = True  # stop_consuming() already had it confirmed
        else:
            try:
                await await_bounded(self.queue.cancel(self.consumer_tag), timeout)
                confirmed = True
            except BROKER_UNREACHABLE_ERRORS as e:
                self.stop_error = e
                confirmed = False
            except Exception as e:
                logger.error("Error stopping AMQP consumer", exc_info=True, extra={"error": str(e)})
                return True

        self.consumer_tag = None

        # Reported on the stop record (no metrics backend): redeliveries of a
        # command still in progress here, settled by that command's outcome.
        if not confirmed:
            abandoned = len(self.pending_messages)
            self.pending_messages.clear()
            self._finishing = set()
            logger.info(
                "AMQP consumer stopped without broker confirmation " "(broker not reachable)",
                extra={
                    "pending_messages": abandoned,
                    "redeliveries_adopted": self.redeliveries_adopted,
                },
            )
            return False

        # Log pending messages (they will be requeued by RabbitMQ)
        if self.pending_messages:
            logger.warning(
                f"Consumer stopped with {len(self.pending_messages)} "
                "pending messages (will be requeued)"
            )

        # Clear pending messages
        self.pending_messages.clear()
        self._finishing = set()

        logger.info(
            "AMQP consumer stopped",
            extra={"redeliveries_adopted": self.redeliveries_adopted},
        )
        return True

    def _note_body_read(self, seconds: float, size: int) -> None:
        """Record a command body read of ``seconds`` (``size`` bytes) when
        it held the event loop at least ``_SLOW_BODY_READ_S``."""
        if seconds < self._SLOW_BODY_READ_S:
            return
        if self._slow_body_reads is None:
            self._slow_body_reads = deque(maxlen=self._SLOW_BODY_READS_KEPT)
        self._slow_body_reads.append((seconds, size))

    def take_slow_body_reads(self) -> "list[tuple[float, int]]":
        """The ``(seconds, body bytes)`` of the slow command body reads
        recorded since the last call (see ``_note_body_read``), and forget
        them."""
        if not self._slow_body_reads:
            return []
        reads = list(self._slow_body_reads)
        self._slow_body_reads.clear()
        return reads

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """
        Handle incoming AMQP message.

        Parses JSON body to Command and pushes to internal queue.

        Args:
            message: Incoming AMQP message
        """
        # The parsed body, once json.loads succeeded (the envelope ids in
        # a validation failure's record are read from it).
        data: Any = None
        try:
            # Payload size limit. Large payloads should travel
            # via blob storage, not the AMQP body. Reject oversize
            # messages straight to DLQ with a single WARNING — no
            # decode, no validate, bounded RSS pressure.
            if self.max_payload_bytes and len(message.body) > self.max_payload_bytes:
                logger.warning(
                    "Payload exceeds KBM_LEDSAS_MAX_PAYLOAD_BYTES; "
                    "dead-lettering (body=%d, max=%d). Use blob storage "
                    "for large data, not the AMQP body.",
                    len(message.body),
                    self.max_payload_bytes,
                    extra=_delivery_fields(message),
                )
                raise _RejectOnArrivalError

            # Deserialize message body to Command. Decoding, parsing, the
            # nesting check and the validation run on the event loop; a
            # slow one is recorded for the event-loop watchdog.
            read_started = time.perf_counter()
            try:
                body = message.body.decode("utf-8")
                try:
                    data = json.loads(body)
                except RecursionError:
                    # json.loads recurses on nested arrays/objects and, with
                    # the interpreter's default limits, gives up only far
                    # deeper than MAX_COMMAND_NESTING_DEPTH (about 1000 levels
                    # on Python 3.11, about 10000 on 3.13), so the body is too
                    # deep: handled below exactly like a body the nesting
                    # limit catches after parsing. If the body cannot nest
                    # past the limit, its depth did not cause the error (the
                    # recursion limit was lowered far below its default, or
                    # the call stack was already hundreds of frames deep): the
                    # error keeps its own text in the generic branch below. A
                    # body with more "[" / "{" than the limit is reported as
                    # nested too deeply, which with default limits it always is.
                    if not _may_nest_deeper_than(body, MAX_COMMAND_NESTING_DEPTH):
                        raise
                    raise _BodyTooDeepError from None

                # Nesting limit (MAX_COMMAND_NESTING_DEPTH), checked before the
                # envelope is validated or a handler sees the command.
                if _may_nest_deeper_than(
                    body, MAX_COMMAND_NESTING_DEPTH
                ) and _nesting_depth_exceeds(data, MAX_COMMAND_NESTING_DEPTH):
                    raise _BodyTooDeepError

                command = Command.model_validate(data)
            finally:
                self._note_body_read(time.perf_counter() - read_started, len(message.body))

            # Enforce envelope.type=="command" on the command
            # exchange. The Envelope schema's Literal accepts four type
            # values (command/response/status/error) because the same
            # model is reused for outbound responses and status updates,
            # but only "command" is legitimate on a *command* queue. A
            # forged response/status/error envelope reaching this queue
            # is either a misconfigured upstream or a hostile reflection
            # attempt — dead-letter it without invoking the handler.
            envelope_type = command.envelope.type
            if envelope_type != "command":
                logger.warning(
                    "Envelope type %r is not 'command' on command " "exchange; dead-lettering",
                    envelope_type,
                    extra={
                        "message_id": command.envelope.message_id,
                        "envelope_type": envelope_type,
                    },
                )
                raise _RejectOnArrivalError

            message_id = command.envelope.message_id

            # Duplicate-id guard. A second delivery with the message_id of a
            # command that is still in progress here (its handler running,
            # or the command waiting for a worker) is one of two things:
            #
            # - The broker's redelivery of that same command (v0.3.5). The
            #   connection it was delivered on was lost while the command
            #   was in progress and has been restored, so the broker
            #   delivered the unacknowledged command again. The original
            #   delivery can no longer be acknowledged (its channel is
            #   closed); the new one can. Hand the new delivery to the
            #   command already in progress: its outcome (ACK, requeue or
            #   dead-letter, and its one reply) settles the new delivery,
            #   so the command does not run twice and is not dead-lettered.
            #   One INFO line.
            # - A true duplicate: a sender reused the message_id, or a
            #   second copy of the message arrived, while the original's
            #   delivery is still live. Overwriting the entry would make the
            #   original un-ack-able, so the duplicate is dead-lettered at
            #   once with one WARNING; the broker's x-death header records it.
            pending = self.pending_messages.get(message_id)
            if (
                pending is not None
                and message_id in self._finishing
                and not is_redelivery_of(pending, message)
            ):
                # The command with this message_id has finished and its
                # reply is being sent, but its delivery is not settled
                # yet: a sender that reuses the id as soon as it has the
                # reply would otherwise be dead-lettered without a reply.
                # Wait (bounded) for that settlement; then this command is
                # a new one (the API reference: once the first command has
                # finished, its message_id is accepted again).
                pending = await self._await_settled(message_id, pending)
            if pending is not None:
                if is_redelivery_of(pending, message):
                    self.pending_messages[message_id] = message
                    self.redeliveries_adopted += 1
                    logger.info(
                        "Redelivered command arrived while it is still in "
                        "progress here (running or waiting for a worker; its "
                        "first delivery was on a broker connection that was "
                        "lost); its outcome settles the redelivery, so the "
                        "command does not run twice",
                        extra={
                            "message_id": message_id,
                            "correlation_id": command.envelope.correlation_id,
                            "command_name": command.envelope.name,
                            "redelivered": bool(getattr(message, "redelivered", False)),
                        },
                    )
                    return
                logger.warning(
                    "Duplicate message_id while original still pending; "
                    "rejecting duplicate to DLQ",
                    extra={"message_id": message_id},
                )
                raise _RejectOnArrivalError

            # Cap pending_messages to bound memory. A hostile sender
            # can otherwise grow it without limit by streaming distinct
            # message_ids that crash before ack/nack. The cap is well
            # above legitimate steady state (prefetch × multiplier).
            if len(self.pending_messages) >= self.max_pending:
                logger.warning(
                    "pending_messages dict at cap; rejecting new message to DLQ "
                    "(in-flight=%d, cap=%d). Investigate stuck handlers.",
                    len(self.pending_messages),
                    self.max_pending,
                    extra={"message_id": message_id},
                )
                raise _RejectOnArrivalError

            self.pending_messages[message_id] = message

            logger.info(
                "Received command",
                extra={
                    "message_id": message_id,
                    "correlation_id": command.envelope.correlation_id,
                    "command_name": command.envelope.name,
                },
            )

            # Push to internal queue for Transport.subscribe()
            await self.command_queue.put(command)

        except _RejectOnArrivalError:
            # Dead-lettered on arrival; its one line is already logged above.
            # The reject runs here, outside the try, so a reject that fails
            # propagates once instead of reaching the generic branch below
            # (a second record and a second reject of the same delivery).
            await message.reject(requeue=False)

        except UnicodeDecodeError as e:
            # The body isn't valid UTF-8 (e.g. a stray 0xff byte, or a
            # binary payload published to a text queue). This is the very
            # first step — ``message.body.decode("utf-8")`` — and it is
            # deterministic: the same bytes always fail the same way, so
            # requeueing would hot-loop. Dead-letter immediately.
            #
            # exc_info=False — like the JSONDecodeError branch, the error
            # message (codec + byte offset) is fully self-contained; a
            # traceback adds no diagnostic value and only amplifies
            # log volume when an attacker floods the queue with binary
            # bodies. Without this branch the decode error falls through
            # to the generic catch-all below (exc_info=True), which is the
            # one traceback that breaks the zero-traceback invariant.
            logger.error(
                "Message body is not valid UTF-8; dead-lettering",
                extra={"error": str(e), **_delivery_fields(message)},
            )
            await message.reject(requeue=False)

        except json.JSONDecodeError as e:
            # Deterministic parse failure: same input → same error. No
            # point retrying. Dead-letter immediately.
            #
            # exc_info=False — the decoder error message is fully
            # self-contained; a Python traceback adds nothing
            # actionable here and contributes to log-noise on
            # malformed-message floods.
            logger.error(
                "Failed to parse message JSON; dead-lettering",
                extra={"error": str(e), **_delivery_fields(message)},
            )
            await message.reject(requeue=False)

        except _BodyTooDeepError:
            # A body nested more than MAX_COMMAND_NESTING_DEPTH levels
            # deep, whether json.loads gave up on it or the walk after
            # parsing caught it: one line and one reject either way (also
            # when the reject itself fails), so the outcome does not depend
            # on which check caught the body, or on the interpreter.
            # Classified explicitly (rather than lumped into the generic
            # "Unexpected error" catch-all below) so operators can alert on
            # the pattern; it is a well-known DoS attack signature.
            #
            # No traceback: for a body json.loads gave up on, the
            # traceback repeats the same decoder frames and adds no
            # diagnostic value beyond "the body is nested too deeply".
            self._log_excessive_nesting(message)
            await message.reject(requeue=False)

        except ValidationError as e:
            # Pydantic envelope validation failed (missing fields, wrong
            # types, etc.). This is deterministic: requeueing would just
            # produce the same error forever and the consumer would burn
            # CPU + disk in a hot loop. Dead-letter immediately.
            #
            # Strip pydantic-docs URL noise from the logged error —
            # walk e.errors() like app.py's config-error path so the
            # log line stays short.
            msgs = []
            for err in e.errors():
                loc = ".".join(str(p) for p in err.get("loc", ()))
                msg = err.get("msg", "")
                if msg.startswith("Value error, "):
                    msg = msg[len("Value error, ") :]
                msgs.append(f"{loc}: {msg}" if loc else msg)
            error_text = "; ".join(msgs) if msgs else str(e)
            logger.error(
                "Envelope failed schema validation; dead-lettering",
                extra={
                    "error": error_text,
                    **_delivery_fields(message),
                    **_envelope_ids(data),
                },
            )
            await message.reject(requeue=False)

        except ValueError as e:
            # json.loads raises a plain ValueError (not JSONDecodeError)
            # for an integer literal longer than the interpreter's
            # int-conversion limit (4300 digits by default). Same input,
            # same error on every delivery: dead-letter it with one line
            # and no traceback. The interpreter's own message suggests
            # raising that limit, which is the wrong fix for hostile or
            # broken input, so it is not repeated. Any other ValueError
            # at this stage keeps the generic branch's handling below.
            if "integer string conversion" not in str(e):
                self._log_unexpected_message_error(e, message)
            else:
                logger.error(
                    "Message body contains a numeric literal too large to " "parse; dead-lettering",
                    extra={"error_type": "NumberTooLarge", **_delivery_fields(message)},
                )
            await message.reject(requeue=False)

        except Exception as e:
            # Deterministic-friendly catch-all. If something in
            # decoding/validation raises a non-ValidationError
            # exception (bug, OOM during model_validate), requeueing
            # would just hot-loop. Dead-letter immediately — a retry
            # won't help if the very first deserialization step crashed.
            # A body nested too deeply is handled by its own branch above,
            # so we always emit exc_info here; that includes a
            # RecursionError the body's depth did not cause (see the
            # json.loads call), which keeps its own text.
            self._log_unexpected_message_error(e, message)
            await message.reject(requeue=False)

    @staticmethod
    def _log_excessive_nesting(message: Any) -> None:
        """The one ERROR (no traceback) for a body nested more than
        MAX_COMMAND_NESTING_DEPTH levels deep."""
        logger.error(
            "Message body exceeds JSON-parsing nesting limit; dead-lettering",
            extra={
                "error_type": "ExcessiveNesting",
                "error": _TOO_DEEP,
                **_delivery_fields(message),
            },
        )

    @staticmethod
    def _log_unexpected_message_error(error: BaseException, message: Any) -> None:
        """The one ERROR (with traceback) for an unexpected failure while
        reading an incoming message."""
        logger.error(
            "Unexpected error processing message; dead-lettering",
            exc_info=error,
            extra={"error": str(error), **_delivery_fields(message)},
        )

    async def consume(self) -> Command:
        """
        Consume next command from the queue.

        This is an async generator method used by Transport.subscribe().

        Returns:
            Next command from the queue

        Raises:
            asyncio.QueueEmpty: If no commands available
        """
        return await self.command_queue.get()

    async def ack(self, message_id: str) -> None:
        """
        Acknowledge successful command processing.

        Removes message from pending set and sends ACK to RabbitMQ.
        Also clears the per-message retry counter so a future delivery
        of the same id (rare but possible) starts fresh.

        Args:
            message_id: Message ID to acknowledge

        Raises:
            ValueError: If message_id not found in pending messages
            BrokerNotReachable: Shutdown is in progress and the ACK could
                not reach the broker. The message is kept as held (still
                unacknowledged, so the broker redelivers it) and the
                condition was logged once at INFO, without a traceback.
        """
        message = self.pending_messages.pop(message_id, None)
        self.retry_counts.pop(message_id, None)
        self._forget_finishing(message_id)
        if not message:
            logger.warning("Cannot ACK unknown message", extra={"message_id": message_id})
            raise ValueError(f"Unknown message_id: {message_id}")

        logger.debug("Acknowledging message", extra={"message_id": message_id})

        try:
            await self._settle(message.ack())
        except Exception as e:
            if self._left_unsettled_at_shutdown(message_id, message, "ACK", e):
                raise BrokerNotReachable(
                    "AMQP broker not reachable during shutdown; ACK not sent"
                ) from e
            if self._delivery_lost(message_id, message, "ACK", e):
                raise DeliveryLost("ACK not sent: broker connection lost") from e
            logger.error(
                "Failed to ACK message",
                exc_info=True,
                extra={"message_id": message_id, "error": str(e)},
            )
            raise

    async def _settle(self, awaitable: Awaitable[Any]) -> Any:
        """Send one ACK / NACK / reject.

        Before shutdown (or without a guard) it is awaited exactly as
        before. Once shutdown has begun it gets at most the guard's
        ``step_timeout``: the frame is normally written at once (with the
        broker stopped it fails at once, with a frozen broker it is
        buffered), so the bound only matters if the connection cannot take
        the frame at all.
        """
        guard = self._shutdown_guard
        if guard is None or not guard.stopping:
            return await awaitable
        result = await await_bounded(awaitable, guard.step_timeout)
        # Written; whether the broker received it is not known (see
        # settled_while_stopping).
        self.settled_while_stopping += 1
        return result

    def _left_unsettled_at_shutdown(
        self,
        message_id: str,
        message: AbstractIncomingMessage,
        operation: str,
        error: BaseException,
    ) -> bool:
        """Handle an ACK / NACK / reject that could not reach the broker
        after shutdown began.

        Returns False (the caller keeps its existing ERROR path) unless
        shutdown is in progress AND ``error`` means the broker is
        unreachable. Otherwise the message is put back as held: it stays
        unacknowledged, so the broker redelivers it once the connection is
        gone, and it counts in the transport's single shutdown report.
        One INFO line, no traceback; the guard is latched so the transport
        skips its remaining broker round trips.
        """
        guard = self._shutdown_guard
        if guard is None or not guard.stopping or not is_broker_unreachable(error):
            return False
        guard.mark_unreachable(error)
        self.pending_messages[message_id] = message
        self.unsettled_at_shutdown += 1
        logger.info(
            "%s not sent: AMQP broker not reachable during shutdown (%s). "
            "The message stays unacknowledged; the broker redelivers it.",
            operation,
            type(error).__name__,
            extra={
                "message_id": message_id,
                "operation": operation,
                "error_class": type(error).__name__,
            },
        )
        return True

    def _delivery_lost(
        self,
        message_id: str,
        message: AbstractIncomingMessage,
        operation: str,
        error: BaseException,
    ) -> bool:
        """Handle an ACK / NACK / reject that failed because the connection
        the message arrived on is gone (outside shutdown, e.g. a broker
        restart while the handler ran).

        Returns False (the caller keeps its existing ERROR path) unless
        the failure means the delivery's connection or channel is gone
        (see :func:`delivery_connection_gone`). Otherwise logs one WARNING
        without a traceback and returns True: the broker put the message
        back in its queue when the connection dropped and delivers it
        again, so the command runs again (a broker restart keeps only a
        message that was published persistent, delivery mode 2, as the
        documented publishers do). The message is not kept as
        pending (its delivery can never be settled), so the redelivered
        copy is accepted rather than refused as a duplicate. The retry
        count is left as it is: a requeue that could not be sent already
        counted its attempt, and the redelivery is the next attempt. A
        dead-letter that could not be sent keeps the count the command
        had reached (``nack`` puts it back), so the redelivery does not
        start a new retry budget. An ACK has cleared it: the command
        succeeded, and its redelivery is a new run.
        """
        if not delivery_connection_gone(error, message):
            return False
        logger.warning(
            "%s not sent: the connection to the AMQP broker was lost (%s). "
            "The broker delivers the command again, so it runs again "
            "(after a broker restart, only if it was published persistent).",
            operation,
            type(error).__name__,
            extra={
                "message_id": message_id,
                "operation": operation,
                "error_class": type(error).__name__,
            },
        )
        return True

    # How long a command that reuses the message_id of a finished command
    # waits for that command's settlement (its reply being sent, then its
    # ACK or dead-letter) before it is refused as a duplicate, and how
    # often it checks.
    DUPLICATE_SETTLE_WAIT_S = 10.0
    _SETTLE_CHECK_S = 0.01

    def mark_finishing(self, message_id: str) -> None:
        """Note that the pending command ``message_id`` has finished: its
        one reply is about to be sent and its ACK or dead-letter follows.
        Until it is settled, a command that reuses its message_id waits
        for the settlement (at most ``DUPLICATE_SETTLE_WAIT_S``) instead
        of being dead-lettered as a duplicate at once. Called by the
        transport before it publishes the reply."""
        if message_id in self.pending_messages:
            if not isinstance(self._finishing, set):
                self._finishing = set()
            self._finishing.add(message_id)

    def _forget_finishing(self, message_id: str) -> None:
        if message_id in self._finishing:
            self._finishing.discard(message_id)

    async def _await_settled(
        self, message_id: str, pending: AbstractIncomingMessage
    ) -> AbstractIncomingMessage | None:
        """Wait until ``pending`` (a finished command) is no longer the
        pending delivery of ``message_id``, at most
        ``DUPLICATE_SETTLE_WAIT_S``; return the pending delivery then (None
        once it is settled)."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.DUPLICATE_SETTLE_WAIT_S
        while self.pending_messages.get(message_id) is pending and loop.time() < deadline:
            await asyncio.sleep(self._SETTLE_CHECK_S)
        return self.pending_messages.get(message_id)

    def delivery_is_stale(self, message_id: str) -> bool:
        """True when the pending message arrived on a channel that has
        since closed (for example after a broker restart): it can no
        longer be acknowledged or rejected."""
        return _channel_closed(self.pending_messages.get(message_id))

    async def release(self, message_id: str) -> None:
        """Give a pending message back to the broker without counting it
        as a failed attempt (its retry count is unchanged).

        Used when the command's reply could not be sent because the broker
        connection was lost. If the message's channel is already closed
        the broker has requeued it by itself and it is simply dropped
        here; otherwise it is requeued. Never raises.
        """
        message = self.pending_messages.pop(message_id, None)
        self._forget_finishing(message_id)
        if message is None or _channel_closed(message):
            return
        try:
            await self._settle(message.nack(requeue=True))
        except Exception:
            logger.debug(
                "Requeue after a lost connection not sent",
                exc_info=True,
                extra={"message_id": message_id},
            )

    async def nack(self, message_id: str, requeue: bool = False) -> None:
        """
        Reject command processing (negative acknowledgment).

        A NACKed message goes to the DLQ (requeue=False) or back to the
        queue (requeue=True), per the queue's dead-letter configuration.

        Retry-count bookkeeping:
        - ``requeue=True``: increment retry_counts[message_id] BEFORE
          calling nack — the next delivery will see the higher count.
        - ``requeue=False`` (DLQ): clear retry_counts — the message is
          terminal, no further attempt will read its counter. If the
          dead-letter cannot be sent because the delivery's connection is
          gone (``DeliveryLost``), the count is put back: the broker
          redelivers the command, and that redelivery must not get a new
          retry budget.

        Args:
            message_id: Message ID to reject
            requeue: Whether to requeue (default: False -> DLQ)

        Raises:
            ValueError: If message_id not found in pending messages
            BrokerNotReachable: Shutdown is in progress and the NACK /
                reject could not reach the broker (see ``ack``).
        """
        message = self.pending_messages.pop(message_id, None)
        self._forget_finishing(message_id)
        if not message:
            logger.warning("Cannot NACK unknown message", extra={"message_id": message_id})
            raise ValueError(f"Unknown message_id: {message_id}")

        # Capture the retry_count we want to *log* BEFORE mutating
        # the dict. For requeue=True we log the new (incremented)
        # count; for requeue=False (DLQ) we log the count this message
        # ended at, not 0 from the just-cleared slot — otherwise the
        # final-DLQ line misleadingly reports retry_count: 0 right
        # after the app-level "Max retries (N) exceeded" line.
        prior_count = self.retry_counts.get(message_id, 0)
        if requeue:
            self._set_retry_count(message_id, prior_count + 1)
            logged_count = prior_count + 1
        else:
            self.retry_counts.pop(message_id, None)
            logged_count = prior_count

        logger.info(
            "Rejecting message",
            extra={
                "message_id": message_id,
                "requeue": requeue,
                "retry_count": logged_count,
            },
        )

        try:
            if requeue:
                # Requeue (retry)
                await self._settle(message.nack(requeue=True))
            else:
                # Send to DLQ (via x-dead-letter-exchange)
                await self._settle(message.reject(requeue=False))

        except Exception as e:
            operation = "Requeue" if requeue else "Dead-letter"
            if self._left_unsettled_at_shutdown(message_id, message, operation, e):
                raise BrokerNotReachable(
                    "AMQP broker not reachable during shutdown; " f"{operation.lower()} not sent"
                ) from e
            if self._delivery_lost(message_id, message, operation, e):
                if not requeue and prior_count:
                    # The command had reached its terminal outcome. Keep
                    # its retry count so the broker's redelivery is not
                    # handed a fresh retry budget: a retryable failure
                    # whose retries were spent goes straight back to its
                    # terminal outcome on its next failure.
                    self._set_retry_count(message_id, prior_count)
                raise DeliveryLost(f"{operation} not sent: broker connection lost") from e
            logger.error(
                "Failed to NACK message",
                exc_info=True,
                extra={"message_id": message_id, "error": str(e)},
            )
            raise

    def _set_retry_count(self, message_id: str, count: int) -> None:
        """Record ``count`` as the message's retry count.

        Bound the map (v0.3.3): a requeued message whose redelivery lands
        on ANOTHER replica orphans its slot here forever, so the oldest
        slot is evicted at the cap instead of growing unboundedly.
        Eviction only resets that message's local retry budget.
        """
        if message_id not in self.retry_counts and len(self.retry_counts) >= 10_000:
            self.retry_counts.pop(next(iter(self.retry_counts)))
        self.retry_counts[message_id] = count

    def get_retry_count(self, message_id: str) -> int:
        """How many times this message has been NACK-requeued.

        Returns 0 for never-retried or unknown ids. app.py consults
        this before deciding to requeue vs DLQ a Retryable failure,
        enforcing KBM_LEDSAS_MAX_RETRIES.
        """
        return self.retry_counts.get(message_id, 0)

    @property
    def pending_count(self) -> int:
        """Number of messages awaiting acknowledgment."""
        return len(self.pending_messages)
