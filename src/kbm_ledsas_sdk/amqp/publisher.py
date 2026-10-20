"""
AMQP publisher for responses and status updates.

Publishes replies and status updates to RabbitMQ with publisher confirms.

Publishes go through a small pool of long-lived plain (not "robust")
channels on the shared connection: each publish checks one channel out, so
a channel-level error (e.g. publishing to a `reply_to` exchange the caller
forgot to declare) affects only that publish. A channel the broker closed,
or one a failed publish used, is discarded and replaced on a later
checkout; it is never reopened in place, so the connection's automatic
recovery never replays it.
"""

import asyncio
import dataclasses
import inspect
import json
from collections.abc import Awaitable
from typing import Any, TypeVar

import aio_pika
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractConnection, AbstractExchange, AbstractRobustConnection
from aio_pika.exceptions import DeliveryError, PublishError

from kbm_ledsas_sdk.amqp.consumer import BrokerShutdownGuard, is_broker_unreachable
from kbm_ledsas_sdk.models.messages import Response, Status
from kbm_ledsas_sdk.utils.logging import _command_fields, masked_logger

logger = masked_logger(__name__)

T = TypeVar("T")

#: Channels kept open for publishing when no pool size is given (the SDK's
#: default handler concurrency).
DEFAULT_PUBLISH_CHANNELS = 4


def _closing_is_observer(connection: Any) -> bool:
    """True when ``connection.closing`` hands out a new observer future on
    every access while the connection is open (aiormq 7), False when it is
    the connection's own closing future (aiormq 6). Read without touching
    the attribute, which on aiormq 7 would create an observer."""
    cls = connection if isinstance(connection, type) else type(connection)
    return isinstance(inspect.getattr_static(cls, "closing", None), property)


def _release_observer(connection: Any, observer: Any, callback: Any) -> None:
    """Drop the close callback a channel registered on its connection.

    On aiormq 6 ``observer`` is the connection's own closing future: only
    the callback is removed. On aiormq 7 it is an observer created for this
    channel; cancelling it also detaches it from the connection (aiormq
    removes its hook from the connection's closing future when an observer
    is cancelled). Without this, every channel opened and closed on a
    long-lived connection would leave its callback (and, on aiormq 7, the
    observer) behind until the connection closes. Never raises."""
    if observer is None:
        return
    try:
        observer.remove_done_callback(callback)
        if (
            _closing_is_observer(connection)
            and observer is not getattr(connection, "_closing", None)
            and not observer.done()
        ):
            observer.cancel()
    except Exception:  # noqa: BLE001 - releasing must never fail a publish
        logger.debug("Releasing a channel's close callback failed (ignored)", exc_info=True)


_CHANNEL_CLASS_CACHE: dict = {}


def _publisher_channel_class() -> Any:
    """The channel class publishes use.

    A subclass of ``aio_pika.Channel`` that releases the close callback it
    registers on the connection when it closes (see ``_release_observer``).
    aio-pika's own channel removes that callback through a second access of
    ``connection.closing``, which on aiormq 7 is a fresh observer, so the
    removal does nothing; and it never removes it when the broker closes the
    channel. When the installed aio-pika does not have the internals the
    subclass relies on, plain ``aio_pika.Channel`` is used."""
    base = aio_pika.Channel
    cached = _CHANNEL_CLASS_CACHE.get(base)
    if cached is not None:
        return cached
    cls = _build_publisher_channel_class(base)
    _CHANNEL_CLASS_CACHE[base] = cls
    return cls


def _build_publisher_channel_class(base: Any) -> Any:
    try:
        from aio_pika.abc import UnderlayChannel
        from aio_pika.tools import OneShotCallback

        fields = {f.name for f in dataclasses.fields(UnderlayChannel)}
        supported = (
            fields == {"channel", "close_callback"}
            and inspect.iscoroutinefunction(getattr(base, "_open", None))
            and inspect.iscoroutinefunction(getattr(base, "_on_close", None))
            and inspect.iscoroutinefunction(getattr(base, "_on_open", None))
            and callable(OneShotCallback)
        )
    except Exception:  # noqa: BLE001 - any other layout: plain channel
        supported = False
    if not supported:
        logger.debug(
            "aio-pika's channel internals differ from the known layout: "
            "publishing on plain aio_pika.Channel"
        )
        return base

    @dataclasses.dataclass(frozen=True)
    class _ReleasingUnderlay(UnderlayChannel):  # type: ignore[misc,valid-type]
        """aio-pika's underlay record, plus the observer to release."""

        connection: Any = None
        observer: Any = None

        async def close(self, exc: Any = None) -> Any:
            if self.close_callback.finished.is_set():
                _release_observer(self.connection, self.observer, self.close_callback)
                return
            await self.channel.close(exc)
            await self.close_callback.wait()
            # The channel is closed now: its ``closing`` is its own future.
            self.channel.closing.remove_done_callback(self.close_callback)
            _release_observer(self.connection, self.observer, self.close_callback)

    class _PublisherChannel(base):  # type: ignore[misc,valid-type]
        """A plain channel that does not leave its close callback on the
        connection (see ``_publisher_channel_class``)."""

        async def _open(self) -> None:
            transport = self._connection.transport
            if transport is None:
                from aio_pika.exceptions import ChannelInvalidStateError

                raise ChannelInvalidStateError("No active transport in channel")
            await transport.ready()
            connection = transport.connection
            close_callback = OneShotCallback(self._on_close)
            await connection.ready()
            observer = connection.closing
            observer.add_done_callback(close_callback)
            try:
                underlay_channel = await connection.channel(
                    publisher_confirms=self.publisher_confirms,
                    on_return_raises=self.on_return_raises,
                    channel_number=self._channel_number,
                )
            except BaseException:
                _release_observer(connection, observer, close_callback)
                raise
            underlay_channel.closing.add_done_callback(close_callback)
            channel = _ReleasingUnderlay(
                channel=underlay_channel,
                close_callback=close_callback,
                connection=connection,
                observer=observer,
            )
            self._channel = channel
            try:
                await self._on_open()
            except BaseException as e:
                await channel.close(e)
                self._channel = None
                raise
            # Reopened: reset the explicit-close marker the way the installed
            # aio-pika keeps it (a future from 9.5, a bool before).
            closed = self._closed
            if isinstance(closed, asyncio.Future):
                if closed.done():
                    self._closed = asyncio.get_running_loop().create_future()
            else:
                self._closed = False

        async def _on_close(self, closing: asyncio.Future) -> Any:
            try:
                return await super()._on_close(closing)
            finally:
                channel = self._channel
                if isinstance(channel, _ReleasingUnderlay):
                    _release_observer(channel.connection, channel.observer, channel.close_callback)

    _PublisherChannel.__name__ = _PublisherChannel.__qualname__ = "PublisherChannel"
    return _PublisherChannel


def _is_unroutable_return(e: BaseException) -> bool:
    """True when the broker RETURNED a mandatory publish (Basic.Return).

    aiormq raises ``PublishError`` (a ``DeliveryError`` subclass) for the
    returned-unroutable case specifically; a *bare* ``DeliveryError`` means
    the broker refused to confirm the publish (Basic.Nack — e.g. a queue
    ``x-overflow: reject-publish`` policy or a broker-side error), which is
    a different operator condition. This predicate is the single place
    that distinction lives — the transport layer imports it to pick the
    right counter and operator hint, so classification cannot drift
    between the two modules.
    """
    return isinstance(e, PublishError)


def _classify_publish_error(e: BaseException) -> tuple[bool, str | None]:
    """Classify a publish-time exception as an expected operator condition.

    Returns ``(is_expected, short_reason)``. When ``is_expected`` is True
    the upper layer (Transport.send_response / send_status) is expected
    to log one operator-facing line — this publisher layer should log
    at DEBUG only, NOT dump the underlying ~30-line aio_pika → aiormq →
    pamqp traceback.

    Expected classes:

    - ``ChannelNotFoundEntity`` — the caller's ``reply_to`` exchange does
      not exist on the broker. Normal operator misconfiguration.
    - ``ChannelAccessRefused`` / ``ChannelPreconditionFailed`` — the broker
      refused the publish and closed the channel: no write permission on
      the exchange (or an internal exchange), or a message larger than the
      broker's ``max_message_size``. A broker decision, not an SDK fault.
    - ``PublishError`` (DeliveryError subclass) — the exchange exists but
      the broker returned the mandatory publish (Basic.Return): no queue
      is bound for the routing key, so the message is unroutable. Normal
      operator misconfiguration (the caller declared its reply exchange
      but forgot to declare/bind the reply queue, or the queue was
      auto-deleted).
    - bare ``DeliveryError`` — the broker refused to confirm the publish
      (Basic.Nack; e.g. queue overflow with a reject-publish policy).
      Distinct condition, same noise treatment: an AMQP-level refusal
      carries no diagnostic value in a Python traceback.
    - ``ValueError("Max length exceeded for exchange")`` — pamqp's
      protocol-layer check that an exchange name fits in 127 bytes.
      SDKConfig + envelope schema now cap
      reply_to and tenant+service combinations at 127 chars *before*
      they reach here, so this branch is theoretically unreachable —
      but anyone bypassing the SDK validators (custom tests,
      direct DirectTransport construction) could still trigger it.
      The pamqp traceback adds no diagnostic value beyond the message,
      so suppress it the same way.
    """
    name = type(e).__name__
    if name == "ChannelNotFoundEntity":
        return True, "reply_to exchange missing on broker"
    if name == "ChannelAccessRefused":
        return True, (
            "broker refused the publish (ACCESS_REFUSED: no write permission "
            "on the exchange, or it is an internal exchange)"
        )
    if name == "ChannelPreconditionFailed":
        if "message size" in str(e):
            return True, (
                "broker refused the publish: the message is larger than its " "max_message_size"
            )
        return True, "broker refused the publish (PRECONDITION_FAILED)"
    if isinstance(e, DeliveryError):
        if _is_unroutable_return(e):
            return True, ("reply_to exchange has no bound queue (message unroutable)")
        return True, "broker did not confirm the publish (Basic.Nack)"
    if isinstance(e, ValueError) and "Max length exceeded" in str(e):
        return True, "exchange name exceeds AMQP 127-byte protocol limit"
    return False, None


def _channel_closed(channel: Any) -> bool:
    """``channel.is_closed``, True when it cannot be read."""
    try:
        return bool(channel.is_closed)
    except Exception:  # noqa: BLE001
        return True


class AMQPPublisher:
    """
    Publishes responses and status updates to RabbitMQ.

    Publishes the replies and status updates of the commands this service
    handles, with publisher confirms.

    Features:
    - Publisher confirms for guaranteed delivery
    - Proper AMQP message properties (correlation_id, headers, etc.)
    - Persistent messages (survive broker restart)
    - Routes to exchange specified in reply_to field
    - A small pool of long-lived plain channels: each publish checks one
      out, so a publish failure affects only its own publish. A channel the
      broker closed (missing exchange, refused publish), or one a failed
      publish used, is discarded, never reopened in place, and replaced on
      a later checkout; nothing is left to redeclare after a reconnect.

    Attributes:
        connection: AMQP connection (shared); the publishing channels are
            opened on it.
    """

    def __init__(
        self,
        connection: AbstractConnection,
        shutdown_guard: BrokerShutdownGuard | None = None,
        pool_size: int = DEFAULT_PUBLISH_CHANNELS,
    ):
        """
        Initialize publisher.

        Args:
            connection: AMQP connection. Publishes use channels opened on
                this connection (see ``pool_size``).
            shutdown_guard: Optional guard shared with the transport. Once
                the transport begins shutting down, every broker round trip
                of a publish (channel open, exchange check, publish +
                confirm, channel close) is bounded by it, so a broker that
                stops answering cannot hold a command's reply, and with it
                the shutdown, indefinitely. Without a guard, round trips are
                awaited as before.
            pool_size: How many idle publishing channels are kept open for
                the next publishes (at least 1; the transport passes the
                handler concurrency). More concurrent publishes than this
                open extra channels, which are closed after their publish.
        """
        self.connection = connection
        self._shutdown_guard = shutdown_guard
        self._confirms_enabled = False
        self._pool_size = max(1, int(pool_size))
        # Idle open channels, most recently used last.
        self._idle: list[Any] = []
        logger.info("AMQP publisher initialized")

    async def _round_trip(self, awaitable: Awaitable[T]) -> T:
        """Await one broker round trip, bounded by the guard if present."""
        if self._shutdown_guard is None:
            return await awaitable
        return await self._shutdown_guard.round_trip(awaitable)

    def _unreachable_at_shutdown(self, error: BaseException) -> bool:
        """True when ``error`` means the broker could not be reached after
        the transport began shutting down: the transport reports that
        condition once, so this layer logs it at DEBUG only."""
        guard = self._shutdown_guard
        return guard is not None and guard.stopping and is_broker_unreachable(error)

    def _skip_channel_close(self) -> bool:
        """During shutdown with the broker known unreachable, the channel
        is not closed here: closing the connection releases it locally."""
        guard = self._shutdown_guard
        return guard is not None and guard.fail_fast

    async def _open_transient_channel(self) -> Any:
        """Open one publishing channel.

        On a robust connection (the transport's) the channel is opened as a
        plain channel, NOT through ``connection.channel()``: that would
        return a robust channel, registered with the connection for
        automatic recovery. When the broker then closed it (a missing reply
        exchange, ACCESS_REFUSED, an oversized message), the robust channel
        reopened itself and stayed registered: every refused publish left
        one open channel behind until the connection hit the broker's
        channel limit, and after a reconnect the connection re-ran the
        channel's remembered passive exchange check, failing the whole
        reconnect once that exchange was gone. A plain channel is never
        recovered. Its class releases the close callback it registers on
        the connection when it closes (see ``_publisher_channel_class``).

        ``on_return_raises=True``: aio-pika does NOT raise on a returned
        mandatory publish by default; it resolves normally with the
        returned message, which would silently swallow an unroutable
        response. With this flag a Basic.Return raises PublishError (a
        DeliveryError subclass) so the failure chain fires.
        """
        connection = self.connection
        if isinstance(connection, AbstractRobustConnection):
            channel = _publisher_channel_class()(
                connection,
                publisher_confirms=True,
                on_return_raises=True,
            )
            await channel
            return channel
        return await connection.channel(on_return_raises=True)

    async def _checkout(self) -> Any:
        """An open idle channel, or a newly opened one. Idle channels that
        closed meanwhile (the broker closed them, or the connection was
        lost) are dropped."""
        while self._idle:
            channel = self._idle.pop()
            if not _channel_closed(channel):
                return channel
        return await self._round_trip(self._open_transient_channel())

    async def _checkin(self, channel: Any, failed: bool) -> None:
        """Return a channel after its publish: kept for the next publish
        when the publish succeeded, the channel is open and the pool has
        room; otherwise closed (a channel the broker already closed stays
        closed)."""
        if (
            not failed
            and not _channel_closed(channel)
            and len(self._idle) < self._pool_size
            and not self._stopping()
        ):
            self._idle.append(channel)
            return
        try:
            if not self._skip_channel_close() and not _channel_closed(channel):
                await self._round_trip(channel.close())
        except Exception:
            logger.debug("Publisher channel close failed (ignored)", exc_info=True)

    def _stopping(self) -> bool:
        guard = self._shutdown_guard
        return guard is not None and bool(getattr(guard, "stopping", False))

    async def close(self) -> None:
        """Close the idle publishing channels (at shutdown, before the
        connection). Skipped when the broker is known unreachable: closing
        the connection releases them locally. Never raises."""
        idle, self._idle = self._idle, []
        for channel in idle:
            await self._checkin(channel, failed=True)

    async def enable_confirms(self) -> None:
        """
        Mark publisher as ready to publish.

        aio-pika channels created via ``connection.channel()`` use publisher
        confirms by default (each ``await exchange.publish(...)`` returns
        only when the broker has confirmed the message). We keep this
        method on the public API for backwards compatibility, but it no
        longer mutates a long-lived channel — confirms are enabled
        per-channel and the channels here are transient.
        """
        if not self._confirms_enabled:
            logger.info("Enabling publisher confirms")
            self._confirms_enabled = True
            logger.info("Publisher confirms enabled")

    async def publish_response(
        self,
        response: Response,
        exchange_name: str,
        routing_key: str = "response",
    ) -> None:
        """
        Publish response message with confirms.

        Args:
            response: Response message to publish
            exchange_name: Exchange name from command's reply_to field
            routing_key: Routing key (default: "response")

        Raises:
            RuntimeError: If confirms not enabled
            Exception: If publish fails or message is returned
        """
        if not self._confirms_enabled:
            raise RuntimeError("Publisher confirms not enabled. Call enable_confirms() first.")

        # Serialize response to JSON
        body = json.dumps(response.model_dump(mode="json")).encode("utf-8")

        # Build AMQP message with properties
        message = Message(
            body=body,
            correlation_id=response.envelope.correlation_id,
            message_id=response.envelope.message_id,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,  # Survive broker restart
            headers={
                "trace_id": response.envelope.trace_id,
                "sent_at": response.envelope.sent_at,
                "message_type": "response",
                "schema_version": response.envelope.schema_version,
                "message_version": response.envelope.message_version,
            },
        )

        logger.debug(
            "Publishing response message",
            extra={
                "exchange": exchange_name,
                "routing_key": routing_key,
                **_command_fields(response.envelope),
            },
        )

        # During shutdown with the broker known unreachable, fail fast
        # (BrokerNotReachable) without opening a channel.
        if self._shutdown_guard is not None:
            self._shutdown_guard.check()

        # A plain channel opened with on_return_raises=True (see
        # _open_transient_channel), from the pool.
        channel = await self._checkout()
        failed = True
        try:
            # Passive declare: only check existence, do NOT redeclare with our
            # own args. This lets the caller own the exchange's
            # type/durability/etc. and prevents PRECONDITION_FAILED channel
            # closures from arg mismatches. This publish's channel is the
            # blast-radius if the exchange is missing.
            exchange: AbstractExchange = await self._round_trip(
                channel.get_exchange(exchange_name, ensure=True)
            )

            # Publish mandatory and wait for confirm. If the exchange has
            # no queue bound for this routing key the broker returns the
            # message (Basic.Return) and aio-pika raises DeliveryError —
            # an unroutable response must surface as a failure (the caller
            # NACKs the command to DLQ) instead of vanishing silently.
            await self._round_trip(
                exchange.publish(
                    message=message,
                    routing_key=routing_key,
                    mandatory=True,
                )
            )

            failed = False
            logger.info(
                "Response message published successfully",
                extra={
                    "exchange": exchange_name,
                    "routing_key": routing_key,
                    **_command_fields(response.envelope),
                },
            )

        except Exception as e:
            # Demote expected publish-time
            # error classes to DEBUG. The upper layer (Transport.
            # send_response) logs one clean operator-facing ERROR with
            # the exchange name + counter + NACK action — there's no
            # value in this layer also logging at ERROR or dumping the
            # underlying aio_pika → aiormq → pamqp traceback. Unknown
            # errors still get a full ERROR with exc_info.
            is_expected, reason = _classify_publish_error(e)
            if self._unreachable_at_shutdown(e):
                logger.debug(
                    "publish_response: AMQP broker not reachable during "
                    "shutdown (%s); reply_to=%r (upper layer reports it)",
                    type(e).__name__,
                    exchange_name,
                    extra=_command_fields(response.envelope),
                )
            elif is_expected:
                logger.debug(
                    "publish_response: %s; reply_to=%r " "(upper layer will log + DLQ)",
                    reason,
                    exchange_name,
                    extra=_command_fields(response.envelope),
                )
            else:
                logger.error(
                    "Failed to publish response message",
                    exc_info=True,
                    extra={
                        "exchange": exchange_name,
                        "routing_key": routing_key,
                        **_command_fields(response.envelope),
                        "error": str(e),
                    },
                )
            raise
        finally:
            # Back to the pool after a success; closed after a failure. A
            # channel the broker closed (missing exchange, refused publish)
            # is already closed and, being a plain channel, stays closed.
            await self._checkin(channel, failed)

    async def publish_status(
        self,
        status: Status,
        exchange_name: str,
        routing_key: str = "status",
    ) -> None:
        """
        Publish status update message with confirms.

        Args:
            status: Status message to publish
            exchange_name: Exchange name from command's reply_to field
            routing_key: Routing key (default: "status")

        Raises:
            RuntimeError: If confirms not enabled
            Exception: If publish fails or message is returned
        """
        if not self._confirms_enabled:
            raise RuntimeError("Publisher confirms not enabled. Call enable_confirms() first.")

        # Serialize status to JSON
        body = json.dumps(status.model_dump(mode="json")).encode("utf-8")

        # Build AMQP message with properties
        message = Message(
            body=body,
            correlation_id=status.envelope.correlation_id,
            message_id=status.envelope.message_id,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            headers={
                "trace_id": status.envelope.trace_id,
                "sent_at": status.envelope.sent_at,
                "message_type": "status",
                "schema_version": status.envelope.schema_version,
                "message_version": status.envelope.message_version,
            },
        )

        logger.debug(
            "Publishing status message",
            extra={
                "exchange": exchange_name,
                "routing_key": routing_key,
                **_command_fields(status.envelope),
            },
        )

        # See publish_response(): fail fast during shutdown once the broker
        # is known unreachable.
        if self._shutdown_guard is not None:
            self._shutdown_guard.check()

        # A plain channel opened with on_return_raises=True (see
        # _open_transient_channel), from the pool.
        channel = await self._checkout()
        failed = True
        try:
            # Passive declare: only check existence, do NOT redeclare with our
            # own args. See publish_response() for rationale.
            exchange: AbstractExchange = await self._round_trip(
                channel.get_exchange(exchange_name, ensure=True)
            )

            # Publish mandatory so an unroutable status raises DeliveryError
            # just like responses do (parity across both publish sites).
            # Statuses remain best-effort: Transport.send_status swallows
            # the failure with a WARNING and a counter bump.
            await self._round_trip(
                exchange.publish(
                    message=message,
                    routing_key=routing_key,
                    mandatory=True,
                )
            )

            failed = False
            logger.info(
                "Status message published successfully",
                extra={
                    "exchange": exchange_name,
                    "routing_key": routing_key,
                    **_command_fields(status.envelope),
                },
            )

        except Exception as e:
            # Mirror publish_response —
            # demote expected publish-time error classes to DEBUG;
            # Transport.send_status logs the one operator-facing
            # WARNING with counter + context.
            is_expected, reason = _classify_publish_error(e)
            if self._unreachable_at_shutdown(e):
                logger.debug(
                    "publish_status: AMQP broker not reachable during "
                    "shutdown (%s); reply_to=%r (upper layer reports it)",
                    type(e).__name__,
                    exchange_name,
                    extra=_command_fields(status.envelope),
                )
            elif is_expected:
                logger.debug(
                    "publish_status: %s; reply_to=%r "
                    "(upper layer will log; status is best-effort)",
                    reason,
                    exchange_name,
                    extra=_command_fields(status.envelope),
                )
            else:
                logger.error(
                    "Failed to publish status message",
                    exc_info=True,
                    extra={
                        "exchange": exchange_name,
                        "routing_key": routing_key,
                        **_command_fields(status.envelope),
                        "error": str(e),
                    },
                )
            raise
        finally:
            await self._checkin(channel, failed)
