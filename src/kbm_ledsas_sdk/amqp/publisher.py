"""
AMQP publisher for responses and status updates.

Publishes replies and status updates to RabbitMQ with publisher confirms.

Each publish opens a fresh, short-lived AMQP channel so that a channel-level
error (e.g. publishing to a `reply_to` exchange the caller forgot to declare)
cannot leave a persistent publisher channel in a half-broken state. The
underlying connection is shared across publishes; the short-lived channels
are plain (not "robust") channels, so a channel the broker closes is never
reopened by the connection's automatic recovery.
"""

import json
import logging
from collections.abc import Awaitable
from typing import Any, TypeVar

import aio_pika
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractConnection, AbstractExchange, AbstractRobustConnection
from aio_pika.exceptions import DeliveryError, PublishError

from kbm_ledsas_sdk.amqp.consumer import BrokerShutdownGuard, is_broker_unreachable
from kbm_ledsas_sdk.models.envelope import Envelope  # noqa: F401
from kbm_ledsas_sdk.models.messages import Response, Status

logger = logging.getLogger(__name__)

T = TypeVar("T")


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
    - Fresh channel per publish, so a publish failure cannot poison a
      persistent channel's state for subsequent publishes. The channel is a
      plain channel: when the broker closes it (missing exchange, refused
      publish) nothing reopens it, so a refused publish leaves no channel
      behind and nothing to redeclare after a reconnect.

    Attributes:
        connection: AMQP connection (shared); a transient channel is opened
            for each publish.
    """

    def __init__(
        self,
        connection: AbstractConnection,
        shutdown_guard: BrokerShutdownGuard | None = None,
    ):
        """
        Initialize publisher.

        Args:
            connection: AMQP connection. Each publish opens a transient
                channel on this connection and closes it afterwards.
            shutdown_guard: Optional guard shared with the transport. Once
                the transport begins shutting down, every broker round trip
                of a publish (channel open, exchange check, publish +
                confirm, channel close) is bounded by it, so a broker that
                stops answering cannot hold a command's reply, and with it
                the shutdown, indefinitely. Without a guard, round trips are
                awaited as before.
        """
        self.connection = connection
        self._shutdown_guard = shutdown_guard
        self._confirms_enabled = False
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
        """Open the short-lived channel for one publish.

        On a robust connection (the transport's) the channel is opened as a
        plain ``aio_pika.Channel``, NOT through ``connection.channel()``:
        that would return a robust channel, registered with the connection
        for automatic recovery. When the broker then closed it (a missing
        reply exchange, ACCESS_REFUSED, an oversized message), the robust
        channel reopened itself and stayed registered: every refused publish
        left one open channel behind until the connection hit the broker's
        channel limit, and after a reconnect the connection re-ran the
        channel's remembered passive exchange check, failing the whole
        reconnect once that exchange was gone. A plain channel is never
        recovered.

        ``on_return_raises=True``: aio-pika does NOT raise on a returned
        mandatory publish by default; it resolves normally with the
        returned message, which would silently swallow an unroutable
        response. With this flag a Basic.Return raises PublishError (a
        DeliveryError subclass) so the failure chain fires.
        """
        connection = self.connection
        if isinstance(connection, AbstractRobustConnection):
            channel = aio_pika.Channel(
                connection,
                publisher_confirms=True,
                on_return_raises=True,
            )
            await channel
            return channel
        return await connection.channel(on_return_raises=True)

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
                "message_id": response.envelope.message_id,
                "correlation_id": response.envelope.correlation_id,
            },
        )

        # During shutdown with the broker known unreachable, fail fast
        # (BrokerNotReachable) without opening a channel.
        if self._shutdown_guard is not None:
            self._shutdown_guard.check()

        # A plain channel opened with on_return_raises=True (see
        # _open_transient_channel).
        channel = await self._round_trip(self._open_transient_channel())
        try:
            # Passive declare: only check existence, do NOT redeclare with our
            # own args. This lets the caller (orchestrator) own the exchange's
            # type/durability/etc. and prevents PRECONDITION_FAILED channel
            # closures from arg mismatches. The transient channel is the
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

            logger.info(
                "Response message published successfully",
                extra={
                    "exchange": exchange_name,
                    "routing_key": routing_key,
                    "message_id": response.envelope.message_id,
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
                )
            elif is_expected:
                logger.debug(
                    "publish_response: %s; reply_to=%r " "(upper layer will log + DLQ)",
                    reason,
                    exchange_name,
                )
            else:
                logger.error(
                    "Failed to publish response message",
                    exc_info=True,
                    extra={
                        "exchange": exchange_name,
                        "routing_key": routing_key,
                        "message_id": response.envelope.message_id,
                        "error": str(e),
                    },
                )
            raise
        finally:
            # Always close the transient channel, even on failure. A channel
            # the broker closed (missing exchange, refused publish) is
            # already closed and, being a plain channel, stays closed.
            try:
                if not self._skip_channel_close() and not channel.is_closed:
                    await self._round_trip(channel.close())
            except Exception:
                logger.debug("Transient publisher channel close failed (ignored)", exc_info=True)

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
                "message_id": status.envelope.message_id,
                "correlation_id": status.envelope.correlation_id,
            },
        )

        # See publish_response(): fail fast during shutdown once the broker
        # is known unreachable.
        if self._shutdown_guard is not None:
            self._shutdown_guard.check()

        # A plain channel opened with on_return_raises=True (see
        # _open_transient_channel).
        channel = await self._round_trip(self._open_transient_channel())
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

            logger.info(
                "Status message published successfully",
                extra={
                    "exchange": exchange_name,
                    "routing_key": routing_key,
                    "message_id": status.envelope.message_id,
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
                )
            elif is_expected:
                logger.debug(
                    "publish_status: %s; reply_to=%r "
                    "(upper layer will log; status is best-effort)",
                    reason,
                    exchange_name,
                )
            else:
                logger.error(
                    "Failed to publish status message",
                    exc_info=True,
                    extra={
                        "exchange": exchange_name,
                        "routing_key": routing_key,
                        "message_id": status.envelope.message_id,
                        "error": str(e),
                    },
                )
            raise
        finally:
            try:
                if not self._skip_channel_close() and not channel.is_closed:
                    await self._round_trip(channel.close())
            except Exception:
                logger.debug("Transient publisher channel close failed (ignored)", exc_info=True)
