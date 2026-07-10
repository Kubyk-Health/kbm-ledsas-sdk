"""
AMQP publisher for responses and status updates.

Implements reliable publishing with publisher confirms for Direct transport mode.

Each publish opens a fresh, short-lived AMQP channel so that a channel-level
error (e.g. publishing to a `reply_to` exchange the caller forgot to declare)
cannot leave a persistent publisher channel in a half-broken state. The
underlying RobustConnection is shared across publishes.
"""

import json
import logging

from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractConnection, AbstractExchange
from aio_pika.exceptions import DeliveryError, PublishError

from kbm_ledsas_sdk.models.messages import Response, Status

logger = logging.getLogger(__name__)


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
    if isinstance(e, DeliveryError):
        if _is_unroutable_return(e):
            return True, "reply_to exchange has no bound queue (message unroutable)"
        return True, "broker did not confirm the publish (Basic.Nack)"
    if isinstance(e, ValueError) and "Max length exceeded" in str(e):
        return True, "exchange name exceeds AMQP 127-byte protocol limit"
    return False, None


class AMQPPublisher:
    """
    Publishes responses and status updates to RabbitMQ.

    This publisher is used in Direct transport mode (dev/testing).
    It provides reliable message delivery using publisher confirms.

    Features:
    - Publisher confirms for guaranteed delivery
    - Proper AMQP message properties (correlation_id, headers, etc.)
    - Persistent messages (survive broker restart)
    - Routes to exchange specified in reply_to field
    - Fresh channel per publish, so a publish failure cannot poison a
      persistent channel's state for subsequent publishes.

    Attributes:
        connection: AMQP connection (shared); a transient channel is opened
            for each publish.
    """

    def __init__(self, connection: AbstractConnection):
        """
        Initialize publisher.

        Args:
            connection: AMQP connection. Each publish opens a transient
                channel on this connection and closes it afterwards.
        """
        self.connection = connection
        self._confirms_enabled = False
        logger.info("AMQP publisher initialized")

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

        # on_return_raises=True: aio-pika does NOT raise on a returned
        # mandatory publish by default — it resolves normally with the
        # returned message, which would silently swallow an unroutable
        # response. With this flag a Basic.Return raises PublishError
        # (a DeliveryError subclass) so the failure chain fires.
        channel = await self.connection.channel(on_return_raises=True)
        try:
            # Passive declare: only check existence, do NOT redeclare with our
            # own args. This lets the caller (orchestrator) own the exchange's
            # type/durability/etc. and prevents PRECONDITION_FAILED channel
            # closures from arg mismatches. The transient channel is the
            # blast-radius if the exchange is missing.
            exchange: AbstractExchange = await channel.get_exchange(exchange_name, ensure=True)

            # Publish mandatory and wait for confirm. If the exchange has
            # no queue bound for this routing key the broker returns the
            # message (Basic.Return) and aio-pika raises DeliveryError —
            # an unroutable response must surface as a failure (the caller
            # NACKs the command to DLQ) instead of vanishing silently.
            await exchange.publish(
                message=message,
                routing_key=routing_key,
                mandatory=True,
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
            if is_expected:
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
            # Always close the transient channel, even on failure. A failed
            # channel is closed by the broker; calling close() on it is a
            # no-op and is safer than leaking a half-closed channel.
            try:
                if not channel.is_closed:
                    await channel.close()
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

        # on_return_raises=True: see publish_response() — required for a
        # returned mandatory publish to raise instead of resolving.
        channel = await self.connection.channel(on_return_raises=True)
        try:
            # Passive declare: only check existence, do NOT redeclare with our
            # own args. See publish_response() for rationale.
            exchange: AbstractExchange = await channel.get_exchange(exchange_name, ensure=True)

            # Publish mandatory so an unroutable status raises DeliveryError
            # just like responses do (parity across both publish sites).
            # Statuses remain best-effort: Transport.send_status swallows
            # the failure with a WARNING and a counter bump.
            await exchange.publish(
                message=message,
                routing_key=routing_key,
                mandatory=True,
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
            if is_expected:
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
                if not channel.is_closed:
                    await channel.close()
            except Exception:
                logger.debug("Transient publisher channel close failed (ignored)", exc_info=True)
