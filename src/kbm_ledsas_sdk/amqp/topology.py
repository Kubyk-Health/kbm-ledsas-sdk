"""
AMQP topology declaration for RabbitMQ exchanges and queues.

Declares the messaging infrastructure for Direct transport mode.
"""

import logging

from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel, AbstractQueue

logger = logging.getLogger(__name__)


def build_exchange_name(prefix: str, tenant: str | None, service_name: str) -> str:
    """
    Build exchange name with optional tenant prefix.

    Format:
    - With tenant: {prefix}.{tenant}.{service_name}.v1
    - Without tenant: {prefix}.{service_name}.v1

    Args:
        prefix: Exchange prefix (e.g., "cmd", "dlq")
        tenant: Optional tenant ID
        service_name: Service name

    Returns:
        str: Fully qualified exchange name
    """
    if tenant:
        return f"{prefix}.{tenant}.{service_name}.v1"
    return f"{prefix}.{service_name}.v1"


def build_queue_name(prefix: str, tenant: str | None, service_name: str) -> str:
    """
    Build queue name with optional tenant prefix.

    Format:
    - With tenant: {prefix}.{tenant}.{service_name}.v1
    - Without tenant: {prefix}.{service_name}.v1

    Args:
        prefix: Queue prefix (e.g., "queue", "dlq.queue")
        tenant: Optional tenant ID
        service_name: Service name

    Returns:
        str: Fully qualified queue name
    """
    if tenant:
        return f"{prefix}.{tenant}.{service_name}.v1"
    return f"{prefix}.{service_name}.v1"


class TopologyInfo:
    """
    Information about declared AMQP topology.

    Attributes:
        cmd_exchange: Command exchange name
        cmd_queue: Command queue name
        dlq_exchange: DLQ exchange name
        dlq_queue: DLQ queue name
        cmd_queue_obj: Command queue object for consuming
    """

    def __init__(
        self,
        cmd_exchange: str,
        cmd_queue: str,
        dlq_exchange: str,
        dlq_queue: str,
        cmd_queue_obj: AbstractQueue,
    ):
        self.cmd_exchange = cmd_exchange
        self.cmd_queue = cmd_queue
        self.dlq_exchange = dlq_exchange
        self.dlq_queue = dlq_queue
        self.cmd_queue_obj = cmd_queue_obj


async def declare_topology(
    channel: AbstractChannel,
    service_name: str,
    tenant: str | None = None,
) -> TopologyInfo:
    """
    Declare AMQP topology for command processing with DLQ.

    Creates:
    1. DLQ exchange (topic) - for failed messages
    2. DLQ queue - bound to DLQ exchange
    3. Command exchange (topic) - for incoming commands
    4. Command queue - bound to command exchange with DLQ config

    The command queue is configured with:
    - Dead letter exchange pointing to DLQ exchange
    - Durable: yes (survives broker restart)
    - Auto-delete: no (persists when no consumers)

    Args:
        channel: AMQP channel for declarations
        service_name: Service name for topology naming
        tenant: Optional tenant ID for multi-tenancy

    Returns:
        TopologyInfo: Topology names and queue object

    Raises:
        Exception: If topology declaration fails
    """
    # Build exchange/queue names
    cmd_exchange_name = build_exchange_name("cmd", tenant, service_name)
    cmd_queue_name = build_queue_name("queue", tenant, service_name)
    dlq_exchange_name = build_exchange_name("dlq", tenant, service_name)
    dlq_queue_name = build_queue_name("dlq.queue", tenant, service_name)

    logger.info(
        "Declaring AMQP topology",
        extra={
            "cmd_exchange": cmd_exchange_name,
            "cmd_queue": cmd_queue_name,
            "dlq_exchange": dlq_exchange_name,
            "dlq_queue": dlq_queue_name,
            "tenant": tenant,
            "service": service_name,
        },
    )

    try:
        # 1. Declare DLQ exchange (must exist before command queue references it)
        dlq_exchange = await channel.declare_exchange(
            dlq_exchange_name,
            ExchangeType.TOPIC,
            durable=True,
            auto_delete=False,
        )
        logger.debug(f"Declared DLQ exchange: {dlq_exchange_name}")

        # 2. Declare DLQ queue with retention bounds.
        #
        # A long-running service hit by a flood of malformed
        # envelopes, oversize payloads, schema failures, etc. previously
        # filled the DLQ indefinitely (the SDK's in-process DoS surfaces
        # are bounded by `pending_messages` cap and
        # `max_payload_bytes`; the broker-side DLQ was not). Apply two
        # native broker arguments so operators get sane behavior by
        # default without writing a drain consumer on day one:
        #
        # - ``x-message-ttl``: 7 days. Old DLQ entries are useful for
        #   post-mortem for a week — past that they almost always go
        #   uninvestigated and only consume disk.
        # - ``x-max-length``: 100 000 messages. Cap the queue so a
        #   hostile sender can't blow up the broker's disk; on overflow
        #   the broker drops the oldest entries (default behavior).
        #
        # NOTE on upgrade: queues declared by an older SDK without these
        # arguments will fail to re-declare with PRECONDITION_FAILED.
        # Operators upgrading from an earlier version must delete the
        # existing DLQ (`dlq.queue.{tenant}.{service}.v1`) once before
        # restarting; the SDK will re-create it with the new arguments.
        # Customers who want different limits can pre-declare the DLQ
        # with their own arguments; the SDK does not redeclare an
        # existing queue, it just consults the broker's view.
        dlq_queue = await channel.declare_queue(
            dlq_queue_name,
            durable=True,
            auto_delete=False,
            arguments={
                "x-message-ttl": 7 * 24 * 60 * 60 * 1000,  # 7 days in ms
                "x-max-length": 100_000,
            },
        )
        logger.debug(f"Declared DLQ queue: {dlq_queue_name}")

        # 3. Bind DLQ queue to DLQ exchange (routing key: #).
        # ``#`` accepts every routing key so dead-lettered messages
        # land here regardless of how the broker tagged them. Note that
        # this also means any broker user with publish rights to the DLQ
        # exchange can post directly into the DLQ. The SDK trusts the
        # broker as the security boundary — restrict publish rights on
        # ``dlq.*`` exchanges in production deployments (the default
        # RabbitMQ ``guest:guest`` user is loopback-only by default in
        # the bundled docker-compose).
        await dlq_queue.bind(dlq_exchange, routing_key="#")
        logger.debug("Bound DLQ queue to DLQ exchange")

        # 4. Declare command exchange
        cmd_exchange = await channel.declare_exchange(
            cmd_exchange_name,
            ExchangeType.TOPIC,
            durable=True,
            auto_delete=False,
        )
        logger.debug(f"Declared command exchange: {cmd_exchange_name}")

        # 5. Declare command queue with DLQ configuration
        cmd_queue = await channel.declare_queue(
            cmd_queue_name,
            durable=True,
            auto_delete=False,
            arguments={
                # Dead letter exchange - where failed messages go
                "x-dead-letter-exchange": dlq_exchange_name,
                # Dead letter routing key - use "failed" for DLQ routing
                "x-dead-letter-routing-key": "failed",
            },
        )
        logger.debug(f"Declared command queue: {cmd_queue_name}")

        # 6. Bind command queue to command exchange (routing key: #)
        await cmd_queue.bind(cmd_exchange, routing_key="#")
        logger.debug("Bound command queue to command exchange")

        logger.info(
            "AMQP topology declared successfully",
            extra={
                "cmd_exchange": cmd_exchange_name,
                "cmd_queue": cmd_queue_name,
                "dlq_exchange": dlq_exchange_name,
                "dlq_queue": dlq_queue_name,
            },
        )

        return TopologyInfo(
            cmd_exchange=cmd_exchange_name,
            cmd_queue=cmd_queue_name,
            dlq_exchange=dlq_exchange_name,
            dlq_queue=dlq_queue_name,
            cmd_queue_obj=cmd_queue,
        )

    except Exception as e:
        # Pamqp's Exchange.Declare /
        # Queue.Declare raises ``ValueError("Max length exceeded for
        # exchange")`` (or "...for queue") when a name exceeds AMQP's
        # 127-byte protocol cap. SDKConfig now enforces a budget on
        # tenant + service_name at config-load (see runtime/security.py
        # ``check_topology_name_budget``), so this branch is normally
        # unreachable — but bypassing SDKConfig (custom tests, direct
        # construction) could still land here. The full pamqp
        # traceback adds nothing the message doesn't already say;
        # suppress it and re-raise so the upper layer sees the ValueError.
        if isinstance(e, ValueError) and "Max length exceeded" in str(e):
            logger.error(
                "Failed to declare AMQP topology: %s. "
                "An exchange or queue name exceeds AMQP's 127-byte "
                "protocol limit. Shorten KBM_LEDSAS_TENANT and/or "
                "KBM_LEDSAS_SERVICE_NAME — combined length must fit the "
                "worst-case 'dlq.queue.{tenant}.{service}.v1' template.",
                e,
                extra={"error": str(e)},
            )
        else:
            logger.error(
                "Failed to declare AMQP topology",
                exc_info=True,
                extra={"error": str(e)},
            )
        raise
