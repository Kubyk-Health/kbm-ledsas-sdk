"""
Direct transport implementation using RabbitMQ and Azure Blob Storage.

The SDK connects to RabbitMQ for command/response messaging and to Azure
Blob Storage for blob operations.
"""

import asyncio
import inspect
import logging
import re
from collections.abc import AsyncIterator

import aio_pika
import aiormq.exceptions

from kbm_ledsas_sdk.amqp.consumer import (
    BROKER_UNREACHABLE_ERRORS,
    AMQPConsumer,
    BrokerNotReachable,
    BrokerShutdownGuard,
    await_bounded,
    is_broker_unreachable,
)
from kbm_ledsas_sdk.amqp.publisher import AMQPPublisher, _is_unroutable_return
from kbm_ledsas_sdk.amqp.topology import declare_topology
from kbm_ledsas_sdk.blob.azure_client import AzureBlobClient
from kbm_ledsas_sdk.blob.direct_operations import DirectBlobOperations
from kbm_ledsas_sdk.blob.operations import BlobOperations
from kbm_ledsas_sdk.models.messages import Command, Response, Status
from kbm_ledsas_sdk.runtime.security import (
    check_transport_security as _check_transport_security,
)
from kbm_ledsas_sdk.runtime.security import (
    scrub_url_credentials as _scrub_url_credentials,
)
from kbm_ledsas_sdk.transport.base import DeliveryLost, Transport, TransportUnavailable
from kbm_ledsas_sdk.utils.logging import _command_fields, masked_logger

logger = masked_logger(__name__)

# The URL-security check and credential scrubber
# now live in ``kbm_ledsas_sdk.runtime.security`` so SDKConfig's
# model_validator can call them at config-load time — well before the
# AzureBlobClient is constructed. Re-exported here under the private
# underscore names so any external test code that imported
# ``_check_transport_security`` / ``_scrub_url_credentials`` from this
# module keeps working unchanged.

_URL_USERINFO = re.compile(r"(\w+://)[^/\s@]+@")


# An object repr a client library quotes in an exception's text, for
# example aiormq's RuntimeError('<Connection: "amqp://user:******@host:5672/"
# at 0x10a7d9860> closed') when a publish meets a closed connection: it
# holds the broker URL (user name included) and a memory address.
_OBJECT_REPR = re.compile(r"<([A-Za-z_][\w.]*)(?::[^<>]*?)?\s+at\s+0x[0-9a-fA-F]+>")


def _publish_error_text(error: BaseException) -> str:
    """``str(error)`` for a reply- or status-publish failure record: an
    object repr becomes ``<ClassName>`` and a URL's user name and password
    become ``***:***``, as in every other record that names the broker."""
    text = _OBJECT_REPR.sub(r"<\1>", str(error))
    return _URL_USERINFO.sub(r"\1***:***@", text)[:300]


def _describe_error(error: BaseException) -> str:
    """Short, credential-free text for an exception (no traceback)."""
    text = _URL_USERINFO.sub(r"\1***@", str(error))
    return text[:300]


def _is_connect_failure(error: BaseException) -> bool:
    """True for errors that mean the broker could not be reached or refused
    the connection during the AMQP handshake. Covers the protocol library's
    own exceptions (raised for a handshake ``Connection.Close`` on some
    versions) by class name, so it is not imported here."""
    if isinstance(error, (aiormq.exceptions.AMQPError, OSError, asyncio.TimeoutError)):
        return True
    return any(
        cls.__name__ == "PAMQPException" and cls.__module__.startswith("pamqp")
        for cls in type(error).__mro__
    )


def _describe_connect_error(error: BaseException) -> str:
    """Operator-facing reason a connection attempt failed, without a
    traceback. When the broker refused the connection with a
    Connection.Close (unknown virtual host, access refused, ...), its reply
    code and text are the useful part, so they are surfaced instead of the
    client library's generic "Expected Connection.OpenOk" message."""
    seen: set[int] = set()
    chain: list[BaseException] = []
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        for arg in getattr(current, "args", ()):
            reply_text = getattr(arg, "reply_text", None)
            if reply_text:
                code = getattr(arg, "reply_code", None)
                return f"broker refused the connection: {code} {reply_text}".replace("None ", "")
        current = current.__cause__ or current.__context__
    # Some client-library versions report the broker's Connection.Close
    # during the handshake without its reply text. Name the usual causes.
    if any("Connection.Close" in str(e) for e in chain):
        return (
            "the broker closed the connection during the handshake "
            "(Connection.Close; the client library does not report the "
            "broker's reason). The usual cause is a virtual host that does "
            "not exist, or a user without access to it: check the path after "
            "the port in the URL"
        )
    return _describe_cause(error)


_INEQUIVALENT_ARG = re.compile(
    r"PRECONDITION_FAILED - inequivalent arg '([^']+)' for (queue|exchange) '([^']+)'"
)


def _setup_refusal_hint(reason: str) -> str:
    """Remedy for a known setup refusal, or "" when none applies.

    ``PRECONDITION_FAILED - inequivalent arg ...`` means a queue or
    exchange the service declares already exists on the broker with other
    settings (typically created by an older release or another tool, or
    left in a reused local broker volume). Re-declaring cannot change
    them; deleting the object once lets the service re-create it.
    """
    m = _INEQUIVALENT_ARG.search(reason)
    if not m:
        return ""
    _arg, kind, name = m.groups()
    hint = (
        f"The {kind} '{name}' already exists on the broker with different "
        f"settings than this SDK declares. Delete it once (move or drain "
        f"any messages in it first if you need them); the service re-creates "
        f"it at startup"
    )
    if kind == "queue" and name.startswith("dlq.queue."):
        hint += " with the current settings (7-day message TTL, 100,000-message cap)"
    return hint + "."


def _describe_cause(error: BaseException | None) -> str:
    """Operator-facing cause of an unreachable broker: the error class and
    its message, so an unexpected error that was classified as an outage can
    still be told apart from a real one (no traceback)."""
    if error is None:
        return "unknown"
    text = _describe_error(error)
    if isinstance(error, BrokerNotReachable):
        return text
    name = type(error).__name__
    return f"{name} ({text})" if text else name


def _recorded_connection_class(created: list) -> type:
    """A ``RobustConnection`` subclass that appends each instance to
    ``created`` (``connect_robust`` gives the caller no reference to the
    connection until its first attempt succeeds)."""

    class _RecordedRobustConnection(aio_pika.RobustConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    return _RecordedRobustConnection


class DirectTransport(Transport):
    """
    Direct transport using RabbitMQ and Azure Blob SDK.

    Connects to RabbitMQ for command/response messaging and to Azure
    Blob Storage for blob operations.

    Attributes:
        rabbitmq_url: RabbitMQ connection URL
        blob_conn_string: Azure Blob connection string
        service_name: Service name for topology naming
        tenant: Optional tenant ID
        prefetch_count: RabbitMQ prefetch count
        default_container: Default blob container
        connection: RabbitMQ connection
        channel: RabbitMQ channel
        consumer: AMQP consumer
        publisher: AMQP publisher
        azure_client: Azure Blob client
        blob_ops: Blob operations interface
    """

    #: Upper bound, in seconds, for each broker round trip made once
    #: shutdown has begun: the reply/status publishes of commands still in
    #: flight (channel open, exchange check, publish + confirm, channel
    #: close) and, in stop(), the consumer cancel, channel close and
    #: connection close. A healthy broker answers in milliseconds; the
    #: bound only matters when the broker is unreachable or stalled. Once
    #: one round trip finds the broker unreachable, the remaining ones are
    #: skipped (only the local connection close is still attempted), so at
    #: most two waits can run to the bound (10 s with the default), well
    #: inside a 30-60 s termination grace period. An ACK / NACK / reject
    #: sent after shutdown began has the same bound but is not a round
    #: trip: it only waits if the connection cannot take the frame at all
    #: (with the broker stopped it fails at once; with a frozen broker it
    #: is buffered). Handler run time itself is never bounded here.
    SHUTDOWN_STEP_TIMEOUT_S: float = 5.0

    def __init__(
        self,
        rabbitmq_url: str,
        blob_conn_string: str,
        service_name: str,
        tenant: str | None = None,
        prefetch_count: int = 10,
        default_container: str = "dev",
        max_payload_bytes: int = 16 * 1024 * 1024,
        connect_timeout: float = 15.0,
    ):
        """
        Initialize Direct transport.

        Args:
            rabbitmq_url: RabbitMQ connection URL (amqp://...)
            blob_conn_string: Azure Blob Storage connection string
            service_name: Service name for queue/exchange naming
            tenant: Optional tenant ID for multi-tenancy
            prefetch_count: Number of messages to prefetch
            default_container: Default blob container for uploads
            max_payload_bytes: Reject inbound messages whose body
                exceeds this size. Forwarded to AMQPConsumer.
            connect_timeout: Seconds one connection attempt to the broker
                may take (KBM_LEDSAS_CONNECT_TIMEOUT). A broker that does
                not answer (a firewall dropping the packets) then fails the
                start within this time, with one clean ERROR line, instead
                of after the operating system's own connect timeout (75 s
                to over 2 minutes), which an orchestrator's liveness probe
                may not wait for.
        """
        self.rabbitmq_url = rabbitmq_url
        self.blob_conn_string = blob_conn_string
        self.service_name = service_name
        self.tenant = tenant
        self.prefetch_count = prefetch_count
        self.default_container = default_container
        self.max_payload_bytes = max_payload_bytes
        self.connect_timeout = connect_timeout

        # AMQP components (initialized in start())
        self.connection: aio_pika.Connection | None = None
        # Consumer/topology channel. The publisher uses its own transient
        # channel per publish (see AMQPPublisher) so a publish-side channel
        # error (e.g. missing reply exchange) cannot interfere with the
        # consumer's ack flow.
        self.channel: aio_pika.Channel | None = None
        self.consumer: AMQPConsumer | None = None
        self.publisher: AMQPPublisher | None = None

        # Operational counters, split into two so an ops dashboard
        # can tell apart "the orchestrator's reply infrastructure is
        # broken" (response path, every failure dead-letters a command)
        # from "the orchestrator isn't listening to status updates"
        # (status path, best-effort and benign). Both are exposed for
        # logging and tests; not currently wired into a metrics backend.
        self.reply_publish_failures: int = 0
        self.status_publish_failures: int = 0
        # Subset of reply_publish_failures: the reply exchange EXISTS but
        # the mandatory publish came back unroutable (Basic.Return — no
        # queue bound for the routing key; see publisher's
        # _is_unroutable_return). Distinguishes "orchestrator never
        # declared its reply exchange" from "declared it but the reply
        # queue is missing/unbound". Statuses stay best-effort with the
        # single status_publish_failures counter.
        self.reply_unroutable_failures: int = 0
        # Replies whose publish could not be confirmed because the broker was
        # unreachable (or did not answer) after shutdown began. The command
        # is then left unacknowledged (the broker redelivers it); reported
        # once by stop(). A stalled broker may still deliver such a reply
        # late, so the caller can receive it twice (at-least-once).
        self.replies_undelivered_at_shutdown: int = 0
        # Replies not sent because the broker connection was lost while the
        # command ran (outside shutdown). The command is redelivered.
        self.replies_lost_with_connection: int = 0

        # Bounds broker round trips once shutdown begins (shared with the
        # publisher). See begin_shutdown().
        self._shutdown_guard = BrokerShutdownGuard(self.SHUTDOWN_STEP_TIMEOUT_S)

        # True between a lost broker connection and the client's automatic
        # reconnect: one WARNING when it is lost, one INFO when it is back.
        self._broker_connection_lost: bool = False
        # The error the broker connection was lost with (None while it is
        # up): the cause reported when a shutdown begins during the outage.
        self._broker_connection_lost_error: BaseException | None = None

        # Blob components
        self.azure_client = AzureBlobClient(blob_conn_string, default_container)
        self.blob_ops = DirectBlobOperations(self.azure_client, default_container)

        logger.info(
            "DirectTransport initialized",
            extra={
                "service_name": service_name,
                "tenant": tenant,
                "prefetch": prefetch_count,
            },
        )

    async def start(self) -> None:
        """
        Initialize transport and connect to RabbitMQ.

        Connects to RabbitMQ, declares topology, starts consumer and publisher.

        Raises:
            Exception: If connection or topology declaration fails
        """
        logger.info("Starting DirectTransport")

        try:
            # Refuse cleartext AMQP to non-loopback hosts unless
            # explicitly opted in. Raises ValueError on misuse.
            _check_transport_security(self.rabbitmq_url)
            # Scrub user:password from the URL before logging.
            # KBM_LEDSAS_LOG_LEVEL=DEBUG is exactly the level a stuck
            # customer turns on; the URL in plaintext would put their
            # broker credentials in their log aggregator.
            logger.info(
                "Connecting to the AMQP broker at %s (timeout %gs)",
                _scrub_url_credentials(self.rabbitmq_url),
                self.connect_timeout,
            )
            # The connection object being made, so a start that is given
            # up while it connects can close it (see below).
            connecting: list = []
            try:
                # The timeout bounds each connection attempt, including
                # later reconnects; a first attempt that times out fails
                # the start (TransportUnavailable below).
                self.connection = await aio_pika.connect_robust(
                    self.rabbitmq_url,
                    timeout=self.connect_timeout,
                    connection_class=_recorded_connection_class(connecting),
                )
            except asyncio.CancelledError:
                # A stop signal while connecting (ServiceApp cancels the
                # start). The client's automatic reconnect runs in a task
                # of its own that survives the cancellation and would keep
                # trying (and keep the process alive) unless the
                # connection is closed.
                await self._close_unstarted(connecting)
                raise
            except aiormq.exceptions.ProbableAuthenticationError:
                raise  # credentials: dedicated handling below
            except Exception as e:
                # Broker unreachable (connection refused, DNS, timeout) or
                # it refused the connection during setup (unknown virtual
                # host, ...): an expected operator-facing condition. One
                # clean line below, no traceback. Anything else (a
                # malformed URL is a ValueError) keeps its own handling.
                if not _is_connect_failure(e):
                    raise
                if isinstance(e, asyncio.TimeoutError):
                    reason = (
                        f"no answer within {self.connect_timeout:g}s "
                        "(KBM_LEDSAS_CONNECT_TIMEOUT); a firewall may be "
                        "dropping the connection, or the host or port is wrong"
                    )
                else:
                    reason = _describe_connect_error(e)
                raise TransportUnavailable(
                    "Could not connect to the AMQP broker at "
                    f"{_scrub_url_credentials(self.rabbitmq_url)}: "
                    f"{reason}. Check "
                    "KBM_LEDSAS_RABBITMQ_URL (host, port, virtual host) and "
                    "that the broker is running."
                ) from e

            # Report broker outages / recoveries as SDK log events.
            self._watch_connection_state(self.connection)

            # Create the consumer/topology channel.
            self.channel = await self.connection.channel()
            logger.debug("RabbitMQ consumer channel created")
            # Name a close of it by the broker at its consumer_timeout.
            self._watch_consumer_channel(self.channel)

            # Declare topology (exchanges, queues) on the consumer channel
            topology = await declare_topology(
                channel=self.channel,
                service_name=self.service_name,
                tenant=self.tenant,
            )
            logger.info(
                "Topology declared",
                extra={
                    "cmd_exchange": topology.cmd_exchange,
                    "cmd_queue": topology.cmd_queue,
                },
            )

            # Initialize publisher on the shared connection; each publish
            # opens its own short-lived channel. The guard bounds its broker
            # round trips once shutdown begins.
            self.publisher = AMQPPublisher(self.connection, shutdown_guard=self._shutdown_guard)
            await self.publisher.enable_confirms()
            logger.debug("Publisher initialized (transient-channel mode)")

            # Initialize consumer
            self.consumer = AMQPConsumer(
                topology.cmd_queue_obj,
                self.prefetch_count,
                max_payload_bytes=self.max_payload_bytes,
                shutdown_guard=self._shutdown_guard,
            )
            await self.consumer.start()
            logger.debug("Consumer started")

            logger.info("DirectTransport started successfully")

        except asyncio.CancelledError:
            # The start was given up (a stop signal) after the connection
            # was made: close what was opened, bounded like any stop.
            logger.info("DirectTransport start cancelled; stopping")
            await self.stop()
            raise
        except ValueError as e:
            # Intentional config-policy refusals (cleartext-AMQP
            # rejection, etc.) raise plain ValueError. app.py's
            # configuration-error path will log a single clean
            # "Configuration error: ..." line with the actionable
            # message; we just need to clean up and re-raise. Suppress
            # the 25-line traceback here — the customer doesn't need
            # the SDK's call stack to know the URL is misconfigured.
            logger.debug(
                "DirectTransport.start refused by config-policy check: %s",
                e,
            )
            await self.stop()
            raise
        except TransportUnavailable as e:
            logger.error("%s", e)
            await self.stop()
            raise
        except aiormq.exceptions.ProbableAuthenticationError as e:
            # Broker rejected our credentials. This is an expected
            # operator-error class — wrong KBM_LEDSAS_RABBITMQ_URL
            # password, missing vhost ACL, etc. The aiormq/aio_pika
            # internals traceback (~60 lines) has no diagnostic value
            # for the customer; the broker's reply text is the actual
            # signal. Log one clean line; app.py's ServiceApp.run()
            # catches this class and exits without re-logging.
            logger.error(
                "AMQP authentication refused by broker — check "
                "KBM_LEDSAS_RABBITMQ_URL credentials. Broker reply: %s",
                e,
            )
            await self.stop()
            raise
        except aiormq.exceptions.AMQPError as e:
            # The broker refused a setup step after connecting (e.g. the
            # user has no permission on the virtual host's exchanges or
            # queues, or a queue exists with different arguments): one
            # clean line with the broker's reason, no traceback.
            reason = _describe_connect_error(e)
            hint = _setup_refusal_hint(reason)
            if hint:
                reason = f"{reason}. {hint}"
            logger.error(
                "AMQP broker refused the startup sequence (declaring the "
                "service's exchanges and queues): %s",
                reason,
                extra={"error": reason},
            )
            await self.stop()
            raise TransportUnavailable(f"AMQP broker refused the startup sequence: {reason}") from e
        except Exception as e:
            logger.error("Failed to start DirectTransport", exc_info=True, extra={"error": str(e)})
            # Cleanup on failure
            await self.stop()
            raise

    async def _close_unstarted(self, connecting: list) -> None:
        """Close a connection whose first attempt was cancelled (its
        reconnect task ends with it). Bounded like every stop step."""
        for connection in connecting:
            try:
                await await_bounded(connection.close(), self.SHUTDOWN_STEP_TIMEOUT_S)
            except BaseException as e:  # noqa: BLE001 - never block the stop
                if isinstance(e, asyncio.CancelledError):
                    raise
                logger.debug("Closing the unstarted connection failed: %s", e)

    async def subscribe(self) -> AsyncIterator[Command]:
        """
        Subscribe to incoming commands.

        Yields commands from RabbitMQ queue as they arrive.

        Yields:
            Command: Incoming command messages

        Raises:
            RuntimeError: If transport not started
        """
        if not self.consumer:
            raise RuntimeError("Transport not started. Call start() first.")

        logger.info("Subscribing to commands")

        try:
            # Consume commands indefinitely
            while True:
                command = await self.consumer.consume()
                logger.debug(
                    "Yielding command",
                    extra={
                        **_command_fields(command.envelope),
                    },
                )
                yield command

        except Exception as e:
            logger.error("Error in subscribe loop", exc_info=True, extra={"error": str(e)})
            raise

    async def ack(self, message_id: str) -> None:
        """
        Acknowledge successful command processing.

        Args:
            message_id: Message ID from command envelope

        Raises:
            RuntimeError: If transport not started
            ValueError: If message_id unknown
        """
        if not self.consumer:
            raise RuntimeError("Transport not started. Call start() first.")

        logger.debug("ACK: %s", message_id, extra=self._held_command_fields(message_id))
        await self.consumer.ack(message_id)

    async def nack(self, message_id: str, requeue: bool) -> None:
        """
        Negative acknowledge - command processing failed.

        Args:
            message_id: Message ID from command envelope
            requeue: True to retry, False to send to DLQ

        Raises:
            RuntimeError: If transport not started
            ValueError: If message_id unknown
        """
        if not self.consumer:
            raise RuntimeError("Transport not started. Call start() first.")

        logger.debug(
            "NACK: %s (requeue=%s)",
            message_id,
            requeue,
            extra=self._held_command_fields(message_id),
        )
        await self.consumer.nack(message_id, requeue)

    def _held_command_fields(self, message_id: str) -> dict:
        """The identity fields of the command ``message_id`` the consumer
        holds (see ``AMQPConsumer.command_fields``); only ``message_id``
        when the consumer is not an ``AMQPConsumer`` (a test double)."""
        if isinstance(self.consumer, AMQPConsumer):
            return self.consumer.command_fields(message_id)
        return dict(message_id=message_id)  # noqa: C408

    async def send_response(self, response: Response) -> bool:
        """
        Send response message to orchestrator.

        Returns True on success (or when there is no reply_to), False when
        the publish was attempted but failed (e.g. the caller's reply
        exchange does not exist, or a channel error occurred). The caller
        should treat a False return as a Permanent failure and NACK the
        command with requeue=False — retrying the handler will produce the
        same failure and would otherwise loop until DLQ.

        Args:
            response: Response message with envelope and payload

        Returns:
            bool: True if published (or skipped), False on publish failure.

        Raises:
            RuntimeError: If transport not started
            DeliveryLost: The broker connection was lost while the command
                ran, so neither the reply nor the command's ACK / NACK can
                be sent. Logged once (WARNING); the broker delivers the
                command again.
        """
        if not self.publisher:
            raise RuntimeError("Transport not started. Call start() first.")

        # The command has finished: from here its caller can have the
        # reply before the ACK reaches the broker, so a command that reuses
        # its message_id right away waits for the settlement instead of
        # being dead-lettered as a duplicate (AMQPConsumer.mark_finishing).
        mark_finishing = getattr(self.consumer, "mark_finishing", None)
        if callable(mark_finishing):
            mark_finishing(response.envelope.message_id)

        # Extract exchange name from reply_to field. reply_to is an
        # arbitrary caller-owned exchange name — the caller declares (and
        # binds a queue to) whatever exchange it wants replies on; the SDK
        # does not impose a naming convention here.
        exchange_name = response.envelope.reply_to

        # Skip if no reply_to specified
        if not exchange_name:
            logger.info(
                "Skipping response send (no reply_to specified)",
                extra={
                    **_command_fields(response.envelope),
                },
            )
            return True

        logger.debug(
            "Sending response",
            extra={
                **_command_fields(response.envelope),
                "exchange": exchange_name,
            },
        )

        try:
            await self.publisher.publish_response(
                response=response,
                exchange_name=exchange_name,
                routing_key="response",
            )
            return True
        except Exception as e:
            if self._unreachable_at_shutdown(e):
                # Shutdown in progress and the broker cannot be reached (or
                # does not answer): not a reply-infrastructure problem, and
                # the command can be neither ACKed nor dead-lettered. It
                # stays unacknowledged and the broker redelivers it; stop()
                # reports the outage once (WARNING). "Not confirmed" rather
                # than "not delivered": a broker that stalled (e.g. blocked
                # publishers on a resource alarm) may already hold the
                # publish and deliver it once it recovers, so the caller can
                # receive this reply twice (at-least-once).
                self.replies_undelivered_at_shutdown += 1
                logger.info(
                    "Reply to exchange '%s' not confirmed: AMQP broker not "
                    "reachable during shutdown (%s). The command stays "
                    "unacknowledged and the broker redelivers it; the reply "
                    "may still arrive late, so the caller can receive it "
                    "twice.",
                    exchange_name,
                    type(e).__name__,
                    extra={
                        **_command_fields(response.envelope),
                        "exchange": exchange_name,
                        "error_class": type(e).__name__,
                    },
                )
                return False
            if self._reply_lost_with_connection(e, response.envelope.message_id):
                # The broker connection was lost while the command ran
                # (e.g. a broker restart): not a reply-infrastructure
                # problem. The command's delivery belongs to the lost
                # connection, so the broker returns it to the queue and
                # delivers it again; give it back here and say so once.
                self.replies_lost_with_connection += 1
                logger.warning(
                    "Reply to exchange '%s' not sent: the connection to the "
                    "AMQP broker was lost (%s). The command was not "
                    "acknowledged; the broker delivers it again, so it runs "
                    "again (after a broker restart, only if it was published "
                    "persistent).",
                    exchange_name,
                    type(e).__name__,
                    extra={
                        **_command_fields(response.envelope),
                        "exchange": exchange_name,
                        "error_class": type(e).__name__,
                    },
                )
                if self.consumer is not None:
                    await self.consumer.release(response.envelope.message_id)
                raise DeliveryLost("reply not sent: broker connection lost") from e
            self.reply_publish_failures += 1
            # Point the operator at the actual cause:
            # - unroutable return (Basic.Return): the exchange exists but
            #   no queue is bound — bump the distinguishing sub-counter.
            # - bare DeliveryError (Basic.Nack): the broker refused to
            #   confirm the publish (e.g. reject-publish overflow policy).
            # - pamqp 127-byte ValueError: the exchange name itself was
            #   rejected on protocol grounds. The envelope schema caps
            #   reply_to at 127, so reaching this branch means the value
            #   bypassed the SDK validator.
            # - anything else: exchange missing / channel error.
            # A DeliveryError's args can embed the returned message, body
            # bytes included — never format the exception itself into the
            # log line; use a short static description instead.
            if isinstance(e, aio_pika.exceptions.DeliveryError):
                if _is_unroutable_return(e):
                    self.reply_unroutable_failures += 1
                    error_text = "message returned unroutable (Basic.Return)"
                    action_hint = (
                        "the exchange exists but has no queue bound for "
                        "routing key 'response' — verify the caller declared "
                        "AND bound its reply queue (durable, not auto-delete)."
                    )
                else:
                    error_text = "broker did not confirm the publish (Basic.Nack)"
                    action_hint = (
                        "the broker rejected the reply publish — check the "
                        "reply queue's policies (e.g. x-overflow: "
                        "reject-publish) and broker health."
                    )
            elif isinstance(e, ValueError) and "Max length exceeded" in str(e):
                error_text = _publish_error_text(e)
                action_hint = (
                    "the exchange name is longer than AMQP's 127-byte "
                    "protocol limit. Use a shorter reply_to."
                )
            elif type(e).__name__ == "ChannelAccessRefused":
                error_text = _publish_error_text(e)
                action_hint = (
                    "the broker user in KBM_LEDSAS_RABBITMQ_URL may not "
                    "publish to this exchange (no write permission), or it "
                    "is an internal exchange; reply_to must name the "
                    "caller's own reply exchange."
                )
            elif type(e).__name__ == "ChannelPreconditionFailed" and "message size" in str(e):
                error_text = _publish_error_text(e)
                action_hint = (
                    "the reply is larger than the broker accepts "
                    "(max_message_size); return a large result as a blob "
                    "reference instead."
                )
            elif type(e).__name__ == "ChannelNotFoundEntity":
                error_text = _publish_error_text(e)
                action_hint = "verify the orchestrator pre-declared this exchange."
            else:
                error_text = _publish_error_text(e)
                action_hint = (
                    "the broker or the connection refused the publish (see "
                    "the error); verify the reply exchange exists and "
                    "accepts this service's publishes."
                )
            logger.error(
                "Failed to send response to reply_to exchange '%s': %s "
                "(failures so far: %d). Command will be NACKed to DLQ; "
                "%s",
                exchange_name,
                error_text,
                self.reply_publish_failures,
                action_hint,
                extra={
                    **_command_fields(response.envelope),
                    "exchange": exchange_name,
                    "error": error_text,
                    "reply_publish_failures": self.reply_publish_failures,
                    "reply_unroutable_failures": self.reply_unroutable_failures,
                },
            )
            return False

    async def send_status(self, status: Status) -> None:
        """
        Send status update to orchestrator.

        Statuses are best-effort: if the reply_to exchange does not exist
        (or the publish fails for another reason), the failure is logged as
        one WARNING and the command continues (no exception).

        Args:
            status: Status message with envelope and payload

        Raises:
            RuntimeError: If transport not started
        """
        if not self.publisher:
            raise RuntimeError("Transport not started. Call start() first.")

        # Extract exchange name from reply_to field
        exchange_name = status.envelope.reply_to

        # Skip if no reply_to specified
        if not exchange_name:
            logger.info(
                "Skipping status send (no reply_to specified)",
                extra={
                    **_command_fields(status.envelope),
                },
            )
            return

        logger.debug(
            "Sending status",
            extra={
                **_command_fields(status.envelope),
                "exchange": exchange_name,
            },
        )

        try:
            await self.publisher.publish_status(
                status=status,
                exchange_name=exchange_name,
                routing_key="status",
            )
        except Exception as e:
            if self._unreachable_at_shutdown(e):
                # Best-effort status during shutdown with the broker gone:
                # INFO only; stop() reports the outage once (WARNING).
                logger.info(
                    "Status to exchange '%s' not confirmed: AMQP broker not "
                    "reachable during shutdown (%s). Continuing.",
                    exchange_name,
                    type(e).__name__,
                    extra={
                        **_command_fields(status.envelope),
                        "exchange": exchange_name,
                        "error_class": type(e).__name__,
                    },
                )
                return
            # Status is informational and best-effort: log and continue.
            # Bump the *status* failure counter (not the response
            # one) so an ops dashboard can distinguish "orchestrator's
            # reply infra is broken" from "orchestrator isn't listening
            # to status updates."
            self.status_publish_failures += 1
            # A DeliveryError's args can embed the returned message, body
            # bytes included — never format the exception itself into the
            # log line; use a short static description instead.
            if isinstance(e, aio_pika.exceptions.DeliveryError):
                error_text = (
                    "message returned unroutable (Basic.Return)"
                    if _is_unroutable_return(e)
                    else "broker did not confirm the publish (Basic.Nack)"
                )
            else:
                error_text = _publish_error_text(e)
            logger.warning(
                "Failed to send status to reply_to exchange '%s': %s "
                "(status failures so far: %d). Continuing.",
                exchange_name,
                error_text,
                self.status_publish_failures,
                extra={
                    **_command_fields(status.envelope),
                    "exchange": exchange_name,
                    "error": error_text,
                    "status_publish_failures": self.status_publish_failures,
                },
            )

    def _watch_connection_state(self, connection: object) -> None:
        """Log broker outages and recoveries as SDK events.

        The client reconnects by itself (``/ready`` answers 503 meanwhile),
        but only the client library's own log lines showed it. Register on
        the connection's close and reconnect callbacks so the SDK log
        (``KBM_LEDSAS_LOG_FORMAT``) carries one WARNING when the
        connection is lost and one INFO once it is restored.
        """
        close_callbacks = getattr(connection, "close_callbacks", None)
        reconnect_callbacks = getattr(connection, "reconnect_callbacks", None)
        if close_callbacks is None or reconnect_callbacks is None:
            return
        try:
            close_callbacks.add(self._on_broker_connection_closed)
            reconnect_callbacks.add(self._on_broker_reconnected)
        except Exception:  # pragma: no cover - never block startup on logging
            logger.debug("Could not watch the broker connection state", exc_info=True)

    def _watch_consumer_channel(self, channel: object) -> None:
        """Register on the consumer channel's close callbacks, so that a
        close by the broker at its consumer_timeout (a command held
        unacknowledged too long) is reported as such (see
        ``AMQPConsumer.note_channel_closed``). The connection stays up and
        the channel is reopened by the client; only the client library's own
        records showed the close, and the commands whose delivery the broker
        took back were later reported as if the connection had been lost."""
        close_callbacks = getattr(channel, "close_callbacks", None)
        add = getattr(close_callbacks, "add", None)
        # A real channel's add() is a plain method; a coroutine function
        # here is a test double, whose call would never run.
        if add is None or inspect.iscoroutinefunction(add):
            return
        try:
            add(self._on_consumer_channel_closed)
        except Exception:  # pragma: no cover - never block startup on logging
            logger.debug("Could not watch the consumer channel", exc_info=True)

    def _on_consumer_channel_closed(
        self, _sender: object, error: BaseException | None = None
    ) -> None:
        """Close callback of the consumer channel (see
        ``_watch_consumer_channel``). Never raises."""
        consumer = self.consumer
        if consumer is None:
            return
        try:
            consumer.note_channel_closed(error)
        except Exception:  # pragma: no cover - a callback must not raise
            logger.debug("Consumer channel close not classified", exc_info=True)

    def _on_broker_connection_closed(
        self, _sender: object, error: BaseException | None = None
    ) -> None:
        """Close callback: one WARNING per outage (not on our own close)."""
        if self._shutdown_guard.stopping or self._broker_connection_lost:
            return
        if error is None or isinstance(error, asyncio.CancelledError):
            return  # closed locally, not lost
        self._broker_connection_lost = True
        self._broker_connection_lost_error = error
        logger.warning(
            "Lost the connection to the AMQP broker (%s); reconnecting "
            "automatically. Commands not yet acknowledged are redelivered "
            "by the broker; /ready answers 503 until the connection is back.",
            _describe_error(error) or type(error).__name__,
            extra={"error_class": type(error).__name__},
        )

    def _on_broker_reconnected(self, _sender: object, *_args: object) -> None:
        """Reconnect callback: one INFO once the connection is back."""
        if not self._broker_connection_lost:
            return
        self._broker_connection_lost = False
        self._broker_connection_lost_error = None
        logger.info("Reconnected to the AMQP broker; consuming commands again")

    def begin_shutdown(self) -> None:
        """Start shutting down: bound every broker round trip from now on.

        Called by the service when it stops taking new work, before the
        commands still in flight are drained (and again, idempotently, by
        stop()). Their reply and status publishes then get at most
        ``SHUTDOWN_STEP_TIMEOUT_S`` per broker round trip, including one
        already waiting, so a stopped or stalled broker cannot hold the
        drain until the platform kills the process; their ACK / NACK /
        reject gets the same bound. A reply that cannot be confirmed, or a
        settlement that cannot be sent, leaves its command unacknowledged
        for redelivery (see ``broker_unreachable_at_shutdown``). Handler
        run time is not bounded, and with a healthy broker nothing changes.
        """
        self._shutdown_guard.step_timeout = self.SHUTDOWN_STEP_TIMEOUT_S
        self._shutdown_guard.begin_shutdown()
        # The connection was already lost when the shutdown began (the
        # client is still reconnecting): the broker counts as unreachable
        # from the start, so the service cancels its running handlers at
        # once (their reply and acknowledgement cannot be sent on the lost
        # connection, and the broker delivers the commands again) and stop()
        # skips the round trips, instead of waiting for handlers to finish
        # and for a reconnection.
        if self._connection_down():
            # Latched without a cause unless the outage's error is known:
            # the first round trip that fails names it, else the report
            # says the connection was already lost (see stop()).
            self._shutdown_guard.mark_unreachable(self._broker_connection_lost_error)

    def take_slow_body_reads(self) -> "list[tuple[float, int]]":
        """The ``(seconds, body bytes)`` of each command body whose reading
        held the event loop at least 0.05 s since the last call (decoding,
        parsing and checking it run on the event loop), and forget them.
        The event-loop watchdog names them when the loop was blocked."""
        if self.consumer is None:
            return []
        return self.consumer.take_slow_body_reads()

    @property
    def broker_unreachable_at_shutdown(self) -> bool:
        """True once shutdown has begun and the broker was found unreachable.

        While True, a command whose reply could not be confirmed, or whose
        ACK / requeue / dead-letter could not be sent (the consumer then
        raises ``BrokerNotReachable``, a ``ConnectionError``), must be left
        unacknowledged rather than dead-lettered: neither an ACK nor a
        reject can reach the broker reliably, and the broker redelivers
        every unacknowledged message once this connection is gone.
        """
        return self._shutdown_guard.fail_fast

    def _reply_lost_with_connection(self, error: BaseException, message_id: str) -> bool:
        """True when a reply publish failed because the broker connection
        is gone, outside shutdown.

        Requires both a connection-level error and evidence that the
        connection really dropped (the connection is down or reconnecting,
        or the command arrived on a channel that has closed since). A
        failure while the connection is healthy keeps the existing
        handling (the command is dead-lettered), so a defect can never
        requeue a command over and over.
        """
        if not is_broker_unreachable(error):
            return False
        if self._broker_connection_lost or not self.is_ready():
            return True
        consumer = self.consumer
        return consumer is not None and consumer.delivery_is_stale(message_id)

    def _unreachable_at_shutdown(self, error: BaseException) -> bool:
        """Classify a publish failure as "broker unreachable during shutdown"
        (latching the guard), as opposed to a broker refusal or a failure in
        normal operation, both of which keep their existing handling."""
        guard = self._shutdown_guard
        if guard.stopping and is_broker_unreachable(error):
            guard.mark_unreachable(error)
            return True
        return False

    async def stop_consuming(self) -> list[Command]:
        """Stop receiving new commands at shutdown (called by the service
        when it stops taking commands, before the drain).

        The consumer is cancelled (one broker round trip, bounded by
        ``SHUTDOWN_STEP_TIMEOUT_S``; skipped once the broker is known to be
        unreachable) while the channel stays open, so the commands still
        running reply and are acknowledged as usual, and the broker sends
        the commands this service gives back to the other replicas instead
        of to this one. The commands received but not yet handed to the
        service are given back at once and returned. Never raises.
        """
        if self.consumer is None:
            return []
        return await self.consumer.stop_consuming(
            timeout=self.SHUTDOWN_STEP_TIMEOUT_S,
            skip_cancel=self._shutdown_guard.broker_unreachable,
        )

    async def release(self, message_id: str) -> None:
        """Give a command's delivery back to the broker without counting a
        retry and without replying (for example a command that got a worker
        only after shutdown began). Dropped if its channel has closed (the
        broker already has it back). Never raises."""
        if self.consumer is not None:
            await self.consumer.release(message_id)

    def get_blob_operations(self) -> BlobOperations:
        """
        Get blob operations interface.

        Returns:
            DirectBlobOperations instance
        """
        return self.blob_ops

    async def stop(self) -> None:
        """
        Graceful shutdown of transport.

        Stops consumer, closes connections, and cleans up resources.

        Every step that needs a broker round trip (consumer cancel,
        channel close, connection close) is bounded by
        ``SHUTDOWN_STEP_TIMEOUT_S``, like the reply/status publishes of
        commands drained after ``begin_shutdown()``. If the broker is
        unreachable or does not answer (an outage while the client is
        reconnecting, or a stalled broker), shutdown stops waiting for it:
        the remaining round trips are skipped and the connection is closed
        locally, which also ends the client's reconnect loop. A single
        WARNING reports this, with the cause (error class and message, no
        traceback); unacknowledged messages, including commands whose reply
        could not be confirmed or whose ACK / requeue / dead-letter could
        not be sent, are redelivered by the broker, so nothing is lost (a
        finished command runs again, at-least-once). Without these bounds a
        SIGTERM during a broker outage would wait for a reconnection that
        never comes, until the platform kills the process.
        """
        logger.info("Stopping DirectTransport")

        step_timeout = self.SHUTDOWN_STEP_TIMEOUT_S
        guard = self._shutdown_guard
        self.begin_shutdown()
        # Read before consumer.stop() clears them; reported if the broker
        # turns out to be unreachable.
        pending_at_stop = self.consumer.pending_count if self.consumer else 0
        if not isinstance(pending_at_stop, int):
            pending_at_stop = 0
        unsettled = getattr(self.consumer, "unsettled_at_shutdown", 0)
        if not isinstance(unsettled, int):
            unsettled = 0
        # False when stop() runs because start() failed before the consumer
        # was subscribed: start() already logged the failure as one line.
        was_consuming = bool(
            self.consumer is not None and getattr(self.consumer, "consumer_tag", None)
        )

        # Stop consumer. Once the broker is known unreachable (e.g. a reply
        # publish of a drained command already found it gone), the cancel
        # round trip is skipped altogether.
        if self.consumer:
            try:
                if guard.broker_unreachable:
                    stopped = await self.consumer.stop(timeout=step_timeout, skip_cancel=True)
                else:
                    stopped = await self.consumer.stop(timeout=step_timeout)
                # Only an explicit False means "broker not reachable".
                if stopped is False:
                    cause = getattr(self.consumer, "stop_error", None)
                    guard.mark_unreachable(
                        cause
                        if isinstance(cause, BaseException)
                        else BrokerNotReachable("connection to the broker already lost")
                    )
                logger.debug("Consumer stopped")
            except Exception:
                logger.error("Error stopping consumer", exc_info=True)

        # Close consumer/topology channel. Publisher channels are transient
        # and already closed by AMQPPublisher.publish_*() in their finally.
        # Skipped when the broker is unreachable: closing the connection
        # below releases the channel locally without a round trip.
        if not guard.broker_unreachable and self.channel and not self.channel.is_closed:
            try:
                await await_bounded(self.channel.close(), step_timeout)
                logger.debug("Channel closed")
            except BROKER_UNREACHABLE_ERRORS as e:
                guard.mark_unreachable(e)
            except Exception:
                logger.error("Error closing channel", exc_info=True)

        # Close connection. Always attempted: on a robust connection this
        # also stops the reconnect loop, and with the broker gone it
        # completes locally.
        if self.connection and not self.connection.is_closed:
            try:
                await await_bounded(self.connection.close(), step_timeout)
                logger.debug("Connection closed")
            except BROKER_UNREACHABLE_ERRORS as e:
                guard.mark_unreachable(e)
            except Exception:
                logger.error("Error closing connection", exc_info=True)

        if guard.broker_unreachable:
            cause = guard.cause or BrokerNotReachable("connection to the broker already lost")
            undelivered = self.replies_undelivered_at_shutdown
            message = (
                "AMQP broker not reachable during shutdown (connection lost, "
                "or no answer within %g s); stopped without waiting for it "
                "(connection closed locally). Unacknowledged messages (%d "
                "held by this consumer) are redelivered by the broker."
            )
            args: list = [step_timeout, pending_at_stop]
            if undelivered:
                message += (
                    " %d finished command(s) could not confirm their reply "
                    "and run again on redelivery; a reply may still arrive "
                    "late, so the caller can receive it twice."
                )
                args.append(undelivered)
            if unsettled:
                message += (
                    " %d finished command(s) could not be acknowledged, "
                    "requeued or dead-lettered and run again on redelivery."
                )
                args.append(unsettled)
            # Read after the consumer stopped: a settlement can still be
            # written while it stops.
            settled = getattr(self.consumer, "settled_while_stopping", 0)
            if not isinstance(settled, int):
                settled = 0
            if settled:
                message += (
                    " %d acknowledgement(s), requeue(s) or dead-letter(s) "
                    "were sent after the stop signal; the broker does not "
                    "answer them, so they may not have reached it, and "
                    "those commands may be delivered and run again."
                )
                args.append(settled)
            message += " Cause: %s."
            args.append(_describe_cause(cause))
            # One WARNING for a real shutdown. When stop() runs because
            # start() failed, start() already logged the failure as one
            # clean line; this is detail for it, so INFO.
            logger.log(
                logging.WARNING if was_consuming else logging.INFO,
                message,
                *args,
                extra={
                    "pending_messages": pending_at_stop,
                    "replies_undelivered": undelivered,
                    "commands_unsettled": unsettled,
                    "settlements_unconfirmed": settled,
                    "shutdown_step_timeout_s": step_timeout,
                    "cause_error_class": (type(cause).__name__ if cause else None),
                    "cause_error": _describe_error(cause) if cause else None,
                },
            )

        # Close Azure Blob client. Don't re-log "Azure Blob client
        # closed" here — AzureBlobClient.close() already logs an INFO
        # record from kbm_ledsas_sdk.blob.azure_client; a second DEBUG
        # line from this layer duplicates the lifecycle event without
        # adding information.
        try:
            await self.azure_client.close()
        except Exception:
            logger.error("Error closing Azure client", exc_info=True)

    def _connection_down(self) -> bool:
        """True when the broker connection is lost (closed, or the client is
        reconnecting). A transport that never connected is not "down"."""
        connection = self.connection
        if connection is None:
            return False
        if self._broker_connection_lost or connection.is_closed:
            return True
        connected = getattr(connection, "connected", None)
        if connected is not None and hasattr(connected, "is_set"):
            return not connected.is_set()
        return False

    def is_ready(self) -> bool:
        """True while the broker connection is actually up.

        A robust connection that lost the broker and is reconnecting is
        not "closed" (only an explicit close() closes it), so
        ``is_closed`` alone kept /ready at 200 through a whole broker
        outage. Readiness also requires the connection's ``connected``
        state (cleared while reconnecting) and an open consumer channel.
        """
        connection = self.connection
        if connection is None or connection.is_closed:
            return False
        connected = getattr(connection, "connected", None)
        if connected is not None and hasattr(connected, "is_set"):
            if not connected.is_set():
                return False
        transport = getattr(connection, "transport", None)
        if transport is None:
            return False
        channel = self.channel
        if channel is None or getattr(channel, "is_closed", False) is True:
            return False
        return True

    def get_retry_count(self, message_id: str) -> int:
        """Forward to the consumer's in-process retry tracker.

        app.py reads this before deciding whether to requeue or DLQ a
        Retryable failure, enforcing KBM_LEDSAS_MAX_RETRIES. Returns 0
        when the transport hasn't started or the id is unknown.
        """
        if self.consumer is None:
            return 0
        return self.consumer.get_retry_count(message_id)
