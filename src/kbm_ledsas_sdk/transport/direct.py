"""
Direct transport implementation using RabbitMQ and Azure Blob Storage.

The SDK connects to RabbitMQ for command/response messaging and to Azure
Blob Storage for blob operations.
"""

import logging
from collections.abc import AsyncIterator

import aio_pika
import aiormq.exceptions

from kbm_ledsas_sdk.amqp.consumer import AMQPConsumer
from kbm_ledsas_sdk.amqp.publisher import AMQPPublisher
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
from kbm_ledsas_sdk.transport.base import Transport

logger = logging.getLogger(__name__)

# The URL-security check and credential scrubber
# now live in ``kbm_ledsas_sdk.runtime.security`` so SDKConfig's
# model_validator can call them at config-load time — well before the
# AzureBlobClient is constructed. Re-exported here under the private
# underscore names so any external test code that imported
# ``_check_transport_security`` / ``_scrub_url_credentials`` from this
# module keeps working unchanged.


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

    def __init__(
        self,
        rabbitmq_url: str,
        blob_conn_string: str,
        service_name: str,
        tenant: str | None = None,
        prefetch_count: int = 10,
        default_container: str = "dev",
        max_payload_bytes: int = 16 * 1024 * 1024,
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
        """
        self.rabbitmq_url = rabbitmq_url
        self.blob_conn_string = blob_conn_string
        self.service_name = service_name
        self.tenant = tenant
        self.prefetch_count = prefetch_count
        self.default_container = default_container
        self.max_payload_bytes = max_payload_bytes

        # AMQP components (initialized in start())
        self.connection: aio_pika.Connection | None = None
        # Consumer/topology channel. The publisher uses its own transient
        # channel per publish (see AMQPPublisher) so a publish-side channel
        # error (e.g. missing reply exchange) cannot interfere with the
        # consumer's ack flow.
        self.channel: aio_pika.Channel | None = None
        self.consumer: AMQPConsumer | None = None
        self.publisher: AMQPPublisher | None = None

        # Operational counters: split into two so an ops dashboard
        # can tell apart "the orchestrator's reply infrastructure is
        # broken" (response path, every failure dead-letters a command)
        # from "the orchestrator isn't listening to status updates"
        # (status path, best-effort and benign). Both are exposed for
        # logging and tests; not currently wired into a metrics backend.
        self.reply_publish_failures: int = 0
        self.status_publish_failures: int = 0

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
            logger.debug(
                "Connecting to RabbitMQ: %s",
                _scrub_url_credentials(self.rabbitmq_url),
            )
            self.connection = await aio_pika.connect_robust(self.rabbitmq_url)

            # Create the consumer/topology channel.
            self.channel = await self.connection.channel()
            logger.debug("RabbitMQ consumer channel created")

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
            # opens its own short-lived channel.
            self.publisher = AMQPPublisher(self.connection)
            await self.publisher.enable_confirms()
            logger.debug("Publisher initialized (transient-channel mode)")

            # Initialize consumer
            self.consumer = AMQPConsumer(
                topology.cmd_queue_obj,
                self.prefetch_count,
                max_payload_bytes=self.max_payload_bytes,
            )
            await self.consumer.start()
            logger.debug("Consumer started")

            logger.info("DirectTransport started successfully")

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
        except Exception as e:
            logger.error(
                "Failed to start DirectTransport",
                exc_info=True,
                extra={"error": str(e)},
            )
            # Cleanup on failure
            await self.stop()
            raise

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
                        "message_id": command.envelope.message_id,
                        "correlation_id": command.envelope.correlation_id,
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

        logger.debug(f"ACK: {message_id}")
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

        logger.debug(f"NACK: {message_id} (requeue={requeue})")
        await self.consumer.nack(message_id, requeue)

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
        """
        if not self.publisher:
            raise RuntimeError("Transport not started. Call start() first.")

        # Extract exchange name from reply_to field
        # reply_to format: "resp.{tenant}.{service}.v1"
        exchange_name = response.envelope.reply_to

        # Skip if no reply_to specified
        if not exchange_name:
            logger.info(
                "Skipping response send (no reply_to specified)",
                extra={
                    "message_id": response.envelope.message_id,
                    "correlation_id": response.envelope.correlation_id,
                },
            )
            return True

        logger.debug(
            "Sending response",
            extra={
                "message_id": response.envelope.message_id,
                "correlation_id": response.envelope.correlation_id,
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
            self.reply_publish_failures += 1
            # When pamqp rejected the exchange
            # name on protocol grounds (length > 127), point the
            # operator at the actual cause rather than at the orchestrator.
            # The envelope schema now caps reply_to at 127, so reaching
            # this branch means the value bypassed the SDK validator —
            # log a more specific hint.
            if isinstance(e, ValueError) and "Max length exceeded" in str(e):
                action_hint = (
                    "the exchange name is longer than AMQP's 127-byte "
                    "protocol limit. Use a shorter reply_to."
                )
            else:
                action_hint = "verify the orchestrator pre-declared this exchange."
            logger.error(
                "Failed to send response to reply_to exchange '%s': %s "
                "(failures so far: %d). Command will be NACKed to DLQ; "
                "%s",
                exchange_name,
                e,
                self.reply_publish_failures,
                action_hint,
                extra={
                    "message_id": response.envelope.message_id,
                    "correlation_id": response.envelope.correlation_id,
                    "exchange": exchange_name,
                    "error": str(e),
                    "reply_publish_failures": self.reply_publish_failures,
                },
            )
            return False

    async def send_status(self, status: Status) -> None:
        """
        Send status update to orchestrator.

        If the reply_to exchange doesn't exist, the status is logged and
        skipped (no exception). This is useful for testing without setting
        up response infrastructure.

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
                    "message_id": status.envelope.message_id,
                    "correlation_id": status.envelope.correlation_id,
                },
            )
            return

        logger.debug(
            "Sending status",
            extra={
                "message_id": status.envelope.message_id,
                "correlation_id": status.envelope.correlation_id,
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
            # Status is informational and best-effort: log and continue.
            # Bump the *status* failure counter (not the response
            # one) so an ops dashboard can distinguish "orchestrator's
            # reply infra is broken" from "orchestrator isn't listening
            # to status updates."
            self.status_publish_failures += 1
            logger.warning(
                "Failed to send status to reply_to exchange '%s': %s "
                "(status failures so far: %d). Continuing.",
                exchange_name,
                e,
                self.status_publish_failures,
                extra={
                    "message_id": status.envelope.message_id,
                    "correlation_id": status.envelope.correlation_id,
                    "exchange": exchange_name,
                    "error": str(e),
                    "status_publish_failures": self.status_publish_failures,
                },
            )

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
        """
        logger.info("Stopping DirectTransport")

        # Stop consumer
        if self.consumer:
            try:
                await self.consumer.stop()
                logger.debug("Consumer stopped")
            except Exception:
                logger.error("Error stopping consumer", exc_info=True)

        # Close consumer/topology channel. Publisher channels are transient
        # and already closed by AMQPPublisher.publish_*() in their finally.
        if self.channel and not self.channel.is_closed:
            try:
                await self.channel.close()
                logger.debug("Channel closed")
            except Exception:
                logger.error("Error closing channel", exc_info=True)

        # Close connection
        if self.connection and not self.connection.is_closed:
            try:
                await self.connection.close()
                logger.debug("Connection closed")
            except Exception:
                logger.error("Error closing connection", exc_info=True)

        # Close Azure Blob client. Don't re-log "Azure Blob client
        # closed" here — AzureBlobClient.close() already logs an INFO
        # record from kbm_ledsas_sdk.blob.azure_client; a second DEBUG
        # line from this layer duplicates the lifecycle event without
        # adding information.
        try:
            await self.azure_client.close()
        except Exception:
            logger.error("Error closing Azure client", exc_info=True)

    def is_ready(self) -> bool:
        return bool(self.connection) and not self.connection.is_closed

    def get_retry_count(self, message_id: str) -> int:
        """Forward to the consumer's in-process retry tracker.

        app.py reads this before deciding whether to requeue or DLQ a
        Retryable failure, enforcing KBM_LEDSAS_MAX_RETRIES. Returns 0
        when the transport hasn't started or the id is unknown.
        """
        if self.consumer is None:
            return 0
        return self.consumer.get_retry_count(message_id)
