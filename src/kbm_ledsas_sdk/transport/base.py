"""
Transport interface for the SDK.

The Transport abstraction handles command subscription, ack/nack,
response and status publishing, and provides access to blob operations.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from ..blob.operations import BlobOperations
from ..models.messages import Command, Response, Status


class TransportConnectionLost(ConnectionError):
    """The transport's underlying connection died and cannot recover
    in-process.

    Raised from the subscribe iterator so the application exits promptly
    (fail-fast) instead of consuming from a dead transport forever. The
    process supervisor (container restart policy, orchestrator) is the
    recovery mechanism: a fresh process reconnects and resubscribes.
    """


class DeliveryLost(ConnectionError):
    """A command could not be settled (acknowledged, requeued or
    dead-lettered) because the broker connection it arrived on was lost.

    Nothing more can be done with that delivery: the broker returned the
    command to its queue when the connection dropped and delivers it
    again, so it runs again. The transport has already logged this once
    (one WARNING, no traceback); the service lets the command go without
    logging it again.
    """


class TransportUnavailable(ConnectionError):
    """The transport could not connect at startup (endpoint unreachable,
    or the connection was refused during setup).

    The transport logs the cause once, as a single line; ``ServiceApp.run()``
    then exits non-zero without a traceback so the process supervisor
    restarts the service.
    """


class Transport(ABC):
    """
    Abstract transport for SDK <-> RabbitMQ + Azure Blob communication.

    Implementation:
    - DirectTransport: aio-pika + Azure Blob SDK.

    Lifecycle:
    1. create_transport(config) -> Transport
    2. await transport.start()
    3. async for cmd in transport.subscribe(): ...
    4. await transport.ack(message_id) or transport.nack(message_id, requeue)
    5. await transport.send_response(response) or transport.send_status(status)
    6. await transport.stop()
    """

    @abstractmethod
    async def start(self) -> None:
        """
        Initialize transport and connect to the broker.

        Connects to RabbitMQ, declares exchanges and queues, and starts
        consuming.

        Raises:
            Retryable: Connection failures, temporary unavailability
            Permanent: Invalid configuration, authentication failure
        """

    @abstractmethod
    async def subscribe(self) -> AsyncIterator[Command]:
        """
        Subscribe to incoming commands.

        Yields commands as they arrive. The SDK calls this once at startup
        and iterates over commands until shutdown.

        Yields:
            Command: Incoming command messages

        Example:
            >>> async for cmd in transport.subscribe():
            ...     print(f"Received: {cmd.envelope.name}")
            ...     # Process command
            ...     await transport.ack(cmd.envelope.message_id)
        """

    @abstractmethod
    async def ack(self, message_id: str) -> None:
        """
        Acknowledge successful command processing.

        Tells the broker that the command was processed successfully.
        The message is removed from the queue and won't be redelivered.

        Args:
            message_id: ID of the message to acknowledge (from envelope)

        Raises:
            Retryable: Temporary ack failure (connection issue)
        """

    @abstractmethod
    async def nack(self, message_id: str, requeue: bool) -> None:
        """
        Negative acknowledge — command processing failed.

        Args:
            message_id: ID of the message to nack (from envelope)
            requeue: If True, requeue for retry with backoff.
                    If False, send to DLQ (permanent failure)

        Raises:
            Retryable: Temporary nack failure (connection issue)

        Example:
            >>> try:
            ...     result = await handler(ctx, req)
            ... except Retryable as e:
            ...     await transport.nack(message_id, requeue=True)  # Retry
            ... except Permanent as e:
            ...     await transport.nack(message_id, requeue=False)  # DLQ
        """

    @abstractmethod
    async def send_response(self, response: Response) -> bool:
        """
        Send a response message to the caller's reply_to exchange.

        The response indicates success or failure of command processing.
        Response is routed to the exchange specified in
        ``response.envelope.reply_to``.

        Returns:
            bool: True if the response was published successfully, False if
                  publish was attempted but failed (e.g. reply_to exchange
                  missing, channel error). When False, the caller should
                  treat the command as Permanent-failed (NACK no-requeue);
                  retrying the handler will produce the same failure.
                  When reply_to is empty / no response is needed, returns True.

        Args:
            response: Response message with envelope and payload

        Example:
            >>> response = Response(
            ...     envelope=cmd.envelope,  # Copy envelope from command
            ...     payload={"result_uri": "azblob://..."}
            ... )
            >>> if not await transport.send_response(response):
            ...     await transport.nack(message_id, requeue=False)
        """

    @abstractmethod
    async def send_status(self, status: Status) -> None:
        """
        Send a status update to the caller's reply_to exchange.

        Status messages provide progress updates during long-running
        processing. They are informational and don't affect
        command/response correlation.

        Args:
            status: Status message with envelope and payload

        Raises:
            Retryable: Temporary publishing failure

        Example:
            >>> status = Status(
            ...     envelope=cmd.envelope,
            ...     payload={"stage": "downloading", "progress": 0.25}
            ... )
            >>> await transport.send_status(status)
        """

    @abstractmethod
    def get_blob_operations(self) -> BlobOperations:
        """
        Get blob operations interface.

        Returns a BlobOperations instance for this transport. The SDK
        exposes this to handler code as ``ctx.blob``.

        Returns:
            BlobOperations: Interface for blob upload/download

        Example:
            >>> blob_ops = transport.get_blob_operations()
            >>> data = await blob_ops.download_bytes(blob_ref)
        """

    @abstractmethod
    def is_ready(self) -> bool:
        """
        Report whether the transport is connected and ready to accept work.

        Used by the SDK's health server as the default readiness signal.
        Must be cheap (no network I/O) — implementations should reflect the
        cached connection state, not actively probe.

        Returns:
            True if the transport is fully started AND its underlying
            connection is open.
        """

    @abstractmethod
    async def stop(self) -> None:
        """
        Graceful shutdown of transport.

        Stops consuming, waits for in-flight acks, and closes the broker
        connection.

        Raises:
            Retryable: Temporary shutdown issue (logged but not fatal)
        """
