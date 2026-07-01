"""
Mock transport for unit testing.

Provides an in-memory implementation of the Transport interface that:
- Stores commands in a queue for testing
- Records acks/nacks for verification
- Captures sent responses and status updates
- Provides a mock BlobOperations implementation

Use this in unit tests to test SDK logic without real infrastructure.
"""

import asyncio
from collections.abc import AsyncIterator

from ..blob.operations import BlobOperations
from ..models.blob import BlobRef
from ..models.messages import Command, Response, Status
from .base import Transport


class MockBlobOperations(BlobOperations):
    """
    Mock blob operations for testing.

    Stores uploaded blobs in memory and allows pre-populating blobs for download tests.
    """

    def __init__(self):
        """Initialize mock blob storage."""
        self.blobs: dict[str, bytes] = {}  # uri -> data
        self.upload_count = 0
        self.download_count = 0

    def add_blob(self, uri: str, data: bytes) -> None:
        """
        Pre-populate a blob for download tests.

        Args:
            uri: Blob URI (azblob://...)
            data: Blob data
        """
        self.blobs[uri] = data

    async def download_bytes(self, blob_ref: BlobRef) -> bytes:
        """Download blob from mock storage."""
        self.download_count += 1
        if blob_ref.uri not in self.blobs:
            raise FileNotFoundError(f"Blob not found: {blob_ref.uri}")
        return self.blobs[blob_ref.uri]

    async def download_json(self, blob_ref: BlobRef) -> dict:
        """Download and parse JSON from mock storage."""
        import json

        data = await self.download_bytes(blob_ref)
        return json.loads(data.decode("utf-8"))

    async def upload_bytes(
        self,
        container: str,
        data: bytes,
        path: str | None = None,
        overwrite: bool = False,
    ) -> BlobRef:
        """Upload bytes to mock storage."""
        self.upload_count += 1
        if path is None:
            path = f"mock-{self.upload_count}.bin"

        uri = f"azblob://{container}/{path}?versionId=mock-{self.upload_count}"
        # Mirror Azure semantics: refuse to overwrite when overwrite=False.
        # The version_id changes per upload so the key in self.blobs is
        # the path-only form, not the versioned URI.
        path_key = f"azblob://{container}/{path}"
        if not overwrite and any(k.startswith(path_key) for k in self.blobs):
            from azure.core.exceptions import ResourceExistsError

            raise ResourceExistsError(message=f"Blob {path_key} already exists")
        self.blobs[uri] = data
        return BlobRef(uri=uri)

    async def upload_json(
        self,
        container: str,
        obj: dict,
        path: str | None = None,
        overwrite: bool = False,
    ) -> BlobRef:
        """Upload JSON to mock storage."""
        import json

        data = json.dumps(obj).encode("utf-8")
        return await self.upload_bytes(container, data, path, overwrite=overwrite)

    async def upload_text(
        self,
        container: str,
        text: str,
        path: str | None = None,
        overwrite: bool = False,
    ) -> BlobRef:
        """Upload UTF-8 text to mock storage."""
        data = text.encode("utf-8")
        return await self.upload_bytes(container, data, path, overwrite=overwrite)

    async def download_text(self, blob_ref: BlobRef) -> str:
        """Download and decode UTF-8 text from mock storage."""
        data = await self.download_bytes(blob_ref)
        return data.decode("utf-8")

    async def upload_stream(
        self,
        container: str,
        stream: AsyncIterator[bytes],
        path: str | None = None,
        progress_callback=None,
        overwrite: bool = False,
    ) -> BlobRef:
        """Upload from async stream to mock storage."""
        chunks = []
        async for chunk in stream:
            chunks.append(chunk)
        data = b"".join(chunks)
        return await self.upload_bytes(container, data, path, overwrite=overwrite)

    async def download_stream(self, blob_ref: BlobRef) -> AsyncIterator[bytes]:
        """Download blob as async stream from mock storage."""
        data = await self.download_bytes(blob_ref)
        # Yield in chunks (4KB default)
        chunk_size = 4096
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]


class MockTransport(Transport):
    """
    Mock transport for unit testing.

    Usage:
        >>> transport = MockTransport()
        >>> transport.add_command(command)
        >>> await transport.start()
        >>> async for cmd in transport.subscribe():
        ...     # Process command
        ...     await transport.ack(cmd.envelope.message_id)
        >>> assert transport.ack_count == 1
        >>> assert len(transport.sent_responses) == 1
    """

    def __init__(self):
        """Initialize mock transport."""
        self.started = False
        self.stopped = False

        # Command queue for subscribe()
        self.command_queue: asyncio.Queue[Command] = asyncio.Queue()
        self.commands_to_deliver: list[Command] = []

        # Tracking acks/nacks
        self.acked_messages: list[str] = []
        self.nacked_messages: list[tuple[str, bool]] = []  # (message_id, requeue)

        # Tracking sent messages
        self.sent_responses: list[Response] = []
        self.sent_statuses: list[Status] = []

        # Blob operations
        self.blob_ops = MockBlobOperations()

    def add_command(self, command: Command) -> None:
        """
        Add a command to be delivered via subscribe().

        Args:
            command: Command to deliver

        Example:
            >>> transport.add_command(cmd1)
            >>> transport.add_command(cmd2)
            >>> async for cmd in transport.subscribe():
            ...     print(cmd.envelope.name)
            # Yields: cmd1, cmd2
        """
        self.commands_to_deliver.append(command)

    async def start(self) -> None:
        """Initialize mock transport."""
        if self.started:
            raise RuntimeError("Transport already started")
        self.started = True

        # Enqueue all commands
        for cmd in self.commands_to_deliver:
            await self.command_queue.put(cmd)

    async def subscribe(self) -> AsyncIterator[Command]:
        """Subscribe to commands from the queue."""
        if not self.started:
            raise RuntimeError("Transport not started (call await transport.start())")

        while True:
            try:
                # Get command with timeout to allow graceful shutdown
                cmd = await asyncio.wait_for(self.command_queue.get(), timeout=0.1)
                yield cmd
            except TimeoutError:
                # Check if stopped
                if self.stopped:
                    break
                # Otherwise keep waiting
                continue

    async def ack(self, message_id: str) -> None:
        """Record message acknowledgment."""
        self.acked_messages.append(message_id)

    async def nack(self, message_id: str, requeue: bool) -> None:
        """Record message negative acknowledgment."""
        self.nacked_messages.append((message_id, requeue))

    async def send_response(self, response: Response) -> bool:
        """Record sent response. Always succeeds in the mock."""
        self.sent_responses.append(response)
        return True

    async def send_status(self, status: Status) -> None:
        """Record sent status update."""
        self.sent_statuses.append(status)

    def get_blob_operations(self) -> BlobOperations:
        """Get mock blob operations."""
        return self.blob_ops

    async def stop(self) -> None:
        """Stop mock transport."""
        self.stopped = True

    def is_ready(self) -> bool:
        """Mock readiness — true between start() and stop()."""
        return self.started and not self.stopped

    @property
    def ack_count(self) -> int:
        """Number of messages acked."""
        return len(self.acked_messages)

    @property
    def nack_count(self) -> int:
        """Number of messages nacked."""
        return len(self.nacked_messages)

    @property
    def response_count(self) -> int:
        """Number of responses sent."""
        return len(self.sent_responses)

    @property
    def status_count(self) -> int:
        """Number of status updates sent."""
        return len(self.sent_statuses)
