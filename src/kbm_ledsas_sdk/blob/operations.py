"""
Blob operations interface for the SDK.

Provides methods for uploading/downloading data to/from Azure Blob Storage.
Streaming operations support multi-GB files with minimal memory footprint.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable

from ..models.blob import BlobRef

# Default chunk size: 4MB (optimal for Azure blob storage performance)
DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024

# Type aliases for progress callbacks
DownloadProgressCallback = Callable[[int, int], None]  # (bytes_downloaded, total_bytes)
UploadProgressCallback = Callable[[int], None]  # (bytes_uploaded)


def _ensure_blob_ref(blob_ref: str | BlobRef) -> BlobRef:
    """
    Convert a string URI to BlobRef if needed.

    Args:
        blob_ref: Either a BlobRef instance or an azblob:// URI string

    Returns:
        BlobRef instance

    Raises:
        ValueError: If string is not a valid azblob:// URI
    """
    if isinstance(blob_ref, str):
        return BlobRef.from_uri(blob_ref)
    return blob_ref


class BlobOperations(ABC):
    """
    Abstract interface for blob storage operations.

    The SDK provides this interface to handler code via ``ExecutionContext.blob``.
    Implementation: :class:`DirectBlobOperations` (Azure Blob SDK).
    """

    @abstractmethod
    async def download_bytes(self, blob_ref: str | BlobRef) -> bytes:
        """
        Download blob contents as bytes.

        Args:
            blob_ref: Reference to the blob to download (BlobRef or azblob:// URI string)

        Returns:
            Blob contents as bytes

        Raises:
            Retryable: Transient blob storage errors (throttling, network issues)
            Permanent: Blob not found, access denied, invalid URI

        Example:
            >>> data = await ctx.blob.download_bytes("azblob://dev/input.json")
            >>> print(len(data))
            1024
        """

    @abstractmethod
    async def download_json(self, blob_ref: str | BlobRef) -> dict:
        """
        Download blob contents and parse as JSON.

        Args:
            blob_ref: Reference to the blob to download (BlobRef or azblob:// URI string)

        Returns:
            Parsed JSON as dict

        Raises:
            Retryable: Transient blob storage errors
            Permanent: Blob not found, invalid JSON, access denied

        Example:
            >>> data = await ctx.blob.download_json("azblob://dev/config.json")
            >>> print(data["version"])
            "1.0"
        """

    @abstractmethod
    async def upload_bytes(
        self,
        container: str,
        data: bytes,
        path: str | None = None,
        overwrite: bool = False,
    ) -> BlobRef:
        """
        Upload bytes to blob storage.

        Args:
            container: Container name
            data: Bytes to upload
            path: Optional blob path (auto-generated if not provided)
            overwrite: If True, replace an existing blob at ``path``;
                if False (default) and the blob already exists, raises
                ``BlobAlreadyExists``. Set ``overwrite=True`` when keying
                output paths on ``ctx.idempotency_key`` so DLQ replays
                of the same logical request rewrite the same blob
                instead of being rejected as duplicates.

        Returns:
            BlobRef with URI including versionId

        Raises:
            BlobAlreadyExists: When ``overwrite=False`` and ``path`` exists
            Retryable: Transient blob storage errors
            Permanent: Invalid container, access denied, quota exceeded

        Example:
            >>> ref = await ctx.blob.upload_bytes(container="results", data=b"hello")
            >>> print(ref.uri)
            "azblob://results/abc123.bin?versionId=xyz"
        """

    @abstractmethod
    async def upload_json(
        self,
        container: str,
        obj: dict,
        path: str | None = None,
        overwrite: bool = False,
    ) -> BlobRef:
        """
        Serialize object to JSON and upload to blob storage.

        Args:
            container: Container name
            obj: Dict to serialize as JSON
            path: Optional blob path (auto-generated if not provided)
            overwrite: See :meth:`upload_bytes` (default False; set True
                for the idempotency-key replay pattern).

        Returns:
            BlobRef with URI including versionId

        Raises:
            BlobAlreadyExists: When ``overwrite=False`` and ``path`` exists
            Retryable: Transient blob storage errors
            Permanent: Invalid container, access denied, serialization error
        """

    @abstractmethod
    async def upload_text(
        self,
        container: str,
        text: str,
        path: str | None = None,
        overwrite: bool = False,
    ) -> BlobRef:
        """
        Upload UTF-8 text to blob storage.

        Args:
            container: Container name
            text: Text string to upload (will be encoded as UTF-8)
            path: Optional blob path (auto-generated if not provided)
            overwrite: See :meth:`upload_bytes` (default False).

        Returns:
            BlobRef with URI including versionId

        Raises:
            BlobAlreadyExists: When ``overwrite=False`` and ``path`` exists
            Retryable: Transient blob storage errors
            Permanent: Invalid container, access denied
        """

    @abstractmethod
    async def download_text(self, blob_ref: str | BlobRef) -> str:
        """
        Download blob contents and decode as UTF-8 text.

        Args:
            blob_ref: Reference to the blob to download (BlobRef or azblob:// URI string)

        Returns:
            Text content as string

        Raises:
            Retryable: Transient blob storage errors
            Permanent: Blob not found, invalid UTF-8, access denied

        Example:
            >>> text = await ctx.blob.download_text("azblob://logs/message.txt")
            >>> print(text)
            "Hello, World!"
        """

    @abstractmethod
    async def upload_stream(
        self,
        container: str,
        stream: AsyncIterator[bytes],
        path: str | None = None,
        progress_callback: UploadProgressCallback | None = None,
        overwrite: bool = False,
    ) -> BlobRef:
        """
        Upload data from async stream to blob storage.

        Uses true streaming upload - data flows directly to storage without
        buffering the entire content in memory. Memory usage is bounded by
        the chunk size of the incoming stream regardless of total file size.

        Supports multi-GB files with <50MB peak memory usage.

        Args:
            container: Container name
            stream: Async iterator yielding byte chunks
            path: Optional blob path (auto-generated if not provided)
            progress_callback: Optional callback(bytes_uploaded) for progress reporting

        Returns:
            BlobRef with URI including versionId

        Raises:
            Retryable: Transient blob storage errors
            Permanent: Invalid container, access denied

        Example:
            >>> async def file_stream():
            ...     async with aiofiles.open("large.bin", "rb") as f:
            ...         while chunk := await f.read(4 * 1024 * 1024):
            ...             yield chunk
            >>> ref = await ctx.blob.upload_stream(
            ...     container="uploads",
            ...     stream=file_stream(),
            ...     progress_callback=lambda b: print(f"Uploaded {b} bytes")
            ... )
        """

    @abstractmethod
    async def download_stream(
        self,
        blob_ref: str | BlobRef,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        progress_callback: DownloadProgressCallback | None = None,
    ) -> AsyncIterator[bytes]:
        """
        Download blob contents as true streaming async iterator.

        Uses Azure SDK's streaming download to transfer data without
        loading entire blob into memory. Memory usage is bounded by
        chunk_size regardless of blob size.

        Supports multi-GB files with <50MB peak memory usage.

        Args:
            blob_ref: Reference to the blob to download (BlobRef or azblob:// URI string)
            chunk_size: Chunk size in bytes (default 4MB for optimal Azure performance)
            progress_callback: Optional callback(bytes_downloaded, total_bytes)

        Returns:
            Async iterator yielding byte chunks

        Raises:
            Retryable: Transient blob storage errors
            Permanent: Blob not found, access denied

        Example:
            >>> async with aiofiles.open("output.bin", "wb") as f:
            ...     async for chunk in ctx.blob.download_stream(
            ...         blob_ref,
            ...         progress_callback=lambda d, t: print(f"{d}/{t} bytes")
            ...     ):
            ...         await f.write(chunk)
        """
