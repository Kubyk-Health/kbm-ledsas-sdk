"""
Azure Blob Storage client wrapper for Direct transport mode.

Simplified blob client for development/testing scenarios.
Supports true streaming for multi-GB file transfers with minimal memory footprint.
"""

import logging
import uuid
from collections.abc import AsyncIterator, Callable

from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
    ServiceRequestError,
    ServiceResponseError,
)
from azure.storage.blob.aio import BlobServiceClient

from kbm_ledsas_sdk.models.blob import BlobRef

# Default chunk size: 4MB (optimal for Azure blob storage performance)
DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024

# Type alias for progress callbacks
ProgressCallback = Callable[[int, int], None]  # (bytes_transferred, total_bytes)

# HTTP status codes that mean "transient — retry would help".
# Per Azure SDK guidance: 408 timeout, 429 throttling, 500/502/503/504.
_TRANSIENT_HTTP_STATUS = frozenset({408, 429, 500, 502, 503, 504})


logger = logging.getLogger(__name__)


def is_transient_azure_error(exc: BaseException) -> bool:
    """Classify an Azure exception as transient (worth retrying).

    Used by the SDK's blob layer to decide whether to dump a full
    traceback on error. Customer handlers that catch ``AzureError`` can
    use this as a hint for whether to re-raise as ``errors.Retryable``
    vs ``errors.Permanent``.
    """
    if isinstance(exc, (ServiceRequestError, ServiceResponseError)):
        return True
    if isinstance(exc, HttpResponseError):
        return getattr(exc, "status_code", None) in _TRANSIENT_HTTP_STATUS
    return False


def _is_client_4xx(exc: BaseException) -> bool:
    """True for HTTP 4xx responses that aren't worth a 30-line traceback.

    Excludes 408 / 429 (already classified as transient by
    :func:`is_transient_azure_error`). Used by the blob layer to log
    InvalidResourceName, AuthenticationFailed, etc. as a single short
    WARNING — the Azure response body adds noise without changing
    operator action.
    """
    if not isinstance(exc, HttpResponseError):
        return False
    status = getattr(exc, "status_code", None)
    if not isinstance(status, int):
        return False
    return 400 <= status < 500 and status not in _TRANSIENT_HTTP_STATUS


def _short_azure_error(exc: BaseException) -> str:
    """One-line summary of an Azure error.

    The Azure SDK's ``str(exc)`` includes the full XML response body,
    a RequestId, a server timestamp, and multiple newlines — useful
    once during root-cause analysis, very noisy in steady-state logs.
    This helper pulls out just ``status_code`` + ``error_code`` + a
    short reason and drops the rest.
    """
    parts: list[str] = []
    status = getattr(exc, "status_code", None)
    if status is not None:
        parts.append(f"HTTP {status}")
    error_code = getattr(exc, "error_code", None)
    if error_code:
        parts.append(str(error_code))
    reason = getattr(exc, "reason", None)
    if reason:
        parts.append(str(reason))
    if not parts:
        # Fall back to first line of str(exc), truncated.
        return str(exc).splitlines()[0][:200]
    return " ".join(parts)


def build_blob_uri(container: str, path: str, version_id: str | None = None) -> str:
    """
    Build Azure Blob URI with optional version ID.

    Args:
        container: Container name
        path: Blob path (with or without leading /)
        version_id: Optional version ID

    Returns:
        Blob URI in azblob:// format
    """
    # Ensure path starts with /
    if not path.startswith("/"):
        path = f"/{path}"

    base_uri = f"azblob://{container}{path}"

    if version_id:
        return f"{base_uri}?versionId={version_id}"
    return base_uri


class AzureBlobClient:
    """
    Azure Blob Storage client.

    Provides simple upload/download operations with URI-based addressing
    and automatic versioning.

    Features:
    - Connection string-based authentication
    - URI-based download
    - Upload with metadata and version tracking
    - Async operations

    Attributes:
        service_client: Azure BlobServiceClient instance
        default_container: Default container for uploads
    """

    def __init__(self, connection_string: str, default_container: str = "dev"):
        """
        Initialize Azure Blob client.

        Args:
            connection_string: Azure Storage connection string
            default_container: Default container name for uploads

        Raises:
            ValueError: If connection string is empty
        """
        if not connection_string:
            raise ValueError("Blob storage connection string required")

        self.service_client = BlobServiceClient.from_connection_string(connection_string)
        self.default_container = default_container

        logger.info(
            "Azure Blob client initialized",
            extra={"default_container": default_container},
        )

    async def download_blob(self, blob_ref: BlobRef) -> bytes:
        """
        Download blob by reference.

        Args:
            blob_ref: BlobRef with container, path, and optional version ID

        Returns:
            Blob data as bytes

        Raises:
            ResourceNotFoundError: If blob not found
            Exception: If download fails
        """
        # Extract container and path (strip leading / for Azure SDK)
        container = blob_ref.container
        path = blob_ref.path.lstrip("/")
        version_id = blob_ref.version_id

        logger.debug(
            "Downloading blob",
            extra={
                "container": container,
                "path": path,
                "version_id": version_id,
            },
        )

        try:
            # Get blob client
            blob_client = self.service_client.get_blob_client(container=container, blob=path)

            # Download with version ID if specified
            if version_id:
                download_stream = await blob_client.download_blob(version_id=version_id)
            else:
                download_stream = await blob_client.download_blob()

            # Read all data
            data = await download_stream.readall()

            logger.debug(
                "Blob downloaded successfully",
                extra={
                    "container": container,
                    "path": path,
                    "size": len(data),
                },
            )

            return data

        except ResourceNotFoundError:
            logger.warning(
                "Blob not found",
                extra={
                    "container": container,
                    "path": path,
                    "version_id": version_id,
                },
            )
            raise

        except Exception as e:
            # Client-side 4xx (InvalidResourceName,
            # AuthenticationFailed, etc.) → single WARNING with short
            # summary, no XML body, no traceback. 5xx / transport →
            # ERROR + traceback as before.
            client_4xx = _is_client_4xx(e)
            transient = is_transient_azure_error(e)
            if client_4xx:
                logger.warning(
                    "Failed to download blob (client error)",
                    extra={
                        "container": container,
                        "path": path,
                        "error": _short_azure_error(e),
                    },
                )
            else:
                logger.error(
                    "Failed to download blob",
                    exc_info=not transient,
                    extra={
                        "container": container,
                        "path": path,
                        "transient": transient,
                        "error": (
                            _short_azure_error(e) if isinstance(e, HttpResponseError) else str(e)
                        ),
                    },
                )
            raise

    async def upload_blob(
        self,
        container: str,
        data: bytes,
        path: str | None = None,
        metadata: dict[str, str] | None = None,
        overwrite: bool = False,
    ) -> BlobRef:
        """
        Upload blob with optional metadata.

        Args:
            container: Container name
            data: Blob data
            path: Optional blob path (auto-generated if not provided)
            metadata: Optional blob metadata
            overwrite: If True, replace existing blob at ``path``. Default
                False to refuse to clobber an existing blob (use True for
                the idempotency-key replay pattern).

        Returns:
            BlobRef with URI including version ID

        Raises:
            ResourceExistsError: If ``overwrite=False`` and ``path`` exists.
            Exception: If upload fails for other reasons.
        """
        # Generate path if not provided.
        # UUID supplies the uniqueness; the prior ``{timestamp}_``
        # prefix only leaked server time into blob listings without
        # adding collision resistance. Bare UUID is enough.
        if not path:
            path = f"{uuid.uuid4()}.bin"

        # Strip leading / for Azure SDK
        path_clean = path.lstrip("/")

        logger.debug(
            "Uploading blob",
            extra={
                "container": container,
                "path": path_clean,
                "size": len(data),
                "metadata": metadata,
                "overwrite": overwrite,
            },
        )

        try:
            # Get blob client
            blob_client = self.service_client.get_blob_client(container=container, blob=path_clean)

            # Upload with metadata
            result = await blob_client.upload_blob(
                data=data,
                metadata=metadata or {},
                overwrite=overwrite,
            )

            # Get version ID from result
            version_id = result.get("version_id")

            # Build URI (with leading /)
            uri = build_blob_uri(container=container, path=path_clean, version_id=version_id)

            logger.debug(
                "Blob uploaded successfully",
                extra={
                    "container": container,
                    "path": path_clean,
                    "version_id": version_id,
                    "uri": uri,
                },
            )

            return BlobRef.from_uri(uri)

        except ResourceExistsError:
            # Expected when overwrite=False and blob already exists.
            # Caller (usually the example handler) decides how to react.
            # No traceback — the path + container alone tell the story.
            logger.info(
                "Blob already exists (overwrite=False)",
                extra={
                    "container": container,
                    "path": path_clean,
                },
            )
            raise

        except Exception as e:
            # Real upload failure (network, auth, permissions, …).
            # Classify transient vs client-4xx vs definitive so log
            # noise scales with actionability. See M6 in download_blob.
            client_4xx = _is_client_4xx(e)
            transient = is_transient_azure_error(e)
            if client_4xx:
                logger.warning(
                    "Failed to upload blob (client error)",
                    extra={
                        "container": container,
                        "path": path_clean,
                        "error": _short_azure_error(e),
                    },
                )
            else:
                logger.error(
                    "Failed to upload blob",
                    exc_info=not transient,
                    extra={
                        "container": container,
                        "path": path_clean,
                        "transient": transient,
                        "error": (
                            _short_azure_error(e) if isinstance(e, HttpResponseError) else str(e)
                        ),
                    },
                )
            raise

    async def get_blob_size(self, blob_ref: BlobRef) -> int:
        """
        Get the size of a blob in bytes.

        Args:
            blob_ref: BlobRef with container, path, and optional version ID

        Returns:
            Blob size in bytes

        Raises:
            ResourceNotFoundError: If blob not found
        """
        container = blob_ref.container
        path = blob_ref.path.lstrip("/")
        version_id = blob_ref.version_id

        blob_client = self.service_client.get_blob_client(container=container, blob=path)

        if version_id:
            properties = await blob_client.get_blob_properties(version_id=version_id)
        else:
            properties = await blob_client.get_blob_properties()

        return properties.size

    async def download_blob_stream(
        self,
        blob_ref: BlobRef,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        progress_callback: ProgressCallback | None = None,
    ) -> AsyncIterator[bytes]:
        """
        Download blob as streaming async iterator.

        Uses Azure SDK's StorageStreamDownloader.chunks() for true streaming
        without loading entire blob into memory. Memory usage is bounded by
        chunk_size regardless of blob size.

        Args:
            blob_ref: BlobRef with container, path, and optional version ID
            chunk_size: Chunk size in bytes (default 4MB for Azure optimal performance)
            progress_callback: Optional callback(bytes_downloaded, total_size)

        Yields:
            bytes: Chunks of blob data

        Raises:
            ResourceNotFoundError: If blob not found
            Exception: If download fails

        Example:
            >>> async for chunk in client.download_blob_stream(blob_ref):
            ...     await file.write(chunk)
        """
        container = blob_ref.container
        path = blob_ref.path.lstrip("/")
        version_id = blob_ref.version_id

        logger.debug(
            "Starting streaming download",
            extra={
                "container": container,
                "path": path,
                "version_id": version_id,
                "chunk_size": chunk_size,
            },
        )

        try:
            blob_client = self.service_client.get_blob_client(container=container, blob=path)

            # Get blob properties for total size (needed for progress)
            if version_id:
                properties = await blob_client.get_blob_properties(version_id=version_id)
            else:
                properties = await blob_client.get_blob_properties()

            total_size = properties.size
            bytes_downloaded = 0

            # Get download stream
            if version_id:
                download_stream = await blob_client.download_blob(
                    version_id=version_id,
                    max_concurrency=1,  # Sequential for true streaming
                )
            else:
                download_stream = await blob_client.download_blob(
                    max_concurrency=1,
                )

            # Use chunks() iterator - this is true streaming, no buffering
            async for chunk in download_stream.chunks():
                bytes_downloaded += len(chunk)

                if progress_callback:
                    try:
                        progress_callback(bytes_downloaded, total_size)
                    except Exception as cb_error:
                        logger.warning(
                            "Progress callback error (ignored)",
                            extra={"error": str(cb_error)},
                        )

                yield chunk

            logger.debug(
                "Streaming download complete",
                extra={
                    "container": container,
                    "path": path,
                    "total_bytes": bytes_downloaded,
                },
            )

        except ResourceNotFoundError:
            logger.warning(
                "Blob not found for streaming download",
                extra={
                    "container": container,
                    "path": path,
                    "version_id": version_id,
                },
            )
            raise

        except Exception as e:
            client_4xx = _is_client_4xx(e)
            transient = is_transient_azure_error(e)
            if client_4xx:
                logger.warning(
                    "Streaming download failed (client error)",
                    extra={
                        "container": container,
                        "path": path,
                        "error": _short_azure_error(e),
                    },
                )
            else:
                logger.error(
                    "Streaming download failed",
                    exc_info=not transient,
                    extra={
                        "container": container,
                        "path": path,
                        "error": (
                            _short_azure_error(e) if isinstance(e, HttpResponseError) else str(e)
                        ),
                    },
                )
            raise

    async def upload_blob_stream(
        self,
        container: str,
        stream: AsyncIterator[bytes],
        path: str | None = None,
        metadata: dict[str, str] | None = None,
        progress_callback: Callable[[int], None] | None = None,
        overwrite: bool = False,
    ) -> BlobRef:
        """
        Upload blob from async stream without buffering entire content.

        Uses Azure SDK's streaming upload capability. Memory usage is bounded
        by the chunk size of incoming stream regardless of total file size.

        Args:
            container: Container name
            stream: Async iterator yielding byte chunks
            path: Optional blob path (auto-generated if not provided)
            metadata: Optional blob metadata
            progress_callback: Optional callback(bytes_uploaded)

        Returns:
            BlobRef with URI including version ID

        Raises:
            Exception: If upload fails

        Example:
            >>> async def file_stream():
            ...     async with aiofiles.open("large.bin", "rb") as f:
            ...         while chunk := await f.read(4 * 1024 * 1024):
            ...             yield chunk
            >>> blob_ref = await client.upload_blob_stream("data", file_stream())
        """
        # Generate path if not provided (bare UUID, see upload_blob).
        if not path:
            path = f"{uuid.uuid4()}.bin"

        path_clean = path.lstrip("/")

        logger.debug(
            "Starting streaming upload",
            extra={
                "container": container,
                "path": path_clean,
                "metadata": metadata,
            },
        )

        try:
            blob_client = self.service_client.get_blob_client(container=container, blob=path_clean)

            # Wrap stream with progress tracking if callback provided
            async def tracked_stream():
                bytes_uploaded = 0
                async for chunk in stream:
                    bytes_uploaded += len(chunk)
                    if progress_callback:
                        try:
                            progress_callback(bytes_uploaded)
                        except Exception as cb_error:
                            logger.warning(
                                "Progress callback error (ignored)",
                                extra={"error": str(cb_error)},
                            )
                    yield chunk

            # Azure SDK accepts async iterators directly for streaming upload
            # This streams data without buffering the entire content
            result = await blob_client.upload_blob(
                data=tracked_stream(),
                metadata=metadata or {},
                overwrite=overwrite,
                max_concurrency=1,  # Sequential for consistent streaming
            )

            version_id = result.get("version_id")
            uri = build_blob_uri(container=container, path=path_clean, version_id=version_id)

            logger.debug(
                "Streaming upload complete",
                extra={
                    "container": container,
                    "path": path_clean,
                    "version_id": version_id,
                    "uri": uri,
                },
            )

            return BlobRef.from_uri(uri)

        except ResourceExistsError:
            logger.info(
                "Blob already exists (streaming upload, overwrite=False)",
                extra={"container": container, "path": path_clean},
            )
            raise

        except Exception as e:
            client_4xx = _is_client_4xx(e)
            transient = is_transient_azure_error(e)
            if client_4xx:
                logger.warning(
                    "Streaming upload failed (client error)",
                    extra={
                        "container": container,
                        "path": path_clean,
                        "error": _short_azure_error(e),
                    },
                )
            else:
                logger.error(
                    "Streaming upload failed",
                    exc_info=not transient,
                    extra={
                        "container": container,
                        "path": path_clean,
                        "transient": transient,
                        "error": (
                            _short_azure_error(e) if isinstance(e, HttpResponseError) else str(e)
                        ),
                    },
                )
            raise

    async def close(self) -> None:
        """Close the blob service client."""
        await self.service_client.close()
        logger.info("Azure Blob client closed")

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
