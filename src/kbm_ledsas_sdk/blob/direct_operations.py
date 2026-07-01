"""
Blob operations implementation using the Azure Blob SDK.

Supports true streaming for multi-GB file transfers with minimal memory footprint.
"""

import json
import logging
from collections.abc import AsyncIterator, Callable

from kbm_ledsas_sdk.blob.azure_client import DEFAULT_CHUNK_SIZE, AzureBlobClient
from kbm_ledsas_sdk.blob.operations import BlobOperations, _ensure_blob_ref
from kbm_ledsas_sdk.models.blob import BlobRef

logger = logging.getLogger(__name__)


class DirectBlobOperations(BlobOperations):
    """
    BlobOperations implementation using the Azure Blob SDK.

    Connects directly to Azure Blob Storage.

    Attributes:
        azure_client: Azure Blob client wrapper
        default_container: Default container for uploads
    """

    def __init__(self, azure_client: AzureBlobClient, default_container: str = "dev"):
        """
        Initialize direct blob operations.

        Args:
            azure_client: Configured Azure Blob client
            default_container: Default container for uploads
        """
        self.azure_client = azure_client
        self.default_container = default_container
        logger.info(
            "DirectBlobOperations initialized",
            extra={"default_container": default_container},
        )

    async def download_bytes(self, blob_ref: str | BlobRef) -> bytes:
        """
        Download blob contents as bytes.

        Args:
            blob_ref: Reference to the blob to download (BlobRef or azblob:// URI string)

        Returns:
            Blob contents as bytes

        Raises:
            ResourceNotFoundError: If blob not found
            Exception: For other download errors
        """
        blob_ref = _ensure_blob_ref(blob_ref)
        logger.debug("Downloading blob as bytes", extra={"uri": blob_ref.uri})

        # Azure_client classifies + logs every
        # failure mode (transient / client_4xx / network) with the
        # appropriate severity. Re-raise without re-logging — otherwise
        # every download_bytes failure produces two records that say
        # the same thing.
        data = await self.azure_client.download_blob(blob_ref)
        logger.info(
            "Downloaded blob as bytes",
            extra={"uri": blob_ref.uri, "size": len(data)},
        )
        return data

    async def download_json(self, blob_ref: str | BlobRef) -> dict | list:
        """
        Download blob contents and parse as JSON.

        Args:
            blob_ref: Reference to the blob to download (BlobRef or azblob:// URI string)

        Returns:
            Parsed JSON (dict or list — matches the blob's JSON root type).

        Raises:
            ResourceNotFoundError: If blob not found
            json.JSONDecodeError: If blob content is not valid JSON
            Exception: For other download errors
        """
        blob_ref = _ensure_blob_ref(blob_ref)

        try:
            # Call azure_client directly (not self.download_bytes) so this
            # public method emits exactly one INFO log.
            data = await self.azure_client.download_blob(blob_ref)

            # Parse JSON
            obj = json.loads(data.decode("utf-8"))

            # Don't log JSON key names at INFO — blob contents are
            # frequently sensitive (config with `api_key`, `password`,
            # `client_secret` keys, etc.). The keys go to DEBUG only.
            logger.info(
                "Downloaded blob as JSON",
                extra={"uri": blob_ref.uri, "size": len(data)},
            )
            if logger.isEnabledFor(logging.DEBUG) and isinstance(obj, dict):
                logger.debug(
                    "Downloaded JSON top-level keys",
                    extra={"uri": blob_ref.uri, "keys": list(obj.keys())},
                )

            return obj

        except json.JSONDecodeError as e:
            logger.error(
                "Failed to parse blob as JSON",
                exc_info=True,
                extra={"uri": blob_ref.uri, "error": str(e)},
            )
            raise

        except Exception:
            # azure_client already classified + logged the underlying
            # error. Re-raise without double-logging.
            raise

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
            overwrite: If True, replace an existing blob at ``path``.
                Default False; set True for the idempotency-key replay
                pattern (see docs/SDK_API_REFERENCE.md).

        Returns:
            BlobRef with URI including versionId

        Raises:
            ResourceExistsError: If ``overwrite=False`` and ``path`` exists.
            Exception: If upload fails for other reasons.
        """
        logger.debug(
            "Uploading blob from bytes",
            extra={
                "container": container,
                "path": path,
                "size": len(data),
                "overwrite": overwrite,
            },
        )

        # azure_client classifies + handles ResourceExistsError / transient
        # errors with appropriate log levels. Don't wrap with another
        # except-log layer here — that would double-emit the record.
        blob_ref = await self.azure_client.upload_blob(
            container=container,
            data=data,
            path=path,
            metadata=None,
            overwrite=overwrite,
        )

        logger.info("Uploaded blob from bytes", extra={"uri": blob_ref.uri})

        return blob_ref

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
            overwrite: See :meth:`upload_bytes`.

        Returns:
            BlobRef with URI including versionId

        Raises:
            TypeError: If obj cannot be serialized to JSON
            ResourceExistsError: If ``overwrite=False`` and ``path`` exists.
            Exception: If upload fails for other reasons.
        """
        try:
            json_str = json.dumps(obj)
            data = json_str.encode("utf-8")
        except TypeError as e:
            logger.error(
                "Failed to serialize object to JSON",
                exc_info=True,
                extra={"error": str(e)},
            )
            raise

        # Call azure_client directly so this public method emits exactly
        # one INFO log. Error classification is azure_client's job.
        blob_ref = await self.azure_client.upload_blob(
            container=container,
            data=data,
            path=path,
            metadata=None,
            overwrite=overwrite,
        )

        logger.info(
            "Uploaded blob from JSON",
            extra={"uri": blob_ref.uri, "size": len(data)},
        )

        return blob_ref

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
            overwrite: See :meth:`upload_bytes`.

        Returns:
            BlobRef with URI including versionId

        Raises:
            TypeError: If text is not a string
            ResourceExistsError: If ``overwrite=False`` and ``path`` exists.
            Exception: If upload fails for other reasons.
        """
        # Validate input type - provide helpful error for common mistake
        if not isinstance(text, str):
            raise TypeError(
                f"upload_text() requires a string, got {type(text).__name__}. "
                f"For dict or list data, use upload_json() instead."
            )

        data = text.encode("utf-8")

        blob_ref = await self.azure_client.upload_blob(
            container=container,
            data=data,
            path=path,
            metadata=None,
            overwrite=overwrite,
        )

        logger.info(
            "Uploaded blob from text",
            extra={"uri": blob_ref.uri, "length": len(text), "size": len(data)},
        )

        return blob_ref

    async def download_text(self, blob_ref: str | BlobRef) -> str:
        """
        Download blob contents and decode as UTF-8 text.

        Args:
            blob_ref: Reference to the blob to download (BlobRef or azblob:// URI string)

        Returns:
            Text content as string

        Raises:
            UnicodeDecodeError: If blob content is not valid UTF-8
            Exception: For other download errors
        """
        blob_ref = _ensure_blob_ref(blob_ref)

        # Azure_client owns failure-mode logging.
        # Only UnicodeDecodeError is unique to the text path and worth
        # logging here.
        data = await self.azure_client.download_blob(blob_ref)
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as e:
            logger.error(
                "Failed to decode blob as UTF-8 text",
                extra={"uri": blob_ref.uri, "error": str(e)},
            )
            raise

        logger.info(
            "Downloaded blob as text",
            extra={"uri": blob_ref.uri, "length": len(text), "size": len(data)},
        )
        return text

    async def upload_stream(
        self,
        container: str,
        stream: AsyncIterator[bytes],
        path: str | None = None,
        progress_callback: Callable[[int], None] | None = None,
        overwrite: bool = False,
    ) -> BlobRef:
        """
        Upload data from async stream to blob storage.

        Uses true streaming upload - data flows directly to Azure without
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
            Exception: If upload fails

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
        logger.debug(
            "Uploading blob from stream (true streaming)",
            extra={"container": container, "path": path},
        )

        # azure_client classifies + handles ResourceExistsError / transient
        # errors with appropriate log levels.
        blob_ref = await self.azure_client.upload_blob_stream(
            container=container,
            stream=stream,
            path=path,
            progress_callback=progress_callback,
            overwrite=overwrite,
        )

        logger.info("Uploaded blob from stream", extra={"uri": blob_ref.uri})

        return blob_ref

    async def download_stream(
        self,
        blob_ref: str | BlobRef,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> AsyncIterator[bytes]:
        """
        Download blob contents as true streaming async iterator.

        Uses Azure SDK's StorageStreamDownloader.chunks() for true streaming
        without loading entire blob into memory. Memory usage is bounded by
        chunk_size regardless of blob size.

        Supports multi-GB files with <50MB peak memory usage.

        Args:
            blob_ref: Reference to the blob to download (BlobRef or azblob:// URI string)
            chunk_size: Chunk size in bytes (default 4MB for optimal Azure performance)
            progress_callback: Optional callback(bytes_downloaded, total_size)

        Yields:
            Byte chunks (default 4MB chunks)

        Raises:
            Exception: If download fails

        Example:
            >>> async with aiofiles.open("output.bin", "wb") as f:
            ...     async for chunk in ctx.blob.download_stream(
            ...         blob_ref,
            ...         progress_callback=lambda d, t: print(f"{d}/{t} bytes")
            ...     ):
            ...         await f.write(chunk)
        """
        blob_ref = _ensure_blob_ref(blob_ref)
        logger.debug(
            "Downloading blob as stream (true streaming)",
            extra={"uri": blob_ref.uri, "chunk_size": chunk_size},
        )

        # Azure_client.download_blob_stream owns
        # failure-mode logging. Just re-raise.
        async for chunk in self.azure_client.download_blob_stream(
            blob_ref,
            chunk_size=chunk_size,
            progress_callback=progress_callback,
        ):
            yield chunk
