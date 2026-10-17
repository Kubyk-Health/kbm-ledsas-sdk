"""
Blob operations implementation using the Azure Blob SDK.

Supports true streaming for multi-GB file transfers with minimal memory footprint.
"""

import json
import logging
import sys
from collections.abc import AsyncIterator, Callable

from kbm_ledsas_sdk.blob.azure_client import DEFAULT_CHUNK_SIZE, AzureBlobClient
from kbm_ledsas_sdk.blob.operations import (
    BlobOperations,
    _check_container,
    _check_upload_data,
    _check_upload_stream,
    _encode_text,
    _ensure_blob_ref,
    _json_dumps_for_blob,
    _mark_non_retryable,
)
from kbm_ledsas_sdk.models.blob import BlobRef

logger = logging.getLogger(__name__)

# The Content-Type stored with a blob written by upload_json / upload_text
# (what a browser, a download tool or a storage browser sees).
# upload_bytes and upload_stream store the storage service's default,
# application/octet-stream.
JSON_CONTENT_TYPE = "application/json"
TEXT_CONTENT_TYPE = "text/plain; charset=utf-8"


def _refuse_blob_json(
    blob_ref: BlobRef, error: BaseException, detail: str, valid_json: bool = False
) -> None:
    """Log content ``download_json`` cannot parse and mark ``error`` as not
    worth retrying.

    The blob's content is the problem, so every retry would fail the same
    way: one concise ERROR line (no traceback) and a marker that makes the
    SDK treat the error as Permanent if the handler lets it propagate. The
    exception's type is unchanged. ``valid_json`` is True for content that is
    valid JSON Python's parser cannot read (too deep, too long a number).
    """
    logger.error(
        "Failed to parse blob as JSON: %s",
        detail,
        extra={"uri": blob_ref.uri, "error": detail},
    )
    what = "cannot be parsed as JSON" if valid_json else "is not valid JSON"
    _mark_non_retryable(error, f"blob content {what} ({detail})")


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
            "DirectBlobOperations initialized", extra={"default_container": default_container}
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
            ValueError: If the content holds an integer with more digits
                than Python converts (4300 by default)
            RecursionError: If the content is nested more levels deep than
                Python's JSON parser supports
            Exception: For other download errors

        The three content errors (and ``UnicodeDecodeError`` for content
        that is not UTF-8) are logged once and, if the handler lets them
        propagate, replied as ``Permanent`` and not retried: the same
        content fails the same way on every attempt.
        """
        blob_ref = _ensure_blob_ref(blob_ref)

        # Call azure_client directly (not self.download_bytes) so this
        # public method emits exactly one INFO log. azure_client already
        # classifies and logs a failed download; it propagates unchanged.
        data = await self.azure_client.download_blob(blob_ref)

        try:
            obj = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            _refuse_blob_json(blob_ref, e, str(e))
            raise
        except RecursionError as e:
            # Python's JSON parser gives up on content nested more deeply
            # than its recursion limit allows (about 1000 levels): the
            # same on every attempt.
            _refuse_blob_json(
                blob_ref,
                e,
                "it is nested more levels deep than Python's JSON parser supports",
                valid_json=True,
            )
            raise
        except ValueError as e:
            # An integer literal longer than the interpreter's
            # int-conversion limit is refused with a plain ValueError (not
            # JSONDecodeError). The interpreter's own text suggests raising
            # that limit, the wrong fix for broken input, so it is not
            # repeated.
            if "integer string conversion" not in str(e):
                raise
            _refuse_blob_json(
                blob_ref,
                e,
                f"it holds an integer with more than "
                f"{sys.get_int_max_str_digits()} digits, more than Python "
                f"converts",
                valid_json=True,
            )
            raise

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
                pattern (see QUICKSTART_DIRECT_MODE.md §3.5).

        Returns:
            BlobRef with URI including versionId

        Raises:
            TypeError: If ``data`` is not bytes-like or ``container`` is
                not a string (refused before any I/O; Permanent if uncaught).
            ValueError: If ``container`` or ``path`` is invalid (refused
                before any I/O; Permanent if uncaught).
            ResourceExistsError: If ``overwrite=False`` and ``path`` exists.
            Exception: If upload fails for other reasons.
        """
        _check_container(container)
        # bytearray / memoryview become bytes (the storage client would
        # otherwise iterate a bytearray as a sequence of ints).
        data = _check_upload_data(data)
        logger.debug(
            "Uploading blob from bytes",
            extra={"container": container, "path": path, "size": len(data), "overwrite": overwrite},
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
            TypeError: If obj cannot be serialized to JSON (ValueError for
                a circular reference). Uncaught, either is replied as
                Permanent (not retried): the same object fails again.
            TypeError / ValueError: If ``container`` or ``path`` is invalid
                (refused before any I/O; Permanent if uncaught).
            ResourceExistsError: If ``overwrite=False`` and ``path`` exists.
            Exception: If upload fails for other reasons.
        """
        _check_container(container)
        try:
            # Standard JSON: NaN / Infinity are written as null, as in
            # replies (json.dumps would write the non-standard NaN token).
            json_str = _json_dumps_for_blob(obj)
            data = json_str.encode("utf-8")
        except (TypeError, ValueError) as e:
            # The argument is the problem, so a retry fails the same way:
            # one ERROR line (no traceback) and the non-retryable marker.
            logger.error("Failed to serialize object to JSON: %s", e, extra={"error": str(e)})
            text = str(e)
            # Text with a lone surrogate has a JSON form (UTF-8 cannot encode
            # it): its refusal names the problem itself, said once.
            reason = (
                text
                if text.startswith("upload_json() refused:")
                else f"upload_json() got an object with no JSON form ({e})"
            )
            _mark_non_retryable(e, reason)
            raise

        # Call azure_client directly so this public method emits exactly
        # one INFO log. Error classification is azure_client's job. The
        # blob is stored as JSON (Content-Type application/json; an
        # auto-generated name ends in .json).
        blob_ref = await self.azure_client.upload_blob(
            container=container,
            data=data,
            path=path,
            metadata=None,
            overwrite=overwrite,
            content_type=JSON_CONTENT_TYPE,
            auto_suffix=".json",
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
            ValueError: If text holds a lone surrogate, which UTF-8 cannot
                encode.
            TypeError / ValueError: If ``container`` or ``path`` is invalid.
                All of these are refused before any I/O (Permanent if
                uncaught).
            ResourceExistsError: If ``overwrite=False`` and ``path`` exists.
            Exception: If upload fails for other reasons.
        """
        # Validate input type - provide helpful error for common mistake.
        # Marked non-retryable: the same argument fails on every attempt.
        if not isinstance(text, str):
            err = TypeError(
                f"upload_text() requires a string, got {type(text).__name__}. "
                f"For dict or list data, use upload_json() instead."
            )
            _mark_non_retryable(err, str(err))
            raise err
        _check_container(container)

        # Text UTF-8 cannot encode (a lone surrogate) is refused here, not
        # retried as an UnicodeEncodeError.
        data = _encode_text(text)

        # Stored as UTF-8 text (Content-Type text/plain; charset=utf-8; an
        # auto-generated name ends in .txt).
        blob_ref = await self.azure_client.upload_blob(
            container=container,
            data=data,
            path=path,
            metadata=None,
            overwrite=overwrite,
            content_type=TEXT_CONTENT_TYPE,
            auto_suffix=".txt",
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
            UnicodeDecodeError: If blob content is not valid UTF-8.
                Uncaught, it is replied as Permanent (not retried): the
                same content fails the same way again.
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
            # Content problem, like non-JSON content in download_json: one
            # ERROR line (no traceback) and the non-retryable marker. The
            # exception type is unchanged.
            logger.error(
                "Failed to decode blob as UTF-8 text: %s",
                e,
                extra={"uri": blob_ref.uri, "error": str(e)},
            )
            _mark_non_retryable(e, f"blob content is not valid UTF-8 text ({e})")
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
            TypeError: If ``stream`` is not an async iterable.
            TypeError / ValueError: If ``container`` or ``path`` is invalid.
                Both are refused before any I/O (Permanent if uncaught).
            ResourceExistsError: If ``overwrite=False`` and ``path`` exists.
            Exception: If upload fails for other reasons.

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
        _check_container(container)
        _check_upload_stream(stream)
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

        Reads the blob one ranged request per chunk, so the whole blob is
        never held in memory: every yielded chunk is exactly ``chunk_size``
        bytes (the last one may be shorter) and memory use is bounded by
        ``chunk_size`` regardless of blob size.

        Args:
            blob_ref: Reference to the blob to download (BlobRef or azblob:// URI string)
            chunk_size: Size of each yielded chunk in bytes (default 4 MiB)
            progress_callback: Optional callback(bytes_downloaded, total_size)

        Yields:
            Byte chunks of ``chunk_size`` bytes (the last may be shorter)

        Raises:
            ResourceNotFoundError: If the blob (or version) does not exist
            ValueError: If ``blob_ref`` is not a valid azblob:// URI, or
                ``chunk_size`` is not a positive integer
            Exception: For other download errors

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
