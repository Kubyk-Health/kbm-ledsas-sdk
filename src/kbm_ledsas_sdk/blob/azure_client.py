"""
Azure Blob Storage client.

Uploads and downloads blobs addressed by azblob:// URIs. Supports true
streaming for multi-GB file transfers with minimal memory footprint.
"""

import base64
import binascii
import logging
import uuid
from collections.abc import AsyncIterator, Callable
from urllib.parse import urlsplit

from azure.core import MatchConditions
from azure.core.exceptions import (
    AzureError,  # noqa: F401
    ClientAuthenticationError,
    HttpResponseError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
    ServiceRequestError,
    ServiceResponseError,
)
from azure.storage.blob import ContentSettings
from azure.storage.blob.aio import BlobServiceClient, ExponentialRetry

from kbm_ledsas_sdk.blob.operations import (
    _check_upload_path,
    _mark_non_retryable,
    _mark_retryable,
)
from kbm_ledsas_sdk.models.blob import BlobRef

# Reason given when the blob a download_stream reads is overwritten during
# the download (HTTP 412 of the ETag check below).
BLOB_CHANGED_DURING_DOWNLOAD = (
    "the blob changed while it was being downloaded (it was overwritten "
    "during download_stream); a new attempt reads its new content"
)


# Default chunk size: 4MB (optimal for Azure blob storage performance)
DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024

# Type alias for progress callbacks
ProgressCallback = Callable[[int, int], None]  # (bytes_transferred, total_bytes)

# The storage client's own retry policy, per SDK call. Its library default
# (15 s initial backoff growing to ~25 s, 3 retries, 20 s connect timeout)
# made one call against unreachable storage block for about 85 s, and the
# SDK retries the whole handler on top of that (KBM_LEDSAS_MAX_RETRIES), so
# a caller waited 5-6 minutes for a reply. Bounded here: 2 retries after
# ~3 s and ~5 s (1 s jitter), 10 s to open a connection and 10 s of
# silence while waiting for an answer (the read timeout restarts with each
# piece of the answer, so it bounds silence, not transfer time). One call
# then gives up after about 10 s when storage refuses the connection, and
# after about 40 s when storage does not answer at all (3 attempts of
# 10 s plus the backoff; the library's 60 s read timeout made that about
# 190 s). The handler-level retries still cover longer outages.
STORAGE_RETRY_TOTAL = 2
STORAGE_RETRY_INITIAL_BACKOFF_S = 1
STORAGE_RETRY_INCREMENT_BASE = 2
STORAGE_RETRY_JITTER_S = 1
STORAGE_CONNECTION_TIMEOUT_S = 10
STORAGE_READ_TIMEOUT_S = 10

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


# The reply a caller gets when the service's own storage credentials cannot
# sign a request (see _credential_failure); the log record says what to fix.
CREDENTIALS_CANNOT_SIGN = (
    "blob storage access is misconfigured on the service (its storage "
    "credentials cannot sign requests)"
)


def _credential_failure(exc: BaseException) -> str | None:
    """The log text for a request the storage client could not sign with
    the configured account key (``AzureSigningError``, raised before
    anything is sent, so it has no HTTP status), or None.

    It fails the same way on every attempt (the key is read once, at
    start), so it is not worth a retry. The account key is never quoted.
    """
    if not isinstance(exc, ClientAuthenticationError):
        return None
    if isinstance(getattr(exc, "status_code", None), int):
        return None  # a storage response (403 AuthenticationFailed): a 4xx
    first = (str(exc).strip().splitlines() or [""])[0][:120]
    detail = f"{type(exc).__name__}: {first}" if first else type(exc).__name__
    return (
        f"the storage credentials cannot sign requests ({detail}); check "
        f"the AccountKey in KBM_LEDSAS_BLOB_CONN_STRING"
    )


def _not_found_message(exc: BaseException, streaming: bool = False) -> str:
    """``Container not found`` or ``Blob not found`` (from the storage
    error code), so a missing container is not reported as a missing
    blob."""
    what = "Container" if getattr(exc, "error_code", None) == "ContainerNotFound" else "Blob"
    return f"{what} not found for streaming download" if streaming else f"{what} not found"


def _connection_string_problem(connection_string: str, service_client) -> str | None:
    """What makes a connection string the storage client accepted unusable,
    or None. Checked at start, because the client itself only fails on
    first use: a string without a credential (no AccountKey and no
    SharedAccessSignature, or an empty one) gets every request refused
    with HTTP 403, an AccountKey that is not base64 (a trailing ``=`` lost
    in a copy) fails every request with a signing error, and a BlobEndpoint
    without ``http://`` / ``https://`` is silently read as an https URL
    (the local emulator speaks http). The value is never quoted."""
    settings = {}
    for part in connection_string.strip().rstrip(";").split(";"):
        key, sep, value = part.partition("=")
        if sep:
            settings[key.strip().upper()] = value.strip()
    endpoint = settings.get("BLOBENDPOINT")
    if endpoint is not None:
        try:
            parts = urlsplit(endpoint)
            host = parts.hostname
        except ValueError:
            host = None
            parts = None
        if parts is None or parts.scheme.lower() not in ("http", "https") or not host:
            return "its BlobEndpoint is not an http:// or https:// URL with a host"
    credential = getattr(service_client, "credential", None)
    account_key = getattr(credential, "account_key", None)
    url = getattr(service_client, "url", None)
    if credential is None and isinstance(url, str) and not urlsplit(url).query:
        # Neither an AccountKey nor a SharedAccessSignature (the client
        # carries a SAS in its URL): the client would send every request
        # without credentials, and storage refuses them (HTTP 403).
        return (
            "it carries no credential: neither an AccountKey nor a "
            "SharedAccessSignature, so storage would refuse every request "
            "with HTTP 403"
        )
    if isinstance(account_key, str) and not account_key.strip():
        return (
            "its AccountKey or SharedAccessSignature is empty, so storage "
            "would refuse every request with HTTP 403"
        )
    if isinstance(account_key, str):
        try:
            # The decoding the client uses to sign each request.
            base64.b64decode(account_key.encode("utf-8"))
        except (binascii.Error, ValueError):
            return (
                "its AccountKey is not valid base64; a trailing '=' or '==' "
                "may have been cut off when it was copied"
            )
    return None


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

        try:
            self.service_client = BlobServiceClient.from_connection_string(
                connection_string,
                retry_policy=ExponentialRetry(
                    initial_backoff=STORAGE_RETRY_INITIAL_BACKOFF_S,
                    increment_base=STORAGE_RETRY_INCREMENT_BASE,
                    retry_total=STORAGE_RETRY_TOTAL,
                    random_jitter_range=STORAGE_RETRY_JITTER_S,
                ),
                connection_timeout=STORAGE_CONNECTION_TIMEOUT_S,
                read_timeout=STORAGE_READ_TIMEOUT_S,
            )
        except ValueError as e:
            # The client library's text ("Connection string is either
            # blank or malformed.") does not say which setting to fix.
            # The value itself is never repeated (it holds the account key).
            reason = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
            raise ValueError(
                f"KBM_LEDSAS_BLOB_CONN_STRING is not a usable storage "
                f"connection string ({reason[:200]}). Use the connection "
                f"string from the storage account's access keys, or the "
                f"local storage emulator's string from .env.example."
            ) from None
        problem = _connection_string_problem(connection_string, self.service_client)
        if problem is not None:
            raise ValueError(
                f"KBM_LEDSAS_BLOB_CONN_STRING is not a usable storage "
                f"connection string ({problem}). Use the connection "
                f"string from the storage account's access keys, or the "
                f"local storage emulator's string from .env.example."
            )
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

        except ResourceNotFoundError as e:
            logger.warning(
                _not_found_message(e),
                extra={
                    "container": container,
                    "path": path,
                    "version_id": version_id,
                    "error_code": getattr(e, "error_code", None),
                },
            )
            raise

        except Exception as e:
            # Client-side 4xx (InvalidResourceName,
            # AuthenticationFailed, etc.) → single WARNING with short
            # summary, no XML body, no traceback. Transient (transport,
            # 408/429/500/502/503/504) → INFO, no traceback. Anything
            # else (e.g. 501/505/507) → ERROR + traceback.
            credential = _credential_failure(e)
            client_4xx = _is_client_4xx(e)
            transient = is_transient_azure_error(e)
            if credential is not None:
                # Not worth a retry, and its stack adds nothing: one line.
                _mark_non_retryable(e, CREDENTIALS_CANNOT_SIGN)
                logger.warning(
                    "Failed to download blob: %s",
                    credential,
                    extra={"container": container, "path": path},
                )
            elif client_4xx:
                logger.warning(
                    "Failed to download blob (client error)",
                    extra={
                        "container": container,
                        "path": path,
                        "error": _short_azure_error(e),
                    },
                )
            elif transient:
                # A transient storage failure (unreachable, timeout,
                # throttling, 500/502/503/504): INFO, no traceback. If the
                # handler lets it propagate, the SDK logs the attempt once
                # (WARNING) and retries the command.
                logger.info(
                    "Failed to download blob (transient storage error)",
                    extra={
                        "container": container,
                        "path": path,
                        "transient": True,
                        "error": (
                            _short_azure_error(e) if isinstance(e, HttpResponseError) else str(e)
                        ),
                    },
                )
            else:
                logger.error(
                    "Failed to download blob",
                    exc_info=True,
                    extra={
                        "container": container,
                        "path": path,
                        "transient": False,
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
        content_type: str | None = None,
        auto_suffix: str = ".bin",
    ) -> BlobRef:
        """
        Upload blob with optional metadata.

        Args:
            container: Container name
            data: Blob data
            path: Optional blob path (auto-generated if not provided:
                ``<uuid><auto_suffix>``)
            metadata: Optional blob metadata
            overwrite: If True, replace existing blob at ``path``. Default
                False to refuse to clobber an existing blob (use True for
                the idempotency-key replay pattern).
            content_type: The blob's stored Content-Type (what a browser
                or a download tool sees). None stores the storage
                service's default, ``application/octet-stream``.
            auto_suffix: Extension of an auto-generated path.

        Returns:
            BlobRef with URI including version ID

        Raises:
            ResourceExistsError: If ``overwrite=False`` and ``path`` exists.
            Exception: If upload fails for other reasons.
        """
        # Refuse a path the returned URI could not represent before
        # anything is written (see _check_upload_path).
        _check_upload_path(path)

        # Generate path if not provided.
        # UUID supplies the uniqueness; the prior ``{timestamp}_``
        # prefix only leaked server time into blob listings without
        # adding collision resistance. Bare UUID is enough.
        if not path:
            path = f"{uuid.uuid4()}{auto_suffix}"

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

            # Upload with metadata (and the content type, when given)
            upload_kwargs = {}
            if content_type is not None:
                upload_kwargs["content_settings"] = ContentSettings(content_type=content_type)
            result = await blob_client.upload_blob(
                data=data,
                metadata=metadata or {},
                overwrite=overwrite,
                **upload_kwargs,
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
            # noise scales with actionability (same classification as
            # download_blob).
            credential = _credential_failure(e)
            client_4xx = _is_client_4xx(e)
            transient = is_transient_azure_error(e)
            if credential is not None:
                # Not worth a retry, and its stack adds nothing: one line.
                _mark_non_retryable(e, CREDENTIALS_CANNOT_SIGN)
                logger.warning(
                    "Failed to upload blob: %s",
                    credential,
                    extra={"container": container, "path": path_clean},
                )
            elif client_4xx:
                logger.warning(
                    "Failed to upload blob (client error)",
                    extra={
                        "container": container,
                        "path": path_clean,
                        "error": _short_azure_error(e),
                    },
                )
            elif transient:
                # A transient storage failure (unreachable, timeout,
                # throttling, 500/502/503/504): INFO, no traceback. If the
                # handler lets it propagate, the SDK logs the attempt once
                # (WARNING) and retries the command.
                logger.info(
                    "Failed to upload blob (transient storage error)",
                    extra={
                        "container": container,
                        "path": path_clean,
                        "transient": True,
                        "error": (
                            _short_azure_error(e) if isinstance(e, HttpResponseError) else str(e)
                        ),
                    },
                )
            else:
                logger.error(
                    "Failed to upload blob",
                    exc_info=True,
                    extra={
                        "container": container,
                        "path": path_clean,
                        "transient": False,
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

        Reads the blob with one ranged request per chunk, so every yielded
        chunk is exactly ``chunk_size`` bytes (the last one may be shorter)
        and memory use is bounded by ``chunk_size`` regardless of blob
        size. Without a version ID, every range request is conditioned on
        the ETag read at the start, so a blob that is overwritten while it
        is being streamed fails the download (ResourceModifiedError, HTTP
        412) instead of mixing old and new content. That error is marked
        worth retrying: uncaught in a handler, the command is retried and
        the new attempt reads the new content.

        Args:
            blob_ref: BlobRef with container, path, and optional version ID
            chunk_size: Size of each yielded chunk in bytes (default 4 MiB);
                must be at least 1
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
        if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size < 1:
            # Invalid argument: fails the same way on every attempt.
            err = ValueError(f"chunk_size must be a positive integer, got {chunk_size!r}")
            _mark_non_retryable(err, str(err))
            raise err

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

        bytes_downloaded = 0
        try:
            blob_client = self.service_client.get_blob_client(container=container, blob=path)

            # Get blob properties for total size (needed for progress)
            if version_id:
                properties = await blob_client.get_blob_properties(version_id=version_id)
            else:
                properties = await blob_client.get_blob_properties()

            total_size = properties.size
            bytes_downloaded = 0

            # One ranged read per chunk: the client library's own chunk
            # iterator ignores the caller's chunk size and buffers its
            # first (up to 32 MiB) request, so it neither honors
            # chunk_size nor bounds memory by it.
            if version_id:
                pinned = {"version_id": version_id}
            else:
                pinned = {
                    "etag": properties.etag,
                    "match_condition": MatchConditions.IfNotModified,
                }

            while bytes_downloaded < total_size:
                length = min(chunk_size, total_size - bytes_downloaded)
                downloader = await blob_client.download_blob(
                    offset=bytes_downloaded,
                    length=length,
                    max_concurrency=1,
                    **pinned,
                )
                chunk = await downloader.readall()
                if not chunk:
                    # Blob shrank under a versioned read: stop instead of
                    # looping on empty ranges.
                    break
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

        except ResourceNotFoundError as e:
            logger.warning(
                _not_found_message(e, streaming=True),
                extra={
                    "container": container,
                    "path": path,
                    "version_id": version_id,
                    "error_code": getattr(e, "error_code", None),
                },
            )
            raise

        except ResourceModifiedError as e:
            if version_id:
                raise
            # The ETag check above: the blob was overwritten during the
            # download. Not a refusal of the request: a new attempt reads
            # the new content, so the error is marked worth retrying.
            # INFO, no traceback; if the handler lets it propagate, the SDK
            # logs the attempt once (WARNING) and retries the command.
            _mark_retryable(e, BLOB_CHANGED_DURING_DOWNLOAD)
            logger.info(
                "Streaming download stopped: the blob changed while it was " "being downloaded",
                extra={
                    "container": container,
                    "path": path,
                    "bytes_downloaded": bytes_downloaded,
                    "error": _short_azure_error(e),
                },
            )
            raise

        except Exception as e:
            credential = _credential_failure(e)
            client_4xx = _is_client_4xx(e)
            transient = is_transient_azure_error(e)
            if credential is not None:
                # Not worth a retry, and its stack adds nothing: one line.
                _mark_non_retryable(e, CREDENTIALS_CANNOT_SIGN)
                logger.warning(
                    "Streaming download failed: %s",
                    credential,
                    extra={"container": container, "path": path},
                )
            elif client_4xx:
                logger.warning(
                    "Streaming download failed (client error)",
                    extra={
                        "container": container,
                        "path": path,
                        "error": _short_azure_error(e),
                    },
                )
            elif transient:
                # A transient storage failure (unreachable, timeout,
                # throttling, 500/502/503/504): INFO, no traceback. If the
                # handler lets it propagate, the SDK logs the attempt once
                # (WARNING) and retries the command.
                logger.info(
                    "Streaming download failed (transient storage error)",
                    extra={
                        "container": container,
                        "path": path,
                        "transient": True,
                        "error": (
                            _short_azure_error(e) if isinstance(e, HttpResponseError) else str(e)
                        ),
                    },
                )
            else:
                logger.error(
                    "Streaming download failed",
                    exc_info=True,
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
        _check_upload_path(path)

        # Generate path if not provided (bare UUID, as in upload_blob).
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
            credential = _credential_failure(e)
            client_4xx = _is_client_4xx(e)
            transient = is_transient_azure_error(e)
            if credential is not None:
                # Not worth a retry, and its stack adds nothing: one line.
                _mark_non_retryable(e, CREDENTIALS_CANNOT_SIGN)
                logger.warning(
                    "Streaming upload failed: %s",
                    credential,
                    extra={"container": container, "path": path_clean},
                )
            elif client_4xx:
                logger.warning(
                    "Streaming upload failed (client error)",
                    extra={
                        "container": container,
                        "path": path_clean,
                        "error": _short_azure_error(e),
                    },
                )
            elif transient:
                # A transient storage failure (unreachable, timeout,
                # throttling, 500/502/503/504): INFO, no traceback. If the
                # handler lets it propagate, the SDK logs the attempt once
                # (WARNING) and retries the command.
                logger.info(
                    "Streaming upload failed (transient storage error)",
                    extra={
                        "container": container,
                        "path": path_clean,
                        "transient": True,
                        "error": (
                            _short_azure_error(e) if isinstance(e, HttpResponseError) else str(e)
                        ),
                    },
                )
            else:
                logger.error(
                    "Streaming upload failed",
                    exc_info=True,
                    extra={
                        "container": container,
                        "path": path_clean,
                        "transient": False,
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
