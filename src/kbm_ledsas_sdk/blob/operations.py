"""
Blob operations interface for the SDK.

Provides methods for uploading/downloading data to/from Azure Blob Storage.
Streaming operations support multi-GB files with minimal memory footprint.
"""

import json
import math
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from typing import Any

from ..models.blob import (
    LONE_SURROGATE,
    BlobRef,
    blob_name_problem,
    container_name_problem,
    refused_character,
)

# Default chunk size: 4MB (optimal for Azure blob storage performance)
DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024

# Type aliases for progress callbacks
DownloadProgressCallback = Callable[[int, int], None]  # (bytes_downloaded, total_bytes)
UploadProgressCallback = Callable[[int], None]  # (bytes_uploaded)

# Attribute the blob helpers set on an exception whose cause is the blob's
# content (e.g. download_json on content that is not valid JSON): the same
# content fails the same way on every attempt. The value is a short
# reason. If the handler lets such an exception propagate, the SDK treats
# it as a Permanent error (not retried) instead of an UnexpectedError.
# The exception's type is unchanged, so `except json.JSONDecodeError` /
# `except ValueError` in handler code keeps working.
NON_RETRYABLE_MARKER = "ledsas_non_retryable"


def _mark_non_retryable(exc: BaseException, reason: str) -> None:
    """Tag ``exc`` as non-retryable (see NON_RETRYABLE_MARKER)."""
    try:
        setattr(exc, NON_RETRYABLE_MARKER, reason)
    except Exception:  # pragma: no cover - exceptions without __dict__
        pass


# Attribute the blob helpers set on an exception that a new attempt is
# expected to get past although its class looks permanent: an HTTP 412
# from the SDK's own consistency check in download_stream (the blob was
# overwritten while it was being read; a new attempt reads the new
# content). The value is a short reason. If the handler lets such an
# exception propagate, the SDK retries the command and logs one WARNING
# per attempt without a traceback, as for a transient storage error. The
# exception's type is unchanged.
RETRYABLE_MARKER = "ledsas_retryable"


def _mark_retryable(exc: BaseException, reason: str) -> None:
    """Tag ``exc`` as worth retrying (see RETRYABLE_MARKER)."""
    try:
        setattr(exc, RETRYABLE_MARKER, reason)
    except Exception:  # pragma: no cover - exceptions without __dict__
        pass


def _lone_surrogate_in(text: str) -> tuple[int, int] | None:
    """``(code point, position)`` of the first lone surrogate in ``text``
    (a ``\\ud800``-style escape a JSON request can carry), or None."""
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as e:
        return (ord(text[e.start]) if e.start < len(text) else 0), e.start
    return None


def _find_lone_surrogate(value: Any, path: str, depth: int = 0) -> str | None:
    """``<path> ...`` naming the first string (a dict key or a value) in
    ``value`` that holds a lone surrogate, or None.

    JSON writes such a string as an escape (``"\\ud800"``), which RFC 8259
    leaves unpredictable: strict parsers refuse it, and .NET's
    System.Text.Json reads the document but throws when the string is
    read. The SDK therefore refuses it in what it writes (replies, status
    updates, ``upload_json``), as ``upload_text`` refuses text UTF-8
    cannot encode.
    """
    if isinstance(value, str):
        found = _lone_surrogate_in(value)
        if found is None:
            return None
        code, pos = found
        return (
            f"{path} is text with a lone surrogate U+{code:04X} at position "
            f"{pos}, which UTF-8 cannot encode"
        )
    if depth > 300:
        return None
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str):
                found = _lone_surrogate_in(key)
                if found is not None:
                    code, pos = found
                    return (
                        f"{path} has a key with a lone surrogate U+{code:04X} "
                        f"at position {pos}, which UTF-8 cannot encode"
                    )
            shown = json.dumps(key if isinstance(key, str) else str(key))
            problem = _find_lone_surrogate(item, f"{path}[{shown}]", depth + 1)
            if problem is not None:
                return problem
    elif isinstance(value, (list, tuple)):
        for i, item in enumerate(value):
            problem = _find_lone_surrogate(item, f"{path}[{i}]", depth + 1)
            if problem is not None:
                return problem
    return None


# Longest blob name the storage service accepts.
_BLOB_NAME_MAX = 1024


def _check_upload_path(path: str | None) -> None:
    """Refuse an upload ``path`` that the returned ``azblob://`` URI could
    not represent, BEFORE anything is written.

    The blob name is the path as given (one leading ``/`` is ignored), and
    the URI the upload returns is ``azblob://<container>/<path>``. A ``?``
    or ``#`` in the path would become a query string or a fragment of that
    URI (``q/x.bin?versionId=abc`` would come back as a reference to a
    version that does not exist, and ``x#y`` cannot be referenced at all),
    control characters and Unicode format characters are refused by every
    blob URI, and ``.`` / ``..`` segments, empty segments (``a//b``) and a
    trailing ``/`` are refused when the URI is read back. A backslash is
    refused: storage may store it as ``/`` (``a\\b`` stored as ``a/b``), so
    the returned URI would not name the blob written. ``None`` or ``""``
    (auto-generated name) is fine. The ValueError is marked non-retryable,
    so a handler that lets it propagate gets one Permanent reply.
    """
    if path is None or path == "":
        return
    problem: str | None = None
    if not isinstance(path, str):
        problem = f"must be a string, got {type(path).__name__}"
    else:
        name = path[1:] if path.startswith("/") else path
        if not name.strip("/"):
            problem = "is empty"
        elif len(name) > _BLOB_NAME_MAX:
            # The limit is storage's, on the blob name: the ignored
            # leading '/' does not count.
            problem = f"is longer than {_BLOB_NAME_MAX} characters"
        elif "?" in path or "#" in path:
            problem = "contains '?' or '#', which a blob URI cannot carry in a blob name"
        else:
            # The same characters a blob URI refuses (refused_character):
            # control characters (the ASCII ones, DEL and U+0080 to
            # U+009F), Unicode format characters, the line / paragraph
            # separators and lone surrogates.
            refused = refused_character(path)
            if refused is not None:
                o, kind = refused
                if kind in ("line separator", "paragraph separator"):
                    problem = f"contains the {kind} U+{o:04X}"
                elif kind == LONE_SURROGATE:
                    problem = f"contains a lone surrogate U+{o:04X}, which UTF-8 " f"cannot encode"
                else:
                    problem = f"contains the control or format character U+{o:04X}"
            if problem is None:
                problem = blob_name_problem(name)
    if problem is not None:
        err = ValueError(
            f"Invalid blob path: the path {problem}. A blob path is at most "
            f"{_BLOB_NAME_MAX} characters (a leading '/' not counted), with "
            f"no '?', '#', '\\', control characters, empty segments ('//') "
            f"or '.' / '..' segments, and does not end with '/'."
        )
        _mark_non_retryable(err, f"invalid blob path (the path {problem})")
        raise err


def _refuse_argument(err: Exception, reason: str) -> None:
    """Raise ``err`` marked non-retryable: an invalid argument fails the
    same way on every attempt, so a handler that lets it propagate gets one
    Permanent reply (no retries, no traceback)."""
    _mark_non_retryable(err, reason)
    raise err


def _ensure_blob_ref(blob_ref: str | BlobRef) -> BlobRef:
    """
    Convert a string URI to BlobRef if needed.

    Args:
        blob_ref: Either a BlobRef instance or an azblob:// URI string

    Returns:
        BlobRef instance

    Raises:
        ValueError: If string is not a valid azblob:// URI, or a BlobRef
            holds a URI that is not (one built without the checks, for
            example with ``object.__setattr__``): a BlobRef is checked
            again here, before the blob call uses or logs its URI.
        TypeError: If ``blob_ref`` is neither a string nor a BlobRef (for
            example ``None`` from a request field that is missing). Marked
            non-retryable, like the ValueError.
    """
    if isinstance(blob_ref, BlobRef):
        # Checked again (revalidate_instances): the result is a BlobRef
        # whose URI passed from_uri's checks.
        return BlobRef.model_validate(blob_ref)
    if isinstance(blob_ref, str):
        return BlobRef.from_uri(blob_ref)
    hint = " (is a required request field missing?)" if blob_ref is None else ""
    _refuse_argument(
        TypeError(
            f"blob_ref must be an azblob:// URI string or a BlobRef, got "
            f"{type(blob_ref).__name__}{hint}"
        ),
        f"invalid blob reference (got {type(blob_ref).__name__}{hint})",
    )
    raise AssertionError("unreachable")  # pragma: no cover


def _check_container(container: Any) -> None:
    """Refuse an upload ``container`` the storage service would refuse,
    before any I/O (non-retryable TypeError / ValueError)."""
    problem = container_name_problem(container)
    if problem is None:
        return
    err_class = TypeError if not isinstance(container, str) else ValueError
    _refuse_argument(
        err_class(f"Invalid container: {problem}"),
        f"invalid container ({problem})",
    )


def _check_upload_data(data: Any) -> bytes:
    """``data`` for ``upload_bytes`` as ``bytes``. ``bytearray`` and
    ``memoryview`` are converted; anything else (a ``str``, ``None``, a
    number) is refused (non-retryable TypeError)."""
    if isinstance(data, bytes):
        return data
    if isinstance(data, (bytearray, memoryview)):
        return bytes(data)
    if isinstance(data, str):
        msg = "upload_bytes() takes bytes, got str; for text use upload_text()"
    else:
        msg = (
            f"upload_bytes() takes bytes (or bytearray / memoryview), got " f"{type(data).__name__}"
        )
    _refuse_argument(TypeError(msg), msg)
    raise AssertionError("unreachable")  # pragma: no cover


def _check_upload_stream(stream: Any) -> None:
    """Refuse an ``upload_stream`` source that is not an async iterable of
    byte chunks (non-retryable TypeError)."""
    if hasattr(stream, "__aiter__"):
        return
    msg = (
        f"upload_stream() takes an async iterable of bytes (for example an "
        f"async generator), got {type(stream).__name__}; for bytes in "
        f"memory use upload_bytes()"
    )
    _refuse_argument(TypeError(msg), msg)


def _check_stream_chunk(chunk: Any) -> bytes:
    """One chunk an ``upload_stream`` source yielded, as ``bytes``.
    ``bytearray`` and ``memoryview`` are converted; anything else (a
    ``str``, ``None``, a number, a dict) is refused (non-retryable
    TypeError), before it reaches the storage service: a ``str`` was stored
    UTF-8 encoded without a word, and the other types failed inside the
    storage client, logged as a storage failure and retried."""
    if isinstance(chunk, bytes):
        return chunk
    if isinstance(chunk, (bytearray, memoryview)):
        return bytes(chunk)
    msg = (
        f"upload_stream() got a chunk of type {type(chunk).__name__}; the "
        f"stream must yield bytes"
    )
    _refuse_argument(TypeError(msg), msg)
    raise AssertionError("unreachable")  # pragma: no cover


def _check_overwrite(overwrite: Any) -> None:
    """Refuse an upload ``overwrite`` that is not a bool (non-retryable
    TypeError), before anything is written: the storage client reads it by
    truthiness, so ``overwrite="false"`` (a flag read from the environment
    or a request field) replaced an existing blob."""
    if type(overwrite) is bool:
        return
    msg = f"overwrite must be True or False, got {type(overwrite).__name__}"
    _refuse_argument(TypeError(msg), msg)


def _encode_text(text: str) -> bytes:
    """UTF-8 bytes of ``text`` for ``upload_text``. Text that UTF-8 cannot
    encode (a lone surrogate such as ``"\\ud800"``, which a JSON request
    can carry) is refused with a non-retryable ValueError naming the
    position, instead of an uncaught UnicodeEncodeError that is retried."""
    try:
        return text.encode("utf-8")
    except UnicodeEncodeError as e:
        code = ord(text[e.start]) if e.start < len(text) else 0
        msg = (
            f"upload_text() got text that UTF-8 cannot encode (a lone "
            f"surrogate U+{code:04X} at position {e.start})"
        )
        _refuse_argument(ValueError(msg), msg)
        raise AssertionError("unreachable")  # pragma: no cover  # noqa: B904


def _replace_non_finite(value: Any) -> Any:
    """``value`` with every NaN / Infinity float replaced by None."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _replace_non_finite(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_replace_non_finite(v) for v in value]
    return value


def _json_dumps_for_blob(obj: Any) -> str:
    """Standard JSON (RFC 8259) for ``upload_json``: NaN and Infinity,
    which have no JSON form, are written as ``null``, as in replies.
    Text with a lone surrogate (which UTF-8 cannot encode) is refused with
    a ValueError naming where it is, as in replies. Other ``json.dumps``
    errors (an object with no JSON form, a circular reference) propagate
    unchanged."""
    try:
        text = json.dumps(obj, allow_nan=False)
    except ValueError as e:
        if "Out of range float values" not in str(e):
            raise
        text = json.dumps(_replace_non_finite(obj), allow_nan=False)
    # json.dumps writes every non-ASCII character as an escape, so a lone
    # surrogate can only be present when the text holds "\ud".
    if "\\ud" in text:
        problem = _find_lone_surrogate(obj, "obj")
        if problem is not None:
            raise ValueError(f"upload_json() refused: {problem}")
    return text


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
            ValueError: If ``blob_ref`` is not a valid azblob:// URI
            TypeError: If ``blob_ref`` is neither a string nor a BlobRef
                (for example ``None`` from a missing request field)
            Exception: Storage errors from the underlying client, e.g. a
                missing blob. Uncaught in a handler, a missing blob, an
                invalid URI or an invalid ``blob_ref`` is replied as
                Permanent (not retried); other storage errors are retried.

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
            ValueError: If ``blob_ref`` is not a valid azblob:// URI, or the
                content is not valid JSON (``json.JSONDecodeError`` is a
                ValueError), or it holds an integer with more digits than
                Python converts (4300 by default).
            RecursionError: If the content is nested more levels deep than
                Python's JSON parser supports. Uncaught in a handler, any
                of these content errors is replied as Permanent (not
                retried): the same content fails the same way every time.
            Exception: Storage errors from the underlying client, e.g. a
                missing blob.

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
                if False (default) and the blob already exists, the upload
                fails with the storage service's "blob already exists"
                error (error code ``BlobAlreadyExists``; the exception
                class is named in the API reference). Set ``overwrite=True`` when keying
                output paths on ``ctx.idempotency_key`` so DLQ replays
                of the same logical request rewrite the same blob
                instead of being rejected as duplicates. It must be
                ``True`` or ``False``: any other value (``"false"``, ``1``,
                ``None``) is refused before anything is written and,
                uncaught, is replied as Permanent (not retried).

        Returns:
            BlobRef with URI including versionId

        Raises:
            TypeError: If ``data`` is not bytes-like (``bytes``,
                ``bytearray``, ``memoryview``; a ``str`` belongs in
                :meth:`upload_text`) or ``container`` is not a string.
            ValueError: If ``container`` is not a valid container name
                (3 to 63 characters: lowercase letters, digits and single
                hyphens) or ``path`` is not a valid blob path. Both are
                raised before anything is written and, uncaught, are
                replied as Permanent (not retried).
            Exception: When ``overwrite=False`` and ``path`` already exists
                (the storage service's ``BlobAlreadyExists`` error), and for
                other storage errors (access denied).

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

        NaN and Infinity, which have no JSON form, are written as ``null``
        (as in replies), so the blob is standard JSON (RFC 8259).

        Raises:
            TypeError: If ``obj`` is not JSON-serializable (ValueError for
                a circular reference), or ``container`` is not a string.
            ValueError: If ``container`` is not a valid container name
                (3 to 63 characters: lowercase letters, digits and single
                hyphens) or ``path`` is not a valid blob path.
                These errors are raised before anything is written and,
                uncaught, are replied as Permanent (not retried).
            Exception: When ``overwrite=False`` and ``path`` already exists
                (the storage service's ``BlobAlreadyExists`` error), and for
                other storage errors (access denied).
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
            TypeError: If ``text`` is not a string, or ``container`` is
                not a string.
            ValueError: If ``text`` holds a lone surrogate (e.g.
                ``"\\ud800"``), which UTF-8 cannot encode, ``container`` is
                not a valid container name (3 to 63 characters: lowercase
                letters, digits and single hyphens) or ``path`` is not a
                valid blob path. These errors are raised before anything is
                written and, uncaught, are replied as Permanent (not
                retried).
            Exception: When ``overwrite=False`` and ``path`` already exists
                (the storage service's ``BlobAlreadyExists`` error), and for
                other storage errors (access denied).
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
            ValueError: If ``blob_ref`` is not a valid azblob:// URI
            UnicodeDecodeError: If the content is not valid UTF-8
            Exception: Storage errors from the underlying client, e.g. a
                missing blob.

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
            stream: Async iterator yielding byte chunks (an async generator
                or any object with ``__aiter__``; a plain iterator or a
                list is refused with TypeError, replied as Permanent)
            path: Optional blob path (auto-generated if not provided)
            progress_callback: Optional callback(bytes_uploaded) for progress reporting

        Returns:
            BlobRef with URI including versionId

        Raises:
            TypeError: If ``stream`` is not an async iterable, or
                ``container`` is not a string (raised before anything is
                written), or the stream yields a chunk that is not
                ``bytes``, ``bytearray`` or ``memoryview`` (encode text
                first: a ``str`` chunk is refused, not stored encoded).
            ValueError: If ``container`` is not a valid container name
                (3 to 63 characters: lowercase letters, digits and single
                hyphens) or ``path`` is not a valid blob path (raised
                before anything is written).
                These errors are replied as Permanent if uncaught (not
                retried).
            Exception: When ``overwrite=False`` and ``path`` already exists
                (the storage service's ``BlobAlreadyExists`` error), and for
                other storage errors (access denied). An exception raised
                by ``stream`` itself (its own read failed) is raised
                unchanged: it is the handler's own error, not a storage
                failure.

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

        Transfers the blob without loading it into memory: chunks of at
        most ``chunk_size`` bytes are yielded as they arrive, so memory
        use is bounded by the chunk size regardless of blob size.

        Args:
            blob_ref: Reference to the blob to download (BlobRef or azblob:// URI string)
            chunk_size: Size of each yielded chunk in bytes
            progress_callback: Optional callback(bytes_downloaded, total_bytes)

        Returns:
            Async iterator yielding byte chunks

        Raises:
            ValueError: If ``blob_ref`` is not a valid azblob:// URI
            Exception: Storage errors from the underlying client (for
                example a missing blob)

        Example:
            >>> async with aiofiles.open("output.bin", "wb") as f:
            ...     async for chunk in ctx.blob.download_stream(
            ...         blob_ref,
            ...         progress_callback=lambda d, t: print(f"{d}/{t} bytes")
            ...     ):
            ...         await f.write(chunk)
        """
