"""
BlobRef model for Azure Blob Storage URI references.

Handles parsing and validation of azblob:// URIs with optional version IDs.
"""

import re
import unicodedata
from typing import Any
from urllib.parse import parse_qsl, urlparse

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

# Azure container names: 3 to 63 characters, lowercase letters, digits and
# single hyphens, starting and ending with a letter or digit, plus the
# service's special containers ($root, $logs, $web). The storage service
# refuses any other name, so it is refused here, before any I/O, with the
# rule in the message.
_CONTAINER_NAME = re.compile(r"^(?:\$root|\$logs|\$web|[a-z0-9][a-z0-9-]{1,61}[a-z0-9])$")

CONTAINER_NAME_RULE = (
    "a container name is 3 to 63 characters: lowercase letters, digits and "
    "single hyphens, starting and ending with a letter or digit"
)


# The kind refused_character() reports for a lone surrogate.
LONE_SURROGATE = "lone surrogate"

# Characters refused in a blob URI and in a blob path, by Unicode category:
# control characters (category Cc: the ASCII control characters, DEL and
# U+0080 to U+009F, where U+009B is a one-byte terminal escape introducer
# and U+0085 a line break), format characters (category Cf: zero-width
# characters, bidirectional overrides such as U+202E), the line and
# paragraph separators (categories Zl and Zp: U+2028, U+2029) and lone
# surrogates (category Cs: "\ud800", which a JSON request can carry; UTF-8
# cannot encode it, so storage could not hold the name). That covers every
# character the SDK's log formatters escape.
_REFUSED_CATEGORIES = {
    "Cc": "control character",
    "Cf": "Unicode format character (an invisible character)",
    "Zl": "line separator",
    "Zp": "paragraph separator",
    "Cs": LONE_SURROGATE,
}


def refused_character(text: str) -> tuple | None:
    """The first character of ``text`` a blob URI or blob path may not
    hold, as ``(code point, kind)`` (kind: ``"control character"``, ...),
    or None."""
    for ch in text:
        kind = _REFUSED_CATEGORIES.get(unicodedata.category(ch))
        if kind is not None:
            return ord(ch), kind
    return None


def _lone_surrogate_in_uri(code_point: int) -> str:
    return (
        f"URI contains a lone surrogate U+{code_point:04X}, which UTF-8 "
        f"cannot encode, so it cannot name a blob"
    )


def container_name_problem(name: Any) -> str | None:
    """Why ``name`` is not a valid blob container name, or None."""
    if not isinstance(name, str):
        return f"must be a string, got {type(name).__name__}"
    if not _CONTAINER_NAME.fullmatch(name) or "--" in name:
        return f"{name[:70]!r} is not valid: {CONTAINER_NAME_RULE}"
    return None


# Longest blob name the storage service accepts (characters).
BLOB_NAME_MAX = 1024


def blob_name_problem(name: str) -> str | None:
    """Why ``name`` (a blob name inside its container, without the ``/``
    that follows the container in a URI) is not a name the SDK writes or
    reads, or None. The same rule applies to upload paths and to the
    names of ``BlobRef`` URIs, so a name the SDK accepts is the name
    storage reads or writes.

    Refused: an empty name, a backslash (storage may take ``a\\b`` as
    ``a/b``: an upload would store another name than the one returned, and
    a read would read the blob ``a/b`` while ``.path`` says ``a\\b``), a
    name ending in ``/`` (``dir/``), an empty segment (``a//b``; in a URI
    also ``azblob://c//x``, which the storage client would read as ``x``),
    and a ``.`` or ``..`` segment.
    """
    if not name.strip("/"):
        return "is empty"
    if "\\" in name:
        return (
            "contains a backslash ('\\'), which storage may take as '/'; "
            "separate path segments with '/'"
        )
    if name.endswith("/"):
        return "ends with '/'"
    if "" in name.split("/"):
        return "contains an empty segment ('//')"
    if any(seg in (".", "..") for seg in re.split(r"[/\\]", name)):
        return "contains a '.' or '..' segment"
    return None


# The only query parameter a blob URI may carry. Matched case-insensitively:
# the storage REST API spells it ``versionid``; the SDK writes ``versionId``.
_VERSION_ID_KEY = "versionid"

# Characters a version id may hold once percent-decoded. Azure blob version
# ids are timestamps such as ``2026-07-21T10:00:00.1234567Z``; the rule also
# admits simple tokens (``abc123``). Separators (``;``, ``&``, ``=``),
# whitespace, ``%`` and control characters are refused, so a value such as
# ``1;sig=...`` or ``%0a`` cannot ride along in ``.uri`` / ``.version_id``.
_VERSION_ID_VALUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:._-]{0,127}$")

# Longest URI text an error message quotes (after redaction).
_SHOWN_URI_MAX = 160


def _redacted_uri_text(value: str) -> str:
    """``value`` as received (its scheme's letter case, leading whitespace
    and slashes kept), with the parts that may hold a secret replaced.

    Everything from the first ``?`` or ``#`` is replaced by
    ``?<redacted>`` / ``#<redacted>``, and everything between the start
    of the authority (after ``//``, else the start) and the last ``@``
    before them by ``<redacted>`` (user info; a password may itself hold
    a ``/``, so the last ``@`` is used, not the end of the authority).
    """
    cuts = [i for i in (value.find("?"), value.find("#")) if i != -1]
    head = value[: min(cuts)] if cuts else value
    suffix = ("?<redacted>" if "?" in value else "") + ("#<redacted>" if "#" in value else "")
    at = head.rfind("@")
    if at != -1:
        start = head.find("//")
        start = start + 2 if start != -1 and start < at else 0
        head = head[:start] + "<redacted>" + head[at:]
    return head + suffix


def _shown_uri(value: str) -> str:
    """The text a validation error may quote for a rejected URI.

    An invalid URI is often something else pasted by mistake: an https
    URL with a SAS token (``?...&sig=...``), or a URI with credentials in
    it. Error messages are logged and can reach the caller in the error
    reply, so the query and fragment are replaced by ``?<redacted>`` /
    ``#<redacted>`` and user info by ``<redacted>@``. The rest is quoted
    as received (not re-serialised: an upper-case scheme or a leading
    space stays visible), control characters are escaped, and the text is
    shortened.
    """
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"
    shown = _redacted_uri_text(value)
    if len(shown) > _SHOWN_URI_MAX:
        shown = shown[: _SHOWN_URI_MAX - 3] + "..."
    return repr(shown)


def _scheme_problem(value: str) -> str:
    """The message for a URI that does not start with ``azblob://``,
    naming what is wrong with its start."""
    shown = _shown_uri(value)
    stripped = value.lstrip()
    if stripped != value and stripped[:9].lower() == "azblob://":
        return (
            f"URI must start with azblob:// (this one starts with "
            f"whitespace; remove it), got: {shown}"
        )
    if value[:9].lower() == "azblob://":
        return (
            f"URI must start with azblob:// in lower case (the scheme is "
            f"case-sensitive here), got: {shown}"
        )
    if value[:7].lower() == "azblob:":
        return f"URI must start with azblob:// (two slashes after 'azblob:'), " f"got: {shown}"
    return f"URI must start with azblob://, got: {shown}"


# Placeholder for the rejected value in a BlobRef ValidationError's
# errors() / json(): see BlobRef._input_not_kept.
_INPUT_NOT_KEPT = "<not kept: a rejected blob URI may hold a secret>"


def _error_without_input(exc: ValidationError) -> ValidationError:
    """``exc`` with every error's ``input`` replaced by a placeholder.

    pydantic keeps the rejected value in ``errors()[i]["input"]`` and in
    ``json()`` even with ``hide_input_in_errors`` (which only governs
    ``str()`` / ``repr()``). Error reporters serialise those, and a
    rejected blob URI is often a SAS URL or a URI with a password.
    """
    details: list[Any] = []
    for err in exc.errors(include_url=False):
        detail: dict[str, Any] = {"type": err["type"], "loc": err["loc"], "input": _INPUT_NOT_KEPT}
        if "ctx" in err:
            detail["ctx"] = err["ctx"]
        details.append(detail)
    try:
        return ValidationError.from_exception_data(exc.title, details, hide_input=True)
    except Exception:  # an error type that cannot be rebuilt: keep the text only
        return ValidationError.from_exception_data(
            exc.title,
            [
                {
                    "type": "value_error",
                    "loc": err["loc"],
                    "input": _INPUT_NOT_KEPT,
                    "ctx": {
                        "error": ValueError(str(err.get("msg", "")).removeprefix("Value error, "))
                    },
                }
                for err in exc.errors(include_url=False)
            ],
            hide_input=True,
        )


class BlobRef(BaseModel):
    """
    Reference to an Azure Blob Storage object.

    The URI format is: azblob://<container>/<path>[?versionId=<version>]

    ``<container>`` is a bare Azure container name (lowercase letters,
    digits, single hyphens). The only query parameter accepted is
    ``versionId`` (any letter case), whose value must look like a blob
    version id (letters, digits, ``:``, ``.``, ``-``, ``_``; at most 128
    characters, checked after percent-decoding). Anything else (user info, a port, other
    query parameters, a ``#`` fragment) is rejected with a ValueError
    (pydantic ValidationError) instead of being silently ignored.

    ``uri`` is the only field. Any other keyword argument is rejected
    (``BlobRef(uri=..., version_id=...)`` would otherwise drop the
    version silently); to pin a version, put it in the URI. Validation
    errors never quote the query string, a fragment or user info of the
    rejected value (they may hold a SAS signature or a password), and
    the error text does not include the input value.

    A BlobRef is immutable: assigning ``ref.uri = ...`` raises a
    ValidationError (create a new one with ``BlobRef.from_uri``).
    ``model_copy(update=...)``, ``copy.replace()``, ``model_construct()``
    and ``model_validate()`` of an existing instance check the URI as
    ``from_uri`` does, and the ``ctx.blob`` calls check a BlobRef again
    before they use or log it, so a BlobRef the SDK reads always holds a
    URI that passed these checks.

    The blob name (the part after ``azblob://<container>/``) must not be
    empty, end with ``/``, contain a backslash, an empty segment (``//``)
    or a ``.`` / ``..`` segment (the same rule as upload paths).
    ``errors()`` / ``json()`` of the ValidationError do not keep the
    rejected value either (their ``input`` is a placeholder).

    The blob name is taken literally: it is not percent-decoded
    (``azblob://data/a b.json`` names the blob ``a b.json``, and
    ``azblob://data/a%20b.json`` the blob ``a%20b.json``). Only the
    ``versionId`` value is percent-decoded. Pass a blob URI on as a plain
    string: a URI library that encodes or normalizes it changes a name
    with a space or a non-ASCII letter into another blob's name.

    Examples:
        - azblob://dev/input/dataset-123.json
        - azblob://dev/input/dataset-123.json?versionId=abc123
        - azblob://results/output/report.json?versionId=def456

    Properties:
        container: The Azure Blob Storage container name
        path: The blob path within the container
        version_id: Optional version ID for immutable blob versioning
    """

    # hide_input_in_errors: str(ValidationError) must not repeat the
    # rejected value (it may be a URL with a SAS signature or a password).
    # frozen: an assignment would skip the URI checks (a SAS signature
    # assigned to .uri would then be logged by the blob calls).
    # revalidate_instances: a BlobRef given where a BlobRef is validated
    # (model_validate, a BlobRef field of another model) is checked again,
    # not passed through as it is.
    model_config = ConfigDict(
        extra="forbid",
        hide_input_in_errors=True,
        frozen=True,
        revalidate_instances="always",
    )

    uri: str = Field(
        ...,
        description="Azure Blob Storage URI in azblob://<container>/<path> format",
        min_length=1,
    )

    @model_validator(mode="before")
    @classmethod
    def _only_uri(cls, data: Any) -> Any:
        """Refuse keyword arguments other than ``uri`` with a clear message."""
        if isinstance(data, dict):
            others = sorted(str(k) for k in data if k != "uri")
            if others:
                raise ValueError(
                    f"BlobRef takes only 'uri'; got {', '.join(others)}. "
                    f"To pin a blob version, put it in the URI: "
                    f"azblob://<container>/<path>?versionId=<version>"
                )
            uri = data.get("uri")
            if isinstance(uri, str):
                # pydantic's own string check refuses a lone surrogate
                # before validate_uri_format runs, with a message that
                # does not say what is wrong ("unable to parse raw data as
                # a unicode string"): refused here, by the same rule.
                # Looked for on its own (not only as the first refused
                # character), so a control character before it does not
                # bring the opaque message back.
                surrogate = next((ord(c) for c in uri if 0xD800 <= ord(c) <= 0xDFFF), None)
                if surrogate is not None:
                    raise ValueError(_lone_surrogate_in_uri(surrogate))
        return data

    @model_validator(mode="wrap")
    @classmethod
    def _input_not_kept(cls, data: Any, handler: Any) -> Any:
        """A rejected value is not kept in the ValidationError.

        ``hide_input_in_errors`` keeps it out of ``str(error)`` only;
        ``error.errors()`` and ``error.json()`` (what error reporters
        serialise) would still hold it. Defined after ``_only_uri`` so it
        wraps every check, that one included.
        """
        try:
            return handler(data)
        except ValidationError as exc:
            raise _error_without_input(exc) from None

    def __setattr__(self, name: str, value: Any) -> None:
        # The frozen-instance error of an assignment would keep the
        # assigned value too.
        try:
            super().__setattr__(name, value)
        except ValidationError as exc:
            raise _error_without_input(exc) from None

    # pydantic's copy and construct helpers set field values without
    # running the validators: model_copy(update={"uri": ...}) (and
    # copy.replace(), which calls it), model_construct() and the
    # deprecated copy() / construct() would otherwise give a BlobRef whose
    # URI was never checked (a SAS signature or a password in .uri, which
    # the blob calls log). Each builds the BlobRef as from_uri does.

    def model_copy(self, *, update: Any | None = None, deep: bool = False) -> "BlobRef":
        """A copy of this BlobRef; ``update`` (a mapping, only ``uri``)
        gives a new one, checked as :meth:`from_uri` checks a URI."""
        data: dict[Any, Any] = {"uri": self.__dict__.get("uri")}
        if update:
            data.update(dict(update))
        return type(self).model_validate(data)

    def copy(
        self,
        *,
        include: Any = None,
        exclude: Any = None,
        update: Any | None = None,
        deep: bool = False,
    ) -> "BlobRef":
        """Deprecated pydantic alias of :meth:`model_copy` (``include`` and
        ``exclude`` cannot drop the only field)."""
        if include is not None or exclude is not None:
            raise TypeError("BlobRef.copy() takes no include / exclude (uri is its only field)")
        return self.model_copy(update=update, deep=deep)

    @classmethod
    def model_construct(cls, _fields_set: set | None = None, **values: Any) -> "BlobRef":
        """Build a BlobRef from ``uri=...``, checked as :meth:`from_uri`
        checks it (pydantic's construct would skip the checks)."""
        return cls.model_validate(values)

    @field_validator("uri")
    @classmethod
    def validate_uri_format(cls, v: str) -> str:
        """Validate that URI follows azblob:// format and contains no control characters.

        Control-character rejection closes a silent-strip inconsistency:
        Python 3.11+ ``urlparse`` strips newlines, tabs, and CR from the
        netloc during parsing (per CVE-2023-24329) so the ``.container``
        and ``.path`` properties return sanitized values, but the stored
        ``.uri`` attribute holds the raw input verbatim. Customer code
        that interpolates ``.uri`` into a text-format log message could
        otherwise see the control bytes survive to the terminal. Reject
        at the boundary instead — matches the strictness already applied
        to envelope ``trace_id`` / ``idempotency_key`` / ``reply_to``.
        """
        if not v.startswith("azblob://"):
            raise ValueError(_scheme_problem(v))

        # Azure caps blob names at 1024 chars; 2048 bounds the whole URI
        # (scheme + container + path + optional versionId query) so
        # downstream code never handles an unbounded value.
        if len(v) > 2048:
            raise ValueError(f"URI exceeds the 2048-character bound (got {len(v)} chars)")

        # Control characters (ASCII, DEL, U+0080 to U+009F), Unicode
        # "format" characters (zero-width, bidi overrides like U+202E: they
        # survive urlparse and can spoof what a human reviewer or customer
        # filesystem code sees), the line / paragraph separators and lone
        # surrogates are refused, by the one definition upload paths use
        # too (refused_character).
        refused = refused_character(v)
        if refused is not None:
            o, kind = refused
            if kind == "control character":
                raise ValueError(
                    f"URI contains control character U+{o:04X}, which is "
                    f"not allowed in a blob URI (got: {_shown_uri(v)})"
                )
            if kind == _REFUSED_CATEGORIES["Cf"]:
                raise ValueError(
                    f"URI contains Unicode format character U+{o:04X} (an "
                    f"invisible character), which is not allowed in a blob URI"
                )
            if kind == LONE_SURROGATE:
                raise ValueError(_lone_surrogate_in_uri(o))
            raise ValueError(
                f"URI contains a {kind} U+{o:04X}, which is not allowed in a " f"blob URI"
            )

        parsed = urlparse(v)
        if not parsed.netloc:
            raise ValueError(f"URI missing container name: {_shown_uri(v)}")
        if not parsed.path or parsed.path == "/":
            raise ValueError(f"URI missing blob path: {_shown_uri(v)}")

        # The part after azblob:// is a bare container name. User info
        # ("user:pw@container") or a port would otherwise be taken as part
        # of the container name and the credentials carried into logs.
        container = parsed.netloc
        if "@" in container or ":" in container:
            raise ValueError(
                "URI container must be a bare container name (no user " "info, credentials or port)"
            )
        if not _CONTAINER_NAME.fullmatch(container) or "--" in container:
            raise ValueError(f"Invalid container name {container!r}: {CONTAINER_NAME_RULE}")

        # A '#' starts a URI fragment: everything after it would silently
        # drop out of the blob path.
        if parsed.fragment or v.endswith("#"):
            raise ValueError(
                "URI contains '#': a fragment is not part of a blob URI "
                "(the text after '#' would be dropped from the blob path)"
            )

        # Only an optional versionId may follow '?'. Anything else (a SAS
        # 'sig', a 'snapshot', a typo) is rejected instead of being
        # silently ignored and kept in .uri, where it would be logged. A
        # bare trailing '?' (no query at all) is refused too.
        if "?" in v and not parsed.query:
            raise ValueError(
                "URI ends with '?' but carries no versionId: remove the '?', "
                "or use ?versionId=<version>"
            )
        if parsed.query:
            pairs = parse_qsl(parsed.query, keep_blank_values=True)
            unknown = sorted({k for k, _ in pairs if k.lower() != _VERSION_ID_KEY})
            if unknown or not pairs:
                raise ValueError(
                    "Unsupported query parameter(s) "
                    f"{', '.join(repr(k) for k in unknown) or repr(parsed.query)}"
                    ": a blob URI may only carry ?versionId=<version>"
                )
            versions = [val for k, val in pairs if k.lower() == _VERSION_ID_KEY]
            if len(versions) != 1 or not versions[0]:
                raise ValueError("versionId must appear once, with a non-empty value")
            # Checked after percent-decoding: the raw-URI control-character
            # check above cannot see an encoded '%0a'.
            if not _VERSION_ID_VALUE.fullmatch(versions[0]):
                raise ValueError(
                    "Invalid versionId value: a blob version id "
                    "is a timestamp such as 2026-07-21T10:00:00.1234567Z "
                    "(letters, digits, ':', '.', '-', '_'; at most 128 "
                    "characters)"
                )

        # A '..' segment is just a literal key in Azure's flat namespace,
        # but customer code routinely maps blob paths onto local paths
        # (the docs themselves show csv_uri.split("/") patterns) — reject
        # the traversal shape before it can reach a filesystem. A '.'
        # segment is refused too, as for upload paths: an HTTP client
        # removes it from the request after signing it, so storage then
        # refuses the request with a misleading authentication error.
        # A backslash counts as a separator for this check (some storage
        # implementations store 'a\..\b' as 'a/../b').
        segments = re.split(r"[/\\]", parsed.path)
        if ".." in segments:
            raise ValueError(
                f"URI path contains a '..' segment, which is not allowed "
                f"in a blob URI (got: {_shown_uri(v)})"
            )
        if "." in segments:
            raise ValueError(
                f"URI path contains a '.' segment, which a blob name cannot "
                f"carry (got: {_shown_uri(v)})"
            )

        # The blob name is the path after the '/' that follows the
        # container. An empty name ('azblob://c//') would reach the storage
        # client and fail there as an unexpected error, and an empty
        # segment ('azblob://c//x', 'a//b'), a trailing '/' or a backslash
        # ('a\\b', which storage may take as 'a/b') would name a different
        # blob than the one read or written: .path would not be the name
        # read, so a check on it (a prefix, say) would not hold.
        name_problem = blob_name_problem(parsed.path[1:])
        if name_problem is None and len(parsed.path[1:]) > BLOB_NAME_MAX:
            name_problem = (
                f"is longer than {BLOB_NAME_MAX} characters, the longest "
                f"blob name storage accepts"
            )
        if name_problem == "is empty":
            raise ValueError(f"URI missing blob path: {_shown_uri(v)}")
        if name_problem is not None:
            raise ValueError(f"URI blob name {name_problem} (got: {_shown_uri(v)})")

        return v

    @classmethod
    def from_uri(cls, uri: str) -> "BlobRef":
        """
        Create a BlobRef from a URI string.

        Args:
            uri: Azure Blob Storage URI (azblob://<container>/<path>[?versionId=...])

        Returns:
            BlobRef instance

        Raises:
            ValueError: If URI format is invalid

        Example:
            >>> ref = BlobRef.from_uri("azblob://dev/input.json?versionId=abc")
            >>> ref.container
            'dev'
            >>> ref.path
            '/input.json'
            >>> ref.version_id
            'abc'
        """
        return cls(uri=uri)

    @property
    def container(self) -> str:
        """
        Extract container name from URI.

        Returns:
            Container name (netloc part of the URI)

        Example:
            >>> ref = BlobRef.from_uri("azblob://dev/input.json")
            >>> ref.container
            'dev'
        """
        parsed = urlparse(self.uri)
        return parsed.netloc

    @property
    def path(self) -> str:
        """
        Extract blob path from URI (without query parameters).

        Returns:
            Blob path within container (includes leading /)

        Example:
            >>> ref = BlobRef.from_uri("azblob://dev/folder/file.json")
            >>> ref.path
            '/folder/file.json'
        """
        parsed = urlparse(self.uri)
        return parsed.path

    @property
    def version_id(self) -> str | None:
        """
        Extract version ID from URI query parameters.

        Returns:
            Version ID if present, None otherwise

        Example:
            >>> ref = BlobRef.from_uri("azblob://dev/file.json?versionId=abc123")
            >>> ref.version_id
            'abc123'
        """
        parsed = urlparse(self.uri)
        if not parsed.query:
            return None

        # Case-insensitive: ?versionid= (the storage REST spelling) and
        # ?versionId= select the same version.
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            if key.lower() == _VERSION_ID_KEY and value:
                return value
        return None

    def __str__(self) -> str:
        """String representation returns the URI."""
        return self.uri

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"BlobRef(uri='{self.uri}')"
