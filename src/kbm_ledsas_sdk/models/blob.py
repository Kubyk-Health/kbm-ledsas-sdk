"""
BlobRef model for Azure Blob Storage URI references.

Handles parsing and validation of azblob:// URIs with optional version IDs.
"""

import unicodedata
from urllib.parse import parse_qs, urlparse

from pydantic import BaseModel, Field, field_validator


class BlobRef(BaseModel):
    """
    Reference to an Azure Blob Storage object.

    The URI format is: azblob://<container>/<path>[?versionId=<version>]

    Examples:
        - azblob://dev/input/dataset-123.json
        - azblob://dev/input/dataset-123.json?versionId=abc123
        - azblob://results/output/report.json?versionId=def456

    Properties:
        container: The Azure Blob Storage container name
        path: The blob path within the container
        version_id: Optional version ID for immutable blob versioning
    """

    uri: str = Field(
        ...,
        description="Azure Blob Storage URI in azblob://<container>/<path> format",
        min_length=1,
    )

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
            raise ValueError(f"URI must start with azblob://, got: {v!r}")

        # Azure caps blob names at 1024 chars; 2048 bounds the whole URI
        # (scheme + container + path + optional versionId query) so
        # downstream code never handles an unbounded value.
        if len(v) > 2048:
            raise ValueError(f"URI exceeds the 2048-character bound (got {len(v)} chars)")

        for c in v:
            o = ord(c)
            if o < 0x20 or o == 0x7F:
                raise ValueError(
                    f"URI contains control character U+{o:04X}; "
                    f"reject at the boundary (got: {v!r})"
                )
            # Unicode "format" characters (zero-width, bidi overrides
            # like U+202E) survive urlparse and can spoof what a human
            # reviewer or customer filesystem code sees — same boundary
            # rejection as ASCII control bytes.
            if unicodedata.category(c) == "Cf":
                raise ValueError(
                    f"URI contains Unicode format character U+{o:04X}; " f"reject at the boundary"
                )

        parsed = urlparse(v)
        if not parsed.netloc:
            raise ValueError(f"URI missing container name: {v}")
        if not parsed.path or parsed.path == "/":
            raise ValueError(f"URI missing blob path: {v}")

        # A '..' segment is just a literal key in Azure's flat namespace,
        # but customer code routinely maps blob paths onto local paths
        # (the docs themselves show csv_uri.split("/") patterns) — reject
        # the traversal shape before it can reach a filesystem.
        if ".." in parsed.path.split("/"):
            raise ValueError(
                f"URI path contains a '..' segment; " f"reject at the boundary (got: {v!r})"
            )

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

        query_params = parse_qs(parsed.query)
        version_id_list = query_params.get("versionId", [])
        return version_id_list[0] if version_id_list else None

    def __str__(self) -> str:
        """String representation returns the URI."""
        return self.uri

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"BlobRef(uri='{self.uri}')"
