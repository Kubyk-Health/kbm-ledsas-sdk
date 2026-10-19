"""download_json / upload_json keep to a dict or a list; message models check their envelope type; error attributes and messages."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pydantic
import pytest

from kbm_ledsas_sdk import errors
from kbm_ledsas_sdk.app import ServiceApp
from kbm_ledsas_sdk.blob.operations import (
    NON_RETRYABLE_MARKER,
)
from kbm_ledsas_sdk.models.blob import BlobRef
from kbm_ledsas_sdk.models.envelope import Envelope
from kbm_ledsas_sdk.models.messages import Command, Response, Status
from kbm_ledsas_sdk.transport.mock import MockBlobOperations, MockTransport


@contextmanager
def capture(logger_name: str = "kbm_ledsas_sdk", level: int = logging.DEBUG) -> Iterator[list]:
    """The records reaching ``logger_name`` (a handler attached to it, so
    the capture does not depend on propagation)."""
    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    target = logging.getLogger(logger_name)
    handler = _Capture(level=level)
    previous = target.level
    target.addHandler(handler)
    target.setLevel(level)
    try:
        yield records
    finally:
        target.removeHandler(handler)
        target.setLevel(previous)


def _envelope(**over) -> Envelope:
    fields = dict(
        schema_version="1.0",
        type="command",
        name="Work",
        message_version="1.0",
        message_id="11111111-2222-4333-8444-555555555555",
        correlation_id="66666666-7777-4888-8999-aaaaaaaaaaaa",
        idempotency_key="idem-1",
        sent_at=datetime.now(UTC),
        trace_id="trace-1",
        reply_to="resp.dev.example_caller.v1",
    )
    fields.update(over)
    return Envelope(**fields)


def _app(transport: MockTransport, **config) -> ServiceApp:
    app = ServiceApp("svc-1")
    app.transport = transport
    app.config = SimpleNamespace(
        handler_timeout=0, max_retries=3, generic_errors=False, concurrency=1, **config
    )
    return app


class TestModelsAndErrors:
    """The message models check their envelope's type; every SDKError has
    user_message; a bare DeadlineExceeded has a message."""

    def test_sdk_error_carries_user_message(self) -> None:
        assert errors.SDKError("x").user_message is None
        assert errors.SDKError("x", user_message="y").user_message == "y"
        assert (
            errors.Retryable("a", "b").user_message == "b"
            and errors.Permanent("a").user_message is None
        )
        with pytest.raises(TypeError):
            errors.SDKError("x", user_message=3)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "cls, good", [(Command, "command"), (Response, "response"), (Status, "status")]
    )
    def test_the_envelope_type_must_match(self, cls, good) -> None:
        for kind in ("command", "response", "status"):
            envelope = _envelope().model_copy(update={"type": kind})
            if kind == good:
                cls(envelope=envelope, payload={})
            else:
                with pytest.raises(pydantic.ValidationError) as info:
                    cls(envelope=envelope, payload={})
                assert f"envelope.type is {kind!r}, not {good!r}" in str(info.value)

    async def test_a_bare_deadline_exceeded_has_a_message(self) -> None:
        transport = MockTransport()
        app = _app(transport)

        @app.handler("Work")
        async def work(ctx, payload):
            raise errors.DeadlineExceeded()

        await app._handle_command(Command(envelope=_envelope(), payload={}), asyncio.Semaphore(1))
        [reply] = transport.sent_responses
        assert reply.payload["error"] == {
            "code": "DeadlineExceeded",
            "message": "Processing deadline exceeded",
            "retryable": False,
        }


SCALARS = [("5", "int"), ('"x"', "str"), ("true", "bool"), ("null", "null"), ("1.5", "float")]


def _direct_ops(content: bytes = b"{}"):
    from kbm_ledsas_sdk.blob.direct_operations import DirectBlobOperations

    client = SimpleNamespace(download_blob=AsyncMock(return_value=content), upload_blob=AsyncMock())
    return DirectBlobOperations(client, "dev"), client  # type: ignore[arg-type]


def _mock_ops(content: bytes = b"{}"):
    ops = MockBlobOperations()
    ops.blobs["azblob://dev/x.json"] = content
    return ops


class TestJsonRoot:
    """download_json returns a dict or a list (a scalar root is a
    non-retryable ValueError); upload_json takes only a dict or a list."""

    @pytest.mark.parametrize("content, kind", SCALARS)
    @pytest.mark.parametrize("impl", ["direct", "mock"])
    async def test_a_scalar_root_is_a_non_retryable_value_error(self, impl, content, kind) -> None:
        ops = _direct_ops(content.encode())[0] if impl == "direct" else _mock_ops(content.encode())
        with capture() as records, pytest.raises(ValueError) as info:
            await ops.download_json(BlobRef(uri="azblob://dev/x.json"))
        assert f"holds JSON whose root is {kind}" in str(info.value)
        assert getattr(info.value, NON_RETRYABLE_MARKER) == (
            f"blob content is JSON but its root is {kind}, not an object or an array"
        )
        assert [r.getMessage() for r in records if r.levelno >= logging.ERROR] == [
            f"Blob content is JSON but its root is {kind}, not an object or an array"
        ]

    @pytest.mark.parametrize("content", [b'{"a": 1}', b"[1, 2]"])
    @pytest.mark.parametrize("impl", ["direct", "mock"])
    async def test_an_object_or_array_is_returned(self, impl, content) -> None:
        ops = _direct_ops(content)[0] if impl == "direct" else _mock_ops(content)
        assert await ops.download_json(BlobRef(uri="azblob://dev/x.json")) == json.loads(content)

    @pytest.mark.parametrize("obj", [5, "text", True, None, 1.5, (1, 2)])
    async def test_upload_refuses_a_scalar_before_any_io(self, obj) -> None:
        ops, client = _direct_ops()
        with pytest.raises(TypeError) as info:
            await ops.upload_json("dev", obj)  # type: ignore[arg-type]
        assert str(info.value).startswith("upload_json() takes a dict or a list")
        assert getattr(info.value, NON_RETRYABLE_MARKER)
        client.upload_blob.assert_not_awaited()

    async def test_uncaught_it_is_one_permanent_reply(self) -> None:
        transport = MockTransport()
        transport.blob_ops.blobs["azblob://dev/x.json"] = b"42"
        app = _app(transport)

        @app.handler("Work")
        async def work(ctx, payload):
            return await ctx.blob.download_json(BlobRef(uri="azblob://dev/x.json"))

        await app._handle_command(Command(envelope=_envelope(), payload={}), asyncio.Semaphore(1))
        [reply] = transport.sent_responses
        assert reply.payload["error"]["code"] == "Permanent"
        assert "its root is int" in reply.payload["error"]["message"]
