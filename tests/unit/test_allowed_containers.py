"""KBM_LEDSAS_ALLOWED_CONTAINERS: the setting, and its check on every blob operation."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from kbm_ledsas_sdk.app import ServiceApp
from kbm_ledsas_sdk.blob.operations import (
    AllowedContainersBlobOperations,
    BlobOperations,
    parse_allowed_containers,
)
from kbm_ledsas_sdk.models.blob import BlobRef
from kbm_ledsas_sdk.models.envelope import Envelope
from kbm_ledsas_sdk.models.errors import ContainerNotAllowed
from kbm_ledsas_sdk.models.messages import Command
from kbm_ledsas_sdk.runtime.config import SDKConfig
from kbm_ledsas_sdk.runtime.context import ExecutionContext
from kbm_ledsas_sdk.transport.mock import MockTransport


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


class _Recorder(BlobOperations):
    """Records each call; nothing is sent anywhere."""

    def __init__(self) -> None:
        self.calls: list = []

    async def download_bytes(self, blob_ref):
        self.calls.append(("download_bytes", blob_ref.uri))
        return b"x"

    async def download_json(self, blob_ref):
        self.calls.append(("download_json", blob_ref.uri))
        return {}

    async def download_text(self, blob_ref):
        self.calls.append(("download_text", blob_ref.uri))
        return "x"

    async def download_stream(self, blob_ref, chunk_size=4, progress_callback=None):
        self.calls.append(("download_stream", blob_ref.uri))
        yield b"x"

    async def upload_bytes(self, container, data, path=None, overwrite=False):
        self.calls.append(("upload_bytes", container))
        return BlobRef(uri=f"azblob://{container}/p")

    async def upload_json(self, container, obj, path=None, overwrite=False):
        self.calls.append(("upload_json", container))
        return BlobRef(uri=f"azblob://{container}/p")

    async def upload_text(self, container, text, path=None, overwrite=False):
        self.calls.append(("upload_text", container))
        return BlobRef(uri=f"azblob://{container}/p")

    async def upload_stream(
        self, container, stream, path=None, progress_callback=None, overwrite=False
    ):
        self.calls.append(("upload_stream", container))
        return BlobRef(uri=f"azblob://{container}/p")


async def _chunks():
    yield b"x"


async def _drain(iterator):
    return [chunk async for chunk in iterator]


def _calls(container: str, ref):
    return {
        "download_bytes": lambda ops: ops.download_bytes(ref),
        "download_json": lambda ops: ops.download_json(ref),
        "download_text": lambda ops: ops.download_text(ref),
        "download_stream": lambda ops: _drain(ops.download_stream(ref)),
        "upload_bytes": lambda ops: ops.upload_bytes(container, b"x"),
        "upload_json": lambda ops: ops.upload_json(container, {}),
        "upload_text": lambda ops: ops.upload_text(container, "x"),
        "upload_stream": lambda ops: ops.upload_stream(container, _chunks()),
    }


class TestAllowedContainersSetting:
    """The setting: parsed, refused when malformed, read from the environment."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            (None, None),
            ("", None),
            ("   ", None),
            ("input", {"input"}),
            (" input , results ", {"input", "results"}),
            ("input,input", {"input"}),
        ],
    )
    def test_parsed(self, raw, expected) -> None:
        result = parse_allowed_containers(raw)
        assert result == (frozenset(expected) if expected is not None else None)

    @pytest.mark.parametrize(
        "raw, text",
        [
            ("a,,b", "has an empty item"),
            ("input,", "has an empty item"),
            (",input", "has an empty item"),
            ("Input", "'Input' is not a valid container name"),
            ("ab", "'ab' is not a valid container name"),
            ("in_put", "'in_put' is not a valid container name"),
        ],
    )
    def test_a_malformed_value_stops_the_start(self, monkeypatch, raw, text) -> None:
        monkeypatch.setenv("KBM_LEDSAS_ALLOWED_CONTAINERS", raw)
        with pytest.raises(ValueError) as info:
            SDKConfig.from_env("svc")
        assert str(info.value).startswith("KBM_LEDSAS_ALLOWED_CONTAINERS")
        assert text in str(info.value)

    def test_read_from_the_environment(self, monkeypatch) -> None:
        monkeypatch.setenv("KBM_LEDSAS_ALLOWED_CONTAINERS", '"input, results"')
        assert SDKConfig.from_env("svc").allowed_containers == frozenset({"input", "results"})
        monkeypatch.delenv("KBM_LEDSAS_ALLOWED_CONTAINERS")
        assert SDKConfig.from_env("svc").allowed_containers is None


class TestAllowedContainersEnforced:
    """Every ctx.blob operation outside the list is refused before the
    storage call; an allowed one goes through."""

    @pytest.mark.parametrize("op", list(_calls("x", "y")))
    @pytest.mark.parametrize("as_uri", [True, False], ids=["uri-string", "blobref"])
    async def test_refused_before_the_storage_call(self, op, as_uri) -> None:
        inner = _Recorder()
        ops = AllowedContainersBlobOperations(inner, frozenset({"input", "results"}))
        uri = "azblob://secrets/k.bin"
        ref = uri if as_uri else BlobRef(uri=uri)
        with (
            capture("kbm_ledsas_sdk.blob.operations") as records,
            pytest.raises(ContainerNotAllowed) as info,
        ):
            await _calls("secrets", ref)[op](ops)
        assert inner.calls == []
        kind = "download" if op.startswith("download") else "upload"
        assert str(info.value) == (
            f"Blob {kind} refused: container 'secrets' is not in KBM_LEDSAS_ALLOWED_CONTAINERS "
            f"(allowed: input, results); nothing was sent to storage"
        )
        assert info.value.container == "secrets" and isinstance(info.value, PermissionError)
        [warning] = records
        assert warning.levelno == logging.WARNING and warning.container == "secrets"

    @pytest.mark.parametrize("op", list(_calls("x", "y")))
    async def test_an_allowed_container_goes_through(self, op) -> None:
        inner = _Recorder()
        ops = AllowedContainersBlobOperations(inner, frozenset({"input"}))
        await _calls("input", "azblob://input/k.bin")[op](ops)
        assert [name for name, _ in inner.calls] == [op]

    def test_ctx_blob_is_wrapped_only_when_set(self) -> None:
        transport = MockTransport()
        plain = ExecutionContext(transport=transport, envelope=_envelope(), payload={})
        assert plain.blob is transport.blob_ops
        limited = ExecutionContext(
            transport=transport,
            envelope=_envelope(),
            payload={},
            allowed_containers=frozenset({"input"}),
        )
        assert isinstance(limited.blob, AllowedContainersBlobOperations)

    async def test_end_to_end_one_permanent_reply(self) -> None:
        transport = MockTransport()
        app = _app(transport, allowed_containers=frozenset({"results"}))

        @app.handler("Work")
        async def work(ctx, payload):
            return {"ref": (await ctx.blob.upload_json("other", {"a": 1})).uri}

        await app._handle_command(Command(envelope=_envelope(), payload={}), asyncio.Semaphore(1))
        [reply] = transport.sent_responses
        assert reply.payload["error"] == {
            "code": "Permanent",
            "retryable": False,
            "message": "container 'other' is not allowed (KBM_LEDSAS_ALLOWED_CONTAINERS)",
        }
        assert transport.blob_ops.upload_count == 0
