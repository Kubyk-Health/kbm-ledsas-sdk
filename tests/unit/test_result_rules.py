"""What a handler returns: the reply rules for results (a read-only mapping included), the reply-size limit, a handler that ends the process, and a ContainerNotAllowed the handler raises."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from kbm_ledsas_sdk.app import ServiceApp
from kbm_ledsas_sdk.models.envelope import Envelope
from kbm_ledsas_sdk.models.messages import Command
from kbm_ledsas_sdk.transport.mock import MockTransport

DEFAULT_MAX_PAYLOAD_BYTES = 16 * 1024 * 1024


def _envelope() -> Envelope:
    return Envelope(
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


async def _replies(
    handler, max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES
) -> list[dict[str, Any]]:
    """The reply payloads (their JSON form) of one command run by ``handler``."""
    transport = MockTransport()
    app = ServiceApp("svc-1")
    app.transport = transport
    app.config = SimpleNamespace(
        handler_timeout=0,
        max_retries=3,
        generic_errors=False,
        concurrency=1,
        max_payload_bytes=max_payload_bytes,
    )
    app.registry.max_reply_bytes = max_payload_bytes
    app.handler("Work")(handler)
    await app._handle_command(Command(envelope=_envelope(), payload={}), asyncio.Semaphore(1))
    return [reply.model_dump(mode="json")["payload"] for reply in transport.sent_responses]


def _returning(value: Any):
    async def work(ctx, payload):
        return value

    return work


def _raising(error: BaseException):
    async def work(ctx, payload):
        raise error

    return work


def _permanent(replies: list[dict[str, Any]]) -> str:
    """The message of the one Permanent, not retryable, error reply."""
    [payload] = replies
    error = payload["error"]
    assert error["code"] == "Permanent" and error["retryable"] is False, error
    return error["message"]


class TestResultRules:
    """ "Return value": a dict (or None) with a JSON form is the reply; any
    other result is replied Permanent at once, naming what is wrong."""

    async def test_none_replies_an_empty_object(self) -> None:
        assert await _replies(_returning(None)) == [{}]

    async def test_a_dict_with_an_error_key_is_a_success(self) -> None:
        assert await _replies(_returning({"valid": False, "error": "field x missing"})) == [
            {"valid": False, "error": "field x missing"}
        ]

    async def test_keys_are_converted_as_json_dumps_does(self) -> None:
        replies = await _replies(_returning({1: "a", "n": {0: 1, None: 2, 1.5: 4}}))
        assert replies == [{"1": "a", "n": {"0": 1, "null": 2, "1.5": 4}}]

    @pytest.mark.parametrize(
        "value, text",
        [
            ([1, 2], "returned list; a handler must return a dict (or None)"),
            ("text", "returned str; a handler must return a dict (or None)"),
        ],
        ids=["list", "str"],
    )
    async def test_a_result_that_is_not_a_dict_is_permanent(self, value, text) -> None:
        assert text in _permanent(await _replies(_returning(value)))

    @pytest.mark.parametrize(
        "value, text",
        [
            ({"b": b"x"}, 'payload["b"] is bytes; a reply carries text, not binary data'),
            ({"n": 10**5000}, 'payload["n"] is an integer with more than 4300 digits'),
            ({"s": "\ud800"}, 'payload["s"] is text with a lone surrogate U+D800'),
            ({1: "a", "1": "b"}, 'payload: two keys become the same JSON key "1"'),
        ],
        ids=["bytes", "big-int", "lone-surrogate", "colliding-keys"],
    )
    async def test_a_dict_with_no_json_form_is_permanent_and_names_where(self, value, text) -> None:
        assert text in _permanent(await _replies(_returning(value)))


class TestReplySizeLimit:
    """A result whose reply is larger than KBM_LEDSAS_MAX_PAYLOAD_BYTES is
    replied Permanent; 0 turns the check off."""

    async def test_a_result_too_large_is_permanent(self) -> None:
        message = _permanent(await _replies(_returning({"x": "a" * 2000}), max_payload_bytes=1000))
        assert "returned a result too large to send as a reply" in message
        assert "the limit is 1000 bytes, KBM_LEDSAS_MAX_PAYLOAD_BYTES" in message

    async def test_zero_turns_the_check_off(self) -> None:
        assert await _replies(_returning({"x": "a" * 2000}), max_payload_bytes=0) == [
            {"x": "a" * 2000}
        ]


class TestProcessExitInHandler:
    """SystemExit / KeyboardInterrupt raised by a handler is replied
    Permanent; the service keeps running."""

    @pytest.mark.parametrize(
        "error, name",
        [
            (SystemExit(3), "SystemExit (3)"),
            (KeyboardInterrupt(), "KeyboardInterrupt (no message)"),
        ],
        ids=["SystemExit", "KeyboardInterrupt"],
    )
    async def test_replied_permanent(self, error, name) -> None:
        message = _permanent(await _replies(_raising(error)))
        assert message == f"the handler raised {name}; a handler must not exit the process"


class TestReadOnlyMappingResult:
    """A handler returns a dict: a read-only mapping (``MappingProxyType``)
    is not one, and is replied Permanent at once."""

    async def test_a_mapping_proxy_is_permanent(self) -> None:
        from types import MappingProxyType

        message = _permanent(await _replies(_returning(MappingProxyType({"a": 1}))))
        assert "returned mappingproxy; a handler must return a dict (or None)" in message


class TestHandlerRaisedContainerNotAllowed:
    """``errors.ContainerNotAllowed`` raised (or re-raised) by handler code
    is replied Permanent once and not retried, like the one ``ctx.blob``
    raises."""

    @pytest.mark.parametrize("wrapped", [False, True], ids=["raised", "re-raised"])
    async def test_replied_permanent_once(self, wrapped) -> None:
        from kbm_ledsas_sdk.errors import ContainerNotAllowed

        runs = []

        async def work(ctx, payload):
            runs.append(1)
            if not wrapped:
                raise ContainerNotAllowed("not mine", container="other-data")
            try:
                raise ContainerNotAllowed("refused", container="other-data")
            except ContainerNotAllowed as e:
                raise ContainerNotAllowed(f"while saving: {e}", e.container) from e

        message = _permanent(await _replies(work))
        assert "container 'other-data' is not allowed (KBM_LEDSAS_ALLOWED_CONTAINERS)" in message
        assert runs == [1]
