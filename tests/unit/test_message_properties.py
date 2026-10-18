"""Message properties that are not valid UTF-8: decoded, dead-lettered once, the next message handled."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from kbm_ledsas_sdk.models.envelope import Envelope
from kbm_ledsas_sdk.models.messages import Command


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
        reply_to="resp.dev.example_orchestrator.v1",
    )
    fields.update(over)
    return Envelope(**fields)


def _content_header_with(message_id: bytes) -> bytes:
    from pamqp import commands, frame, header

    props = commands.Basic.Properties(message_id="PLACEHOLDER", headers={"KEYX": 1})
    raw = frame.marshal(header.ContentHeader(0, 2, props), 1)
    assert len(message_id) == len(b"PLACEHOLDER")
    return raw.replace(b"\x0bPLACEHOLDER", b"\x0b" + message_id)


class TestUndecodableMessageProperty:
    @pytest.fixture(autouse=True)
    def _tolerant_decoders(self):
        from kbm_ledsas_sdk.utils import amqp_text

        assert amqp_text.install()

    def test_the_frame_decodes(self):
        from pamqp import frame

        _, _, decoded = frame.unmarshal(_content_header_with(b"\xff\xfe-bad-badd"))
        assert decoded.properties.message_id == "\udcff\udcfe-bad-badd"

    def test_a_header_name_that_is_not_utf8_decodes(self):
        from pamqp import commands, frame, header

        props = commands.Basic.Properties(headers={"KEYX": {"KEYY": 1}})
        raw = frame.marshal(header.ContentHeader(0, 2, props), 1).replace(b"KEYY", b"K\xffYY")
        _, _, decoded = frame.unmarshal(raw)
        assert decoded.properties.headers == {"KEYX": {"K\udcffYY": 1}}

    def test_valid_utf8_is_unchanged(self):
        from pamqp import commands, frame, header

        props = commands.Basic.Properties(message_id="é-ok", headers={"ké": "v"})
        _, _, decoded = frame.unmarshal(frame.marshal(header.ContentHeader(0, 2, props), 1))
        assert decoded.properties.message_id == "é-ok"
        assert decoded.properties.headers == {"ké": "v"}

    async def test_the_consumer_dead_letters_it_once(self):
        from kbm_ledsas_sdk.amqp.consumer import AMQPConsumer

        queue = MagicMock()
        queue.name = "queue.svc.v1"
        consumer = AMQPConsumer(queue=queue, prefetch_count=10)
        body = Command(envelope=_envelope(), payload={}).model_dump_json().encode()
        message = SimpleNamespace(
            body=body,
            message_id="\udcff\udcfe-bad",
            correlation_id=None,
            reply_to=None,
            content_type=None,
            content_encoding=None,
            type=None,
            user_id=None,
            app_id=None,
            expiration=None,
            cluster_id=None,
            headers={},
            delivery_tag=7,
            redelivered=False,
            reject=AsyncMock(),
            nack=AsyncMock(),
            ack=AsyncMock(),
        )
        with capture() as records:
            await consumer._on_message(message)
        message.reject.assert_awaited_once_with(requeue=False)
        message.ack.assert_not_awaited()
        assert consumer.command_queue.qsize() == 0
        [warning] = [r for r in records if "not valid UTF-8" in r.getMessage()]
        assert warning.getMessage() == (
            "AMQP message property message_id is not valid UTF-8; dead-lettering"
        )
        assert warning.amqp_message_id == "\\xff\\xfe-bad"
        assert not any(r.exc_info for r in records)
        # The record can be written (no lone surrogate reaches the stream).
        json.dumps(vars(warning), default=str).encode("utf-8")
