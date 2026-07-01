"""
Unit tests for AMQPConsumer message routing.

Focus: the malformed-envelope hot-loop regression. A Pydantic ValidationError on the envelope must dead-letter
(reject without requeue) the message — NOT requeue it.
"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from kbm_ledsas_sdk.amqp.consumer import AMQPConsumer


def _make_message(body: bytes):
    """Build a mock AbstractIncomingMessage with async ack/nack/reject."""
    msg = MagicMock()
    msg.body = body
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    msg.reject = AsyncMock()
    return msg


def _make_consumer():
    """AMQPConsumer with a stub queue; we exercise _on_message directly."""
    queue = MagicMock()
    queue.name = "queue.test.v1"
    return AMQPConsumer(queue=queue, prefetch_count=10)


class TestConsumerMalformedMessageRouting:
    """C3 regression: malformed envelopes go to DLQ, not the requeue loop."""

    @pytest.mark.asyncio
    async def test_missing_envelope_rejects_without_requeue(self):
        """{"foo": "bar"} (no envelope/payload) → reject(requeue=False)."""
        consumer = _make_consumer()
        msg = _make_message(b'{"foo": "bar"}')

        await consumer._on_message(msg)

        msg.reject.assert_awaited_once_with(requeue=False)
        msg.nack.assert_not_awaited()
        msg.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_envelope_missing_required_fields_rejects_without_requeue(self):
        """envelope with only `name` (missing 7 required fields) → reject(False)."""
        consumer = _make_consumer()
        body = json.dumps(
            {
                "envelope": {"name": "ProcessCSV"},
                "payload": {"csv_uri": "azblob://x/y.csv"},
            }
        ).encode()
        msg = _make_message(body)

        await consumer._on_message(msg)

        msg.reject.assert_awaited_once_with(requeue=False)
        msg.nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_invalid_json_rejects_without_requeue(self):
        """Invalid JSON syntax → reject(requeue=False) (preserves existing behavior)."""
        consumer = _make_consumer()
        msg = _make_message(b"{not valid json")

        await consumer._on_message(msg)

        msg.reject.assert_awaited_once_with(requeue=False)
        msg.nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_repeated_malformed_messages_each_reject_once(self):
        """Three malformed messages → 3 reject(False) calls, zero nack(True). No hot loop."""
        consumer = _make_consumer()
        bodies = [
            b'{"foo": "bar"}',
            b'{"envelope": {"name": "X"}, "payload": {}}',
            b"not even json",
        ]
        messages = [_make_message(b) for b in bodies]

        for m in messages:
            await consumer._on_message(m)

        for m in messages:
            m.reject.assert_awaited_once_with(requeue=False)
            m.nack.assert_not_awaited()
            m.ack.assert_not_awaited()


class TestConsumerWellFormedMessages:
    """Well-formed messages go onto the internal queue and stay pending until ACK."""

    @pytest.mark.asyncio
    async def test_well_formed_message_is_queued_not_acked(self):
        """A valid Command is pushed to command_queue; ack happens later via ack()."""
        consumer = _make_consumer()
        valid_envelope = {
            "schema_version": "1.0",
            "type": "command",
            "name": "ProcessCSV",
            "message_version": "1.0",
            "message_id": "00000000-0000-0000-0000-000000000001",
            "correlation_id": "00000000-0000-0000-0000-000000000002",
            "idempotency_key": "00000000-0000-0000-0000-000000000003",
            "sent_at": "2026-05-24T10:00:00Z",
            "trace_id": "00-trace",
        }
        body = json.dumps({"envelope": valid_envelope, "payload": {"k": "v"}}).encode()
        msg = _make_message(body)

        await consumer._on_message(msg)

        # The consumer does NOT auto-ACK on receipt; that's the
        # application's call after the handler runs successfully.
        msg.ack.assert_not_awaited()
        msg.nack.assert_not_awaited()
        msg.reject.assert_not_awaited()

        # The command is now on the internal queue and tracked as pending.
        assert consumer.pending_count == 1
        cmd = await asyncio.wait_for(consumer.command_queue.get(), timeout=1.0)
        assert cmd.envelope.name == "ProcessCSV"
