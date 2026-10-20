"""Reply channels open, close and reopen on the installed aio-pika (9.4 and later keep the channel's explicit-close marker differently)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aio_pika
import aiormq.base
from aio_pika.abc import AbstractRobustConnection

from kbm_ledsas_sdk.amqp import publisher as pub_module
from kbm_ledsas_sdk.amqp.publisher import AMQPPublisher


class _FakeUnderlay(aiormq.base.Base):
    """An aiormq channel stand-in with the installed aiormq's closing
    semantics."""

    def __init__(self, connection: Any) -> None:
        super().__init__(loop=asyncio.get_running_loop(), parent=connection)
        self.connection = connection
        self.on_return_callbacks: set = set()
        self.number = 1

    async def _on_close(self, exc: Any = None) -> None:
        return None

    def __str__(self) -> str:
        return "fake-channel"


class _FakeConnection(aiormq.base.Base):
    def __init__(self) -> None:
        super().__init__(loop=asyncio.get_running_loop())

    async def ready(self) -> None:
        return None

    async def channel(self, **_kw: Any) -> _FakeUnderlay:
        return _FakeUnderlay(self)

    async def _on_close(self, exc: Any = None) -> None:
        return None

    def __str__(self) -> str:
        return "fake-connection"


def _robust(connection: _FakeConnection) -> MagicMock:
    robust = MagicMock(spec=AbstractRobustConnection)
    robust.transport = SimpleNamespace(ready=AsyncMock(), connection=connection)
    return robust


class TestPublisherChannelOpen:
    """A reply channel opens, closes and reopens on the installed aio-pika,
    whichever way that version keeps the explicit-close marker."""

    async def test_a_publishing_channel_opens_on_the_installed_aio_pika(self) -> None:
        publisher = AMQPPublisher(_robust(_FakeConnection()))
        channel = await publisher._open_transient_channel()
        assert not channel.is_closed
        await channel.close()
        assert channel.is_closed

    async def test_a_closed_publishing_channel_reopens(self) -> None:
        publisher = AMQPPublisher(_robust(_FakeConnection()))
        channel = await publisher._open_transient_channel()
        await channel.close()
        await channel.reopen()
        assert not channel.is_closed
        await channel.close()

    async def test_a_bool_close_marker_is_reset_to_false(self) -> None:
        """aio-pika before 9.5.0 keeps the marker as a bool."""

        class BoolMarkerChannel(aio_pika.Channel):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                self._closed = True

        cls = pub_module._build_publisher_channel_class(BoolMarkerChannel)
        assert cls is not BoolMarkerChannel
        channel = cls(_robust(_FakeConnection()))
        await channel._open()
        assert channel._closed is False

    async def test_a_future_close_marker_is_renewed_when_done(self) -> None:
        """aio-pika 9.5.0 and later keep the marker as a future."""
        cls = pub_module._build_publisher_channel_class(aio_pika.Channel)
        channel = cls(_robust(_FakeConnection()))
        done = asyncio.get_running_loop().create_future()
        done.set_result(True)
        channel._closed = done
        await channel._open()
        assert isinstance(channel._closed, asyncio.Future)
        assert not channel._closed.done()
