"""The heartbeat in the broker URL is checked; a close without an error is reported as a lost connection."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from contextlib import contextmanager

import pydantic
import pytest

from kbm_ledsas_sdk.runtime.config import SDKConfig, describe_config_errors

AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)


AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    f"AccountKey={AZURITE_KEY};BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"
)


BROKER_URL = "amqp://" + ":".join(("u", "Secret1")) + "@127.0.0.1:5672/"


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


def _transport(url: str = BROKER_URL):
    from kbm_ledsas_sdk.transport.direct import DirectTransport

    return DirectTransport(rabbitmq_url=url, blob_conn_string=AZURITE_CONN, service_name="svc-1")


class TestUrlHeartbeat:
    """A ``?heartbeat=`` in the broker URL is a whole number of seconds
    between 0 and 600; anything else stops the start, without the password."""

    @pytest.mark.parametrize("value", ["abc", "-5", "99999", "1.5", "", "601", "+5"])
    def test_refused(self, value) -> None:
        from kbm_ledsas_sdk.runtime.security import check_broker_url_shape

        with pytest.raises(ValueError) as info:
            check_broker_url_shape(f"{BROKER_URL}?heartbeat={value}")
        assert str(info.value).startswith(
            f"KBM_LEDSAS_RABBITMQ_URL: ?heartbeat={value[:20]!r} is not a whole number"
        )
        assert "Secret1" not in str(info.value)

    @pytest.mark.parametrize("value", ["0", "60", "600"])
    def test_accepted(self, value) -> None:
        from kbm_ledsas_sdk.runtime.security import check_broker_url_shape

        check_broker_url_shape(f"{BROKER_URL}?heartbeat={value}")

    def test_a_configuration_error(self, monkeypatch) -> None:
        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", f"{BROKER_URL}?heartbeat=abc")
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", AZURITE_CONN)
        with pytest.raises(pydantic.ValidationError) as info:
            SDKConfig.from_env("svc")
        text = describe_config_errors(info.value)
        assert "?heartbeat='abc' is not a whole number of seconds between 0 and 600" in text
        assert "Secret1" not in text


class TestCloseWithoutError:
    """A close that carries no error (or a cancellation), as a heartbeat
    timeout does, is reported as a lost connection like any other."""

    @pytest.mark.parametrize("error", [None, asyncio.CancelledError()], ids=["none", "cancelled"])
    def test_reported(self, error) -> None:
        t = _transport()
        with capture("kbm_ledsas_sdk.transport.direct") as records:
            t._on_broker_connection_closed(object(), error)
            t._on_broker_connection_closed(object(), error)  # one WARNING per outage
            t._on_broker_reconnected(object())
        lost = [r for r in records if r.getMessage().startswith("Lost the connection")]
        assert len(lost) == 1 and lost[0].levelno == logging.WARNING
        assert "no frames from the broker within the heartbeat timeout" in lost[0].getMessage()
        assert lost[0].error_class == "HeartbeatTimeout"
        assert [r.getMessage() for r in records if r.getMessage().startswith("Reconnected")] == [
            "Reconnected to the AMQP broker; consuming commands again"
        ]

    @pytest.mark.parametrize("error", [None, asyncio.CancelledError(), ConnectionError("x")])
    def test_silent_once_stopping(self, error) -> None:
        t = _transport()
        t.begin_shutdown()
        with capture("kbm_ledsas_sdk.transport.direct") as records:
            t._on_broker_connection_closed(object(), error)
        assert not [r for r in records if r.getMessage().startswith("Lost the connection")]

    def test_the_cause_kept_for_the_shutdown_report(self) -> None:
        t = _transport()
        t._on_broker_connection_closed(object(), None)
        assert isinstance(t._broker_connection_lost_error, ConnectionError)
        assert "heartbeat timeout" in str(t._broker_connection_lost_error)
