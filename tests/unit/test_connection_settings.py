"""Connection settings: the AMQP heartbeat, no_verify_ssl, cleartext storage endpoints."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

import pydantic
import pytest

from kbm_ledsas_sdk.runtime.config import SDKConfig

AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)


AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    f"AccountKey={AZURITE_KEY};BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"
)


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


class TestHeartbeat:
    @pytest.mark.parametrize(
        "url,heartbeat,expected",
        [
            ("amqp://u:p@h:5672/", 15, "amqp://u:p@h:5672/?heartbeat=15"),
            ("amqp://u:p@h:5672/vh?x=1", 15, "amqp://u:p@h:5672/vh?x=1&heartbeat=15"),
            ("amqp://u:p@h:5672/?heartbeat=30", 15, "amqp://u:p@h:5672/?heartbeat=30"),
            # The client library reads only the key spelled "heartbeat".
            ("amqp://u:p@h:5672/?HEARTBEAT=30", 15, "amqp://u:p@h:5672/?HEARTBEAT=30&heartbeat=15"),
            ("amqp://u:p@h:5672/", 0, "amqp://u:p@h:5672/?heartbeat=0"),
            ("amqp://u:p@h:5672/", None, "amqp://u:p@h:5672/"),
        ],
    )
    def test_url_with_heartbeat(self, url, heartbeat, expected):
        from kbm_ledsas_sdk.transport.direct import url_with_heartbeat

        assert url_with_heartbeat(url, heartbeat) == expected

    def test_the_setting(self, monkeypatch):
        from kbm_ledsas_sdk.runtime.env_vars import get_env_var

        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", "amqp://127.0.0.1:5672/")
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", AZURITE_CONN)
        assert SDKConfig.from_env("svc").heartbeat == 60
        monkeypatch.setenv("KBM_LEDSAS_HEARTBEAT", "5")
        assert SDKConfig.from_env("svc").heartbeat == 5
        monkeypatch.setenv("KBM_LEDSAS_HEARTBEAT", "601")
        with pytest.raises(pydantic.ValidationError):
            SDKConfig.from_env("svc")
        assert get_env_var("KBM_LEDSAS_HEARTBEAT").default == "60"

    async def test_the_connect_url_carries_it(self, monkeypatch):
        from kbm_ledsas_sdk.transport import direct as direct_module
        from kbm_ledsas_sdk.transport._build_direct import build_transport
        from kbm_ledsas_sdk.transport.base import TransportUnavailable

        seen = []

        async def fake_connect(url, **kwargs):
            seen.append(url)
            raise TimeoutError()

        monkeypatch.setattr(direct_module.aio_pika, "connect_robust", fake_connect)
        config = SDKConfig(
            service_name="svc",
            rabbitmq_url="amqp://127.0.0.1:5672/",
            blob_conn_string=AZURITE_CONN,
            heartbeat=9,
        )
        with pytest.raises(TransportUnavailable):
            await build_transport(config).start()
        assert seen == ["amqp://127.0.0.1:5672/?heartbeat=9"]

    def test_a_lost_connection_is_one_sdk_warning(self):
        from kbm_ledsas_sdk.transport.direct import DirectTransport

        transport = DirectTransport("amqp://127.0.0.1:5672/", AZURITE_CONN, "svc")
        with capture() as records:
            transport._on_broker_connection_closed(None, ConnectionResetError("missed heartbeats"))
            transport._on_broker_connection_closed(None, ConnectionResetError("again"))
        lost = [r for r in records if r.getMessage().startswith("Lost the connection")]
        assert len(lost) == 1 and lost[0].levelno == logging.WARNING


class TestNoVerifySsl:
    @pytest.fixture(autouse=True)
    def _reset(self, monkeypatch):
        from kbm_ledsas_sdk.runtime import security

        monkeypatch.setattr(security, "_no_verify_acknowledged", False)
        monkeypatch.delenv("KBM_LEDSAS_ALLOW_INSECURE_AMQP", raising=False)

    @pytest.mark.parametrize(
        "query",
        [
            "no_verify_ssl=1",
            "no_verify_ssl=false",
            "no_verify_ssl=",
            "x=1&no_verify_ssl",
            "no_verify_ssl",
        ],
    )
    def test_refused_without_the_acknowledgment(self, query):
        from kbm_ledsas_sdk.runtime.security import check_transport_security

        with pytest.raises(ValueError, match="no_verify_ssl"):
            check_transport_security(f"amqps://u:p@broker.example.com:5671/?{query}")

    def test_one_warning_with_it(self, monkeypatch):
        from kbm_ledsas_sdk.runtime.security import check_transport_security

        monkeypatch.setenv("KBM_LEDSAS_ALLOW_INSECURE_AMQP", "1")
        with capture() as records:
            check_transport_security("amqps://u:p@broker.example.com:5671/?no_verify_ssl=1")
            check_transport_security("amqps://u:p@broker.example.com:5671/?no_verify_ssl=1")
        warnings = [r for r in records if "no_verify_ssl" in r.getMessage()]
        assert len(warnings) == 1 and warnings[0].levelno == logging.WARNING

    def test_other_options_are_accepted(self):
        from kbm_ledsas_sdk.runtime.security import check_transport_security

        check_transport_security("amqps://u:p@broker.example.com:5671/?cafile=/ca.pem&heartbeat=5")


class TestCleartextStorage:
    @pytest.mark.parametrize(
        "endpoint,warns",
        [
            ("http://10.20.30.40:10000/devstoreaccount1", True),
            ("http://storage.example.com/devstoreaccount1", True),
            ("http://127.0.0.1:10000/devstoreaccount1", False),
            ("http://localhost:10000/devstoreaccount1", False),
            ("https://10.20.30.40/devstoreaccount1", False),
        ],
    )
    def test_the_warning(self, endpoint, warns):
        from kbm_ledsas_sdk.blob.azure_client import AzureBlobClient

        conn = AZURITE_CONN.replace("http://127.0.0.1:10000/devstoreaccount1", endpoint)
        with capture() as records:
            AzureBlobClient(conn, "dev")
        hits = [r for r in records if "travel in cleartext" in r.getMessage()]
        assert len(hits) == (1 if warns else 0)
        if warns:
            assert hits[0].levelno == logging.WARNING
            assert AZURITE_KEY not in hits[0].getMessage()
