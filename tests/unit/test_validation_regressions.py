"""Regression tests for validation and startup-handling fixes.

Covers:
- broker-unreachable at startup exits cleanly (the aiormq
  spelling of ``AMQPConnectionError`` is recognized as an expected
  startup error — no "Fatal error" re-log, no raw traceback).
- ``BlobRef`` rejects '..' path segments, >2048-char URIs, and
  Unicode format characters (zero-width / bidi overrides).
- ``SDKConfig.model_dump()`` / ``model_dump_json()`` redact the
  broker password and Azure account key.
- ``kbm_ledsas_sdk.json_log_formatter()`` is public and renders
  caller ``extra={...}`` fields.
"""

from __future__ import annotations

import json
import logging

import pytest
from pydantic import ValidationError

import kbm_ledsas_sdk
from kbm_ledsas_sdk import ServiceApp
from kbm_ledsas_sdk.models.blob import BlobRef
from kbm_ledsas_sdk.runtime.config import SDKConfig

AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Fak3K3yForT3sts999==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


# ---------------------------------------------------------------------------
# expected-startup-error suppression for broker-unreachable
# ---------------------------------------------------------------------------


def test_aiormq_connection_error_qualname_is_expected() -> None:
    """aio_pika re-exports the aiormq class: the qualname the app-layer
    handler computes is the aiormq spelling. Lock that assumption (if an
    aio_pika upgrade ever changes it, this fails loudly)."""
    import aio_pika.exceptions as apexc

    cls = apexc.AMQPConnectionError
    assert f"{cls.__module__}.{cls.__qualname__}" == ("aiormq.exceptions.AMQPConnectionError")


def test_broker_unreachable_exits_clean_without_traceback(
    monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture
) -> None:
    """Broker down at startup (the 'Connection Refused' case)
    must exit 1 via the expected-startup-error path: no 'Fatal error in
    ServiceApp' re-log and no raw Python traceback frames."""
    monkeypatch.setenv("KBM_LEDSAS_SERVICE_NAME", "probe-svc")
    # Port 5699: deliberately unused -> immediate connection refused.
    monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5699/")
    monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", AZURITE_CONN)
    monkeypatch.setenv("KBM_LEDSAS_MAX_RETRIES", "0")

    app = ServiceApp(service_name="probe-svc")

    @app.handler("Noop")
    async def _noop(ctx, req):  # pragma: no cover - never invoked
        return {}

    with pytest.raises(SystemExit) as exc_info:
        app.run()
    assert exc_info.value.code == 1

    err = capfd.readouterr().err
    assert "ServiceApp exiting (startup failure already" in err
    assert "Fatal error in ServiceApp" not in err
    # The transport layer logs the failure ONCE, with the (possibly
    # chained) traceback embedded in its single-line JSON record's
    # "exception" field — that one record is by design. What must NOT
    # appear: a second record embedding the traceback, or Python's own
    # raw uncaught-exception dump (whose header sits at column 0 on its
    # own line, outside any JSON record).
    traceback_lines = [
        line for line in err.splitlines() if "Traceback (most recent call last)" in line
    ]
    assert len(traceback_lines) == 1
    assert traceback_lines[0].startswith("{")  # inside the JSON record


# ---------------------------------------------------------------------------
# BlobRef boundary hardening
# ---------------------------------------------------------------------------


def test_blobref_rejects_dotdot_segment() -> None:
    with pytest.raises(ValidationError, match="'\\.\\.' segment"):
        BlobRef(uri="azblob://dev/../../etc/passwd")


def test_blobref_rejects_deep_dotdot_segment() -> None:
    with pytest.raises(ValidationError, match="'\\.\\.' segment"):
        BlobRef(uri="azblob://dev/a/../../../../../../etc/shadow")


def test_blobref_allows_dots_inside_a_segment() -> None:
    # 'a..b' and '.hidden' are literal blob-name segments, not traversal.
    assert BlobRef(uri="azblob://dev/a..b/.hidden/file.v1..2.json")


def test_blobref_rejects_oversized_uri() -> None:
    with pytest.raises(ValidationError, match="2048-character bound"):
        BlobRef(uri="azblob://dev/" + "a" * 100_000 + ".json")


def test_blobref_accepts_1024_char_blob_name() -> None:
    # Azure's own blob-name ceiling must still fit inside our URI bound.
    assert BlobRef(uri="azblob://dev/" + "a" * 1024)


@pytest.mark.parametrize(
    "bad_char",
    ["‮", "​", "‎", "﻿"],
    ids=["bidi-RLO", "zero-width-space", "LRM", "BOM-ZWNBSP"],
)
def test_blobref_rejects_unicode_format_chars(bad_char: str) -> None:
    with pytest.raises(ValidationError, match="format character"):
        BlobRef(uri=f"azblob://dev/report{bad_char}.json")


def test_blobref_still_accepts_normal_unicode() -> None:
    # Plain non-format Unicode letters remain accepted (no over-blocking).
    assert BlobRef(uri="azblob://dev/résumé-καρδιά.json")


# ---------------------------------------------------------------------------
# model_dump credential redaction
# ---------------------------------------------------------------------------


def _config_with_secrets() -> SDKConfig:
    return SDKConfig(
        service_name="dump-probe",
        rabbitmq_url="amqp://svc:S3cr3tBrokerPW999@127.0.0.1:5672/",
        blob_conn_string=AZURITE_CONN,
    )


def test_model_dump_redacts_credentials() -> None:
    dumped = _config_with_secrets().model_dump()
    flat = repr(dumped)
    assert "S3cr3tBrokerPW999" not in flat
    assert "Fak3K3yForT3sts999" not in flat
    assert dumped["rabbitmq_url"] == "**redacted**"
    assert dumped["blob_conn_string"] == "**redacted**"


def test_model_dump_json_redacts_credentials() -> None:
    dumped = _config_with_secrets().model_dump_json()
    assert "S3cr3tBrokerPW999" not in dumped
    assert "Fak3K3yForT3sts999" not in dumped
    assert "**redacted**" in dumped


def test_attribute_access_stays_unredacted() -> None:
    # The transport builders read the real values off the attributes.
    cfg = _config_with_secrets()
    assert "S3cr3tBrokerPW999" in cfg.rabbitmq_url
    assert "Fak3K3yForT3sts999" in cfg.blob_conn_string


def test_none_credentials_dump_as_none() -> None:
    cfg = SDKConfig(service_name="dump-probe")
    dumped = cfg.model_dump()
    assert dumped["rabbitmq_url"] is None
    assert dumped["blob_conn_string"] is None


# ---------------------------------------------------------------------------
# public JSON log formatter renders extra={} fields
# ---------------------------------------------------------------------------


def test_json_log_formatter_is_public() -> None:
    assert "json_log_formatter" in kbm_ledsas_sdk.__all__
    formatter = kbm_ledsas_sdk.json_log_formatter()
    assert isinstance(formatter, logging.Formatter)


def test_json_log_formatter_renders_extra_fields() -> None:
    formatter = kbm_ledsas_sdk.json_log_formatter()
    record = logging.LogRecord(
        name="handler.SayHello",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Greeting requested",
        args=(),
        exc_info=None,
    )
    record.correlation_id = "corr-abc-123"  # what extra={...} does
    out = json.loads(formatter.format(record))
    assert out["message"] == "Greeting requested"
    assert out["correlation_id"] == "corr-abc-123"
    assert out["level"] == "INFO"
