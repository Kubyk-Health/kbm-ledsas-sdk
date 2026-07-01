"""
Regression tests for config credential-handling and env-parsing fixes.

Covers the following fixes:

- ``SDKConfig.__str__`` must not leak the broker
  password or Azure account key (only ``__repr__`` was redacted before).
- ``SDKConfig.from_env`` must strip a matched pair of
  surrounding quotes from ``KBM_LEDSAS_RABBITMQ_URL`` /
  ``KBM_LEDSAS_BLOB_CONN_STRING`` so ``docker run --env-file`` (which does
  no quote stripping) behaves like the shell ``source .env`` path — and so
  the cleartext-AMQP guard can't be bypassed by a quoted scheme.
- a non-UTF-8 AMQP body must dead-letter with a
  single clean ERROR (no traceback), not fall through to the generic
  catch-all (which logged ``exc_info=True``).
"""

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from kbm_ledsas_sdk.amqp.consumer import AMQPConsumer
from kbm_ledsas_sdk.runtime.config import SDKConfig, _strip_surrounding_quotes


# ---------------------------------------------------------------------------
# credential redaction on str()/f-string, not just repr()
# ---------------------------------------------------------------------------
class TestConfigStrDoesNotLeakCredentials:
    _SECRET_URL = "amqp://user:topsecret@127.0.0.1:5672/"
    _SECRET_CONN = "DefaultEndpointsProtocol=http;AccountKey=SUPERSECRETKEY;"

    def _cfg(self) -> SDKConfig:
        return SDKConfig(
            service_name="svc",
            rabbitmq_url=self._SECRET_URL,
            blob_conn_string=self._SECRET_CONN,
            dev_mode=True,
        )

    def test_str_hides_broker_password_and_account_key(self):
        s = str(self._cfg())
        assert "topsecret" not in s
        assert "SUPERSECRETKEY" not in s

    def test_fstring_hides_credentials(self):
        s = f"{self._cfg()}"
        assert "topsecret" not in s
        assert "SUPERSECRETKEY" not in s

    def test_repr_still_redacted(self):
        s = repr(self._cfg())
        assert "topsecret" not in s
        assert "SUPERSECRETKEY" not in s

    def test_str_equals_repr(self):
        cfg = self._cfg()
        assert str(cfg) == repr(cfg)

    def test_logging_percent_s_does_not_leak(self):
        # The footgun in the finding: logger.info("cfg: %s", config)
        cfg = self._cfg()
        rendered = "loaded config: %s" % cfg
        assert "topsecret" not in rendered
        assert "SUPERSECRETKEY" not in rendered


# ---------------------------------------------------------------------------
# surrounding-quote stripping for env-delivered connection settings
# ---------------------------------------------------------------------------
class TestStripSurroundingQuotes:
    def test_strips_matched_double_quotes(self):
        assert _strip_surrounding_quotes('"amqp://x"') == "amqp://x"

    def test_strips_matched_single_quotes(self):
        assert _strip_surrounding_quotes("'amqp://x'") == "amqp://x"

    def test_leaves_unquoted_untouched(self):
        assert _strip_surrounding_quotes("amqp://x") == "amqp://x"

    def test_leaves_asymmetric_quote_untouched(self):
        assert _strip_surrounding_quotes('"amqp://x') == '"amqp://x'

    def test_none_passes_through(self):
        assert _strip_surrounding_quotes(None) is None

    def test_strips_only_one_pair(self):
        assert _strip_surrounding_quotes('""amqp://x""') == '"amqp://x"'


class TestFromEnvStripsQuotes:
    def test_quoted_rabbitmq_url_is_parsed_as_amqp(self, monkeypatch):
        # Simulate docker --env-file delivering the quoted .env value verbatim.
        monkeypatch.setenv("KBM_LEDSAS_MODE", "direct")
        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", '"amqp://guest:guest@127.0.0.1:5672/"')
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", '"AccountKey=k;"')
        cfg = SDKConfig.from_env("svc")
        assert cfg.rabbitmq_url == "amqp://guest:guest@127.0.0.1:5672/"
        assert cfg.blob_conn_string == "AccountKey=k;"

    def test_quoted_cleartext_nonlocal_url_still_refused(self, monkeypatch):
        # The guard must fire on the stripped value, not be bypassed by quotes.
        monkeypatch.setenv("KBM_LEDSAS_MODE", "direct")
        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", '"amqp://user:pass@10.255.255.1:5672/"')
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", '"AccountKey=k;"')
        with pytest.raises(ValueError):
            SDKConfig.from_env("svc")


# ---------------------------------------------------------------------------
# non-UTF-8 body dead-letters cleanly with no traceback
# ---------------------------------------------------------------------------
def _make_message(body: bytes):
    msg = MagicMock()
    msg.body = body
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    msg.reject = AsyncMock()
    return msg


def _make_consumer():
    queue = MagicMock()
    queue.name = "queue.test.v1"
    return AMQPConsumer(queue=queue, prefetch_count=10)


class TestNonUtf8BodyHandling:
    @pytest.mark.asyncio
    async def test_non_utf8_body_rejects_without_requeue(self):
        consumer = _make_consumer()
        msg = _make_message(b"\xff\xfe\x00bad bytes")

        await consumer._on_message(msg)

        msg.reject.assert_awaited_once_with(requeue=False)
        msg.nack.assert_not_awaited()
        msg.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_utf8_body_logs_single_clean_error_no_traceback(self):
        # Attach a capturing handler directly to the consumer's logger so the
        # assertion does not depend on log propagation to the root logger
        # (other tests in the suite call setup_logging, which can disable
        # propagation on the kbm_ledsas_sdk logger tree).
        consumer = _make_consumer()
        msg = _make_message(b"hello\xffworld")

        records: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        clogger = logging.getLogger("kbm_ledsas_sdk.amqp.consumer")
        handler = _Capture(level=logging.ERROR)
        clogger.addHandler(handler)
        prev_level = clogger.level
        clogger.setLevel(logging.ERROR)
        try:
            await consumer._on_message(msg)
        finally:
            clogger.removeHandler(handler)
            clogger.setLevel(prev_level)

        errors = [r for r in records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        # The dedicated UnicodeDecodeError branch logs with exc_info=False, so
        # the record carries no exception info (the zero-traceback invariant).
        assert errors[0].exc_info is None
        assert "UTF-8" in errors[0].getMessage()
