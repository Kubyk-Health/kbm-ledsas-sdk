"""Masking: nested record fields, less common secret forms, the configured secrets, format characters in the text log, the redacted display of the connection settings."""

from __future__ import annotations

import json
import logging
import pprint
import sys
from types import SimpleNamespace

import pytest

from kbm_ledsas_sdk.runtime.config import SDKConfig
from kbm_ledsas_sdk.utils.logging import JSONFormatter, SecretMaskFilter, TextFormatter

AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)


AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    f"AccountKey={AZURITE_KEY};BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"
)


BROKER_URL = "amqp://" + ":".join(("u", "Secret1")) + "@127.0.0.1:5672/"


def _transport(url: str = BROKER_URL):
    from kbm_ledsas_sdk.transport.direct import DirectTransport

    return DirectTransport(rabbitmq_url=url, blob_conn_string=AZURITE_CONN, service_name="svc-1")


def _record(msg: str = "m", args=(), exc_info=None, **extra) -> logging.LogRecord:
    record = logging.LogRecord("kbm_ledsas_sdk.x", logging.ERROR, __file__, 1, msg, args, exc_info)
    for key, value in extra.items():
        setattr(record, key, value)
    return record


SECRET_FORMS = [
    "amqps://" + ":".join(("u", "Secret1")) + "@h/",
    "AccountKey=KK1KK1;x",
    "Bearer abcdefghijklmnopqrstuvwx",
    "https://a.blob.core.windows.net/c/b?sv=1&sig=SIGSIG1",
]


class TestNestedMasking:
    """A dict, list, tuple or set in a record field is masked item by item."""

    def test_nested_values_are_masked_in_both_formats(self) -> None:
        nested = {
            "u": SECRET_FORMS[0],
            "l": [SECRET_FORMS[1], (SECRET_FORMS[2],)],
            "d": {"deeper": {SECRET_FORMS[3]: SECRET_FORMS[3]}},
        }
        original = json.dumps(nested, default=list)
        record = _record(obj=nested)
        SecretMaskFilter().filter(record)
        for line in (JSONFormatter().format(record), TextFormatter().format(record)):
            for secret in ("Secret1", "KK1KK1", "abcdefghijklmnopqrstuvwx", "SIGSIG1"):
                assert secret not in line, (secret, line)
        assert json.dumps(nested, default=list) == original  # the caller's object is unchanged

    def test_a_recursion_bomb_hits_the_bound(self) -> None:
        bomb: list = []
        bomb.append(bomb)
        record = _record(bomb=bomb, wide=[SECRET_FORMS[0]] * 20_000)
        SecretMaskFilter().filter(record)
        line = JSONFormatter().format(record)
        assert "<withheld: too deeply nested or too large to mask>" in line
        assert "Secret1" not in line

    def test_other_objects_are_masked_as_written(self) -> None:
        record = _record(ns=SimpleNamespace(url=SECRET_FORMS[0]), n=3, flag=True)
        SecretMaskFilter().filter(record)
        assert "Secret1" not in JSONFormatter().format(record)
        assert record.n == 3 and record.flag is True


class TestMaskingForms:
    """Less common secret forms, and the configured secrets in any form."""

    @pytest.mark.parametrize(
        "text, secret",
        [
            ("AccountKey = abcdef12", "abcdef12"),
            ("AccountKey:abcdef12", "abcdef12"),
            ("x%26sig%3DSIGVALUE1%26se%3D1", "SIGVALUE1"),
            ("a&amp;sig=SIGVALUE2&amp;x=1", "SIGVALUE2"),
            ("client_secret=cv123456 next", "cv123456"),
            ("api_key=ak123456 next", "ak123456"),
        ],
    )
    def test_masked(self, text, secret) -> None:
        from kbm_ledsas_sdk.utils.logging import redact_secrets

        assert secret not in redact_secrets(text)

    def test_the_configured_secrets_are_masked_in_any_form(self) -> None:
        import base64

        from kbm_ledsas_sdk.utils.logging import redact_secrets

        # The account key is computed, not written out (secret scanners
        # report a literal one).
        account_key = base64.b64encode(b"ConfiguredKey1").decode()
        SDKConfig(
            service_name="svc",
            rabbitmq_url="amqp://" + ":".join(("u", "Pass%40word9")) + "@127.0.0.1:5672/",
            blob_conn_string=f"AccountKey={account_key};AccountName=a",
        )
        for text in ("pw Pass@word9 quoted", "pw Pass%40word9", f"value {account_key} here"):
            out = redact_secrets(text)
            assert "Pass@word9" not in out and "Pass%40word9" not in out, out
            assert account_key.rstrip("=") not in out, out

    def test_short_values_are_not_registered(self) -> None:
        from kbm_ledsas_sdk.utils.logging import redact_secrets, register_secret

        register_secret("abc")
        assert redact_secrets("abc def") == "abc def"


class TestFormatCharacters:
    """The text log format writes Unicode format characters as escapes."""

    def test_escaped_in_message_field_and_traceback(self) -> None:
        try:
            raise ValueError("bad ‮ here")
        except ValueError:
            exc_info = sys.exc_info()
        record = _record("user=%s", ("a‮b​c⁦d﻿\U000e0041",), exc_info, field="x⁧y")
        line = TextFormatter().format(record)
        for ch in ("‮", "​", "⁦", "⁧", "﻿", "\U000e0041"):
            assert ch not in line
        assert "a\\u202eb\\u200bc\\u2066d\\ufeff\\U000e0041" in line
        assert "bad \\u202e here" in line
        assert "\n  | Traceback" in line  # newlines between traceback lines are kept


class TestRedactedDisplay:
    """The broker URL and the connection string show as '**redacted**' in
    repr() of the configuration and the transport (debuggers, pprint)."""

    def test_vars_repr_and_pprint(self) -> None:
        config = SDKConfig(
            service_name="svc", rabbitmq_url=BROKER_URL, blob_conn_string=AZURITE_CONN
        )
        transport = _transport(BROKER_URL)
        for shown in (
            repr(vars(config)),
            pprint.pformat(vars(config)),
            repr(transport.__dict__),
            pprint.pformat(vars(transport)),
        ):
            assert "Secret1" not in shown and AZURITE_KEY not in shown
            assert "**redacted**" in shown
        assert str(config.rabbitmq_url) == BROKER_URL and transport.rabbitmq_url == BROKER_URL
