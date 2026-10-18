"""The health deployment_id: KBM_LEDSAS_DEPLOYMENT_ID first, then KBM_LASTCOMMITID, quotes removed."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

import pytest


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


class TestDeploymentId:
    @pytest.mark.parametrize(
        "env,expected",
        [
            ({"KBM_LEDSAS_DEPLOYMENT_ID": "build-7"}, "build-7"),
            ({"KBM_LEDSAS_DEPLOYMENT_ID": "build-7", "KBM_LASTCOMMITID": "abc"}, "build-7"),
            ({"KBM_LASTCOMMITID": '"deadbeef"'}, "deadbeef"),
            ({"KBM_LASTCOMMITID": "  cafe\r\n"}, "cafe"),
            ({"KBM_LEDSAS_DEPLOYMENT_ID": "  ", "KBM_LASTCOMMITID": "abc"}, "abc"),
            ({"KBM_LEDSAS_DEPLOYMENT_ID": " 'build-8' ", "KBM_LASTCOMMITID": "abc"}, "build-8"),
            ({"KBM_LEDSAS_DEPLOYMENT_ID": '""', "KBM_LASTCOMMITID": "abc"}, "abc"),
            ({"KBM_LASTCOMMITID": ""}, None),
            ({}, None),
        ],
    )
    def test_the_value(self, monkeypatch, env, expected):
        from kbm_ledsas_sdk.health import server

        monkeypatch.delenv("KBM_LEDSAS_DEPLOYMENT_ID", raising=False)
        monkeypatch.delenv("KBM_LASTCOMMITID", raising=False)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        assert server._deployment_id() == expected

    def test_the_alias_is_a_setting(self, monkeypatch):
        from kbm_ledsas_sdk.runtime.env_vars import get_all_var_names, unknown_settings

        assert unknown_settings({"KBM_LEDSAS_DEPLOYMENT_ID": "x"}, get_all_var_names()) == []

    def test_no_unknown_setting_warning(self, monkeypatch):
        from kbm_ledsas_sdk.transport import _build_direct

        monkeypatch.setenv("KBM_LEDSAS_DEPLOYMENT_ID", "build-7")
        with capture() as records:
            _build_direct.warn_unknown_settings()
        assert not [r for r in records if "not settings" in r.getMessage()]
