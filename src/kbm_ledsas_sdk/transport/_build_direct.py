"""Construct the direct transport (RabbitMQ + Azure Blob Storage).

Imported lazily by :mod:`transport.factory`, so the RabbitMQ and Azure
client libraries are only loaded when a service starts.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from ..utils.logging import masked_logger
from .base import Transport
from .direct import DirectTransport

if TYPE_CHECKING:
    from ..runtime.config import SDKConfig

logger = masked_logger(__name__)


def warn_unknown_settings() -> None:
    """One WARNING naming the ``KBM_LEDSAS_*`` environment variables that
    are not settings of this SDK (a typo such as ``KBM_LEDSAS_PREFECH``
    would otherwise be ignored without a word while the default applies),
    each with the closest setting's name.
    Values are never logged."""
    from ..runtime.env_vars import (
        describe_unknown_settings,
        get_all_var_names,
        unknown_settings,
    )

    known = set(get_all_var_names())
    found = unknown_settings(os.environ, known)
    if found:
        logger.warning(
            "Ignoring environment variable(s) that are not settings of " "this SDK: %s",
            describe_unknown_settings(found),
            extra={"unknown_variables": [name for name, _close in found]},
        )


# A parsed command body takes up to about this many times its size in
# memory (measured: about 24x for a body of empty arrays or objects).
_PARSED_BODY_FACTOR = 25
# With KBM_LEDSAS_MAX_PAYLOAD_BYTES=0 the SDK sets no limit and a body is
# bounded only by the broker's max_message_size, which the SDK cannot read:
# the larger of RabbitMQ's defaults (128 MiB on 3.x; 16 MiB on 4.x) is
# assumed.
_BROKER_DEFAULT_MAX_MESSAGE_BYTES = 128 * 1024 * 1024
# The smallest KBM_LEDSAS_MAX_PAYLOAD_BYTES the WARNING suggests.
_SMALLEST_SUGGESTED_PAYLOAD_BYTES = 64 * 1024
# Where the container's memory limit is read: cgroup v2, then cgroup v1.
_CGROUP_MEMORY_LIMIT_FILES = (
    "/sys/fs/cgroup/memory.max",
    "/sys/fs/cgroup/memory/memory.limit_in_bytes",
)
# A cgroup v1 limit at or above this means "no limit".
_NO_LIMIT_BYTES = 1 << 60


def container_memory_limit() -> int | None:
    """The memory limit of the container the process runs in (bytes), read
    from its cgroup, or None (no limit, or not in a container)."""
    for path in _CGROUP_MEMORY_LIMIT_FILES:
        try:
            with open(path, encoding="ascii") as handle:
                text = handle.read().strip()
        except (OSError, ValueError):
            continue
        if text == "max":
            return None
        try:
            limit = int(text)
        except ValueError:
            continue
        return limit if 0 < limit < _NO_LIMIT_BYTES else None
    return None


def _mib(value: int) -> str:
    if value >= 1 << 30:
        return f"{value / (1 << 30):.1f} GiB"
    return f"{value / (1 << 20):.0f} MiB"


def _suggestion(limit: int, prefetch: int) -> tuple[int, int | None] | None:
    """Settings that keep the worst case at half of ``limit`` or less: a
    power-of-two ``KBM_LEDSAS_MAX_PAYLOAD_BYTES`` (at least 64 KiB) and,
    when even that is too much for ``prefetch`` commands, a lower
    ``KBM_LEDSAS_PREFETCH`` (else None). None when no such pair exists (a
    limit below about 3 MiB)."""
    budget = limit // 2
    per_command = budget // (prefetch * _PARSED_BODY_FACTOR)
    payload = max(
        _SMALLEST_SUGGESTED_PAYLOAD_BYTES,
        1 << max(0, per_command.bit_length() - 1),
    )
    if prefetch * payload * _PARSED_BODY_FACTOR <= budget:
        return payload, None
    fitting_prefetch = budget // (payload * _PARSED_BODY_FACTOR)
    if fitting_prefetch < 1:
        return None
    return payload, fitting_prefetch


def warn_memory_bound(config: SDKConfig) -> None:
    """One WARNING when the command bodies this service may hold parsed at
    once can exceed its container's memory limit.

    Up to ``KBM_LEDSAS_PREFETCH`` commands are received and held parsed at
    once, and a parsed body takes up to about 25 times its size, so the
    worst case is ``PREFETCH x MAX_PAYLOAD_BYTES x 25`` (about 4 GiB with
    the defaults). With ``KBM_LEDSAS_MAX_PAYLOAD_BYTES=0`` (no SDK limit)
    128 MiB bodies are assumed, RabbitMQ 3.x's default max_message_size
    (4.x's is 16 MiB). A service killed for memory gets the same messages
    again after its restart, so this is said once at start, with settings
    that keep the worst case at half the limit or less. Nothing is logged
    outside a container with a memory limit."""
    limit = container_memory_limit()
    if limit is None:
        return
    max_body = config.max_payload_bytes or _BROKER_DEFAULT_MAX_MESSAGE_BYTES
    worst = config.prefetch * max_body * _PARSED_BODY_FACTOR
    if worst <= limit:
        return
    suggestion = _suggestion(limit, config.prefetch)
    if suggestion is None:
        advice = (
            "Lower KBM_LEDSAS_MAX_PAYLOAD_BYTES and KBM_LEDSAS_PREFETCH to "
            "what your service needs"
        )
    elif suggestion[1] is None:
        advice = (
            "Lower KBM_LEDSAS_MAX_PAYLOAD_BYTES to what your commands need "
            f"(for example {suggestion[0]}, which keeps the worst case at "
            "half the limit or less) or KBM_LEDSAS_PREFETCH"
        )
    else:
        advice = (
            "Lower KBM_LEDSAS_MAX_PAYLOAD_BYTES and KBM_LEDSAS_PREFETCH to "
            "what your service needs (for example "
            f"KBM_LEDSAS_MAX_PAYLOAD_BYTES={suggestion[0]} with "
            f"KBM_LEDSAS_PREFETCH={suggestion[1]}, which keeps the worst "
            "case at half the limit or less)"
        )
    if config.max_payload_bytes:
        size_setting = f"KBM_LEDSAS_MAX_PAYLOAD_BYTES={config.max_payload_bytes}"
    else:
        size_setting = (
            "KBM_LEDSAS_MAX_PAYLOAD_BYTES=0 sets no limit, so the broker's "
            "max_message_size applies: 128 MiB is assumed, RabbitMQ 3.x's "
            "default, while 4.x's is 16 MiB"
        )
    logger.warning(
        "Up to KBM_LEDSAS_PREFETCH=%d command bodies of up to %s each "
        "(%s) are held parsed at once, and a parsed body can take about %d "
        "times its size: about %s in the worst case, above this container's "
        "memory limit of %s. A service killed for memory gets the same "
        "messages again after its restart. %s; send large data in a blob.",
        config.prefetch,
        _mib(max_body),
        size_setting,
        _PARSED_BODY_FACTOR,
        _mib(worst),
        _mib(limit),
        advice,
        extra={
            "prefetch": config.prefetch,
            "max_payload_bytes": config.max_payload_bytes,
            "worst_case_bytes": worst,
            "memory_limit_bytes": limit,
        },
    )


# RabbitMQ's default consumer_timeout (seconds): a delivery held
# unacknowledged longer is taken back by the broker, which closes the
# channel it was on and delivers it again.
_BROKER_DEFAULT_CONSUMER_TIMEOUT_S = 1800


def _setting(config: SDKConfig, name: str) -> int | None:
    """``config.<name>`` when it is an int (not a bool), else None."""
    value = getattr(config, name, None)
    if not isinstance(value, int) or isinstance(value, bool):
        return None
    return value


def warn_long_handler_timeout(config: SDKConfig) -> None:
    """One WARNING when ``KBM_LEDSAS_HANDLER_TIMEOUT`` lets a handler run
    longer than RabbitMQ's default consumer_timeout (30 min), or without a
    limit (0). A command held unacknowledged past the broker's
    consumer_timeout is taken back: the broker closes the channel and
    delivers the command again. On this instance the run still in progress
    settles the redelivery; on another instance the command runs again,
    and with several instances it can go on running again without end."""
    timeout = _setting(config, "handler_timeout")
    if timeout is None:
        return
    if timeout != 0 and timeout <= _BROKER_DEFAULT_CONSUMER_TIMEOUT_S:
        return
    shown = "0 (no limit)" if timeout == 0 else f"{timeout}s"
    logger.warning(
        "KBM_LEDSAS_HANDLER_TIMEOUT=%s allows handler runs longer than "
        "RabbitMQ's default consumer_timeout (30 min): the broker takes back "
        "a command left unacknowledged longer than its consumer_timeout and "
        "delivers it again (with several instances, another instance runs "
        "it again while it still runs here). Raise the broker's "
        "consumer_timeout above the longest handler run plus the time a "
        "command waits for a worker",
        shown,
        extra={"handler_timeout_s": timeout},
    )


def warn_prefetch_below_concurrency(config: SDKConfig) -> None:
    """One WARNING when ``KBM_LEDSAS_PREFETCH`` is below
    ``KBM_LEDSAS_CONCURRENCY``: the broker hands this service at most
    PREFETCH unacknowledged commands at a time and a command stays
    unacknowledged while its handler runs, so at most
    min(PREFETCH, CONCURRENCY) handlers run at once."""
    prefetch = _setting(config, "prefetch")
    concurrency = _setting(config, "concurrency")
    if prefetch is None or concurrency is None or prefetch >= concurrency:
        return
    logger.warning(
        "KBM_LEDSAS_PREFETCH=%d is below KBM_LEDSAS_CONCURRENCY=%d: the "
        "broker hands this service at most %d unacknowledged command(s) at a "
        "time, so at most %d handler(s) run at once. Set KBM_LEDSAS_PREFETCH "
        "to at least KBM_LEDSAS_CONCURRENCY to use every worker",
        prefetch,
        concurrency,
        prefetch,
        prefetch,
        extra={"prefetch": prefetch, "concurrency": concurrency},
    )


# Credential-free examples: the error text is logged through the SDK's
# secret masker, which would turn a user:password example into "***".
_MISSING_RABBITMQ_URL = (
    "KBM_LEDSAS_RABBITMQ_URL is not set. Set it to the broker URL, for "
    "example amqp://127.0.0.1:5672/ (put user:password@ before the host)."
)
_MISSING_BLOB_CONN_STRING = (
    "KBM_LEDSAS_BLOB_CONN_STRING is not set. Set it to the storage "
    "account's connection string (it starts with DefaultEndpointsProtocol=)."
)
_SEE_ENV_VARS = (
    'See "Environment Variables" in the SDK API reference '
    "(https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/docs/SDK_API_REFERENCE.md#environment-variables)."
)


def missing_connection_settings(config: SDKConfig) -> list[str]:
    """Messages for every required connection setting that is unset (all
    of them at once, so one restart fixes the configuration), ending with
    a pointer to the settings reference; empty when nothing is missing."""
    problems = []
    if not config.rabbitmq_url:
        problems.append(_MISSING_RABBITMQ_URL)
    if not config.blob_conn_string:
        problems.append(_MISSING_BLOB_CONN_STRING)
    if problems:
        problems.append(_SEE_ENV_VARS)
    return problems


def build_transport(config: SDKConfig) -> Transport:
    """Construct DirectTransport, validating required connection env vars
    (and warning once about ``KBM_LEDSAS_*`` variables that are not
    settings, see :func:`warn_unknown_settings`, about command bodies
    that can exceed the container's memory limit, see
    :func:`warn_memory_bound`, about a handler timeout longer than the
    broker's default consumer_timeout, see
    :func:`warn_long_handler_timeout`, and about a prefetch that caps the
    concurrency, see :func:`warn_prefetch_below_concurrency`)."""
    warn_unknown_settings()
    warn_memory_bound(config)
    warn_long_handler_timeout(config)
    warn_prefetch_below_concurrency(config)
    missing = missing_connection_settings(config)
    if missing:
        raise ValueError(" ".join(missing))

    return DirectTransport(
        rabbitmq_url=config.rabbitmq_url,
        blob_conn_string=config.blob_conn_string,
        service_name=config.service_name,
        tenant=config.tenant,
        prefetch_count=config.prefetch,
        default_container=config.blob_container,
        max_payload_bytes=config.max_payload_bytes,
        connect_timeout=config.connect_timeout,
        heartbeat=config.heartbeat,
        publish_channels=config.concurrency,
    )
