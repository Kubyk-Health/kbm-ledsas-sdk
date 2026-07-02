"""
Transport-security helpers shared by config and transport layers.

Lives here (rather than under ``transport/``) so ``runtime/config.py``
can call ``check_transport_security`` during config validation —
*before* any blob/AMQP client is constructed. Running the check at
config-load avoids ~7 INFO lines of half-built transport state
preceding the actual ERROR.
"""

from __future__ import annotations

import logging
import os
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)


# Hosts that don't need TLS — credentials never leave the local box.
LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", ""})

# Operator escape hatch: set to a truthy value (see _is_truthy below) to
# downgrade the SDK's refusal of ``amqp://`` to non-loopback hosts from
# a hard error to a WARNING. Used by deployments that terminate TLS in
# an upstream proxy (e.g. an in-cluster service mesh).
ALLOW_INSECURE_AMQP_ENV = "KBM_LEDSAS_ALLOW_INSECURE_AMQP"


_TRUTHY_BOOL_VALUES = frozenset({"1", "true", "yes", "on", "t", "y"})


def _is_truthy(value: str | None) -> bool:
    """Consistent boolean parsing across the SDK.

    Accepts ``1``, ``true``, ``yes``, ``on``, ``t``, ``y`` (case-insensitive)
    as truthy. Anything else — including the empty string, ``0``, ``false``,
    ``no``, ``off`` — is falsy. This lets a customer who set
    ``KBM_LEDSAS_ALLOW_INSECURE_AMQP=true`` (the same form they'd use for
    ``KBM_LEDSAS_GENERIC_ERRORS=true``) get the result they expect.
    """
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY_BOOL_VALUES


def scrub_url_credentials(url: str) -> str:
    """Return ``url`` with the userinfo (user:password) replaced by ``***``.

    Used to log connection URLs without leaking credentials. Bad input
    returns the original string unchanged (best-effort scrub).
    """
    try:
        parsed = urlparse(url)
        if parsed.username or parsed.password:
            host = parsed.hostname or ""
            netloc = f"***:***@{host}"
            if parsed.port:
                netloc += f":{parsed.port}"
            return urlunparse(parsed._replace(netloc=netloc))
    except Exception:
        pass
    return url


def check_transport_security(url: str) -> None:
    """Refuse (or warn for) ``amqp://`` against a non-loopback host.

    Cleartext AMQP transmits credentials and message bodies unencrypted.
    Local-dev brokers on loopback are fine; remote brokers must use
    amqps:// unless the operator explicitly opts in via
    KBM_LEDSAS_ALLOW_INSECURE_AMQP=1.
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return
    if parsed.scheme != "amqp":
        return  # amqps:// or unknown scheme — leave alone
    host = (parsed.hostname or "").lower()
    if host in LOOPBACK_HOSTS:
        return
    if _is_truthy(os.getenv(ALLOW_INSECURE_AMQP_ENV)):
        logger.warning(
            "amqp:// to non-loopback host %s — credentials transit in "
            "cleartext. %s acknowledged; proceeding.",
            host,
            ALLOW_INSECURE_AMQP_ENV,
        )
        return
    raise ValueError(
        f"amqp:// to non-loopback host {host!r} would transmit "
        "credentials in cleartext. Use amqps:// for remote brokers, or "
        f"set {ALLOW_INSECURE_AMQP_ENV}=1 if TLS is terminated by an "
        "upstream proxy (e.g. an in-cluster service mesh)."
    )


# AMQP's wire protocol caps exchange and queue
# names at 127 bytes. The SDK builds names by interpolating tenant and
# service_name into fixed prefixes; the worst-case name is
# ``dlq.queue.{tenant}.{service_name}.v1`` — 10 + len(tenant) + 1 +
# len(service_name) + 3 chars. Validate the combined length at
# config-load so the customer sees a clean ValueError pointing at the
# overflow before any I/O.
AMQP_NAME_MAX = 127
# Fixed overhead in the worst-case name "dlq.queue.{T}.{S}.v1":
#   "dlq.queue." (10) + "." between T and S (1) + ".v1" (3) = 14
AMQP_TOPOLOGY_FIXED_OVERHEAD_WITH_TENANT = 14
# Without tenant, the name is "dlq.queue.{S}.v1":
#   "dlq.queue." (10) + ".v1" (3) = 13
AMQP_TOPOLOGY_FIXED_OVERHEAD_WITHOUT_TENANT = 13


def check_topology_name_budget(service_name: str, tenant: str | None) -> None:
    """Refuse service_name/tenant combos that overflow AMQP's 127-byte cap.

    The worst case is the DLQ queue name, which adds ``dlq.queue.`` (10),
    a separator dot (1 if tenant set), and ``.v1`` (3) around the user's
    inputs. We compute the worst-case length and reject up front.
    """
    if tenant:
        fixed = AMQP_TOPOLOGY_FIXED_OVERHEAD_WITH_TENANT
        used = len(tenant) + len(service_name) + fixed
        if used > AMQP_NAME_MAX:
            budget = AMQP_NAME_MAX - fixed
            raise ValueError(
                f"KBM_LEDSAS_TENANT ({len(tenant)} chars) + "
                f"service_name ({len(service_name)} chars) overflow the "
                f"AMQP 127-byte limit on the resulting "
                f"'dlq.queue.{{tenant}}.{{service_name}}.v1' queue name "
                f"(would be {used} chars, max {AMQP_NAME_MAX}). "
                f"Combined length of tenant + service_name must be "
                f"≤ {budget}."
            )
    else:
        fixed = AMQP_TOPOLOGY_FIXED_OVERHEAD_WITHOUT_TENANT
        used = len(service_name) + fixed
        if used > AMQP_NAME_MAX:
            budget = AMQP_NAME_MAX - fixed
            raise ValueError(
                f"service_name ({len(service_name)} chars) overflows the "
                f"AMQP 127-byte limit on the resulting "
                f"'dlq.queue.{{service_name}}.v1' queue name "
                f"(would be {used} chars, max {AMQP_NAME_MAX}). "
                f"service_name must be ≤ {budget} chars."
            )
