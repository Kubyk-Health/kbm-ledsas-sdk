"""
Transport-security helpers shared by config and transport layers.

Lives here (rather than under ``transport/``) so ``runtime/config.py``
can call ``check_transport_security`` during config validation —
*before* any blob/AMQP client is constructed. Running the check at
config-load avoids ~7 INFO lines of half-built transport state
preceding the actual ERROR.
"""

from __future__ import annotations

import os
from urllib.parse import parse_qsl, urlparse

from kbm_ledsas_sdk.utils.logging import mask_url_secrets, masked_logger

logger = masked_logger(__name__)


# Hosts that don't need TLS — credentials never leave the local box.
LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", ""})

# Operator escape hatch: set to a true value (see parse_bool_env below) to
# downgrade the SDK's refusal of ``amqp://`` to non-loopback hosts from
# a hard error to a WARNING. Used by deployments that terminate TLS in
# an upstream proxy (e.g. an in-cluster service mesh).
ALLOW_INSECURE_AMQP_ENV = "KBM_LEDSAS_ALLOW_INSECURE_AMQP"

# Non-loopback hosts whose cleartext acknowledgment was already logged in
# this process (the check runs at config load and again at connect time).
_insecure_hosts_acknowledged: set[str] = set()


def _strip_surrounding_quotes(value: str | None) -> str | None:
    """Strip a single matched pair of surrounding quotes from a value.

    ``docker run --env-file`` (unlike compose's dotenv parser and the
    shell ``set -a; source .env`` path) performs NO quote stripping, so a
    ``.env`` line written ``KBM_LEDSAS_RABBITMQ_URL="amqp://..."`` — the
    quoting the shell-source path requires — reaches the process with the
    literal double quotes still attached. Left intact, the quoted scheme
    no longer parses as ``amqp://`` (yarl yields ``scheme=''``), which both
    breaks the connection with a confusing late error AND silently bypasses
    the cleartext-AMQP refusal in :func:`check_transport_security`. We
    normalize by removing one matched pair of surrounding ``"`` or ``'``
    so both env-delivery styles behave identically. Only a symmetric pair
    is stripped, so a credential that legitimately contains a quote on one
    side is left untouched.
    """
    if value is None:
        return None
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _read_env(name: str, default: str | None = None) -> str | None:
    """Read a ``KBM_LEDSAS_*`` variable the same way however it was set.

    Surrounding whitespace (a trailing carriage return from a Windows
    ``.env`` file, a newline from a secret file) and ONE matched pair of
    surrounding quotes are removed. ``docker run --env-file`` passes quotes
    through verbatim, while the shell ``set -a; . ./.env`` path removes
    them, so ``KBM_LEDSAS_LOG_LEVEL="INFO"`` gives ``INFO`` either way.
    Returns ``default`` when the variable is unset. Every reader of a
    ``KBM_LEDSAS_*`` variable uses this (``SDKConfig.from_env`` through
    ``config._env``, the cleartext-AMQP acknowledgment below, and the
    service's early ``KBM_LEDSAS_DEBUG`` / log-format reads).
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    return _strip_surrounding_quotes(raw.strip()).strip()


_TRUTHY_BOOL_VALUES = frozenset({"1", "true", "yes", "on", "t", "y"})
_FALSY_BOOL_VALUES = frozenset({"0", "false", "no", "off", "f", "n"})


def parse_bool_env(name: str, default: bool = False) -> bool:
    """Read a boolean ``KBM_LEDSAS_*`` setting, refusing any other value.

    ``1``, ``true``, ``yes``, ``on``, ``t``, ``y`` are true and ``0``,
    ``false``, ``no``, ``off``, ``f``, ``n`` false (any letter case; read
    with :func:`_read_env`, so surrounding whitespace and quotes are
    removed). Unset or empty gives ``default``. Any other value (a typo
    such as ``ture``) raises ValueError naming the variable: guessing would
    silently turn a setting off that the operator meant to turn on, or the
    other way round.
    """
    raw = _read_env(name)
    if raw is None or raw == "":
        return default
    value = raw.lower()
    if value in _TRUTHY_BOOL_VALUES:
        return True
    if value in _FALSY_BOOL_VALUES:
        return False
    raise ValueError(
        f"{name}={raw[:40]!r} is not a boolean (use true or false; 1/0, "
        f"yes/no and on/off are accepted too)"
    )


def _is_truthy(value: str | None) -> bool:
    """True for one of the true tokens of :func:`parse_bool_env`.

    ``1``, ``true``, ``yes``, ``on``, ``t``, ``y`` (any letter case) give
    True; anything else gives False. It does not check a setting: the
    boolean settings are read with :func:`parse_bool_env`, which refuses a
    value that is not a boolean. This helper only reads a value that was
    already checked, or a built-in default.
    """
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY_BOOL_VALUES


def scrub_url_credentials(url: str) -> str:
    """Return ``url`` as it may be logged: the userinfo (user:password)
    replaced by ``***:***``, every value of the query string by ``***``
    (``amqp://host/?heartbeat=***&password=***``) and the fragment by
    ``***``; the scheme, host, port and path (the virtual host) are kept.

    Used to log connection URLs without leaking credentials. The URL is
    read as text (see ``mask_url_secrets``), so it fails closed: a URL no
    parser accepts (an out-of-range port, unmatched brackets, no scheme)
    is masked the same way, and anything that is not text is replaced by a
    placeholder.
    """
    if not isinstance(url, str):
        return "<unreadable URL>"
    try:
        return mask_url_secrets(url)
    except Exception:  # pragma: no cover - never return the input unmasked
        return "<unreadable URL>"


def check_url_userinfo_encoding(url: str) -> None:
    """Refuse a broker URL whose user name or password holds an unencoded
    ``@``, ``#`` or ``?``.

    In a URL these characters end the user info (``@``) or start the
    fragment / query (``#``, ``?``), so a password ``p@ss#word`` written
    unencoded into the URL makes the parser read the host as ``ss``: the
    connection goes to the wrong place and any message naming the host
    would quote part of the password. The error names the problem and
    the fix without repeating any part of the URL.
    """
    rest = url.split("://", 1)[1] if "://" in url else ""
    if not rest:
        return
    authority = rest.split("/", 1)[0]
    if "@" not in authority:
        return
    userinfo = authority.rsplit("@", 1)[0]
    if "@" in userinfo or "#" in userinfo or "?" in userinfo:
        raise ValueError(
            "the broker URL's user name or password contains an unencoded "
            "'@', '#' or '?', so the URL cannot be read correctly. "
            "Percent-encode these characters in the credentials ('@' -> %40, "
            "'#' -> %23, '?' -> %3F, '/' -> %2F, ':' in a user name -> %3A), "
            "e.g. with urllib.parse.quote(password, safe='')."
        )


# The schemes a broker URL may use. Any other scheme (http://, a typo, a
# missing scheme) would still be spoken to as plain AMQP by the client
# library, so it is refused instead of slipping past the cleartext check.
BROKER_URL_SCHEMES = ("amqp", "amqps")


def check_broker_url_shape(url: str) -> None:
    """Refuse a broker URL the client library could not use as written.

    Required: the scheme ``amqp://`` or ``amqps://``, a host, and a port
    (if given) between 1 and 65535; no whitespace. Such a URL would
    otherwise reach the client library, which (for a missing scheme or
    host) retries forever and logs the URL, password included, on every
    attempt. The error names the problem without repeating any part of
    the URL. Typical causes are named: surrounding or unmatched quotes,
    and an inline ``# comment`` that ``docker run --env-file`` passes
    through verbatim.
    """
    hint = ""
    stripped = url.strip()
    if stripped[:1] in ("'", '"') or stripped[-1:] in ("'", '"'):
        hint = (
            " It starts or ends with a quote character: quote the whole "
            "value with one matching pair of quotes, or not at all."
        )
    elif "#" in url and any(c.isspace() for c in url):
        hint = (
            " It looks like it carries an inline '# comment': docker run "
            "--env-file passes the rest of the line through verbatim, so "
            "put comments on their own line."
        )
    if any(c.isspace() for c in url):
        raise ValueError(
            "KBM_LEDSAS_RABBITMQ_URL contains whitespace, so it is not a "
            "usable broker URL." + hint
        )
    try:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        host = parsed.hostname
        port = parsed.port  # raises ValueError for a non-numeric / out-of-range port
    except ValueError:
        raise ValueError(
            "KBM_LEDSAS_RABBITMQ_URL has a port that is not a number "
            "between 1 and 65535 (or brackets that do not match), so it is "
            "not a usable broker URL." + hint
        ) from None
    if "://" not in url or scheme not in BROKER_URL_SCHEMES:
        raise ValueError(
            "KBM_LEDSAS_RABBITMQ_URL must start with amqp:// (a broker on "
            "this machine) or amqps:// (TLS), e.g. "
            "amqps://broker.example.com:5671/ (put user:password@ before "
            "the host)." + hint
        )
    if not host:
        raise ValueError(
            "KBM_LEDSAS_RABBITMQ_URL names no host after the scheme, e.g. "
            "amqp://127.0.0.1:5672/." + hint
        )
    if port == 0:
        raise ValueError(
            "KBM_LEDSAS_RABBITMQ_URL has port 0; use the broker's port "
            "(5672 for amqp://, 5671 for amqps://)."
        )


# The broker URL query option of the client library that turns off TLS
# certificate verification. The library reads it as it reads all its
# options: the first value of the key spelled exactly ``no_verify_ssl``
# (letter case counts); verification stays on when that value is "0" or
# the key is absent, and is off for any other value (even "false", or an
# empty value).
_NO_VERIFY_SSL_OPTION = "no_verify_ssl"
_no_verify_acknowledged = False


def _verification_turned_off(query: str) -> bool:
    """True when the client library would not verify the broker's TLS
    certificate for a URL with this query string (the same rule as the
    library's: ``query.get("no_verify_ssl", "0") != "0"``)."""
    for key, value in parse_qsl(query, keep_blank_values=True):
        if key == _NO_VERIFY_SSL_OPTION:
            return value != "0"
    return False


def _check_tls_verification(query: str, allow_insecure: bool) -> None:
    """Refuse a broker URL whose query turns off TLS certificate
    verification (``no_verify_ssl`` with any value except ``0``: the
    client library then accepts any certificate, so a machine in the
    path can read the credentials and the messages), unless
    KBM_LEDSAS_ALLOW_INSECURE_AMQP acknowledges it: then one WARNING per
    process names the option. ``no_verify_ssl=0`` keeps verification on
    and is accepted."""
    global _no_verify_acknowledged
    if not _verification_turned_off(query):
        return
    if not allow_insecure:
        raise ValueError(
            "KBM_LEDSAS_RABBITMQ_URL sets no_verify_ssl to a value other "
            "than 0, which turns off the verification of the broker's TLS "
            "certificate. Remove it (trust a private certificate authority "
            f"with ?cafile=<path> instead), or set {ALLOW_INSECURE_AMQP_ENV}=1 "
            "to accept an unverified broker."
        )
    if not _no_verify_acknowledged:
        _no_verify_acknowledged = True
        logger.warning(
            "KBM_LEDSAS_RABBITMQ_URL sets no_verify_ssl: the broker's TLS "
            "certificate is not verified. %s acknowledged; proceeding.",
            ALLOW_INSECURE_AMQP_ENV,
        )


def check_transport_security(url: str) -> None:
    """Refuse (or warn for) ``amqp://`` against a non-loopback host.

    Cleartext AMQP transmits credentials and message bodies unencrypted.
    Local-dev brokers on loopback are fine; remote brokers must use
    amqps:// unless the operator explicitly opts in via
    KBM_LEDSAS_ALLOW_INSECURE_AMQP=1.

    A URL whose credentials hold an unencoded ``@``, ``#`` or ``?`` is
    refused first (see :func:`check_url_userinfo_encoding`): it would
    parse to the wrong host. Then the URL's shape is checked (see
    :func:`check_broker_url_shape`): only ``amqp://`` and ``amqps://``
    with a host are accepted, so no other spelling can bypass this check.
    A URL whose query sets ``no_verify_ssl`` to any value except ``0`` (TLS
    certificate verification off) needs the same acknowledgment (see :func:`_check_tls_verification`).
    """
    # The acknowledgment is read (and an unrecognized value refused) on
    # every check, not only when it is needed.
    allow_insecure = parse_bool_env(ALLOW_INSECURE_AMQP_ENV)
    check_url_userinfo_encoding(url)
    check_broker_url_shape(url)
    parsed = urlparse(url)
    _check_tls_verification(parsed.query, allow_insecure)
    if parsed.scheme.lower() != "amqp":
        return  # amqps://: TLS
    host = (parsed.hostname or "").lower()
    if host in LOOPBACK_HOSTS:
        return
    if allow_insecure:
        # The check runs at config load and again when the connection is
        # opened; acknowledge each host once per process.
        if host not in _insecure_hosts_acknowledged:
            _insecure_hosts_acknowledged.add(host)
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
