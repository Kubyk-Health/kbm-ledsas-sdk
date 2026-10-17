"""
Health check registry: stores customer-registered callables and runs them.

Each registered check is a sync or async callable returning truthy = healthy.
Exceptions are caught and reported as unhealthy with the exception message,
secrets masked (``redact_secrets``). A failing check is logged once when it
starts failing (or its reason changes), every ``FAILURE_LOG_REPEAT_S`` while
it keeps failing, and once (INFO) when it passes again.

Every check has a time limit (``DEFAULT_CHECK_TIMEOUT_S`` unless registered
with its own): an async check still running at the limit is cancelled and
reported as unhealthy (``timed out after <N>s``), so a dependency that hangs
cannot hang the probe endpoint, pile up abandoned checks, or hold the
service's exit. The checks of one probe run concurrently, so a probe takes
about as long as its slowest check, at most the largest limit. A sync check
runs on the event loop and cannot be interrupted: keep it quick, and write
any check that waits on I/O as ``async def``.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from ..utils.logging import redact_secrets

logger = logging.getLogger(__name__)

CheckCallable = Callable[[], bool | Awaitable[bool]]

# Time limit of a check registered without its own (seconds). Below the
# 3 s probe timeout the deployment examples use, so a hanging check makes
# the probe fail (503) instead of time out.
DEFAULT_CHECK_TIMEOUT_S = 2.0
# Largest time limit a check may be registered with (seconds).
MAX_CHECK_TIMEOUT_S = 60.0
# A check that keeps failing for the same reason is logged again after this
# many seconds, not on every probe.
FAILURE_LOG_REPEAT_S = 600.0


def _checked_timeout(name: str, timeout: float | None) -> float:
    if timeout is None:
        return DEFAULT_CHECK_TIMEOUT_S
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or not 0 < timeout <= MAX_CHECK_TIMEOUT_S
    ):
        raise ValueError(
            f"Health check {name!r}: timeout must be a number of seconds "
            f"greater than 0 and at most {MAX_CHECK_TIMEOUT_S:g}, got {timeout!r}"
        )
    return float(timeout)


def _seconds(value: float) -> str:
    return f"{value:g}s"


@dataclass(frozen=True)
class CheckResult:
    """Result of running a single named check."""

    name: str
    healthy: bool
    detail: str = ""

    def to_response_value(self) -> str:
        if self.healthy:
            return "healthy"
        return f"unhealthy: {self.detail}" if self.detail else "unhealthy"


class HealthCheckRegistry:
    """In-memory registry of named health checks."""

    def __init__(self) -> None:
        self._checks: dict[str, tuple[CheckCallable, float]] = {}
        # Per failing check: its last reported reason and when it was
        # logged (loop time), so a check that keeps failing the same way
        # is logged once per state change (and again every
        # FAILURE_LOG_REPEAT_S), not once per probe.
        self._failing: dict[str, tuple[str, float]] = {}

    def register(self, name: str, check: CheckCallable, timeout: float | None = None) -> None:
        """Register ``check`` under ``name``.

        ``timeout``: seconds the check may run before it is cancelled and
        reported as unhealthy (default ``DEFAULT_CHECK_TIMEOUT_S``, 2 s;
        greater than 0, at most ``MAX_CHECK_TIMEOUT_S``, 60 s).
        """
        if not name or not isinstance(name, str):
            raise ValueError("Health check name must be a non-empty string")
        if name in self._checks:
            raise ValueError(f"Health check already registered: {name!r}")
        self._checks[name] = (check, _checked_timeout(name, timeout))

    def names(self) -> list[str]:
        return list(self._checks)

    async def run_all(self) -> list[CheckResult]:
        """Run every check concurrently, each within its time limit; the
        results keep the registration order."""
        if not self._checks:
            return []
        return list(
            await asyncio.gather(
                *(
                    self._run_one(name, check, timeout)
                    for name, (check, timeout) in self._checks.items()
                )
            )
        )

    async def _run_one(
        self, name: str, check: CheckCallable, timeout: float = DEFAULT_CHECK_TIMEOUT_S
    ) -> CheckResult:
        limit = None
        try:
            value = check()
            if inspect.isawaitable(value):
                async with asyncio.timeout(timeout) as limit:
                    value = await value
        except asyncio.CancelledError:
            raise
        except TimeoutError as e:
            if limit is None or not limit.expired():
                # The check raised TimeoutError itself (its own client's
                # timeout): reported like any other exception.
                return self._raised(name, e)
            self._report_failure(
                name,
                f"timed out after {_seconds(timeout)}",
                "Health check %r did not finish within %s; reported as unhealthy",
                (name, _seconds(timeout)),
            )
            return CheckResult(
                name=name, healthy=False, detail=f"timed out after {_seconds(timeout)}"
            )
        except Exception as e:  # noqa: BLE001 — surface any check error as unhealthy
            return self._raised(name, e)
        try:
            # The truth value of the result is evaluated here, inside the
            # protection: a result whose __bool__ raises (a numpy array of
            # more than one element, a pandas Series: "The truth value of
            # an array ... is ambiguous") is a failing check like one that
            # raised, not an HTTP 500 with a traceback on every probe.
            healthy = bool(value)
        except Exception as e:  # noqa: BLE001
            return self._raised(name, e)
        if healthy:
            self._report_recovery(name)
        else:
            # A falsy result is not logged (the probe's body names the
            # check), but it ends the logged failure: a later exception is
            # logged again.
            self._failing.pop(name, None)
        return CheckResult(name=name, healthy=healthy)

    def _raised(self, name: str, error: BaseException) -> CheckResult:
        """The result for a check that raised ``error``. The exception's
        text comes from code the SDK does not control and may quote a
        secret (a connection string, a URL with a password), so it is
        masked (``redact_secrets``) in the log record and in the verbose
        probe body alike."""
        try:
            text = str(error)
        except Exception:  # noqa: BLE001 — an exception whose __str__ fails
            text = "<no message: its text could not be read>"
        detail = redact_secrets(f"{type(error).__name__}: {text}")
        self._report_failure(name, detail, "Health check %r raised %s", (name, detail))
        return CheckResult(name=name, healthy=False, detail=detail)

    def _report_failure(self, name: str, reason: str, message: str, args: tuple) -> None:
        """One WARNING when check ``name`` starts failing, or fails for
        another ``reason`` than the one last logged; while it keeps failing
        the same way, again only every ``FAILURE_LOG_REPEAT_S`` (a probe
        every few seconds would otherwise repeat the record, and anything
        its text holds, thousands of times a day)."""
        now = _loop_time()
        last = self._failing.get(name)
        if last is not None and last[0] == reason and now - last[1] < FAILURE_LOG_REPEAT_S:
            return
        self._failing[name] = (reason, now)
        if last is not None and last[0] == reason:
            message += " (still failing)"
        logger.warning(message, *args, extra={"health_check": name})

    def _report_recovery(self, name: str) -> None:
        """One INFO when a check whose failure was logged passes again."""
        if self._failing.pop(name, None) is not None:
            logger.info("Health check %r passes again", name, extra={"health_check": name})


def _loop_time() -> float:
    try:
        return asyncio.get_running_loop().time()
    except RuntimeError:  # pragma: no cover - checks always run on a loop
        import time

        return time.monotonic()
