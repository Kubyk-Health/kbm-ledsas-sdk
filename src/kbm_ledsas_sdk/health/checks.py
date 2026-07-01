"""
Health check registry: stores customer-registered callables and runs them.

Each registered check is a sync or async callable returning truthy = healthy.
Exceptions are caught and reported as unhealthy with the exception message.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

CheckCallable = Callable[[], bool | Awaitable[bool]]


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
        self._checks: dict[str, CheckCallable] = {}

    def register(self, name: str, check: CheckCallable) -> None:
        if not name or not isinstance(name, str):
            raise ValueError("Health check name must be a non-empty string")
        if name in self._checks:
            raise ValueError(f"Health check already registered: {name!r}")
        self._checks[name] = check

    def names(self) -> list[str]:
        return list(self._checks)

    async def run_all(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        for name, check in self._checks.items():
            results.append(await self._run_one(name, check))
        return results

    @staticmethod
    async def _run_one(name: str, check: CheckCallable) -> CheckResult:
        try:
            value = check()
            if inspect.isawaitable(value):
                value = await value
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("Health check %r raised %s: %s", name, type(e).__name__, e)
            return CheckResult(name=name, healthy=False, detail=f"{type(e).__name__}: {e}")
        return CheckResult(name=name, healthy=bool(value))
