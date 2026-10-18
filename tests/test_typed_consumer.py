"""A strictly typed consumer of the documented API type-checks.

A consumer that makes every documented call the way the API reference shows
it (``async for`` over ``ctx.blob.download_stream()``, ``upload_json`` with a
list, ``emit_status`` with a ``str``-valued ``Enum``, a ``bytearray`` upload,
the three decorators, ``app.run()``) must pass ``mypy --strict``. The package
ships ``py.typed``, so a strict consumer sees exactly these annotations.
Errors inside the SDK's own modules are not judged here
(``--follow-imports=silent``); only the consumer is.

Needs mypy (in the ``[dev]`` extra): skipped without it, unless
``LEDSAS_REQUIRE_MYPY=1`` is set (CI sets it), which makes a missing mypy a
failure.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SEARCH_ROOT = ROOT / "src"
PACKAGE_DIR = SEARCH_ROOT / "kbm_ledsas_sdk"

_HAVE_MYPY = (
    subprocess.run([sys.executable, "-m", "mypy", "--version"], capture_output=True).returncode == 0
)


@pytest.fixture(autouse=True)
def _mypy_available():
    if not _HAVE_MYPY:
        if os.environ.get("LEDSAS_REQUIRE_MYPY") == "1":
            pytest.fail("mypy is not installed (pip install -e '.[dev]')")
        pytest.skip("mypy not installed (pip install -e '.[dev]')")


CONSUMER = textwrap.dedent("""
    from __future__ import annotations

    import enum
    from collections.abc import AsyncIterator
    from typing import Any

    from kbm_ledsas_sdk import BlobOperations, ServiceApp, errors
    from kbm_ledsas_sdk.models import BlobRef
    from kbm_ledsas_sdk.runtime.context import ExecutionContext


    class Stage(str, enum.Enum):
        DOWNLOADING = "downloading"
        UPLOADING = "uploading"


    app = ServiceApp(service_name="typed-consumer")


    async def _chunks() -> AsyncIterator[bytes]:
        yield b"a,b\\n"
        yield b"1,2\\n"


    async def _count(blob: BlobOperations, uri: str) -> int:
        total = 0
        async for chunk in blob.download_stream(uri):
            total += len(chunk)
        return total


    @app.handler("Process")
    async def process(ctx: ExecutionContext, req: dict[str, Any]) -> dict[str, Any]:
        uri = req.get("csv_uri")
        if not isinstance(uri, str):
            raise errors.Permanent("Validation: csv_uri missing", user_message="csv_uri is required")
        ref = BlobRef.from_uri(uri)
        await ctx.emit_status(Stage.DOWNLOADING, 0.1, note=Stage.DOWNLOADING)
        await ctx.emit_status("processing", 0.5, note="halfway")
        total = await _count(ctx.blob, uri)
        data: bytes = await ctx.blob.download_bytes(ref)
        text: str = await ctx.blob.download_text(uri)
        doc = await ctx.blob.download_json(uri)
        rows = [{"n": total}, {"n": len(data) + len(text)}]
        out_list: BlobRef = await ctx.blob.upload_json(
            container=ref.container, obj=rows, path=f"rows-{ctx.idempotency_key}.json", overwrite=True
        )
        out_dict: BlobRef = await ctx.blob.upload_json(
            container=ref.container, obj={"doc": doc}, path=f"doc-{ctx.idempotency_key}.json", overwrite=True
        )
        raw: BlobRef = await ctx.blob.upload_bytes(
            container=ref.container, data=bytearray(b"xyz"), path=f"raw-{ctx.idempotency_key}.bin", overwrite=True
        )
        txt: BlobRef = await ctx.blob.upload_text(
            container=ref.container, text="hello", path=f"t-{ctx.idempotency_key}.txt", overwrite=True
        )
        streamed: BlobRef = await ctx.blob.upload_stream(
            container=ref.container, stream=_chunks(), path=f"s-{ctx.idempotency_key}.csv", overwrite=True
        )
        await ctx.emit_status(Stage.UPLOADING, 1.0)
        ctx.logger.info("done", extra={"message_id": ctx.message_id, "trace_id": ctx.trace_id})
        return {
            "uris": [out_list.uri, out_dict.uri, raw.uri, txt.uri, streamed.uri],
            "correlation_id": ctx.correlation_id,
            "job_id": ctx.job_id,
        }


    @app.liveness_check("worker_alive")
    def alive() -> bool:
        return True


    @app.readiness_check("model_loaded", timeout=2.0)
    async def ready() -> bool:
        return True


    def main() -> None:
        app.run()

    """)


def _mypy(tmp_path: Path, source: str) -> subprocess.CompletedProcess:
    f = tmp_path / "consumer.py"
    f.write_text(source, encoding="utf-8")
    env = dict(os.environ, MYPYPATH=str(SEARCH_ROOT))
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--strict",
            "--follow-imports=silent",
            "--no-incremental",
            "--cache-dir",
            str(tmp_path / ".mypy_cache"),
            "--python-version",
            "3.11",
            "--config-file",
            os.devnull,
            str(f),
        ],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=env,
    )


def test_documented_calls_type_check_under_mypy_strict(tmp_path):
    proc = _mypy(tmp_path, CONSUMER)
    consumer_errors = [ln for ln in proc.stdout.splitlines() if "consumer.py" in ln]
    assert proc.returncode == 0, "\n".join(consumer_errors) or proc.stdout + proc.stderr


def test_the_check_catches_a_typing_error(tmp_path):
    """Control: the same invocation fails on a consumer with a real type error."""
    proc = _mypy(tmp_path, CONSUMER + "\n\nwrong: int = app\n")
    assert proc.returncode != 0 and "consumer.py" in proc.stdout, proc.stdout + proc.stderr


def test_package_ships_py_typed():
    assert (PACKAGE_DIR / "py.typed").is_file()
