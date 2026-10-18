"""A service written against the documented public API, checked with
``mypy --strict`` in CI (``mypy --config-file tests/typing/mypy.ini``).

It is never run: it exists so that a change to a public signature or type
that breaks a strictly typed consumer fails CI. Keep it to documented calls
(README, docs/SDK_API_REFERENCE.md).
"""

from __future__ import annotations

import enum
from typing import Any

from kbm_ledsas_sdk import ExecutionContext, ServiceApp, errors, json_log_formatter
from kbm_ledsas_sdk.models import BlobRef

app = ServiceApp(service_name="typed-consumer")
formatter = json_log_formatter()


class Stage(enum.Enum):
    DOWNLOADING = "downloading"
    DONE = "done"


@app.handler("ProcessData")
async def process_data(ctx: ExecutionContext, payload: dict[str, Any]) -> dict[str, Any]:
    uri = payload.get("uri")
    if not isinstance(uri, str):
        raise errors.Permanent("Validation: uri missing", user_message="uri is required")
    ref = BlobRef.from_uri(uri)
    container: str = ref.container
    path: str = ref.path
    version_id: str | None = ref.version_id

    await ctx.emit_status(Stage.DOWNLOADING, 0.1)
    await ctx.emit_status("downloading", 0.2, note="started")
    raw: bytes = await ctx.blob.download_bytes(uri)
    text: str = await ctx.blob.download_text(ref)
    data = await ctx.blob.download_json(uri)
    total = 0
    async for chunk in ctx.blob.download_stream(uri, chunk_size=1024 * 1024):
        total += len(chunk)

    key = ctx.idempotency_key
    out: BlobRef = await ctx.blob.upload_json(
        container=container, obj=[1, 2, 3], path=f"list-{key}.json", overwrite=True
    )
    await ctx.blob.upload_json(
        container=container, obj={"n": total}, path=f"dict-{key}.json", overwrite=True
    )
    await ctx.blob.upload_bytes(
        container=container, data=bytearray(raw), path=f"b-{key}.bin", overwrite=True
    )
    await ctx.blob.upload_bytes(container=container, data=raw, overwrite=True)
    await ctx.blob.upload_text(container=container, text=text, path=f"t-{key}.txt", overwrite=True)
    await ctx.emit_status(Stage.DONE, 1.0)
    ids: tuple[str, str, str] = (ctx.correlation_id, ctx.message_id, ctx.trace_id)
    if data is None:
        raise errors.Retryable("storage not ready")
    return {"result_uri": out.uri, "path": path, "version": version_id, "ids": list(ids)}


@app.liveness_check("worker_alive")
def worker_alive() -> bool:
    return True


@app.readiness_check("model_loaded", timeout=2.0)
async def model_loaded() -> bool:
    return True


if __name__ == "__main__":
    app.run()
