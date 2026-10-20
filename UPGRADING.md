# Upgrading

What to check or change when you upgrade `kbm-ledsas-sdk`. Each change is described in full in
[CHANGELOG.md](CHANGELOG.md) and the [API reference](docs/SDK_API_REFERENCE.md).

## Upgrading to 0.3.6 (from 0.3.3)

0.3.4 and 0.3.5 were not released (numbers skipped): upgrade straight from 0.3.3. No public method
or parameter was removed or renamed, and the AMQP envelope (schema `1.0`) and topology are
unchanged, but some behaviour changes and input that was never valid is now refused.

### Checklist

1. Upgrade with `pip install -U --upgrade-strategy eager kbm-ledsas-sdk==0.3.6` (a plain `-U`
   keeps an installed `aio-pika` 9.x), and raise any pin of `cryptography` below 50.0.0,
   `aiohttp` below 3.14.4 or `azure-core` below 1.38.0.
2. Import your service module: handler registration errors now appear there (see "Handlers").
3. Start the service against your configuration: invalid settings now stop the start with one
   `Configuration error` line (see "Configuration").
4. In your callers, stop at the first reply and allow for the retries in the reply timeout
   (see "Callers").
5. Move log alerts from `Uncaught exception in handler` to the `Max retries (N) exceeded` ERROR,
   and update log parsers for the new timestamps (see "Operations").
6. Run your tests against 0.3.6 before you roll it out.

### Callers

- **One reply per command.** A retried attempt no longer replies; the one reply comes when the
  command has finished. Wait at least 30 seconds for a handler that answers in a few seconds
  (the default 3 retries add 7 to 10 seconds of backoff); for longer handlers allow the handler's
  run time × (`KBM_LEDSAS_MAX_RETRIES` + 1) plus the backoff. Unreachable storage adds about 7.5
  seconds per attempt (the storage client's own retries): about 40 seconds with the defaults.
- **Envelopes are checked strictly.** `sent_at` / `deadline` must be ISO 8601 text, `priority` an
  integer, and `schema_version` / `message_version` ASCII digits, 1 to 4 on each side of the dot.
  A `reply_to` naming a `cmd.…` or `dlq.…` exchange, or a reserved broker exchange (`amq.…`,
  such as `amq.topic`), is refused; declare a reply exchange of your own. Such commands are
  dead-lettered before the handler runs. `"reply_to": null` now means fire-and-forget.

### Handlers

- **Registration is checked when your module is imported.** The command name must match
  `^[A-Za-z][A-Za-z0-9_]*$` (at most 64 characters), the version must look like `1.0`, and the
  handler must be `async def handler(ctx, payload)`. A refusal ends in one of:

  ```text
  ValueError: Invalid command name 'Process-CSV': use a letter followed by letters, digits or underscores (at most 64 characters), e.g. 'ProcessDataset' or 'process_data'. ...
  ValueError: Invalid message version '1' for ProcessCSV: use '<major>.<minor>' digits, e.g. '1.0'.
  TypeError: Handler for ProcessCSV v1.0 must be an async function ('async def'): ...
  TypeError: Handler for ProcessCSV v1.0 must accept two positional arguments: the SDK calls it with (ctx, payload), but it takes 1 positional argument.
  ```

- **Message models and `BlobRef` are read-only.** `Envelope`, `Command`, `Response`, `Status` and
  `BlobRef` are immutable: assigning a field raises `pydantic.ValidationError` ("Instance is
  frozen"); in a handler the command is then replied `Permanent` (not retried), with an ERROR
  naming `model_copy(update=...)`. Use `model_copy(update={"reply_to": ...})` or build a new
  object.
- **`Command`, `Response` and `Status` check their envelope's `type`:** `envelope.type is
  'command', not 'response'`. Copy the envelope with `model_copy(update={"type": "response"})`.
- **Results.** Return a JSON-serializable `dict` (or nothing: `None` replies `{}`). Any returned
  dict is a success, even with an `"error"` key; raise `errors.Permanent` or `errors.Retryable` to
  fail. A result with no JSON form, with `bytes` (return `data.decode()`), or larger than
  `KBM_LEDSAS_MAX_PAYLOAD_BYTES` (return a blob reference) is replied `Permanent`, not retried.
- **Errors that cannot succeed on a retry are not retried** (a storage 4xx other than 408/429,
  an invalid blob URI, content that is not JSON or UTF-8, invalid SDK-call arguments). The
  exception types your handler sees are unchanged.

### Blob storage

- **`BlobRef(uri=..., version_id=...)` raises** (0.3.3 ignored `version_id`): put the version in
  the URI, `azblob://<container>/<path>?versionId=<version>`, and read it from `.version_id`.
- **Blob URIs and upload paths follow the storage naming rules** (`ValueError` otherwise). Parse
  URIs with `BlobRef.from_uri(uri)`; never split the string yourself (a versioned blob's URI ends
  in `?versionId=...`). Pass text to `upload_text`, not `upload_bytes`; pass `overwrite` as a bool.
- **`download_json` and `upload_json` take only a JSON object or array.** A scalar JSON root makes
  `download_json` raise `ValueError` (read it with `download_text`); `upload_json` raises
  `TypeError` for anything but a `dict` or a `list` (write it with `upload_text`).
- **New, optional `KBM_LEDSAS_ALLOWED_CONTAINERS`.** Unset, nothing changes. Set to container names
  (`input,results`), it limits every `ctx.blob` call to them; any other container raises
  `errors.ContainerNotAllowed` (`from kbm_ledsas_sdk import errors`; a `PermissionError`, replied
  `Permanent`) before anything is sent to storage.

### Configuration

- **The broker URL** must be `amqp://` or `amqps://` with a host and percent-encoded credentials.
  A URL that sets `no_verify_ssl` to any value except `0` (the AMQP client library then skips the
  TLS certificate check) is refused unless `KBM_LEDSAS_ALLOW_INSECURE_AMQP=1`; to trust a private
  certificate authority use `?cafile=<path>` instead.
- **New `KBM_LEDSAS_HEARTBEAT` (default `60` seconds**, `0` to `600`, `0` off). 0.3.3 set none of
  its own, so the AMQP client library's default of 60 seconds applied: nothing changes unless you
  set it. An invalid `?heartbeat=` in the broker URL (not a whole number from `0` to `600`) is now
  refused at start. A dead broker is noticed within 183 to 213 seconds at the default.
- **Blocking handlers.** A handler that blocks the event loop longer than about two heartbeat
  intervals loses the connection, and a redelivery after a lost connection now counts toward
  `KBM_LEDSAS_MAX_RETRIES` (the command is dead-lettered once they are spent). Run blocking work
  with `await asyncio.to_thread(...)`.
- **Other settings.** Boolean settings accept only boolean values (`true` / `false`, `1` / `0`,
  ...); the storage connection string needs a usable `AccountKey` or `SharedAccessSignature`; a
  health port in use stops the start (give each service on a host its own
  `KBM_LEDSAS_HEALTH_PORT`); `KBM_LEDSAS_SERVICE_NAME` is an optional override of
  `ServiceApp(service_name=...)`; new `KBM_LEDSAS_CONNECT_TIMEOUT` (default 15 seconds).

### Operations

- **Logs.** A retried failure logs one WARNING per attempt (a traceback only on the last): alert
  on the `Max retries (N) exceeded` ERROR. Timestamps are UTC ISO 8601 with milliseconds and a
  `Z`; the text format adds `key=value` fields.
- **A consumer the broker cancels is noticed:** one ERROR (`The AMQP broker cancelled the command
  consumer on <queue> ...`), `/ready` answers `503`, and the SDK declares the queue and consumes
  again (one INFO `Consuming commands from <queue> again`). `/ready` also answers `503` during a
  broker outage and while the service stops.
- **Configured secrets are masked** (`***`): the broker password and the storage `AccountKey`
  and SAS, in log records and in the texts the SDK sends to callers (error messages,
  `user_message`, status `stage` / `note`); a handler's result is sent as returned. `repr()` of
  `SDKConfig.rabbitmq_url` / `blob_conn_string` is `'**redacted**'`; a test that expected a
  secret in the output now sees the mask.
- **`deployment_id`** is read as before, from `KBM_LEDSAS_DEPLOYMENT_ID`, or from its older alias
  `KBM_LASTCOMMITID` when `KBM_LEDSAS_DEPLOYMENT_ID` is unset or blank. The SDK sets neither.
- **Long handlers.** For a handler that can run longer than 30 minutes, have the broker's
  `consumer_timeout` raised.

### Containers

- **Build `FROM ghcr.io/kebormed/kbm-ledsas-base-direct:0.3.6`**, which has the SDK installed; never
  list `kbm-ledsas-sdk` in your `requirements.txt`. See the
  [README](README.md#container-base-image). (`pip install kbm-ledsas-sdk==0.3.6` on any Python
  3.11+ image still works.)
- **End your `Dockerfile` with the numeric `USER 1000:1000`** (the base image's `kbmuser`), as
  the example does: Kubernetes admits a pod with `runAsNonRoot: true` and no `runAsUser` only
  for a numeric user. Or set `runAsUser: 1000` in the pod.

## Older releases

For 0.3.3 (from 0.3.2), see the
[0.3.3 upgrade guide](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.3/UPGRADING.md).

## From a clone

```bash
git fetch --tags
git checkout v0.3.6
pip install -e ".[dev]"
pytest -m "not integration"
```
