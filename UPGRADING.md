# Upgrading

How to upgrade `kbm-ledsas-sdk` safely between releases. The public API stays
additive and the AMQP wire contract stays stable within `0.x` (see
[COMPATIBILITY.md](COMPATIBILITY.md)), so most upgrades are drop-in. Any behavioral
change is documented here and in [CHANGELOG.md](CHANGELOG.md).

## Before any upgrade

1. **Pin the exact version** you are moving to (for example
   `kbm-ledsas-sdk==0.3.3`) and upgrade deliberately.
2. **Read the section below** for your target version, alongside the matching
   [CHANGELOG.md](CHANGELOG.md) entry.
3. **Run your test suite** against the new version in a non-production environment
   before rolling out.

---

## Upgrading to 0.3.3 (from 0.3.2)

**Type:** backwards-compatible — no code changes required. The public API is
unchanged and the AMQP envelope + topology are identical to 0.3.2. There is **one
behavioral change**, and it only affects a *misconfigured* reply setup.

### Behavioral change: unroutable replies now dead-letter the command

Responses (and status updates) are now published with the AMQP **mandatory** flag.

- **Before (0.3.2):** if a caller's `reply_to` exchange existed but had **no queue
  bound** for the `response` routing key, the broker silently discarded the
  response and the command was acknowledged as if it had been delivered — a silent
  data loss.
- **Now (0.3.3):** the broker returns the unroutable message, `send_response()`
  reports the failure, and the command is NACKed to the dead-letter queue with one
  clean ERROR line (and a new `reply_unroutable_failures` counter). The failure is
  now **visible** instead of silent.

Status updates remain best-effort: an unroutable status logs a single WARNING and
processing continues.

### Are you affected?

You are affected **only if** a caller sends commands with `reply_to` set to an
exchange that **exists but has no bound reply queue**. If your callers either leave
`reply_to` empty (fire-and-forget) or correctly bind a reply queue, you will see
**no difference** — replies are delivered exactly as before.

Signs you might be affected: callers that appeared to "work" but never actually
received responses, or commands that completed while their replies seemed to vanish.

### What to do

- **Recommended:** bind a **durable, non-auto-delete** queue to the reply exchange
  for routing key `response`. See
  [`examples/hello_world_service/scripts/send_hello.py`](examples/hello_world_service/scripts/send_hello.py)
  for a runnable caller that does exactly this. An `auto_delete` reply queue
  disappears when its consumer disconnects and turns every in-flight reply
  unroutable, so declare it `durable=True, auto_delete=False`.
- **Or**, for commands that don't need a response, set `reply_to` to `""`
  (fire-and-forget) — the SDK skips the reply entirely.

A correctly-wired service and caller need **no changes**.

### Also in 0.3.3 (no action needed)

- **`reply_unroutable_failures`** counter on the transport, so operators can tell
  "reply exchange missing" apart from "reply queue missing/unbound".
- The per-message **retry counter is now size-bounded** (oldest-entry eviction at
  10,000 tracked messages) — a memory-growth fix for long-running, multi-replica
  deployments. No configuration change.

---

## Upgrading from a clone / source install

If you install from source (`pip install -e .`), check out the target release,
reinstall, and re-run your tests:

```bash
git fetch --tags
git checkout v0.3.3
pip install -e .
pytest
```
