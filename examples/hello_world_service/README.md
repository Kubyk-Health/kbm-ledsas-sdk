# hello-world — a minimal LEDSAS service

The smallest possible LEDSAS service: it answers every `SayHello` command with
`{"greeting": "hello <name>"}` (default `"hello world"`). Use this folder as the starting
point for your own service.

The service talks straight to **RabbitMQ** and **Azure Blob Storage** (Azurite locally),
configured entirely through `KBM_LEDSAS_*` environment variables. Its container image
builds `FROM` the LEDSAS base image `ghcr.io/kebormed/kbm-ledsas-base-direct`, which
comes with the SDK and its runtime dependencies installed (the image tag is the SDK
version); for local development you install the same SDK from PyPI into a venv.

## Layout

```
hello_world_service/
├── main.py                          # the entire service (one handler)
├── scripts/send_hello.py            # test caller: publishes SayHello, prints the reply
├── tests/test_hello_world.py        # unit tests (no broker / no Azurite needed)
├── conftest.py                      # pytest rootdir marker
├── requirements.txt                 # this service's own deps (none yet; never the SDK)
├── Dockerfile                       # one stage, FROM the LEDSAS base image
├── .env.example                     # local development environment
└── deploy/local/docker-compose.yml  # RabbitMQ + Azurite on loopback
```

## Run it locally

```bash
# 1. Start the local infrastructure (RabbitMQ + Azurite).
cd deploy/local && docker compose up -d && cd ../..

# 2. Create a venv and install the SDK. Standalone, install it from PyPI:
python3.11 -m venv .venv     # any Python 3.11+, e.g. python3.12
.venv/bin/pip install kbm-ledsas-sdk==0.3.5
#    Inside a clone of this repository you can install the SDK from source
#    instead (../.. is the repository root): .venv/bin/pip install -e ../..

# 3. Configure the environment.
cp .env.example .env

# 4. Run the service, with .env loaded for this command only (a subshell;
#    set -a exports everything `.` reads), so the values do not stay in
#    your shell.
(set -a; . ./.env; set +a; exec .venv/bin/python main.py)

# 5. In a SECOND terminal, the same way:
(set -a; . ./.env; set +a; exec .venv/bin/python scripts/send_hello.py Ada)
# === Response ===
# greeting: hello Ada

# 6. Health endpoints (service running):
curl http://127.0.0.1:8090/health

# 7. Shut down: Ctrl+C (or SIGTERM — the SDK drains and exits in <1 s), then:
cd deploy/local && docker compose down -v
```

## Tests

No broker or Azurite needed:

```bash
.venv/bin/pip install pytest
.venv/bin/pytest tests/ -v
```

## Container image

```bash
docker build -t hello-world-service:dev .
```

Provide the `KBM_LEDSAS_*` environment variables at runtime (see `.env.example`). Inside
a container, `127.0.0.1` is the container itself: point `KBM_LEDSAS_RABBITMQ_URL` and the
`BlobEndpoint` of `KBM_LEDSAS_BLOB_CONN_STRING` at addresses the container can reach.

### Base image

The `Dockerfile` is a single stage on top of the LEDSAS base image
`ghcr.io/kebormed/kbm-ledsas-base-direct:<SDK version>` (Python 3.11 on Alpine Linux),
which comes with the SDK and its runtime dependencies installed:

- **The tag is the SDK version.** `FROM ghcr.io/kebormed/kbm-ledsas-base-direct:0.3.5`
  runs SDK 0.3.5; change the tag to move the service to another SDK release. The tag is
  written literally so that a dependency bot (for example Dependabot's `docker`
  ecosystem) can propose the next one. `latest` moves with every release, so do not
  build on it. For a fully reproducible build, pin the digest as well:
  `FROM ghcr.io/kebormed/kbm-ledsas-base-direct:0.3.5@sha256:<digest>`.
- **Do not install the SDK again.** Never list `kbm-ledsas-sdk` in `requirements.txt`,
  and never copy an SDK wheel into the image. The base image pins its SDK version, so a
  requirement for a different version fails the build instead of silently replacing it.
  The pin is a pip constraints file that the base image names in its `PIP_CONSTRAINT`
  (and `UV_CONSTRAINT`) environment variable; a pip error that says `(constraint)` comes
  from it.
- **User and paths.** The image runs as the non-root user `kbmuser` (uid 1000) in
  `/app`. The `Dockerfile` switches to `USER root` only around `pip install` (the base
  image turns off per-user site-packages, so packages go into the system site-packages)
  and the final pip-removal step, then back to `USER kbmuser`. It makes `/app` owned by
  root and copies `main.py` without `--chown`, so the running service can neither change
  nor replace its code; the service writes no files in `/app` (use `/tmp` for scratch
  files). Its `LABEL` instruction clears the base image's labels (title, version,
  license, links): add your service's own there.
- **pip is removed at the end of the build.** pip is only needed while building the
  image, never at runtime, so the last root step of the `Dockerfile` uninstalls it. Keep
  every `pip` command above that step (below it pip is gone, so a `pip` command fails the
  build), and install into the base image's Python, not into a virtual environment: the
  step's check fails the build if pip is still installed anywhere in the image. (Python's
  own bundled installer archive under `ensurepip` stays in the image; it is inert as long
  as nothing runs `ensurepip` or creates a virtual environment, which a service image
  must not do.) A service must not install packages while it runs: install everything it
  needs at build time, through `requirements.txt`.
- **One stage.** Keep a single `FROM` line, on this base image.

#### Third-party packages on Alpine (musl libc)

Alpine uses musl libc instead of glibc. pip installs a package as a binary only if it is
pure Python or publishes a `musllinux` wheel; otherwise pip tries to compile it from
source, which usually fails.

| Install cleanly (pure Python, or `musllinux` wheels for amd64 and arm64) | No `musllinux` wheels: do not install on this base image |
|---|---|
| numpy, pandas, scipy, pillow, scikit-image, pyarrow, pydicom | torch, onnxruntime, tensorflow, opencv-python(-headless), scikit-learn, SimpleITK, python-gdcm, pylibjpeg-* |

(Checked on PyPI for Python 3.11 in October 2026. Wheel availability changes between
releases, so re-check a package before you depend on it.)

**PyTorch index trap.** `pip install torch --index-url https://download.pytorch.org/whl/cpu`
can look like it worked: some older wheels on that index carry the generic
`linux_x86_64` tag, which pip on Alpine accepts, but they are built against glibc, so
`import torch` fails afterwards.

## Updating the SDK version

1. Change the base image tag on the `FROM` line of the `Dockerfile` and the
   `kbm-ledsas-sdk==` pin in "Run it locally".
2. Re-run the tests and one local round-trip (steps above).
