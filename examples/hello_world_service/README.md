# hello-world — a minimal LEDSAS service

The smallest LEDSAS service: it answers every `SayHello` command with
`{"greeting": "hello <name>"}` (default `"hello world"`). Copy this folder to start your own
service. It is configured through `KBM_LEDSAS_*` environment variables; its image builds `FROM`
the LEDSAS base image `ghcr.io/kebormed/kbm-ledsas-base-direct`, which has the SDK installed
(the tag is the SDK version).

## Layout

```
hello_world_service/
├── main.py                          # the entire service (one handler)
├── scripts/send_hello.py            # test caller: publishes SayHello, prints the reply
├── tests/test_hello_world.py        # unit tests (no broker / no Azurite needed)
├── conftest.py, pytest.ini          # make this directory its own pytest rootdir
├── requirements.txt                 # this service's own deps (none yet; never the SDK)
├── Dockerfile                       # one stage, FROM the LEDSAS base image
├── .dockerignore                    # keeps .env, .venv, tests out of the build context
├── .env.example                     # local development environment
└── deploy/local/docker-compose.yml  # RabbitMQ + Azurite on loopback
```

## Run it locally

```bash
# 1. Start RabbitMQ + Azurite (host ports 5672, 15672, 10000; to move them, set
#    HELLO_WORLD_AMQP_PORT, HELLO_WORLD_MGMT_PORT, HELLO_WORLD_BLOB_PORT and use the same
#    ports in .env). --wait returns once both are healthy.
cd deploy/local && docker compose up -d --wait && cd ../..

# 2. A venv with the SDK (inside a clone you can use .venv/bin/pip install -e ../.. instead):
python3.11 -m venv .venv     # any Python 3.11+
.venv/bin/pip install kbm-ledsas-sdk==0.3.6

# 3. The local settings.
cp .env.example .env

# 4. The service, with .env loaded for this command only:
(set -a; . ./.env; set +a; exec .venv/bin/python main.py)

# 5. In a SECOND terminal:
(set -a; . ./.env; set +a; exec .venv/bin/python scripts/send_hello.py Ada)
# === Response ===
# greeting: hello Ada

# 6. Health (service running):
curl http://127.0.0.1:8090/health

# 7. Stop with Ctrl+C (running commands get up to KBM_LEDSAS_HANDLER_TIMEOUT,
#    30 min by default), then:
cd deploy/local && docker compose down -v
```

## Tests

No broker or Azurite needed; run them from this directory (its own pytest root):

```bash
.venv/bin/pip install pytest
.venv/bin/pytest tests/ -v
```

## Container image

```bash
docker build -t hello-world-service:dev .
```

Inside a container, `127.0.0.1` is the container itself. To run the image against the local
stack of "Run it locally", join the stack's network and use its service names:

```bash
docker run --rm --network hello-world-net --env-file .env \
  -e KBM_LEDSAS_RABBITMQ_URL="amqp://guest:guest@rabbitmq:5672/" \
  -e KBM_LEDSAS_BLOB_CONN_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;" \
  -e KBM_LEDSAS_ALLOW_INSECURE_AMQP=1 \
  -e KBM_LEDSAS_HEALTH_HOST=0.0.0.0 \
  -p 127.0.0.1:8090:8090 \
  --stop-timeout 60 \
  hello-world-service:dev
```

- **`KBM_LEDSAS_ALLOW_INSECURE_AMQP=1`** acknowledges cleartext `amqp://` to a host other than
  loopback; use it only on a local Docker network, and `amqps://` everywhere else.
- **`KBM_LEDSAS_HEALTH_HOST=0.0.0.0`** makes the health endpoints reachable through `-p`.
- **Give it time to stop:** set `--stop-timeout` (as above), `docker stop -t`, Compose
  `stop_grace_period` or Kubernetes `terminationGracePeriodSeconds` above your longest handler
  run. A command still running when the container is killed runs again after the restart.
- **Restart policy:** a failed start (for example the broker not reachable yet) exits with code 1
  and is not retried. `--rm` above suits a local try; elsewhere run the service under a restart
  policy (`docker run --restart on-failure` instead of `--rm`, Compose `restart: on-failure`,
  Kubernetes `restartPolicy`).
- **Memory:** `.env.example` sets `KBM_LEDSAS_MAX_PAYLOAD_BYTES=1048576` (1 MiB); with the SDK
  default (16 MiB) up to `KBM_LEDSAS_PREFETCH` bodies take about 4 GiB once parsed, and a start
  under a smaller memory limit logs a WARNING. Size both to the memory you give the container.
- **Health check.** The `Dockerfile` declares no `HEALTHCHECK`, so that your orchestrator's own
  probes decide. For plain Docker or Compose, add one that asks `/ready` and writes one line
  into the health log when the answer is not `200`:

  ```dockerfile
  HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-c", "import sys, urllib.request\ntry:\n    urllib.request.urlopen('http://127.0.0.1:8090/ready', timeout=4)\nexcept Exception as e:\n    sys.exit(f'not ready: {e}')"]
  ```

  Change the port when you set another `KBM_LEDSAS_HEALTH_PORT`. `/ready` answers `503`
  during a broker outage and while the service stops; Docker never restarts an unhealthy
  container itself.

### Base image

- **The tag is the SDK version:** `FROM ghcr.io/kebormed/kbm-ledsas-base-direct:0.3.6` runs SDK
  0.3.6. Pin the digest too for a reproducible build
  (`...:0.3.6@sha256:<digest>`), and never build on `latest`. Once you copy this folder into
  your own repository, a `docker` entry in its `.github/dependabot.yml` proposes the next tag:

  ```yaml
    - package-ecosystem: docker
      directory: "/"            # the folder of your Dockerfile
      schedule:
        interval: weekly
  ```

- **Do not install the SDK again:** never list `kbm-ledsas-sdk` in `requirements.txt`. The base
  image's `PIP_CONSTRAINT` file pins `kbm-ledsas-sdk==<version>` and the `cryptography>=50.0.0`
  floor, so a requirement for another version fails the build (a pip error that says
  `(constraint)`).
- **User and paths:** the image runs as `kbmuser` (uid 1000) in `/app`. The `Dockerfile` switches
  to `USER root` only to install and to remove pip, then ends with the numeric `USER 1000:1000`
  (Kubernetes `runAsNonRoot` can only verify a numeric user). `/app` and `main.py` are root-owned,
  so the service cannot change its code; write scratch files to `/tmp`. Its `LABEL` clears the
  base image's labels: add your own.
- **pip is removed at the end of the build:** keep every `pip` command above that step, and
  install into the base image's Python (not a venv); the step fails the build if pip is left
  anywhere. The base image ships without `setuptools`, `wheel` and Python's bundled installer
  under `ensurepip`, so nothing can install packages at runtime: put everything in
  `requirements.txt`.
- **One stage:** keep a single `FROM` line, on this base image.

#### Third-party packages on Alpine (musl libc)

pip installs a binary only for a pure-Python package or one with a `musllinux` wheel:

| Install cleanly (pure Python, or `musllinux` wheels for amd64 and arm64) | No `musllinux` wheels: do not install on this base image |
|---|---|
| numpy, pandas, scipy, pillow, scikit-image, pyarrow, pydicom | torch, onnxruntime, tensorflow, opencv-python(-headless), scikit-learn, SimpleITK, python-gdcm, pylibjpeg-* |

(Checked on PyPI for Python 3.11 in October 2026; re-check before you depend on a package.)
Beware `pip install torch --index-url https://download.pytorch.org/whl/cpu`: some older wheels
there install on Alpine but are built for glibc, so `import torch` fails.

## Updating the SDK version

1. Change the version everywhere this folder names it: the `FROM` tag in the `Dockerfile` (and in
   "Base image" above), the `kbm-ledsas-sdk==` pin in "Run it locally", and the pin in the
   docstring of `main.py`.
2. Re-run the tests and one local round-trip.
