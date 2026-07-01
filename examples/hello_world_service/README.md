# hello-world — a minimal LEDSAS service

The smallest possible LEDSAS service: it answers every `SayHello` command with
`{"greeting": "hello <name>"}` (default `"hello world"`). Use this folder as the starting
point for your own service.

The service talks straight to **RabbitMQ** and **Azure Blob Storage** (Azurite locally),
configured entirely through `KBM_LEDSAS_*` environment variables.

## Layout

```
hello_world_service/
├── main.py                          # the entire service (one handler)
├── scripts/send_hello.py            # test caller: publishes SayHello, prints the reply
├── tests/test_hello_world.py        # unit tests (no broker / no Azurite needed)
├── requirements.txt                 # depends on kbm-ledsas-sdk
├── Dockerfile                       # container image
├── .env.example                     # local development environment
└── deploy/local/docker-compose.yml  # RabbitMQ + Azurite on loopback
```

## Run it locally

```bash
# 1. Start the local infrastructure (RabbitMQ + Azurite).
cd deploy/local && docker compose up -d && cd ../..

# 2. Create a venv and install the SDK. Running inside this repo, install it
#    from source (../.. is the repo root). Standalone (SDK on PyPI), instead
#    run: .venv/bin/pip install -r requirements.txt
python3 -m venv .venv
.venv/bin/pip install -e ../..

# 3. Configure the environment (set -a exports everything `source` reads).
cp .env.example .env
set -a; source .env; set +a

# 4. Run the service.
.venv/bin/python main.py

# 5. In a SECOND terminal (same env: repeat step 3's set -a/source):
.venv/bin/python scripts/send_hello.py Ada
# === Response ===
# greeting: hello Ada

# 6. Health endpoints (service running):
curl http://127.0.0.1:8090/health

# 7. Shut down: Ctrl+C (the SDK drains and exits in <1 s), then:
cd deploy/local && docker compose down -v
```

## Tests

No broker or Azurite needed:

```bash
.venv/bin/pip install -e ../.. pytest
.venv/bin/pytest tests/ -v
```

## Container image

```bash
docker build -t hello-world-service:dev .
```

The image installs `kbm-ledsas-sdk` from PyPI (see `requirements.txt`), so it builds once the SDK is published to PyPI. Provide the
`KBM_LEDSAS_*` environment variables at runtime (see `.env.example`).
