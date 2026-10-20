# Contributing to the KeborMed LEDSAS SDK

Thanks for contributing! Report bugs and request features in
[GitHub Issues](https://github.com/Kubyk-Health/kbm-ledsas-sdk/issues); report security issues
privately ([SECURITY.md](SECURITY.md)). By participating you agree to the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Development setup

Requires **Python 3.11+**.

```bash
git clone https://github.com/Kubyk-Health/kbm-ledsas-sdk.git
cd kbm-ledsas-sdk
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"          # pytest, ruff, black, mypy, build, twine
```

## Running the tests

```bash
pytest -m "not integration"   # the unit tests: no broker or cloud service needed
pytest                        # everything; an integration test skips itself when
                              # RabbitMQ or Azurite does not answer
```

The integration tests (`tests/integration/`, marker `integration`) need a real RabbitMQ and
Azurite. Start the example's local stack (`--wait` returns once both accept connections), then
run them and check that none was skipped:

```bash
docker compose -f examples/hello_world_service/deploy/local/docker-compose.yml up -d --wait
pytest -m integration -rs
```

They use `amqp://guest:guest@127.0.0.1:5672/` unless `LEDSAS_TEST_RABBITMQ_URL` names another
broker, and Azurite on `127.0.0.1:10000` unless `LEDSAS_TEST_BLOB_CONN_STRING` names another
storage account; never point them at a broker or account you share (the tests delete exchanges
and queues). With the stack's ports moved by `HELLO_WORLD_AMQP_PORT` and `HELLO_WORLD_BLOB_PORT`:

```bash
HELLO_WORLD_AMQP_PORT=25672 HELLO_WORLD_BLOB_PORT=20010 docker compose -f examples/hello_world_service/deploy/local/docker-compose.yml up -d --wait
export LEDSAS_TEST_RABBITMQ_URL="amqp://guest:guest@127.0.0.1:25672/"
export LEDSAS_TEST_BLOB_CONN_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:20010/devstoreaccount1;"
pytest -m integration -rs
```

The example service has its own tests, run from its directory:

```bash
cd examples/hello_world_service && pytest -W error::pytest.PytestConfigWarning tests/
```

## Pull request checks

Every pull request runs these checks; all must pass except the advisory `mypy src`:

| Check (workflow) | Local command |
|------------------|---------------|
| Lint (`ci.yml`) | `ruff check .` (auto-fix most findings: `ruff check --fix .`) |
| Formatting (`ci.yml`) | `black --check .` (reformat: `black .`) |
| Typing of the SDK source (`ci.yml`, **advisory**) | `mypy src` |
| Typing of the public modules (`ci.yml`) | `MYPYPATH=src mypy --strict --follow-imports=silent src/kbm_ledsas_sdk/models src/kbm_ledsas_sdk/runtime/context.py src/kbm_ledsas_sdk/blob/operations.py src/kbm_ledsas_sdk/blob/direct_operations.py src/kbm_ledsas_sdk/utils/logging.py src/kbm_ledsas_sdk/app.py` |
| Typing of a strict consumer of the public API (`ci.yml`) | `MYPYPATH=src mypy --config-file tests/typing/mypy.ini tests/typing/typed_consumer.py` |
| The documented calls under `mypy --strict` (`ci.yml`) | `LEDSAS_REQUIRE_MYPY=1 python -m pytest -q tests/test_typed_consumer.py` |
| Unit tests on Python 3.11, 3.12, 3.13, and on 3.11 with `aio-pika` 9.x (`ci.yml`) | `pytest -m "not integration" -W error` |
| Example service tests (`ci.yml`) | see "Running the tests" |
| Integration tests against RabbitMQ and Azurite, none skipped (`ci.yml`) | see "Running the tests" |
| Build twice, identical wheels, metadata check (`ci.yml`) | see "Reproducible build" below |
| DCO sign-off by the commit author (`dco.yml`) | `git log --no-merges --format='%an <%ae>%n%(trailers:key=Signed-off-by)' main..` |
| Secret scans of the tree and the PR's commits (`secret-scan.yml`) | see "Secret scans" below (Docker) |
| CodeQL (`codeql.yml`) | runs on GitHub only |

### Reproducible build

```bash
rm -rf dist build
(umask 022; SOURCE_DATE_EPOCH="$(git log -1 --format=%ct 2>/dev/null || echo 315532800)" python -m build)
twine check dist/*
```

It also works in a release's source archive (`|| echo 315532800` gives a fixed
`SOURCE_DATE_EPOCH` outside git). Only the wheel is reproducible: two builds with the same
`SOURCE_DATE_EPOCH` and `umask 022` give identical wheels; the sdist is not byte-identical.

### Secret scans

The same scans as the `Secret scan` workflow (its `GITLEAKS_IMAGE` and `TRUFFLEHOG_IMAGE`),
from the repository root, with Docker:

```bash
GITLEAKS_IMAGE=ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f
TRUFFLEHOG_IMAGE=ghcr.io/trufflesecurity/trufflehog:3.99.2@sha256:47a84bc18a0d04a165498bbbd3bacfd84176661cbc91f8e0f85467f6a771e99a
docker run --rm -v "$PWD:/repo" "$GITLEAKS_IMAGE" dir /repo --config /repo/.gitleaks.toml --redact -v
docker run --rm -v "$PWD:/repo" "$GITLEAKS_IMAGE" git /repo --config /repo/.gitleaks.toml --redact -v --log-opts="main..HEAD"
docker run --rm -v "$PWD:/repo" "$TRUFFLEHOG_IMAGE" filesystem /repo --only-verified --fail --no-update
docker run --rm -v "$PWD:/repo" "$TRUFFLEHOG_IMAGE" git file:///repo --since-commit main --only-verified --fail --no-update
```

### Typing

`mypy src` still reports errors in some internal modules, so it does not block. The public
modules are strict-clean and that check blocks, as does the typed consumer: when you change a
public signature, extend `tests/typing/typed_consumer.py` (and `tests/test_typed_consumer.py`
for a documented call).

### Guidelines

- Keep public APIs type-hinted (the package ships `py.typed`) and match the surrounding style.
- Add or update tests for any behavior change.
- Add a user-facing change to [CHANGELOG.md](CHANGELOG.md) under `## [Unreleased]`.

## Developer Certificate of Origin (DCO)

We use the [Developer Certificate of Origin](https://developercertificate.org/) instead of a
CLA. **Every commit must be signed off** (`-s`):

```bash
git commit -s -m "Fix retry backoff off-by-one"
```

This appends a line to your commit message:

```
Signed-off-by: Your Name <your.email@example.com>
```

CI checks every commit of a PR except merge commits. To fix a missing sign-off:

```bash
git commit --amend -s --no-edit          # last commit
git rebase --signoff HEAD~N              # last N commits
```

## Pull request process

1. Fork the repository and branch from `main`.
2. Make your change with tests, in focused, signed-off commits.
3. Run the "Pull request checks" locally (at least `ruff`, `black`, the typed consumer and `pytest`).
4. Open a PR against `main` and fill in the template. A maintainer ([CODEOWNERS](CODEOWNERS))
   reviews, usually within a few business days; we squash-merge.

## Releases (maintainers)

A release is a GitHub Release on a `v<version>` tag of a commit on `main`. The `Release`
workflow checks the tag (format, on `main`, equal to `pyproject.toml` and
`kbm_ledsas_sdk.__version__`), runs the tests, builds with the hash-pinned toolchain
(`.github/release-requirements.txt`), checks the built files' SHA-256 before and after the upload
to PyPI (Trusted Publishing), and attaches the files and an SBOM to the Release. It relies on a
ruleset that lets only maintainers create `v*` tags, an environment `pypi` with required
reviewers and a `v*` tag rule, and an environment `testpypi` limited to `main`.

Before tagging:

1. Change every `blob/v<previous>/` and `tree/v<previous>/` link in `README.md`,
   `pyproject.toml` and `src/` to `v<version>`; the `Release` workflow refuses a tag whose links
   name `main` or another tag.
2. Set the `[<version>]` date in `CHANGELOG.md` to the release day (UTC).
3. Rehearse with a manual run of the workflow on `main` (TestPyPI, a `.devN` version).

### Pins that Dependabot does not update

Dependabot updates `pyproject.toml`, the Actions, `.github/*-requirements.txt` (regenerate
them with the `uv pip compile` line at the top of each `.in` file) and the example's
requirements and compose images. Update these by hand, tag plus digest
(`docker buildx imagetools inspect <image>:<tag>`):

- `GITLEAKS_IMAGE` and `TRUFFLEHOG_IMAGE` in `.github/workflows/secret-scan.yml` and under
  "Secret scans" above;
- the RabbitMQ service image and `AZURITE_IMAGE` of the `integration` job in
  `.github/workflows/ci.yml`, and the two `docker run` images of the README quickstart (the same
  tags as the example's `docker-compose.yml`; a test checks it);
- `setuptools==` and `wheel==` in `pyproject.toml` `[build-system]` and
  `.github/release-requirements.in` (kept equal; a test checks it);
- the base image tag in `examples/hello_world_service/Dockerfile` (the SDK version).
