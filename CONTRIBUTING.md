# Contributing to the KeborMed LEDSAS SDK

Thanks for your interest in contributing! This document explains how to set up your
environment, run the tests, and submit changes.

By participating in this project you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md).

## Ways to contribute

- **Report bugs** and **request features** via [GitHub Issues](https://github.com/Kubyk-Health/kbm-ledsas-sdk/issues).
- **Improve documentation** — even small fixes are welcome.
- **Submit code** — bug fixes and features via pull requests (see below).
- **Report security issues privately** — see [SECURITY.md](SECURITY.md); never in a public issue.

## Development setup

Requires **Python 3.11+**.

```bash
git clone https://github.com/Kubyk-Health/kbm-ledsas-sdk.git
cd kbm-ledsas-sdk

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# Editable install with the dev toolchain (pytest, pytest-asyncio, ruff, black,
# mypy, build, twine)
pip install -e ".[dev]"
```

## Running the tests

```bash
pytest -m "not integration"   # the unit tests: no broker or cloud service needed
pytest                        # everything; the integration tests skip themselves
                              # when no RabbitMQ answers (127.0.0.1:5672 by default)
```

The integration tests (`tests/integration/`, marker `integration`) run against a
real RabbitMQ. Start the example's local stack first (`--wait` returns once the
broker accepts connections; without it the tests can start too early and skip
themselves), then run them on their own and check that none was skipped:

```bash
docker compose -f examples/hello_world_service/deploy/local/docker-compose.yml up -d --wait
pytest -m integration -rs
```

They use `amqp://guest:guest@127.0.0.1:5672/` unless `LEDSAS_TEST_RABBITMQ_URL`
names another broker. If you moved the stack's AMQP port with
`HELLO_WORLD_AMQP_PORT`, point the tests at it, and never at a broker you
share (they declare and delete exchanges and queues on it):

```bash
HELLO_WORLD_AMQP_PORT=25672 docker compose -f examples/hello_world_service/deploy/local/docker-compose.yml up -d --wait
LEDSAS_TEST_RABBITMQ_URL="amqp://guest:guest@127.0.0.1:25672/" pytest -m integration -rs
```

The example service has its own tests; run them from its directory (it is its
own pytest root directory):

```bash
cd examples/hello_world_service && pytest -W error::pytest.PytestConfigWarning tests/
```

## Pull request checks

Every pull request runs the checks below; all of them must pass before it can
merge, except the advisory `mypy src`. Each line is the local command:

| Check (workflow) | Local command |
|------------------|---------------|
| Lint (`ci.yml`) | `ruff check .` (auto-fix most findings: `ruff check --fix .`) |
| Formatting (`ci.yml`) | `black --check .` (reformat: `black .`) |
| Typing of the SDK source (`ci.yml`, **advisory**) | `mypy src` |
| Typing of the public modules (`ci.yml`) | `MYPYPATH=src mypy --strict --follow-imports=silent src/kbm_ledsas_sdk/models src/kbm_ledsas_sdk/runtime/context.py src/kbm_ledsas_sdk/blob/operations.py src/kbm_ledsas_sdk/blob/direct_operations.py src/kbm_ledsas_sdk/utils/logging.py` |
| Typing of a strict consumer of the public API (`ci.yml`) | `MYPYPATH=src mypy --config-file tests/typing/mypy.ini tests/typing/typed_consumer.py` |
| The documented calls under `mypy --strict` (`ci.yml`) | `LEDSAS_REQUIRE_MYPY=1 python -m pytest -q tests/test_typed_consumer.py` |
| Unit tests on Python 3.11, 3.12, 3.13 (`ci.yml`) | `pytest -m "not integration"` |
| Example service tests (`ci.yml`) | see "Running the tests" |
| Integration tests against RabbitMQ (`ci.yml`) | see "Running the tests" |
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

This works in a clone and in a release's source archive, which is not a git
repository (there `|| echo 315532800` gives `SOURCE_DATE_EPOCH` a fixed
value). Only the wheel is reproducible: two builds of one commit
with the same `SOURCE_DATE_EPOCH` and `umask 022` give identical wheels; the
sdist is not byte-identical (its tar and gzip headers carry the build time
and your account), and a wheel built under another umask (for example `002`)
records other file modes.

### Secret scans

The `Secret scan` workflow runs gitleaks and trufflehog from the container
images it pins by digest (`GITLEAKS_IMAGE` and `TRUFFLEHOG_IMAGE` in
`.github/workflows/secret-scan.yml`). The same scans locally, from the
repository root, with Docker:

```bash
GITLEAKS_IMAGE=ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f
TRUFFLEHOG_IMAGE=ghcr.io/trufflesecurity/trufflehog:3.99.2@sha256:47a84bc18a0d04a165498bbbd3bacfd84176661cbc91f8e0f85467f6a771e99a
docker run --rm -v "$PWD:/repo" "$GITLEAKS_IMAGE" dir /repo --config /repo/.gitleaks.toml --redact -v
docker run --rm -v "$PWD:/repo" "$GITLEAKS_IMAGE" git /repo --config /repo/.gitleaks.toml --redact -v --log-opts="main..HEAD"
docker run --rm -v "$PWD:/repo" "$TRUFFLEHOG_IMAGE" filesystem /repo --only-verified --fail --no-update
docker run --rm -v "$PWD:/repo" "$TRUFFLEHOG_IMAGE" git file:///repo --since-commit main --only-verified --fail --no-update
```

`mypy src` (strict) still reports errors in some of the SDK's internal modules;
we are fixing them module by module, so it reports without blocking. The
modules behind the public API (the message models, `ExecutionContext`, the
blob operations and the log formatter) are strict-clean, and that check blocks. The typed-consumer check
blocks: a change that breaks strict typing for code that *uses* the SDK fails CI.
When you change a public signature, extend `tests/typing/typed_consumer.py` (and
the consumer in `tests/test_typed_consumer.py` when it changes a documented call).

Guidelines:

- Keep public APIs **type-hinted**; the package ships `py.typed`.
- Match the style of the surrounding code (naming, docstrings, error handling).
- Add or update tests for any behavior change.
- For a user-facing change, add an entry to [CHANGELOG.md](CHANGELOG.md) under
  `## [Unreleased]`.

## Developer Certificate of Origin (DCO)

We use the [Developer Certificate of Origin](https://developercertificate.org/) instead of a
CLA. It is a lightweight way for you to certify that you wrote, or otherwise have the right to
submit, the code you are contributing.

**Every commit must be signed off.** Add the `-s` flag when you commit:

```bash
git commit -s -m "Fix retry backoff off-by-one"
```

This appends a line to your commit message:

```
Signed-off-by: Your Name <your.email@example.com>
```

By signing off, you certify the statements in the [DCO](https://developercertificate.org/).
A CI check enforces that all commits in a PR are signed off (merge commits, such as
the one GitHub's "Update branch" button creates, are not checked). If you forget,
you can amend:

```bash
git commit --amend -s --no-edit          # last commit
git rebase --signoff HEAD~N              # last N commits
```

## Pull request process

1. **Fork** the repo and create a topic branch from `main`
   (`git checkout -b fix/retry-backoff`).
2. Make your change with tests; keep commits focused and **signed off** (`-s`).
3. Run the checks of "Pull request checks" locally (at least `ruff`, `black`,
   the typed-consumer check and `pytest`).
4. Push and open a PR against `main`. Fill in the PR template.
5. A maintainer (see [CODEOWNERS](CODEOWNERS)) will review. Address feedback by pushing
   additional commits; we squash-merge, so you don't need to squash yourself.

We aim to give an initial response within a few business days. Thanks for contributing!

## Releases (maintainers)

A release is a GitHub Release on a `v<version>` tag of a commit on `main`; the
`Release` workflow (`.github/workflows/release.yml`) checks the tag, runs the
tests, builds with the hash-pinned toolchain in `.github/release-requirements.txt`,
publishes to PyPI with Trusted Publishing and attaches the files and the SBOM to
the Release. The repository settings it relies on:

- **Tag protection** (a ruleset on `v*` tags): only maintainers create release
  tags; the workflow also refuses a tag that is not `v<major>.<minor>.<patch>`,
  is not on `main`, or whose version differs from `pyproject.toml` and
  `kbm_ledsas_sdk.__version__`.
- **Environment `pypi`** with required reviewers (a maintainer approves the
  upload) and a deployment tag rule `v*`, and **environment `testpypi`**
  limited to the `main` branch.
- **Digests.** The build job records the SHA-256 of every file it built; the
  publish job checks the downloaded files against them before the upload,
  then checks that PyPI holds exactly those files. The SBOM is generated after
  the upload (it installs the published wheel's dependency closure, which is
  not hash-pinned) and attached to the Release with the files.
- Before tagging: set the `[<version>]` date in `CHANGELOG.md` to the release
  day (UTC, the day PyPI will show), and rehearse with a manual run of the
  `Release` workflow on `main` (TestPyPI, a `.devN` version).

### Pins that Dependabot does not update

Dependabot updates the dependencies in `pyproject.toml`, the GitHub Actions,
the hash-pinned toolchain in `.github/*-requirements.txt` (regenerate it from
the `.in` files with the `uv pip compile` line at the top of each), the
example's `requirements.txt` and its `docker-compose.yml` images. These pins
are updated by hand, deliberately:

- the scanner images `GITLEAKS_IMAGE` and `TRUFFLEHOG_IMAGE` in
  `.github/workflows/secret-scan.yml` (and the matching lines under "Secret
  scans" above), and the RabbitMQ service image of the `integration` job in
  `.github/workflows/ci.yml` (keep it equal to the image of
  `examples/hello_world_service/deploy/local/docker-compose.yml`): tag plus
  digest, from `docker buildx imagetools inspect <image>:<tag>`;
- the build backend, `setuptools==` and `wheel==` in `pyproject.toml`
  `[build-system]` and in `.github/release-requirements.in`, which must stay
  equal (a repository test checks it);
- the base image tag in `examples/hello_world_service/Dockerfile`, which is
  the SDK version and moves with each release.
