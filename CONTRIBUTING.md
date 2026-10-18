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
# mypy, build)
pip install -e ".[dev]"
```

## Running the tests

```bash
pytest -m "not integration"   # the unit tests: no broker or cloud service needed
pytest                        # everything; the integration tests skip themselves
                              # when no RabbitMQ answers on 127.0.0.1:5672
```

The integration tests (`tests/integration/`, marker `integration`) run against a
real RabbitMQ. Start the example's local stack first (`--wait` returns once the
broker accepts connections; without it the tests can start too early and skip
themselves), then run them on their own and check that none was skipped:

```bash
docker compose -f examples/hello_world_service/deploy/local/docker-compose.yml up -d --wait
pytest -m integration -rs
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
| Typing of a strict consumer of the public API (`ci.yml`) | `MYPYPATH=src mypy --config-file tests/typing/mypy.ini tests/typing/typed_consumer.py` |
| The documented calls under `mypy --strict` (`ci.yml`) | `LEDSAS_REQUIRE_MYPY=1 python -m pytest -q tests/test_typed_consumer.py` |
| Unit tests on Python 3.11, 3.12, 3.13 (`ci.yml`) | `pytest -m "not integration"` |
| Example service tests (`ci.yml`) | see "Running the tests" |
| Integration tests against RabbitMQ (`ci.yml`) | see "Running the tests" |
| Build twice, identical wheels, metadata check (`ci.yml`) | `SOURCE_DATE_EPOCH=$(git log -1 --format=%ct) python -m build && twine check dist/*` |
| DCO sign-off by the commit author (`dco.yml`) | `git log --format='%an <%ae>%n%(trailers:key=Signed-off-by)' main..` |
| Secret scans of the tree and the PR's commits (`secret-scan.yml`) | `gitleaks git --config .gitleaks.toml --log-opts="main..HEAD"` |
| CodeQL (`codeql.yml`) | runs on GitHub only |

`mypy src` (strict) still reports errors in the SDK's own modules; we are fixing
them module by module, so it reports without blocking. The typed-consumer check
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
A CI check enforces that all commits in a PR are signed off. If you forget, you can amend:

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
  tags; the workflow also refuses a tag that is not on `main` or whose version
  differs from `pyproject.toml` and `kbm_ledsas_sdk.__version__`.
- **Environment `pypi`** with required reviewers (a maintainer approves the
  upload) and **environment `testpypi`** limited to the `main` branch.
- Before tagging: set the `[<version>]` date in `CHANGELOG.md` to the release
  day, and rehearse with a manual run of the `Release` workflow on `main`
  (TestPyPI, a `.devN` version).
