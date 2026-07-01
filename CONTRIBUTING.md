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

# Editable install with the dev toolchain (pytest, ruff, black, mypy, build)
pip install -e ".[dev]"
```

## Running the tests

```bash
pytest                # full unit test suite
pytest tests/unit -q  # the unit tests, quiet
```

The suite is self-contained — no broker or cloud services required.

## Code style & quality gates

These run in CI. `ruff`, `black`, and `pytest` must pass before a PR can merge;
`mypy` runs in strict mode but is **advisory** for now (we are adopting strict
typing incrementally — please don't add new type errors):

```bash
ruff check .          # lint (must pass)
black --check .       # formatting (must pass)
pytest                # tests (must pass)
mypy src              # static typing (advisory)
```

Run `ruff check --fix .` and `black .` to auto-fix most issues locally.

Guidelines:

- Keep public APIs **type-hinted**; the package ships `py.typed`.
- Match the style of the surrounding code (naming, docstrings, error handling).
- Add or update tests for any behavior change.
- Update [CHANGELOG.md](CHANGELOG.md) under an `## [Unreleased]` heading.

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
3. Ensure `ruff`, `black`, and `pytest` pass locally (`mypy` is advisory).
4. Push and open a PR against `main`. Fill in the PR template.
5. A maintainer (see [CODEOWNERS](CODEOWNERS)) will review. Address feedback by pushing
   additional commits; we squash-merge, so you don't need to squash yourself.

We aim to give an initial response within a few business days. Thanks for contributing!
