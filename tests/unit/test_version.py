"""The package reports one version, and the repository's documents name it.

A release bumps ``pyproject.toml``; these tests fail until every place that
names the current version (the package, the API reference, the changelog,
the compatibility matrix, the notices, the bug-report form and the example
service) says the same.
"""

import re
import tomllib
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as dist_version
from pathlib import Path

import pytest

import kbm_ledsas_sdk

ROOT = Path(__file__).resolve().parents[2]
VERSION = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"][
    "version"
]


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_package_version_matches_pyproject():
    assert kbm_ledsas_sdk.__version__ == VERSION


def test_version_is_a_plain_release_number():
    assert re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", VERSION)


def test_installed_metadata_matches_the_package():
    try:
        installed = dist_version("kbm-ledsas-sdk")
    except PackageNotFoundError:
        pytest.skip("kbm-ledsas-sdk is not installed (tests run from the source tree)")
    assert installed == kbm_ledsas_sdk.__version__


def test_api_reference_names_the_version():
    text = _read("docs/SDK_API_REFERENCE.md")
    assert text.splitlines()[0] == f"# LEDSAS SDK API Reference v{VERSION}"
    assert f"**Version:** {VERSION}\n" in text


def test_changelog_has_an_entry_and_a_compare_link():
    text = _read("CHANGELOG.md")
    assert re.search(rf"^## \[{re.escape(VERSION)}\] - [0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$", text, re.M)
    assert re.search(
        rf"^\[Unreleased\]: https://github\.com/Kubyk-Health/kbm-ledsas-sdk/compare/"
        rf"v{re.escape(VERSION)}\.\.\.HEAD$",
        text,
        re.M,
    )
    assert re.search(rf"^\[{re.escape(VERSION)}\]: https://github\.com/", text, re.M)


def test_compatibility_matrix_marks_the_current_release():
    assert f"| **{VERSION}**" in _read("COMPATIBILITY.md")


def test_upgrading_has_a_section_for_the_current_release():
    assert re.search(rf"^## Upgrading to {re.escape(VERSION)}\b", _read("UPGRADING.md"), re.M)


def test_third_party_notices_header():
    assert _read("THIRD_PARTY_NOTICES.txt").splitlines()[0].startswith(
        f"LEDSAS SDK v{VERSION} - "
    )


def test_bug_report_placeholder():
    assert f'"e.g. {VERSION} (pip show kbm-ledsas-sdk)"' in _read(
        ".github/ISSUE_TEMPLATE/bug_report.yml"
    )


def test_example_service_names_the_version():
    readme = _read("examples/hello_world_service/README.md")
    assert f"kbm-ledsas-sdk=={VERSION}" in readme
    dockerfile = _read("examples/hello_world_service/Dockerfile")
    assert f"FROM ghcr.io/kebormed/kbm-ledsas-base-direct:{VERSION}\n" in dockerfile
