"""The example service's Dockerfile builds on the LEDSAS base image.

``examples/hello_world_service/Dockerfile`` is the shape a service image
takes: one stage ``FROM ghcr.io/kebormed/kbm-ledsas-base-direct:<SDK
version>`` (the image comes with the SDK and its runtime dependencies
installed, and its tag is the SDK version), the service's own dependencies
installed as root and checked with ``pip check``, pip removed as the last
root step, and the service run as the base image's non-root user
``kbmuser``. The tag is written literally so that a dependency bot can
propose the next one.
"""

import re
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "hello_world_service"
DOCKERFILE = EXAMPLE / "Dockerfile"
REQUIREMENTS = EXAMPLE / "requirements.txt"
BASE = "ghcr.io/kebormed/kbm-ledsas-base-direct"
VERSION = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]
SDK_NAME = re.compile(r"kbm[-_.]ledsas[-_.]sdk", re.I)

# The last root step, byte for byte.
PIP_REMOVAL_STEP = (
    "# Last root step: remove the package installer from the final image. It is only\n"
    "# needed while building, never at runtime. Keep every pip command above this line\n"
    "# (below it pip is gone and a pip command fails the build), and install into the\n"
    "# base image's Python as above, not into a virtual environment: the check fails\n"
    "# the build if an installed copy of pip remains in the image.\n"
    "RUN PIP_ROOT_USER_ACTION=ignore python -m pip uninstall --yes pip \\\n"
    ' && ! python -c "import pip" 2>/dev/null \\\n'
    " && ! find / -xdev \\( -path '*/site-packages/pip' -o -name 'pip-*.dist-info' \\) -print | grep .\n"
)


def _instructions(text: str) -> list[tuple[int, str]]:
    """(first line number, instruction) with comments dropped and backslash
    continuations joined."""
    out, buf, start = [], [], None
    for no, raw in enumerate(text.splitlines(), 1):
        stripped = raw.strip()
        if not buf and (not stripped or stripped.startswith("#")):
            continue
        if buf and stripped.startswith("#"):
            continue
        if start is None:
            start = no
        if raw.rstrip().endswith("\\"):
            buf.append(raw.rstrip()[:-1])
            continue
        buf.append(raw)
        out.append((start, " ".join(buf)))
        buf, start = [], None
    if buf:
        out.append((start, " ".join(buf)))
    return out


@pytest.fixture(scope="module")
def text() -> str:
    return DOCKERFILE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def instructions(text: str) -> list[tuple[int, str]]:
    return _instructions(text)


def _word(instruction: str) -> str:
    return instruction.split(None, 1)[0].upper()


# The FROM line: the base image at this SDK version's tag, optionally pinned by
# digest as the documentation recommends. Written at column 0, upper case, one
# space, no build argument, no stage name, no platform flag.
FROM_LINE = re.compile(rf"FROM {re.escape(BASE)}:{re.escape(VERSION)}(?:@sha256:[0-9a-f]{{64}})?")


@pytest.mark.parametrize(
    ("line", "accepted"),
    [
        (f"FROM {BASE}:{VERSION}", True),
        (f"FROM {BASE}:{VERSION}@sha256:{'0' * 64}", True),
        (f"FROM {BASE}:{VERSION}@sha256:{'0' * 63}", False),
        (f"FROM {BASE}:latest", False),
        (f"FROM {BASE}:{VERSION} AS build", False),
        (f"FROM --platform=linux/amd64 {BASE}:{VERSION}", False),
    ],
)
def test_from_line_shape(line, accepted):
    assert bool(FROM_LINE.fullmatch(line)) is accepted


def test_one_literal_from_line_on_the_base_image_of_this_sdk_version(text, instructions):
    froms = [line for _, line in instructions if _word(line) == "FROM"]
    assert len(froms) == 1 and FROM_LINE.fullmatch(froms[0]), froms
    assert [ln for ln in text.splitlines() if FROM_LINE.fullmatch(ln)] == froms
    assert not any(_word(line) == "ARG" for _, line in instructions)


@pytest.mark.parametrize(
    "pattern",
    [r"\buseradd\b", r"\bapt-get\b", r"\.whl\b", r"\btar\s+-xzf\b", SDK_NAME.pattern],
)
def test_no_forbidden_instruction(instructions, pattern):
    rx = re.compile(pattern, re.I)
    assert not [line for _, line in instructions if rx.search(line)]


def test_no_virtual_environment(instructions):
    rx = re.compile(r"\bvenv\b|virtualenv|ensurepip", re.I)
    assert not [line for _, line in instructions if rx.search(line)]


def test_pip_install_runs_as_root_and_is_checked(instructions):
    user, installed, checked = "kbmuser", False, False
    for _, line in instructions:
        if _word(line) == "USER":
            user = line.split(None, 1)[1].strip()
        elif _word(line) == "RUN":
            for cmd in re.split(r"&&|;|\|\|", line):
                if re.search(r"\bpip3?\s+install\b", cmd):
                    assert user in ("root", "0"), f"pip install runs as {user!r}"
                    installed, checked = True, False
                elif re.search(r"\bpip3?\s+check\b", cmd):
                    checked = True
    assert installed and checked


def test_pip_is_removed_as_the_last_root_step(text, instructions):
    assert text.count(PIP_REMOVAL_STEP) == 1
    removal = [
        k
        for k, (_, line) in enumerate(instructions)
        if _word(line) == "RUN" and re.search(r"\bpip3?\s+uninstall\b", line)
    ]
    assert len(removal) == 1
    after = instructions[removal[0] + 1 :]
    assert [line for _, line in after][:1] == ["USER kbmuser"]
    assert [_word(line) for _, line in after[1:]] == ["CMD"]


def test_final_user_is_kbmuser(instructions):
    users = [line.split(None, 1)[1].strip() for _, line in instructions if _word(line) == "USER"]
    assert users[-1] == "kbmuser"


def test_requirements_never_name_the_sdk():
    lines = [
        ln.strip()
        for ln in REQUIREMENTS.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.lstrip().startswith("#")
    ]
    assert not [ln for ln in lines if SDK_NAME.search(ln)]
