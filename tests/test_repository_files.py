"""The repository's documents and workflows say what the package does.

These checks read files of the repository (README, upgrade and compatibility
guides, workflows, packaging files), not the package: they run from a clone,
and the sdist does not carry them (MANIFEST.in prunes tests/).
"""

import re
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
VERSION = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def section(text: str, heading: str, level: int = 2) -> str:
    """The body of a Markdown section, up to the next heading of the same or a
    higher level (lines inside code fences are never headings)."""
    out: list[str] = []
    inside, fence = False, False
    for line in text.splitlines():
        if line.startswith("```"):
            fence = not fence
        heading_level = (
            len(line) - len(line.lstrip("#")) if not fence and line.startswith("#") else 0
        )
        if heading_level and line[heading_level : heading_level + 1] == " ":
            if inside and heading_level <= level:
                break
            if heading_level == level and line[heading_level + 1 :].startswith(heading):
                inside = True
                continue
        if inside:
            out.append(line)
    assert inside, heading
    return "\n".join(out) + "\n"


# Directories a repository scan skips: git's own, virtual environments and
# build output (a clone that followed the example README has a .venv in it).
SKIPPED_DIRS = {".git", ".venv", "venv", "build", "dist", ".tox", "node_modules", "__pycache__"}


def repository_files(base: Path, pattern: str = "*", root: Path = ROOT) -> list[Path]:
    """Files under ``base`` matching ``pattern``, outside ``SKIPPED_DIRS``."""
    return sorted(
        path
        for path in base.rglob(pattern)
        if path.is_file() and not set(path.relative_to(root).parts) & SKIPPED_DIRS
    )


def steps(workflow: str) -> list[str]:
    """The text of each step of a workflow (split on the step list items)."""
    return re.split(r"\n(?=      - (?:name|uses): )", read(f".github/workflows/{workflow}"))[1:]


# --------------------------------------------------------------------------- #
# README
# --------------------------------------------------------------------------- #
README = read("README.md")


def test_readme_lists_both_deployment_id_variables():
    rows = {m.group(1): m.group(0) for m in re.finditer(r"^\| `([A-Z_]+)` \|.*$", README, re.M)}
    assert "KBM_LEDSAS_DEPLOYMENT_ID" in rows
    assert "KBM_LASTCOMMITID" in rows
    assert "KBM_LASTCOMMITID" in rows["KBM_LEDSAS_DEPLOYMENT_ID"]  # the precedence is stated
    assert "Older alias of `KBM_LEDSAS_DEPLOYMENT_ID`" in rows["KBM_LASTCOMMITID"]
    assert "The SDK sets neither" in rows["KBM_LEDSAS_DEPLOYMENT_ID"]


# Wording that says or implies that something other than the user sets a
# deployment-id variable (nothing does: the SDK only reads them).
PLATFORM_SETS = re.compile(
    r"platforms?\b|\bset (?:it|them) for you\b|\b(?:is|are|gets?) set by\b|\binject(?:s|ed)?\b|"
    r"\b(?:Kubernetes|the orchestrator|your orchestrator|CI|the CI)\s+sets?\b",
    re.I,
)


@pytest.mark.parametrize("rel", ["README.md", "UPGRADING.md", "CHANGELOG.md", "COMPATIBILITY.md"])
def test_deployment_id_variables_are_never_said_to_be_set_by_a_platform(rel):
    # Every sentence (or table row) that names either variable: the older name
    # is called an alias, and nothing says a platform (or anything but the
    # user) sets them.
    for unit in text_units(read(rel)):
        if "KBM_LASTCOMMITID" in unit or "KBM_LEDSAS_DEPLOYMENT_ID" in unit:
            assert not PLATFORM_SETS.search(unit), (rel, unit[:300])
        if "KBM_LASTCOMMITID" in unit:
            assert "alias" in unit, (rel, unit[:300])


def text_units(text: str) -> list[str]:
    """Sentences of the prose, and each Markdown table row as one unit."""
    out: list[str] = []
    for block in re.split(r"\n\s*\n", text):
        lines = block.splitlines()
        out += [" ".join(ln.split()) for ln in lines if ln.lstrip().startswith("|")]
        prose = " ".join(" ".join(ln for ln in lines if not ln.lstrip().startswith("|")).split())
        out += [u for u in re.split(r"(?<=[.;])\s", prose) if u]
    return out


def test_platform_wording_check_catches_the_old_readme_row():
    # The row as the README had it (split so that this file does not say it).
    old = (
        "Read for `deployment_id` when `KBM_LEDSAS_DEPLOYMENT_ID` is unset or blank "
        "(some deployment plat" + "forms set it)"
    )
    assert PLATFORM_SETS.search(old)
    row = f"| `KBM_LASTCOMMITID` | {old} | *(optional)* |"
    assert [u for u in text_units(row) if PLATFORM_SETS.search(u)] == [row]
    assert not PLATFORM_SETS.search("The SDK sets neither; set one at deploy time, for example")


def test_readme_does_not_overclaim_typing():
    assert "Fully " + "type-hinted" not in README
    assert "**Type hints** (`py.typed`) on the public API" in README


def test_readme_quickstart_names_the_file_and_a_working_connection_string():
    quickstart = section(README, "Quickstart")
    assert quickstart.index("my_service.py") < quickstart.index("```python")
    assert "AccountKey=..." not in quickstart and "AccountName=..." not in quickstart
    assert "AccountName=devstoreaccount1;AccountKey=" in quickstart
    assert "### Send it a command" in quickstart
    send = section(quickstart, "Send it a command", level=3)
    for needed in ("upload_blob", 'routing_key="response"', "reply_to", "cmd.csv-processor.v1"):
        assert needed in send, needed


def test_readme_quickstart_writes_only_where_the_service_decides():
    quickstart = section(README, "Quickstart")
    service = re.search(r"^```python\n(.*?)^```", quickstart, re.M | re.S).group(1)
    upload = re.search(r"upload_json\((.*?)\n    \)", service, re.S).group(1)
    # The output container is the service's constant, never one the command names.
    assert "container=RESULTS_CONTAINER" in upload, upload
    assert "from_uri(csv_uri).container != INPUT_CONTAINER" in service
    assert "#security-model" in quickstart


def test_readme_send_command_reports_failures_in_one_line():
    send = section(section(README, "Quickstart"), "Send it a command", level=3)
    code = re.search(r"^```python\n(.*?)^```", send, re.M | re.S).group(1)
    compile(code, "send_command.py", "exec")
    for needed in (
        "except ChannelNotFoundEntity:",
        "except TimeoutError:",
        "except (KeyboardInterrupt, asyncio.CancelledError):",
        "add_signal_handler(signal.SIGTERM, asyncio.current_task().cancel)",
        "expiration=WAIT",
        "aio_pika.connect(",
        '{"code", "message", "retryable"} <= error.keys()',  # an error reply: one line, exit 1
        '"x-expires": (WAIT + 60) * 1000',  # a killed run: the broker removes the queue
        "auto_delete=True",
    ):
        assert needed in code, needed
    assert "connect_robust" not in code  # one attempt: an unreachable broker fails at once


def test_readme_features_name_every_blob_method():
    import inspect

    from kbm_ledsas_sdk.blob.operations import BlobOperations

    features = section(README, "Features")
    named = set()
    for group in re.findall(r"`((?:download|upload)_[a-z]+(?:/[a-z]+)*)`", features):
        verb, _, rest = group.partition("_")
        named |= {f"{verb}_{kind}" for kind in rest.split("/")}
    methods = {
        n
        for n, _ in inspect.getmembers(BlobOperations, inspect.isfunction)
        if re.match(r"(?:download|upload)_", n)
    }
    assert methods and methods <= named, sorted(methods - named)


def test_readme_local_development_loads_the_env_file_from_a_clone():
    local = section(README, "Local development")
    assert "From a clone of this repository" in local
    assert "cp .env.example .env" in local
    for command in ("python main.py", "python scripts/send_hello.py"):
        assert re.search(
            rf"\(set -a; \. \./\.env; set \+a; exec {re.escape(command)}", local
        ), command
    assert f"pip install kbm-ledsas-sdk=={VERSION}" in local  # the no-clone alternative


def test_readme_container_section():
    container = section(README, "Container base image")
    assert "minimal shape" in container.lower()
    assert "ENV PIP_ROOT_USER_ACTION=ignore" in container
    for setting in (
        "KBM_LEDSAS_ALLOW_INSECURE_AMQP",
        "KBM_LEDSAS_HEALTH_HOST=0.0.0.0",
        "PIP_CONSTRAINT",
    ):
        assert setting in container, setting
    assert "terminationGracePeriodSeconds" in container
    assert f"FROM ghcr.io/kebormed/kbm-ledsas-base-direct:{VERSION}\n" in container


@pytest.mark.parametrize("rel", ["README.md", "examples/hello_world_service/README.md"])
def test_no_unqualified_shutdown_time(rel):
    assert not re.search(r"<\s?1\s?s\b", read(rel))


def test_readme_points_upgraders_at_the_upgrade_guide_first():
    docs = section(README, "Documentation")
    first = docs.strip().splitlines()[0]
    assert "UPGRADING.md" in first and "start here" in first


# --------------------------------------------------------------------------- #
# COMPATIBILITY, UPGRADING, CONTRIBUTING, SECURITY
# --------------------------------------------------------------------------- #
def test_compatibility_exception_paragraph_lists_the_wire_changes():
    text = read("COMPATIBILITY.md")
    match = re.search(
        rf"\*\*Exception in {re.escape(VERSION)}:\*\*(.*?)(?=^## )", text, re.M | re.S
    )
    assert match, f"no 'Exception in {VERSION}' paragraph"
    para = match.group(1)
    # COMPATIBILITY names the change classes and links the upgrade guide, which has the detail
    for change in ("one reply per command", "envelope types", "reply_to", "health port"):
        assert change in para, change
    assert "UPGRADING.md" in para


def test_compatibility_describes_the_base_image():
    base = section(read("COMPATIBILITY.md"), "Container base image")
    for fact in (
        "Python 3.11",
        "Alpine",
        "PIP_CONSTRAINT=/etc/kbm-ledsas-pip-constraints.txt",
        "uid 1000",
    ):
        assert fact in base, fact


def test_compatibility_says_how_to_verify_the_base_image_without_its_recipe():
    """The recipe is not published: the text says so, names no recipe a reader
    could rebuild from, and points to what each release lists (the digest; the
    SBOM attestation only where the table says the image carries one)."""
    base = " ".join(section(read("COMPATIBILITY.md"), "Container base image").split())
    assert "same recipe" not in base
    assert "is maintained by them and is not published" in base
    assert "carries no `org.opencontainers.image.source` label" in base
    assert "Verify an image by its digest: each release lists the image's digest" in base
    assert "where the table says the image carries an SBOM attestation" in base
    # every published tag row gives its digest and says whether the image carries an SBOM attestation
    rows = [
        line
        for line in section(read("COMPATIBILITY.md"), "Container base image").splitlines()
        if line.startswith("| `") and not line.startswith("| `latest`")
    ]
    assert rows
    for row in rows:
        assert re.search(r"`sha256:[0-9a-f]{64}`", row), row
        assert "SBOM attestation" in row, row


def test_upgrading_shows_the_handler_registration_errors():
    text = read("UPGRADING.md")
    for message in (
        "ValueError: Invalid command name",
        "TypeError: Handler for",
        "must be an async function",
    ):
        assert message in text, message
    assert "when your module is imported" in text
    assert "--upgrade-strategy eager" in text


def test_contributing_documents_the_integration_tests():
    text = read("CONTRIBUTING.md")
    assert "pytest -m integration" in text
    assert "self-contained" not in text
    assert "don't add new type errors" not in text
    marker = tomllib.loads(read("pyproject.toml"))["tool"]["pytest"]["ini_options"]["markers"]
    [integration] = [m for m in marker if m.startswith("integration:")]
    for needed in ("LEDSAS_TEST_RABBITMQ_URL", "LEDSAS_TEST_BLOB_CONN_STRING", "Azurite"):
        assert needed in integration, needed


def test_security_policy_has_one_private_channel_and_targets():
    text = read("SECURITY.md")
    assert "Private Vulnerability Reporting" in text
    assert "on request" not in text and "PGP" not in text
    assert "90 days" in text


# --------------------------------------------------------------------------- #
# Workflows
# --------------------------------------------------------------------------- #
ALL_WORKFLOWS = sorted(p.name for p in WORKFLOWS.glob("*.yml"))


@pytest.mark.parametrize("workflow", ALL_WORKFLOWS)
def test_checkouts_do_not_keep_credentials(workflow):
    for step in steps(workflow):
        if "actions/checkout@" in step:
            assert "persist-credentials: false" in step, step


@pytest.mark.parametrize("workflow", ALL_WORKFLOWS)
def test_container_images_are_pinned_by_digest(workflow):
    text = read(f".github/workflows/{workflow}")
    images = re.findall(r"(?:image:|_IMAGE:)\s*(\S+)", text)
    images += re.findall(
        r"docker run [^\n]*?\b((?:ghcr\.io|docker\.io|mcr\.microsoft\.com)/\S+)", text
    )
    for image in images:
        assert re.search(r"@sha256:[0-9a-f]{64}$", image), image


@pytest.mark.parametrize("workflow", ALL_WORKFLOWS)
def test_run_blocks_take_event_values_from_env(workflow):
    # ${{ ... }} expanded inside a shell script is an injection risk: pass values via env:.
    for step in steps(workflow):
        run = re.search(r"\n        run: (\|\n(?:(?: {10}.*)?\n)*|.*)", step + "\n")
        if run:
            assert "${{" not in run.group(1), step


def test_release_installs_only_hash_pinned_tools():
    for line in read(".github/workflows/release.yml").splitlines():
        if re.search(r"\bpip install\b", line) and "--require-hashes" not in line:
            # The one exception: the environment the SBOM describes (the wheel's
            # dependency closure as a user gets it).
            assert '"$WHEEL"' in line and "--report" in line, line


def test_release_checks_the_tag_and_tests_before_building():
    text = read(".github/workflows/release.yml")
    for needed in (
        "git merge-base --is-ancestor",
        "kbm_ledsas_sdk.__version__",
        "python -m pytest",
    ):
        assert needed in text, needed
    assert text.index("python -m pytest") < text.index("python -m build")


def test_pypi_check_compares_the_build_digests():
    # The publish action adds *.publish.attestation files to dist/ during the upload,
    # so the post-upload check takes the built files from the build job's digests.
    jobs = _jobs("release.yml")
    check = jobs["publish-pypi"].split("Check that PyPI holds exactly these files", 1)[1]
    assert "DIGESTS: ${{ needs.build.outputs.digests }}" in check
    assert 'base64.b64decode(os.environ["DIGESTS"])' in check
    assert 'pathlib.Path("dist")' not in check


def test_release_publish_is_checked_against_pypi():
    publish = "\n".join(s for s in steps("release.yml") if "pypi" in s.lower())
    if "skip-existing: true" in publish:
        after = publish.split("skip-existing: true", 1)[1]
        assert "pypi.org/pypi/" in after and "sha256" in after


def test_builds_are_reproducible_and_release_links_are_pinned():
    release, ci = read(".github/workflows/release.yml"), read(".github/workflows/ci.yml")
    assert "SOURCE_DATE_EPOCH" in release and "SOURCE_DATE_EPOCH" in ci
    assert "a second build" in ci and "cmp " in ci
    # The tagged commit is built as it is: no rewrite of the tree before the build.
    # (A rehearsal changes only the version, in its own step.)
    after_rehearsal = release.split("Rehearsal version", 1)[1].split("\n      - name:", 1)[1]
    assert "sed -i" not in after_rehearsal
    assert "'/blob/main/" in release  # the guard that fails on a link to main
    for step in steps("release.yml") + steps("ci.yml"):
        if "python -m build" in step:
            assert "umask 022" in step, step


def test_the_package_page_and_the_source_link_this_release():
    for rel in ("README.md", "pyproject.toml"):
        text = read(rel)
        assert "/blob/main/" not in text and "/tree/main/" not in text, rel
        links = re.findall(r"kbm-ledsas-sdk/(?:blob|tree)/([^/\s)]+)/", text)
        assert links and set(links) == {f"v{VERSION}"}, (rel, sorted(set(links)))


def test_release_sbom_has_a_root_component_and_hashes():
    release = read(".github/workflows/release.yml")
    assert "--pyproject /tmp/pyproject.toml --mc-type library" in release
    # ... whose version is the wheel's (a rehearsal's <version>.devN too).
    assert 'version = \\"${VERSION}\\"/" pyproject.toml > /tmp/pyproject.toml' in release
    assert ".github/scripts/finish_sbom.py" in release
    finish = read(".github/scripts/finish_sbom.py")
    assert "pkg:pypi/kbm-ledsas-sdk@" in finish and "file://" in finish


def test_release_tests_run_before_the_rehearsal_version():
    # The tests check that the documents name the release version, so they run on
    # the tree as committed: after the .devN rewrite they would fail every rehearsal.
    release = read(".github/workflows/release.yml")
    assert release.index("python -m pytest") < release.index("- name: Rehearsal version")


def test_testpypi_rehearsal_is_repeatable_and_main_only():
    release = read(".github/workflows/release.yml")
    assert ".dev$((" in release
    testpypi = [
        s
        for s in re.split(r"\n(?=  [a-z-]+:\n)", release)
        if s.lstrip().startswith("publish-testpypi")
    ]
    assert testpypi and "github.ref == 'refs/heads/main'" in testpypi[0]


def test_ci_blocks_on_the_typed_consumer_and_runs_the_example_tests():
    ci_steps = steps("ci.yml")
    typed = [s for s in ci_steps if "tests/typing/typed_consumer.py" in s]
    assert (
        typed
        and "continue-on-error" not in typed[0]
        and "--config-file tests/typing/mypy.ini" in typed[0]
    )
    example = [s for s in ci_steps if "examples/hello_world_service" in s]
    assert example and "-W error::pytest.PytestConfigWarning" in example[0]
    assert "pytest -q -m integration" in read(".github/workflows/ci.yml")
    assert (ROOT / "tests" / "typing" / "typed_consumer.py").is_file()


def test_dco_requires_the_authors_sign_off():
    dco = read(".github/workflows/dco.yml")
    assert 'grep -Fqx "Signed-off-by: $author"' in dco
    # Merge commits ("Update branch") carry no authored change: not checked.
    assert 'git rev-list --no-merges "$BASE".."$HEAD"' in dco


def test_release_tag_must_be_a_v_version():
    release = read(".github/workflows/release.yml")
    assert '[[ "$TAG" =~ ^v[0-9]+\\.[0-9]+\\.[0-9]+$ ]]' in release
    assert "${TAG#v}" not in release


def _jobs(workflow: str) -> dict[str, str]:
    text = read(f".github/workflows/{workflow}")
    body = text.split("\njobs:\n", 1)[1]
    return dict(re.findall(r"^  ([a-z-]+):\n((?:^(?:    .*|\s*)\n)*)", body + "\n", re.M))


def test_publish_checks_the_build_digests_and_runs_no_unpinned_code_first():
    jobs = _jobs("release.yml")
    assert "digests: ${{ steps.build.outputs.digests }}" in jobs["build"]
    for name in ("publish-pypi", "publish-testpypi"):
        job = jobs[name]
        assert re.search(r"^    needs: build$", job, re.M), name  # not the SBOM job
        check = job.index("needs.build.outputs.digests")
        assert "sha256sum --check --strict" in job
        assert check < job.index("pypa/gh-action-pypi-publish"), name
    # The SBOM job (unpinned dependency closure) runs after the upload.
    assert "publish-pypi" in jobs["sbom"].split("needs:", 1)[1].splitlines()[0]
    assert re.search(r"^    needs: \[publish-pypi, sbom\]$", jobs["attach-release-assets"], re.M)


def test_secret_scans_cover_the_commits():
    text = read(".github/workflows/secret-scan.yml")
    assert "git /repo" in text and "git file:///repo" in text
    assert "pull_request.base.sha" in text


# --------------------------------------------------------------------------- #
# Packaging and repository settings files
# --------------------------------------------------------------------------- #
def test_sdist_leaves_out_the_tests():
    assert re.search(r"^prune tests$", read("MANIFEST.in"), re.M)


def test_dependabot_covers_what_it_can_update():
    text = read(".github/dependabot.yml")
    entries = re.findall(r"package-ecosystem: (\S+)\n\s+directory: \"([^\"]+)\"", text)
    assert ("docker", "/examples/hello_world_service") not in entries
    assert ("docker-compose", "/examples/hello_world_service/deploy/local") in entries
    assert ("pip", "/.github") in entries
    assert ("pip", "/examples/hello_world_service") in entries


def test_gitleaks_allowlist_regexes_are_anchored():
    text = read(".gitleaks.toml")
    block = re.search(r"regexes = \[(.*?)\n\]", text, re.S).group(1)
    regexes = re.findall(r"'''(.*?)'''", block)
    assert regexes
    for rx in regexes:
        assert rx.startswith("^") and rx.endswith("$"), rx


def test_dev_toolchain_floor_skips_the_yanked_build_release():
    dev = tomllib.loads(read("pyproject.toml"))["project"]["optional-dependencies"]["dev"]
    assert "build>=1.2.1" in dev


def test_every_dev_tool_behind_a_blocking_check_is_bounded():
    from packaging.requirements import Requirement

    dev = {
        Requirement(r).name: Requirement(r).specifier
        for r in tomllib.loads(read("pyproject.toml"))["project"]["optional-dependencies"]["dev"]
    }
    # ruff, black and mypy back blocking CI checks; twine backs the metadata check.
    for tool in ("ruff", "black", "mypy", "twine"):
        assert tool in dev, tool
        assert any(spec.operator == "<" for spec in dev[tool]), (tool, str(dev[tool]))


def test_the_build_backend_pins_are_the_release_toolchain_pins():
    backend = tomllib.loads(read("pyproject.toml"))["build-system"]["requires"]
    lock_in = read(".github/release-requirements.in").splitlines()
    for pin in backend:
        assert pin in lock_in, pin
    assert [ln for ln in lock_in if re.match(r"(?:setuptools|wheel)\b", ln)] == backend


def test_contributing_names_the_pins_updated_by_hand():
    text = " ".join(read("CONTRIBUTING.md").split())
    assert "Pins that Dependabot does not update" in text
    for name in ("GITLEAKS_IMAGE", "TRUFFLEHOG_IMAGE", "RabbitMQ service image", "[build-system]"):
        assert name in text, name
    scan = read(".github/workflows/secret-scan.yml")
    for var in ("GITLEAKS_IMAGE", "TRUFFLEHOG_IMAGE"):
        pinned = re.search(rf"^  {var}: (\S+)$", scan, re.M).group(1)
        assert f"{var}={pinned}" in read("CONTRIBUTING.md"), var  # the local commands match CI


def test_contributing_build_command_runs_without_git():
    text = read("CONTRIBUTING.md")
    build = section(text, "Reproducible build", level=3)
    assert "umask 022" in build and "|| echo 315532800" in build
    assert "twine check dist/*" in build
    dev = tomllib.loads(read("pyproject.toml"))["project"]["optional-dependencies"]["dev"]
    assert any(r.startswith("twine") for r in dev)  # the dev extra installs it
    assert "Only the wheel is reproducible" in " ".join(build.split())


def test_dependencies_name_the_security_floors_and_the_libraries_used_directly():
    deps = tomllib.loads(read("pyproject.toml"))["project"]["dependencies"]
    for needed in ("azure-core>=1.38.0,<2.0", "aiormq>=6.8.0,<8.0", "pamqp>=3.3.0,<5.0"):
        assert needed in deps, needed
    compat = section(read("COMPATIBILITY.md"), "Runtime dependencies")
    rows = dict(re.findall(r"^\| `([a-z0-9-]+)` \| `([^`]+)`", compat, re.M))
    for dep in deps:
        name, spec = re.match(r"([A-Za-z0-9_.-]+)(.*)", dep).groups()
        assert name in rows, name
        assert set(rows[name].split(",")) == set(spec.split(",")), (name, rows[name], spec)


def test_compatibility_names_every_python_release_as_tested_or_untested():
    table = section(read("COMPATIBILITY.md"), "Python")
    for minor in (11, 12, 13):
        assert re.search(rf"^\| 3\.{minor} ", table, re.M), minor
    assert re.search(r"^\| 3\.14 and newer \| Installs, not yet tested \|$", table, re.M)
    classifiers = tomllib.loads(read("pyproject.toml"))["project"]["classifiers"]
    tested = sorted(c.rsplit(" ", 1)[1] for c in classifiers if re.search(r"Python :: 3\.\d+$", c))
    ci = read(".github/workflows/ci.yml")
    assert f"python-version: {json_list(tested)}" in ci, tested


def json_list(items: list[str]) -> str:
    return "[" + ", ".join(f'"{i}"' for i in items) + "]"


# --------------------------------------------------------------------------- #
# Settings, upgrade notes and links (the changes a 0.3.3 user must know about)
# --------------------------------------------------------------------------- #
def _api_reference_env_rows() -> set[str]:
    text = read("docs/SDK_API_REFERENCE.md")
    env = section(text, "Environment Variables")
    return set(re.findall(r"^\| `(KBM_[A-Z0-9_]+)` \|", env, re.M))


def test_api_reference_has_a_row_for_every_setting():
    from kbm_ledsas_sdk.runtime.env_vars import ENV_VAR_REGISTRY

    rows = _api_reference_env_rows()
    missing = [spec.name for spec in ENV_VAR_REGISTRY if spec.name not in rows]
    assert missing == [], missing


def heartbeat_default() -> int:
    """The KBM_LEDSAS_HEARTBEAT default of the SDK source (the value it reads
    when the variable is unset)."""
    config = read("src/kbm_ledsas_sdk/runtime/config.py")
    m = re.search(r'_int_env\(\s*"KBM_LEDSAS_HEARTBEAT",\s*(?:"([0-9]+)"|str\((\w+)\))', config)
    assert m, "no KBM_LEDSAS_HEARTBEAT default in runtime/config.py"
    if m.group(1):
        return int(m.group(1))
    const = re.search(rf"^{m.group(2)}(?::\s*int)?\s*=\s*([0-9]+)\b", config, re.M)
    assert const, m.group(2)
    return int(const.group(1))


def test_readme_configuration_lists_the_heartbeat():
    config = section(README, "Configuration")
    row = re.search(rf"^\| `KBM_LEDSAS_HEARTBEAT` \|.*\| `{heartbeat_default()}` \|$", config, re.M)
    assert row, f"no KBM_LEDSAS_HEARTBEAT row with the SDK's default ({heartbeat_default()})"
    assert "asyncio.to_thread" in row.group(0)


def test_upgrading_tells_the_changes_that_can_break_a_deployment():
    text = " ".join(section(read("UPGRADING.md"), f"Upgrading to {VERSION}").split())
    for needed in (
        "`amq.",  # reply_to naming a reserved broker exchange is refused
        "no_verify_ssl",
        "?cafile=",
        "KBM_LEDSAS_HEARTBEAT",
        f"default `{heartbeat_default()}` seconds",
        "counts toward `KBM_LEDSAS_MAX_RETRIES`",
        "invalid `?heartbeat=`",
        "asyncio.to_thread",
        "Message models and `BlobRef` are read-only",
        "BlobRef(uri=..., version_id=...)",
        "model_copy(update=",
        "ASCII digits",
    ):
        assert needed in text, needed
    assert "send_response()" not in read("UPGRADING.md")


def test_upgrade_guide_lists_the_other_changes():
    # The list lives in the upgrade guide only (COMPATIBILITY links to it).
    para = section(read("UPGRADING.md"), f"Upgrading to {VERSION}")
    for change in ("`amq.…`", "no_verify_ssl", "KBM_LEDSAS_HEARTBEAT", "read-only", "ASCII digits"):
        assert change in para, change
    flat = " ".join(para.split())
    assert "`BlobRef` are read-only" in flat and "BlobRef(uri=..., version_id=...)" in flat
    assert "counts toward `KBM_LEDSAS_MAX_RETRIES`" in flat
    assert "like `BlobRef` " + "already was" not in " ".join(read("UPGRADING.md").split())


def test_readme_quickstart_runs_from_an_empty_directory():
    quickstart = section(README, "Quickstart")
    shell = "\n".join(
        m.group(1) for m in re.finditer(r"^```bash\n(.*?)^```", quickstart, re.M | re.S)
    )
    assert "examples/" not in shell  # no file of a clone is needed
    assert shell.count("docker run -d --name ledsas-quickstart-") == 2
    assert "Server startup complete" in shell  # waits until the broker has started
    assert "docker rm -f -v ledsas-quickstart-rabbitmq ledsas-quickstart-azurite" in quickstart


@pytest.mark.parametrize(
    "form", sorted((ROOT / ".github" / "ISSUE_TEMPLATE").glob("*.yml")), ids=lambda p: p.name
)
def test_issue_form_links_are_absolute(form):
    # An issue form renders at /issues/new: a relative link resolves outside the repository.
    links = re.findall(r"\]\(([^)]+)\)", form.read_text(encoding="utf-8"))
    assert [link for link in links if not link.startswith(("https://", "mailto:"))] == []


PUBLIC_TYPED_MODULES = (
    "src/kbm_ledsas_sdk/models",
    "src/kbm_ledsas_sdk/runtime/context.py",
    "src/kbm_ledsas_sdk/blob/operations.py",
    "src/kbm_ledsas_sdk/blob/direct_operations.py",
    "src/kbm_ledsas_sdk/utils/logging.py",
    "src/kbm_ledsas_sdk/app.py",
)


def test_ci_blocks_on_strict_typing_of_the_public_modules():
    typed = [s for s in steps("ci.yml") if "Type check the public modules" in s]
    assert typed and "continue-on-error" not in typed[0], typed
    step = " ".join(typed[0].split())
    assert "mypy --strict --follow-imports=silent" in step
    for module in PUBLIC_TYPED_MODULES:
        assert module in step, module
        assert (ROOT / module).exists(), module
    assert " ".join(PUBLIC_TYPED_MODULES) in read("CONTRIBUTING.md")


def test_ci_runs_the_typed_consumer_test_and_requires_mypy():
    typed = [s for s in steps("ci.yml") if "tests/test_typed_consumer.py" in s]
    assert typed and "continue-on-error" not in typed[0]
    assert 'LEDSAS_REQUIRE_MYPY: "1"' in typed[0]


def test_configuration_error_points_at_a_reachable_reference():
    from kbm_ledsas_sdk.runtime.config import SDKConfig
    from kbm_ledsas_sdk.transport._build_direct import missing_connection_settings
    from kbm_ledsas_sdk.utils.logging import redact_secrets

    problems = missing_connection_settings(SDKConfig(service_name="svc"))
    pointer = problems[-1]
    assert (
        f"https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v{VERSION}/docs/SDK_API_REFERENCE.md"
        in pointer
    )
    assert pointer.endswith("#environment-variables).")
    assert "***" not in redact_secrets(" ".join(problems))


def bash_blocks(text: str) -> list[str]:
    return re.findall(r"^```bash\n(.*?)^```", text, re.M | re.S)


def waits_for_the_stack(block: str) -> bool:
    """`docker compose ... up -d` is followed by a wait for the broker: `--wait`
    (the compose file's healthchecks) or an `until` loop."""
    for line in block.splitlines():
        if re.search(r"docker compose\b.*\bup\b.*\s-d\b", line):
            return "--wait" in line or "until " in block[block.index(line) :]
    return True


def test_upgrading_and_compatibility_state_the_old_heartbeat_as_the_library_default():
    # 0.3.3 asked for no heartbeat, and the AMQP client library sent its own
    # default (60 s) whatever the broker proposed: not "the broker's interval".
    changelog = read("CHANGELOG.md")
    assert "client library's default" in changelog
    for rel in ("UPGRADING.md", "COMPATIBILITY.md"):
        text = " ".join(read(rel).split())
        assert "broker's interval " + "applied" not in text, rel
    # stated in the upgrade guide (COMPATIBILITY only links to it)
    assert "AMQP client library's default of 60 seconds applied" in " ".join(
        read("UPGRADING.md").split()
    )


def test_upgrading_states_the_heartbeat_detection_time():
    # The client library gives up after 3 * (interval + 1) s and checks every
    # interval // 2 s: 183 to 213 s at the default 60.
    interval = heartbeat_default()
    low, high = 3 * (interval + 1), 3 * (interval + 1) + max(1, interval // 2)
    text = " ".join(section(read("UPGRADING.md"), f"Upgrading to {VERSION}").split())
    assert f"{low} to {high}" in text
    assert "noticed within about " + "30 seconds" not in text


def test_upgrading_source_install_runs_the_tests_it_installs():
    blocks = [b for b in bash_blocks(read("UPGRADING.md")) if "pytest" in b]
    assert blocks
    for block in blocks:
        assert 'pip install -e ".[dev]"' in block, block
        assert 'pytest -m "not integration"' in block, block


def test_contributing_integration_block_waits_for_the_broker():
    blocks = [b for b in bash_blocks(read("CONTRIBUTING.md")) if "-m integration" in b]
    assert blocks
    for block in blocks:
        assert "docker compose" in block and waits_for_the_stack(block), block


def test_readme_local_development_waits_for_the_stack_before_the_service():
    local = section(README, "Local development")
    blocks = [b for b in bash_blocks(local) if "docker compose" in b]
    assert blocks
    for block in blocks:
        assert waits_for_the_stack(block), block
        assert block.index("docker compose") < block.index("python main.py")


def test_waits_for_the_stack_refuses_a_bare_up():
    assert not waits_for_the_stack("docker compose -f x.yml up -d\npython main.py\n")
    assert waits_for_the_stack("docker compose -f x.yml up -d --wait\npython main.py\n")


def test_contributing_documents_the_live_test_broker_variable():
    text = read("CONTRIBUTING.md")
    assert "LEDSAS_TEST_RABBITMQ_URL" in text and "HELLO_WORLD_AMQP_PORT" in text
    live = read("tests/integration/test_reply_unroutable_live.py")
    assert "LEDSAS_TEST_RABBITMQ_URL" in live
    assert "--wait" in live  # the skip message starts the stack the way that works
    # The broker address is read from the environment; 127.0.0.1:5672 only as the default.
    assert re.search(
        r'^DEFAULT_RABBITMQ_URL = "amqp://guest:guest@127\.0\.0\.1:5672/"$', live, re.M
    )
    assert re.search(r"^RABBITMQ_URL = live_rabbitmq_url\(\)$", live, re.M)
    assert not re.search(r'^RABBITMQ_(?:HOST|PORT) = ["0-9]', live, re.M)  # derived from the URL


def test_every_compose_up_in_the_documents_waits_for_the_stack():
    for path in repository_files(ROOT, "*.md") + repository_files(ROOT, "docker-compose*.yml"):
        text = path.read_text(encoding="utf-8")
        for no, line in enumerate(text.splitlines(), 1):
            if re.search(r"docker compose\b.*\bup\b.*\s-d\b", line):
                assert (
                    "--wait" in line or "until " in text
                ), f"{path.relative_to(ROOT)}:{no}: {line}"


# --------------------------------------------------------------------------- #
# The example's README (its wording is checked here, not by the example's own
# tests, so that a copy of the example can rewrite its README freely)
# --------------------------------------------------------------------------- #
EXAMPLE_README = read("examples/hello_world_service/README.md")


def test_example_readme_container_section_runs_against_the_local_stack():
    container = section(EXAMPLE_README, "Container image")
    assert "docker run" in container
    for setting in (
        "KBM_LEDSAS_ALLOW_INSECURE_AMQP",
        "KBM_LEDSAS_HEALTH_HOST=0.0.0.0",
        "--network",
    ):
        assert setting in container, setting
    assert "stop_grace_period" in container or "docker stop -t" in container


def test_example_readme_names_the_port_variables_and_the_handler_timeout():
    run = section(EXAMPLE_README, "Run it locally")
    for var in ("HELLO_WORLD_AMQP_PORT", "HELLO_WORLD_MGMT_PORT", "HELLO_WORLD_BLOB_PORT"):
        assert var in run, var
    assert "KBM_LEDSAS_HANDLER_TIMEOUT" in run


def test_example_readme_states_what_the_base_image_ships():
    text = " ".join(EXAMPLE_README.split())
    assert "stays in the image" not in text and "may still carry" not in text
    assert "The base image ships without `setuptools`, `wheel`" in text


def test_example_wording_names_no_indirect_path():
    for rel in ("examples/hello_world_service/README.md", "examples/hello_world_service/main.py"):
        assert "straight to" not in read(rel), rel


def test_example_dependency_bot_advice_is_for_your_own_repository():
    text = " ".join(EXAMPLE_README.split())
    assert "into your own repository" in text and "package-ecosystem: docker" in EXAMPLE_README
    # Every place the example names the SDK version is listed in "Updating the SDK version".
    update = " ".join(section(EXAMPLE_README, "Updating the SDK version").split())
    pins = {
        "Dockerfile": f"kbm-ledsas-base-direct:{VERSION}"
        in read("examples/hello_world_service/Dockerfile"),
        "main.py": f"kbm-ledsas-sdk=={VERSION}" in read("examples/hello_world_service/main.py"),
        "Run it locally": f"kbm-ledsas-sdk=={VERSION}" in section(EXAMPLE_README, "Run it locally"),
    }
    for where, present in pins.items():
        if present:
            assert where in update, where


HEALTHCHECK_RX = re.compile(r"^HEALTHCHECK [^\n]*\\\n\s*CMD (\[.*\])$", re.M)


def test_example_healthcheck_reports_a_503_in_one_line(tmp_path):
    import json
    import subprocess
    import sys
    import textwrap
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    blocks = re.findall(r"```dockerfile\n(.*?)^\s*```", EXAMPLE_README, re.S | re.M)
    block = next(textwrap.dedent(b) for b in blocks if "HEALTHCHECK" in b)
    cmd = json.loads(HEALTHCHECK_RX.search(block).group(1))
    assert cmd[:2] == ["python", "-c"]

    class Unavailable(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - the http.server API
            self.send_response(503)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Unavailable)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        code = cmd[2].replace(":8090/", f":{server.server_port}/")
        r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    finally:
        server.shutdown()
        server.server_close()
    assert r.returncode == 1
    assert "Traceback" not in r.stderr
    assert r.stderr.strip().splitlines() == ["not ready: HTTP Error 503: Service Unavailable"]
    assert "KBM_LEDSAS_HEALTH_PORT" in block or "KBM_LEDSAS_HEALTH_PORT" in EXAMPLE_README


def test_example_env_says_the_container_setting_is_informational():
    env = read("examples/hello_world_service/.env.example")
    assert "Informational only" in env
    assert "required at startup" not in env and "Default blob container" not in env


def test_upgrading_and_compatibility_keep_no_verify_ssl_zero_apart():
    # The client library verifies the certificate when no_verify_ssl is "0" (or
    # absent), and the SDK accepts that value: only other values are refused.
    for rel in ("UPGRADING.md", "COMPATIBILITY.md"):
        assert "whatever its " + "value" not in " ".join(read(rel).split()), rel
    assert "any value except `0`" in " ".join(read("UPGRADING.md").split())


def test_the_0_3_6_behaviour_changes_are_in_the_upgrade_and_compatibility_guides():
    upgrading = " ".join(section(read("UPGRADING.md"), f"Upgrading to {VERSION}").split())
    for needed in (
        "New, optional `KBM_LEDSAS_ALLOWED_CONTAINERS`",
        "`errors.ContainerNotAllowed` (`from kbm_ledsas_sdk import errors`;",
        "`download_json` and `upload_json` take only a JSON object or array",
        "`Command`, `Response` and `Status` check their envelope's `type`",
        "A consumer the broker cancels is noticed",
        "Configured secrets are masked",
        "`'**redacted**'`",
    ):
        assert needed in upgrading, needed
    compat = " ".join(read("COMPATIBILITY.md").split(f"**Exception in {VERSION}:**", 1)[1].split())
    assert "UPGRADING.md#upgrading-to-036-from-033" in compat  # the list is in the upgrade guide


def test_readme_documents_the_container_allowlist():
    config = section(README, "Configuration")
    row = re.search(r"^\| `KBM_LEDSAS_ALLOWED_CONTAINERS` \|.*\|$", config, re.M)
    assert row and "ContainerNotAllowed" in row.group(0) and "no restriction" in row.group(0)
    quickstart = " ".join(section(README, "Quickstart").split())
    assert "KBM_LEDSAS_ALLOWED_CONTAINERS" in quickstart
    assert "does not authenticate callers" in quickstart


# --------------------------------------------------------------------------- #
# Test and example URLs
# --------------------------------------------------------------------------- #
_USERINFO_URL = re.compile(r"\b([a-z][a-z0-9+.-]*)://[^\s/@:'\"]+:[^\s/@'\"]+@")
_SCANNED_SUFFIXES = {".py", ".md", ".yml", ".yaml", ".toml", ".ini", ".txt", ".example", ".cfg"}


def test_no_test_or_example_writes_a_password_url_other_than_amqp():
    """Secret scanners report a URL that carries a user and a password as a
    credential. Tests and examples build such URLs from parts; only an
    ``amqp://`` URL with the broker's local default user stays literal."""
    hits = []
    for base in ("tests", "examples"):
        for path in repository_files(ROOT / base):
            if path.suffix not in _SCANNED_SUFFIXES and not path.name.startswith(".env"):
                continue
            text = path.read_text(encoding="utf-8")
            for number, line in enumerate(text.splitlines(), 1):
                for match in _USERINFO_URL.finditer(line):
                    if match.group(1) not in ("amqp", "amqps"):
                        hits.append(f"{path.relative_to(ROOT)}:{number}: {match.group(0)}")
    assert hits == []


def test_repository_scans_skip_virtual_environments(tmp_path):
    example = tmp_path / "examples" / "hello_world_service"
    (example / ".venv" / "lib").mkdir(parents=True)
    url = "https://" + ":".join(("u", "p")) + "@h/"  # built from parts (secret scanners)
    (example / ".venv" / "lib" / "dep.py").write_text(f"URL = {url!r}\n")
    (example / "main.py").write_text("print()\n")
    found = repository_files(tmp_path / "examples", root=tmp_path)
    assert found == [example / "main.py"]


def test_the_release_notes_name_the_numeric_example_user():
    """The example ends with ``USER 1000:1000``; the upgrade guide and the
    changelog say why (``runAsNonRoot`` without ``runAsUser``)."""
    upgrading = " ".join(section(read("UPGRADING.md"), f"Upgrading to {VERSION}").split())
    assert "End your `Dockerfile` with the numeric `USER 1000:1000`" in upgrading
    assert "`runAsNonRoot: true` and no `runAsUser`" in upgrading
    entry = " ".join(
        read("CHANGELOG.md").split(f"## [{VERSION}]", 1)[1].split("\n## [", 1)[0].split()
    )
    assert "ends with the numeric `USER 1000:1000`" in entry
    assert "`runAsNonRoot: true` and no `runAsUser` is admitted" in entry


# --------------------------------------------------------------------------- #
# Texts and names the documents quote are the package's own
# --------------------------------------------------------------------------- #
def _logged_text(source: str, prefix: str) -> str:
    """The concatenated string literal of the call in ``source`` whose text
    starts with ``prefix`` (up to its first ``%s``)."""
    import ast

    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value.startswith(prefix):
                return node.value
    raise AssertionError(f"no string starting with {prefix!r}")


def test_upgrading_quotes_the_consumer_cancel_records_of_the_source():
    source = read("src/kbm_ledsas_sdk/transport/direct.py")
    error = _logged_text(source, "The AMQP broker cancelled the command consumer")
    info = _logged_text(source, "Consuming commands from")
    upgrading = " ".join(read("UPGRADING.md").split())
    quoted_error = error.split("%s", 1)[0] + "<queue> ..."
    quoted_info = info.replace("%s", "<queue>")
    assert f"one ERROR (`{quoted_error}`)" in upgrading, quoted_error
    assert f"one INFO `{quoted_info}`" in upgrading, quoted_info


_DOTTED = re.compile(r"\bkbm_ledsas_sdk(?:\.[A-Za-z_][A-Za-z0-9_]*)+")


def resolves_as_written(dotted: str) -> bool:
    """True when ``dotted`` names a module, or an attribute of the module its
    longest importable prefix names (``kbm_ledsas_sdk.errors.X`` passes only
    when ``kbm_ledsas_sdk.errors`` is a module)."""
    import importlib

    parts = dotted.split(".")
    for cut in range(len(parts), 0, -1):
        try:
            obj = importlib.import_module(".".join(parts[:cut]))
        except ImportError:
            continue
        rest = parts[cut:]
        if len(rest) > 1:
            return False  # a dotted path through a non-module
        return not rest or hasattr(obj, rest[0])
    return False


@pytest.mark.parametrize("rel", ["README.md", "UPGRADING.md", "COMPATIBILITY.md"])
def test_every_dotted_package_name_in_the_guides_resolves(rel):
    names = sorted(set(_DOTTED.findall(read(rel))))
    assert [n for n in names if not resolves_as_written(n)] == []


def test_resolves_as_written_refuses_a_path_through_an_attribute():
    assert resolves_as_written("kbm_ledsas_sdk.models.errors.ContainerNotAllowed")
    assert resolves_as_written("kbm_ledsas_sdk.json_log_formatter")
    assert not resolves_as_written("kbm_ledsas_sdk.models.BlobRef.from_uri")
    assert not resolves_as_written("kbm_ledsas_sdk.no_such_name")


def test_upgrading_quotes_the_registration_errors_of_the_package():
    from kbm_ledsas_sdk import ServiceApp

    handlers = section(read("UPGRADING.md"), "Handlers", level=3)
    block = re.search(r"```text\n(.*?)```", handlers, re.S).group(1)
    quoted = [line.strip() for line in block.splitlines() if line.strip()]

    async def one(ctx):
        return None

    def not_async(ctx, payload):
        return None

    raised = []
    for name, version, handler in (
        ("Process-CSV", "1.0", one),
        ("ProcessCSV", "1", one),
        ("ProcessCSV", "1.0", not_async),
        ("ProcessCSV", "1.0", one),
    ):
        with pytest.raises((TypeError, ValueError)) as info:
            ServiceApp(service_name="svc").handler(name, version=version)(handler)
        raised.append(f"{type(info.value).__name__}: {info.value}")
    assert len(quoted) == len(raised)
    for line, actual in zip(quoted, raised, strict=True):
        expected = line[:-3].rstrip() if line.endswith("...") else line
        assert actual.startswith(expected), (line, actual)


def test_upgrading_tells_how_a_handler_that_assigns_a_model_field_ends():
    text = " ".join(read("UPGRADING.md").split())
    assert "in a handler the command is then replied `Permanent` (not retried)" in text
    assert "naming `model_copy(update=...)`" in text


def test_upgrading_names_what_is_masked_and_what_is_not():
    text = " ".join(read("UPGRADING.md").split())
    for needed in ("error messages", "`user_message`", "status `stage` / `note`"):
        assert needed in text, needed
    assert "a handler's result is sent as returned" in text
    assert "wherever they appear" not in text and "reply texts" not in text


def test_waits_cover_a_storage_outage():
    callers = " ".join(section(read("UPGRADING.md"), "Callers", level=3).split())
    assert "Unreachable storage adds about 7.5 seconds per attempt" in callers
    send = section(section(README, "Quickstart"), "Send it a command", level=3)
    assert "are the broker and storage reachable?" in send
    assert "(is my_service.py running?)" not in send


# --------------------------------------------------------------------------- #
# The README quickstart and container section run as written
# --------------------------------------------------------------------------- #
def test_readme_rabbitmq_wait_is_bounded_and_stops_on_a_dead_container():
    shell = bash_blocks(section(README, "Quickstart"))[0]
    assert not re.search(r"^\s*until ", shell, re.M)  # an unbounded wait
    loop = re.search(r"^for i in \$\(seq ([0-9]+)\); do\n(.*?)^done$", shell, re.M | re.S)
    assert loop, shell
    assert int(loop.group(1)) * 2 <= 300
    assert "{{.State.Running}}" in loop.group(2) and "|| break" in loop.group(2)


def test_readme_dockerfile_copies_only_files_the_quickstart_names():
    container = section(README, "Container base image")
    dockerfile = re.search(r"```dockerfile\n(.*?)```", container, re.S).group(1)
    named = " ".join(section(README, "Quickstart").split()) + " " + " ".join(container.split())
    sources = [
        src
        for line in dockerfile.splitlines()
        if line.startswith("COPY ")
        for src in line.split()[1:-1]
    ]
    assert sources and "my_service.py" in sources
    for src in sources:
        assert f"`{src}`" in named, src
    assert 'CMD ["python", "my_service.py"]' in dockerfile


def test_readme_quickstart_logs_in_the_configured_format():
    service = re.search(r"^```python\n(.*?)^```", section(README, "Quickstart"), re.M | re.S).group(
        1
    )
    assert "KBM_LEDSAS_LOG_FORMAT" in service and '== "text"' in service
    assert "json_log_formatter()" in service


def test_container_guidance_names_the_memory_setting_and_a_restart_policy():
    container = " ".join(section(README, "Container base image").split())
    example = " ".join(section(EXAMPLE_README, "Container image").split())
    for text in (container, example):
        assert "KBM_LEDSAS_MAX_PAYLOAD_BYTES" in text
        assert "restart policy" in text and "--restart on-failure" in text
        assert "exits with code 1" in text
    env = read("examples/hello_world_service/.env.example")
    assert re.search(r"^KBM_LEDSAS_MAX_PAYLOAD_BYTES=1048576$", env, re.M)


# --------------------------------------------------------------------------- #
# Contributor flow, workflows and pins
# --------------------------------------------------------------------------- #
# Words for a process outside this repository ("mono" + "repo": the scans of
# this file's own text must not find the word either).
_INTERNAL_PROCESS = re.compile(
    r"\b(?:sync(?:ed|ing)?|mirror(?:ed)?|upstream|" + "mono" + r"repo)\b", re.I
)


def test_contributor_files_describe_only_processes_of_this_repository():
    files = [ROOT / "CONTRIBUTING.md", *repository_files(ROOT / ".github")]
    hits = [
        f"{path.relative_to(ROOT)}:{no}: {line.strip()}"
        for path in files
        for no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if _INTERNAL_PROCESS.search(line)
    ]
    assert hits == []


def test_contributing_names_the_release_link_step():
    releases = " ".join(section(read("CONTRIBUTING.md"), "Releases (maintainers)").split())
    assert "Change every `blob/v<previous>/` and `tree/v<previous>/` link" in releases
    release = read(".github/workflows/release.yml")
    assert 'grep -v "/v${VERSION}/$"' in release  # a link to another tag fails


def test_every_variable_the_integration_tests_read_is_documented():
    contributing = read("CONTRIBUTING.md")
    names = set()
    for path in repository_files(ROOT / "tests" / "integration", "*.py"):
        names |= set(
            re.findall(
                r"""environ(?:\.get\(|\[)\s*["']([A-Z][A-Z0-9_]+)["']""",
                path.read_text(encoding="utf-8"),
            )
        )
    names -= {"KBM_LEDSAS_RABBITMQ_URL", "KBM_LEDSAS_BLOB_CONN_STRING", "KBM_LEDSAS_TENANT"}
    assert {"LEDSAS_TEST_RABBITMQ_URL", "LEDSAS_TEST_BLOB_CONN_STRING"} <= names
    assert [n for n in sorted(names) if n not in contributing] == []


def test_ci_runs_the_integration_tests_with_azurite_and_no_skip():
    jobs = _jobs("ci.yml")
    integration = jobs["integration"]
    assert "AZURITE_IMAGE: mcr.microsoft.com/azure-storage/azurite:" in integration
    assert '"$AZURITE_IMAGE"' in integration and "--skipApiVersionCheck" in integration
    assert "skipped' integration.out" in integration and "exit 1" in integration


def test_ci_unit_tests_fail_on_a_warning_and_cover_aio_pika_9():
    unit = [s for s in steps("ci.yml") if '-m "not integration"' in s]
    assert unit and all("-W error" in s for s in unit), unit
    test_job = _jobs("ci.yml")["test"]
    assert 'aio-pika: "9"' in test_job and '"aio-pika>=9.4.0,<10"' in test_job


def test_every_tool_table_in_pyproject_is_a_tool_the_repository_installs():
    data = tomllib.loads(read("pyproject.toml"))
    installed = {
        re.match(r"[A-Za-z0-9_.-]+", r).group(0).lower()
        for r in data["project"]["optional-dependencies"]["dev"] + data["build-system"]["requires"]
    }
    for tool in data["tool"]:
        assert tool.lower() in installed, tool


def _image(ref: str) -> str:
    return ref.split("@", 1)[0]


def test_the_quickstart_images_are_the_compose_and_ci_images():
    compose = read("examples/hello_world_service/deploy/local/docker-compose.yml")
    compose_images = set(re.findall(r"^\s+image: (\S+)$", compose, re.M))
    quickstart = "\n".join(bash_blocks(section(README, "Quickstart")))
    readme_images = set(
        re.findall(r"((?:mcr\.microsoft\.com/\S+|rabbitmq):[0-9][\w.-]*)", quickstart)
    )
    ci = read(".github/workflows/ci.yml")
    ci_images = {_image(i) for i in re.findall(r"(?:image:|AZURITE_IMAGE:)\s*(\S+)", ci)}
    assert len(compose_images) == 2
    assert readme_images == compose_images == ci_images, (readme_images, compose_images, ci_images)


def test_requirements_headers_name_the_workflows_that_install_them():
    workflows = {p.name: p.read_text(encoding="utf-8") for p in WORKFLOWS.glob("*.yml")}
    for req in sorted((ROOT / ".github").glob("*-requirements.txt")):
        header = " ".join(
            ln.lstrip("# ")
            for ln in req.read_text(encoding="utf-8").splitlines()
            if ln.startswith("#")
        )
        users = {name for name, text in workflows.items() if f".github/{req.name}" in text}
        named = set(re.findall(r"\.github/workflows/([a-z-]+\.yml)", header))
        if "CI build job" in header:
            named.add("ci.yml")
        assert users and named == users, (req.name, named, users)


def test_upgrading_and_the_changelog_name_the_security_floors_of_pyproject():
    deps = tomllib.loads(read("pyproject.toml"))["project"]["dependencies"]
    raw = read("pyproject.toml")
    floors = {
        re.match(r"[a-z-]+", d).group(0)
        for d in deps
        if re.search(rf'Security floor[^\n]*\n(?:\s*#[^\n]*\n)*\s*"{re.escape(d)}"', raw)
    }
    assert floors == {"azure-core", "aiohttp", "cryptography"}, floors
    checklist = section(read("UPGRADING.md"), "Checklist", level=3).split("\n2.", 1)[0]
    for name in floors:
        assert f"`{name}` below" in checklist, name
