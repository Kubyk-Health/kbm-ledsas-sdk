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
        "except KeyboardInterrupt:",
        "expiration=WAIT",
        "aio_pika.connect(",
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
    for change in ("one reply per command", "priority", "reply_to", "health port"):
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
    assert not [m for m in marker if "Azurite" in m]


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
    assert "sed -i" not in release.split("Rehearsal version", 1)[1].split("Tests", 1)[1]
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
    assert "--pyproject pyproject.toml --mc-type library" in release
    assert ".github/scripts/finish_sbom.py" in release
    finish = read(".github/scripts/finish_sbom.py")
    assert "pkg:pypi/kbm-ledsas-sdk@" in finish and "file://" in finish


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
    text = section(read("UPGRADING.md"), f"Upgrading to {VERSION}")
    for needed in (
        "`amq.",  # reply_to naming a reserved broker exchange is refused
        "no_verify_ssl",
        "?cafile=",
        "KBM_LEDSAS_HEARTBEAT",
        f"default `{heartbeat_default()}` seconds",
        "counts toward\n  `KBM_LEDSAS_MAX_RETRIES`",
        "invalid `?heartbeat=`",
        "asyncio.to_thread",
        "Message models and `BlobRef` are read-only",
        "BlobRef(uri=..., version_id=...)",
        "model_copy(update=",
        "ASCII digits",
    ):
        assert needed in text, needed
    assert "send_response()" not in read("UPGRADING.md")


def test_compatibility_exception_paragraph_lists_the_other_changes():
    text = read("COMPATIBILITY.md")
    para = text.split(f"**Exception in {VERSION}:**", 1)[1].split("\n## ", 1)[0]
    for change in ("`amq.…`", "no_verify_ssl", "KBM_LEDSAS_HEARTBEAT", "read-only", "ASCII digits"):
        assert change in para, change
    flat = " ".join(para.split())
    assert "BlobRef` are read-only" in flat and "BlobRef(version_id=...)" in flat
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
        assert "AMQP client library's default of 60 seconds applied" in text, rel


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
    for path in sorted(ROOT.rglob("*.md")) + sorted(ROOT.rglob("docker-compose*.yml")):
        if set(path.relative_to(ROOT).parts) & {".git", ".venv", "venv", "build", "dist"}:
            continue
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
        text = " ".join(read(rel).split())
        assert "any value except `0`" in text, rel
        assert "whatever its " + "value" not in text, rel


def test_the_0_3_6_behaviour_changes_are_in_the_upgrade_and_compatibility_guides():
    upgrading = " ".join(section(read("UPGRADING.md"), f"Upgrading to {VERSION}").split())
    for needed in (
        "New, optional `KBM_LEDSAS_ALLOWED_CONTAINERS`",
        "kbm_ledsas_sdk.errors.ContainerNotAllowed",
        "`download_json` and `upload_json` take only a JSON object or array",
        "`Command`, `Response` and `Status` check their envelope's `type`",
        "A consumer the broker cancels is noticed",
        "Configured secrets are masked wherever they appear",
        "`'**redacted**'`",
    ):
        assert needed in upgrading, needed
    compat = " ".join(read("COMPATIBILITY.md").split(f"**Exception in {VERSION}:**", 1)[1].split())
    for needed in (
        "KBM_LEDSAS_ALLOWED_CONTAINERS",
        "take only a JSON object or array",
        "a consumer the broker cancels is noticed",
        "'**redacted**'",
        "another `type`",
    ):
        assert needed in compat, needed


def test_readme_documents_the_container_allowlist():
    config = section(README, "Configuration")
    row = re.search(r"^\| `KBM_LEDSAS_ALLOWED_CONTAINERS` \|.*\|$", config, re.M)
    assert row and "ContainerNotAllowed" in row.group(0) and "no restriction" in row.group(0)
    quickstart = " ".join(section(README, "Quickstart").split())
    assert "KBM_LEDSAS_ALLOWED_CONTAINERS" in quickstart
    assert "does not authenticate callers" in quickstart
