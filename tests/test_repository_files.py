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
    assert "/blob/${TAG}/" in release and "'/blob/main/" in release


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
    assert 'grep -Fqx "Signed-off-by: $author"' in read(".github/workflows/dco.yml")


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


def test_readme_configuration_lists_the_heartbeat():
    config = section(README, "Configuration")
    row = re.search(r"^\| `KBM_LEDSAS_HEARTBEAT` \|.*\| `15` \|$", config, re.M)
    assert row, "no KBM_LEDSAS_HEARTBEAT row with its default"


def test_upgrading_tells_the_changes_that_can_break_a_deployment():
    text = section(read("UPGRADING.md"), f"Upgrading to {VERSION}")
    for needed in (
        "`amq.",  # reply_to naming a reserved broker exchange is refused
        "no_verify_ssl",
        "?cafile=",
        "KBM_LEDSAS_HEARTBEAT",
        "KBM_LEDSAS_HEARTBEAT=60",
        "Message models are read-only",
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
        "https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/docs/SDK_API_REFERENCE.md"
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
    # interval // 2 s: 48 to 55 s at the default 15.
    interval = 15
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


def test_upgrading_and_compatibility_keep_no_verify_ssl_zero_apart():
    # The client library verifies the certificate when no_verify_ssl is "0" (or
    # absent), and the SDK accepts that value: only other values are refused.
    for rel in ("UPGRADING.md", "COMPATIBILITY.md"):
        text = " ".join(read(rel).split())
        assert "any value except `0`" in text, rel
        assert "whatever its " + "value" not in text, rel
