"""Complete and check the release SBOM (run by .github/workflows/release.yml).

``cyclonedx-py environment --pyproject pyproject.toml --mc-type library``
describes the wheel's runtime dependency closure with the SDK itself as
``metadata.component``. This script adds what that leaves out, then checks
the result:

- ``metadata.component.purl`` (``pkg:pypi/kbm-ledsas-sdk@<version>``) and the
  SHA-256 of the released wheel;
- the SHA-256 of every dependency, from the ``pip install --report`` of the
  environment the SBOM describes (the hash of the file pip installed);
- no ``file://`` URL (a path on the build machine) anywhere in the document;
- valid CycloneDX 1.6.

usage: python finish_sbom.py <raw sbom> <pip report> <wheel> <version> <output>
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path


def _norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def finish(raw: dict, report: dict, wheel: Path, version: str) -> tuple[dict, list[str]]:
    problems: list[str] = []
    hashes: dict[tuple[str, str], str] = {}
    for item in report.get("install", []):
        meta = item.get("metadata", {})
        archive = item.get("download_info", {}).get("archive_info", {})
        digest = archive.get("hashes", {}).get("sha256") or (
            archive.get("hash", "").split("=", 1)[1]
            if archive.get("hash", "").startswith("sha256=")
            else None
        )
        if digest and meta.get("name") and meta.get("version"):
            hashes[(_norm(meta["name"]), meta["version"])] = digest
    root = raw.setdefault("metadata", {}).setdefault("component", {})
    root["purl"] = f"pkg:pypi/kbm-ledsas-sdk@{version}"
    root["hashes"] = [{"alg": "SHA-256", "content": hashlib.sha256(wheel.read_bytes()).hexdigest()}]
    if root.get("name") != "kbm-ledsas-sdk" or root.get("version") != version:
        problems.append(
            f"metadata.component is {root.get('name')} {root.get('version')}, not kbm-ledsas-sdk {version}"
        )
    for comp in raw.get("components", []):
        digest = hashes.get((_norm(comp.get("name", "")), comp.get("version", "")))
        if digest:
            comp["hashes"] = [{"alg": "SHA-256", "content": digest}]
        elif not comp.get("hashes"):
            problems.append(f"no hash for {comp.get('name')} {comp.get('version')}")
    text = json.dumps(raw)
    if "file://" in text:
        problems.append("a file:// URL is left in the SBOM")
    return raw, problems


def main(argv: list[str]) -> int:
    if len(argv) != 6:
        print(__doc__, file=sys.stderr)
        return 2
    raw = json.loads(Path(argv[1]).read_text(encoding="utf-8"))
    report = json.loads(Path(argv[2]).read_text(encoding="utf-8"))
    doc, problems = finish(raw, report, Path(argv[3]), argv[4])
    out = json.dumps(doc, indent=2) + "\n"
    from cyclonedx.schema import SchemaVersion
    from cyclonedx.validation.json import JsonStrictValidator

    error = JsonStrictValidator(SchemaVersion.V1_6).validate_str(out)
    if error:
        problems.append(f"not valid CycloneDX 1.6: {str(error)[:300]}")
    for p in problems:
        print(f"::error::SBOM: {p}")
    if problems:
        return 1
    Path(argv[5]).write_text(out, encoding="utf-8")
    print(
        f"SBOM: {argv[5]}: {len(doc.get('components', []))} components, all hashed; "
        f"root {doc['metadata']['component']['purl']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
