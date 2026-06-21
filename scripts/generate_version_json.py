#!/usr/bin/env python3
"""Generate no-cache frontend deployment metadata for Update Center checks.

Run this script as part of every frontend build before assets are published.
It writes a ``version.json`` file containing the semantic version, deployment
build ID, full version, and release timestamp used by the backend Update Center
endpoints.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))
DEFAULT_OUTPUT = ROOT_DIR / "public" / "version.json"


def _git_short_sha() -> str:
    for command in (("git", "rev-parse", "--short", "HEAD"),):
        try:
            return subprocess.check_output(command, cwd=ROOT_DIR, text=True).strip()
        except Exception:
            continue
    return "nogit"


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def deployment_build_id() -> str:
    explicit = os.environ.get("PHARMACYOS_BUILD_ID") or os.environ.get("VITE_PHARMACYOS_BUILD_ID")
    if explicit:
        return explicit
    provider_build = os.environ.get("RENDER_GIT_COMMIT") or os.environ.get("VERCEL_GIT_COMMIT_SHA") or os.environ.get("GITHUB_SHA")
    if provider_build:
        return f"{_utc_stamp()}-{provider_build[:7]}"
    return f"{_utc_stamp()}-{_git_short_sha()}"


def main() -> None:
    from version_config import get_version_metadata

    build_id = deployment_build_id()
    metadata = get_version_metadata(deployed_build_id=build_id)
    payload = {
        "latest_version": metadata["latest_version"],
        "latest_build": metadata["latest_build"],
        "full_version": metadata["full_version"],
        "release_timestamp": metadata["release_timestamp"],
        "release_date": metadata["release_date"],
    }
    output = Path(os.environ.get("PHARMACYOS_VERSION_JSON_PATH", DEFAULT_OUTPUT))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Generated {output.relative_to(ROOT_DIR)} for {payload['full_version']} at {payload['release_timestamp']}")


if __name__ == "__main__":
    main()
