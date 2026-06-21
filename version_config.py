"""Central PharmacyOS release metadata.

Release automation should append each deployed release to ``RELEASES`` and update
``LATEST_RELEASE_KEY``. Semantic versions and build metadata are intentionally
separate so clients can distinguish product changes from deployment/build IDs.
"""

from copy import deepcopy
from datetime import datetime, timezone
import os
import re
from typing import Literal, TypedDict

UpdateType = Literal["patch", "minor", "major"]
SUPPORTED_UPDATE_TYPES: tuple[UpdateType, ...] = ("patch", "minor", "major")

SEMVER_RE = re.compile(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)")
BUILD_RE = re.compile(r"[0-9]{8}(?:[0-9]{6})?-[A-Za-z0-9][A-Za-z0-9._-]*")


class ReleaseNotes(TypedDict):
    new: list[str]
    improved: list[str]
    fixed: list[str]


class ReleaseMetadata(TypedDict):
    semantic_version: str
    build_id: str
    release_date: str
    release_timestamp: str
    release_notes: ReleaseNotes
    update_type: UpdateType


class VersionMetadata(TypedDict):
    current_version: str
    latest_version: str
    current_build: str
    latest_build: str
    full_version: str
    update_available: bool
    release_date: str
    release_timestamp: str
    release_notes: ReleaseNotes


LATEST_RELEASE_KEY = "3.1.1+20260620-7682338"

RELEASES: dict[str, ReleaseMetadata] = {
    "3.1.1+20260620-7682338": {
        "semantic_version": "3.1.1",
        "build_id": "20260620-7682338",
        "release_date": "2026-06-20T00:00:00Z",
        "release_timestamp": "2026-06-20T00:00:00Z",
        "update_type": "patch",
        "release_notes": {
            "new": [],
            "improved": [
                "Update Center now separates semantic versions from build metadata.",
                "Update availability is calculated from version and release-note changes instead of changing date strings.",
            ],
            "fixed": [
                "Release notes are now tied to each deployed version/build instead of reusing one static fallback.",
            ],
        },
    },
    "3.1.0+20260611-stock-repair": {
        "semantic_version": "3.1.0",
        "build_id": "20260611-stock-repair",
        "release_date": "2026-06-11T00:00:00Z",
        "release_timestamp": "2026-06-11T00:00:00Z",
        "update_type": "minor",
        "release_notes": {
            "new": [
                "Added automatic post-deployment stock repair.",
                "Added admin stock repair endpoint.",
            ],
            "improved": [
                "Improved dashboard/inventory stock consistency.",
            ],
            "fixed": [
                "Fixed legacy purchase-return stock recalculation.",
                "Returned medicines are now removed from expiry/expired alerts.",
            ],
        },
    },
    "3.0.0+20260501-operations": {
        "semantic_version": "3.0.0",
        "build_id": "20260501-operations",
        "release_date": "2026-05-01T00:00:00Z",
        "release_timestamp": "2026-05-01T00:00:00Z",
        "update_type": "major",
        "release_notes": {
            "new": ["Expanded pharmacy operations and reporting backend."],
            "improved": [],
            "fixed": [],
        },
    },
}


def make_full_version(semantic_version: str, build_id: str) -> str:
    return f"{semantic_version}+{build_id}"


def parse_semantic_version(version: str) -> tuple[int, int, int]:
    if not SEMVER_RE.fullmatch(version):
        raise ValueError(f"Version must use semantic versioning: {version}")
    return tuple(int(part) for part in version.split("."))  # type: ignore[return-value]


def compare_semantic_versions(left: str, right: str) -> int:
    left_parts = parse_semantic_version(left)
    right_parts = parse_semantic_version(right)
    return (left_parts > right_parts) - (left_parts < right_parts)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _deployed_build_id(explicit_build_id: str | None = None) -> str | None:
    return explicit_build_id or os.environ.get("PHARMACYOS_BUILD_ID") or os.environ.get("VITE_PHARMACYOS_BUILD_ID")


def validate_release(key: str, release: ReleaseMetadata) -> None:
    expected_key = make_full_version(release["semantic_version"], release["build_id"])
    if key != expected_key:
        raise ValueError(f"Release key {key} must match full version {expected_key}")
    parse_semantic_version(release["semantic_version"])
    if not BUILD_RE.fullmatch(release["build_id"]):
        raise ValueError(f"Build ID must be a date- or timestamp-prefixed build identifier: {release['build_id']}")
    if release["update_type"] not in SUPPORTED_UPDATE_TYPES:
        raise ValueError(f"Unsupported update type: {release['update_type']}")
    notes = release["release_notes"]
    if set(notes) != {"new", "improved", "fixed"}:
        raise ValueError("Release notes must contain new, improved, and fixed lists")
    for category, entries in notes.items():
        if not isinstance(entries, list) or not all(isinstance(entry, str) for entry in entries):
            raise ValueError(f"Release note category {category} must be a list of strings")


def find_release(semantic_version: str, build_id: str | None = None) -> ReleaseMetadata | None:
    for release in RELEASES.values():
        if release["semantic_version"] == semantic_version and (build_id is None or release["build_id"] == build_id):
            return release
    return None


def is_update_available(current: ReleaseMetadata, latest: ReleaseMetadata) -> bool:
    semantic_compare = compare_semantic_versions(latest["semantic_version"], current["semantic_version"])
    if semantic_compare > 0:
        return True
    if semantic_compare < 0:
        return False
    return latest["build_id"] != current["build_id"] or latest["release_notes"] != current["release_notes"]


def get_version_metadata(
    current_version: str | None = None,
    current_build: str | None = None,
    deployed_build_id: str | None = None,
) -> VersionMetadata:
    """Return Update Center metadata for the deployed/latest release.

    ``current_version`` and ``current_build`` describe the caller's installed
    release. If omitted, the currently deployed release is treated as current.
    Build-only changes are marked as updates so deployed frontend assets can
    be refreshed after every deployment.
    """
    if current_version is not None and not isinstance(current_version, str):
        current_version = None
    if current_build is not None and not isinstance(current_build, str):
        current_build = None

    if LATEST_RELEASE_KEY not in RELEASES:
        raise ValueError(f"LATEST_RELEASE_KEY is not present in RELEASES: {LATEST_RELEASE_KEY}")
    for key, release in RELEASES.items():
        validate_release(key, release)

    latest = deepcopy(RELEASES[LATEST_RELEASE_KEY])
    runtime_build_id = _deployed_build_id(deployed_build_id)
    if runtime_build_id and runtime_build_id != latest["build_id"]:
        latest["build_id"] = runtime_build_id
        latest["release_timestamp"] = os.environ.get("PHARMACYOS_RELEASE_TIMESTAMP", _utc_now_iso())
        latest["release_date"] = latest["release_timestamp"]
    current = find_release(current_version, current_build) if current_version else latest
    if current is None and current_version:
        parse_semantic_version(current_version)
        current_notes = (
            latest["release_notes"]
            if current_version == latest["semantic_version"]
            else {"new": [], "improved": [], "fixed": []}
        )
        current = {
            "semantic_version": current_version,
            "build_id": current_build or "unknown",
            "release_date": "",
            "release_timestamp": "",
            "update_type": "patch",
            "release_notes": current_notes,
        }

    assert current is not None
    metadata: VersionMetadata = {
        "current_version": current["semantic_version"],
        "latest_version": latest["semantic_version"],
        "current_build": current["build_id"],
        "latest_build": latest["build_id"],
        "full_version": make_full_version(latest["semantic_version"], latest["build_id"]),
        "update_available": is_update_available(current, latest),
        "release_date": latest["release_date"],
        "release_timestamp": latest["release_timestamp"],
        "release_notes": latest["release_notes"],
    }
    return deepcopy(metadata)


VERSION_METADATA = get_version_metadata()
