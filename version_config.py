"""Central PharmacyOS release metadata.

Future release automation should update only ``VERSION_METADATA`` in this file.
Keep release notes as a multiline string so clients can display them directly.
"""

from copy import deepcopy
import re
from typing import Literal, TypedDict

UpdateType = Literal["patch", "minor", "major"]
SUPPORTED_UPDATE_TYPES: tuple[UpdateType, ...] = ("patch", "minor", "major")


class VersionMetadata(TypedDict):
    version: str
    build: str
    build_id: str
    release_notes: str
    whats_new: list[str]
    version_history: list[dict[str, str]]
    message: str
    updated_at: str
    update_type: UpdateType


VERSION_METADATA: VersionMetadata = {
    "version": "3.1.0",
    "build": "3.1.0",
    "build_id": "20260611-stock-repair",
    "release_notes": """Fixed legacy purchase-return stock recalculation.
Returned medicines are now removed from expiry/expired alerts.
Added automatic post-deployment stock repair.
Added admin stock repair endpoint.
Improved dashboard/inventory stock consistency.""",
    "whats_new": [
        "Purchase-return stock recalculation now repairs legacy records.",
        "Returned medicines no longer appear in expiry alerts.",
        "Administrators can run a stock consistency repair.",
    ],
    "version_history": [
        {"version": "3.1.0", "released_at": "2026-06-11T00:00:00Z", "summary": "Stock consistency and purchase-return repair improvements."},
        {"version": "3.0.0", "released_at": "2026-05-01T00:00:00Z", "summary": "Expanded pharmacy operations and reporting backend."},
    ],
    "message": "PharmacyOS 3.1.0 is ready to install.",
    "updated_at": "2026-06-11T00:00:00Z",
    "update_type": "minor",
}


def get_version_metadata() -> VersionMetadata:
    """Return an isolated copy of the validated release metadata."""
    metadata = deepcopy(VERSION_METADATA)
    if metadata["update_type"] not in SUPPORTED_UPDATE_TYPES:
        raise ValueError(f"Unsupported update type: {metadata['update_type']}")
    if not re.fullmatch(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)", metadata["version"]):
        raise ValueError(f"Version must use semantic versioning: {metadata['version']}")
    return metadata
