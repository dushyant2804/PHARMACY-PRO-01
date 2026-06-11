"""Central PharmacyOS release metadata.

Future release automation should update only ``VERSION_METADATA`` in this file.
Keep release notes as a multiline string so clients can display them directly.
"""

from copy import deepcopy
from typing import Literal, TypedDict

UpdateType = Literal["patch", "minor", "major"]
SUPPORTED_UPDATE_TYPES: tuple[UpdateType, ...] = ("patch", "minor", "major")


class VersionMetadata(TypedDict):
    version: str
    build: str
    release_notes: str
    message: str
    updated_at: str
    update_type: UpdateType


VERSION_METADATA: VersionMetadata = {
    "version": "3.1.0",
    "build": "3.1.0",
    "release_notes": """Fixed legacy purchase-return stock recalculation.
Returned medicines are now removed from expiry/expired alerts.
Added automatic post-deployment stock repair.
Added admin stock repair endpoint.
Improved dashboard/inventory stock consistency.""",
    "message": "PharmacyOS 3.1.0 is ready to install.",
    "updated_at": "2026-06-11T00:00:00Z",
    "update_type": "minor",
}


def get_version_metadata() -> VersionMetadata:
    """Return an isolated copy of the validated release metadata."""
    metadata = deepcopy(VERSION_METADATA)
    if metadata["update_type"] not in SUPPORTED_UPDATE_TYPES:
        raise ValueError(f"Unsupported update type: {metadata['update_type']}")
    return metadata
