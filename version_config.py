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
    "version": "3.0",
    "build": "3.0.0",
    "release_notes": """Centralized PharmacyOS version and build metadata.
Added update type and update timestamp support for the frontend update flow.
Version checks are now explicitly protected from stale cached responses.""",
    "message": "PharmacyOS 3.0 is ready to install.",
    "updated_at": "2026-06-10T00:00:00Z",
    "update_type": "major",
}


def get_version_metadata() -> VersionMetadata:
    """Return an isolated copy of the validated release metadata."""
    metadata = deepcopy(VERSION_METADATA)
    if metadata["update_type"] not in SUPPORTED_UPDATE_TYPES:
        raise ValueError(f"Unsupported update type: {metadata['update_type']}")
    return metadata
