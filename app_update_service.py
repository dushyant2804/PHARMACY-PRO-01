"""Helpers for PharmacyOS backend update notifications.

This module only checks a remote manifest and formats update metadata. It does
not download update payloads, install updates, or modify local data.
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any, Dict, List, Mapping

from app_version import APP_BUILD, APP_VERSION

MANIFEST_TIMEOUT_SECONDS = 5


class ManifestUnavailable(Exception):
    """Raised when the update manifest cannot be fetched or used safely."""


def format_size_label(size_bytes: int) -> str:
    """Convert a byte count into a compact KB/MB/GB label."""
    if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes < 0:
        raise ValueError("size_bytes must be a non-negative integer")
    units = [(1024 ** 3, "GB"), (1024 ** 2, "MB"), (1024, "KB")]
    for divisor, suffix in units:
        if size_bytes >= divisor:
            value = size_bytes / divisor
            label = f"{value:.1f}" if value % 1 else f"{int(value)}"
            return f"{label} {suffix}"
    return f"{size_bytes} bytes"


def _require_string(manifest: Mapping[str, Any], field: str) -> str:
    value = manifest.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Manifest field {field} must be a non-empty string")
    return value


def _require_int(manifest: Mapping[str, Any], field: str, *, minimum: int = 0) -> int:
    value = manifest.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ValueError(f"Manifest field {field} must be an integer >= {minimum}")
    return value


def _require_bool(manifest: Mapping[str, Any], field: str) -> bool:
    value = manifest.get(field)
    if not isinstance(value, bool):
        raise ValueError(f"Manifest field {field} must be a boolean")
    return value


def _require_string_list(manifest: Mapping[str, Any], field: str) -> List[str]:
    value = manifest.get(field)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"Manifest field {field} must be a list of strings")
    return list(value)


def validate_update_manifest(manifest: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate and normalize a PharmacyOS update manifest."""
    latest_version = _require_string(manifest, "latest_version")
    latest_build = _require_int(manifest, "latest_build", minimum=0)
    mandatory = _require_bool(manifest, "mandatory")
    update_size_bytes = _require_int(manifest, "update_size_bytes", minimum=0)
    download_url = _require_string(manifest, "download_url")
    release_date = _require_string(manifest, "release_date")
    whats_new = _require_string_list(manifest, "whats_new")
    return {
        "latest_version": latest_version,
        "latest_build": latest_build,
        "mandatory": mandatory,
        "update_size_bytes": update_size_bytes,
        "download_url": download_url,
        "release_date": release_date,
        "whats_new": whats_new,
    }


def fetch_update_manifest(manifest_url: str) -> Dict[str, Any]:
    """Fetch and validate the remote update manifest."""
    if not manifest_url or not manifest_url.strip():
        raise ManifestUnavailable("Update manifest URL is not configured")
    try:
        request = urllib.request.Request(manifest_url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=MANIFEST_TIMEOUT_SECONDS) as response:
            if response.status < 200 or response.status >= 300:
                raise ManifestUnavailable(f"Manifest returned HTTP {response.status}")
            raw = response.read(1024 * 1024)
        parsed = json.loads(raw.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("Manifest must be a JSON object")
        return validate_update_manifest(parsed)
    except ManifestUnavailable:
        raise
    except Exception as exc:
        raise ManifestUnavailable(str(exc)) from exc


def build_update_check_response(manifest: Mapping[str, Any]) -> Dict[str, Any]:
    """Compare the manifest against the current backend build."""
    latest_build = manifest["latest_build"]
    if latest_build <= APP_BUILD:
        return {
            "update_available": False,
            "current_version": APP_VERSION,
            "current_build": APP_BUILD,
        }
    return {
        "update_available": True,
        "current_version": APP_VERSION,
        "current_build": APP_BUILD,
        "latest_version": manifest["latest_version"],
        "latest_build": latest_build,
        "mandatory": manifest["mandatory"],
        "update_size_bytes": manifest["update_size_bytes"],
        "update_size_label": format_size_label(manifest["update_size_bytes"]),
        "download_url": manifest["download_url"],
        "release_date": manifest["release_date"],
        "whats_new": manifest["whats_new"],
    }
