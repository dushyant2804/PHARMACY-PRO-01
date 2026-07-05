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
MAX_MANIFEST_BYTES = 1024 * 1024


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
    return value.strip()


def _coerce_build(value: Any, field: str) -> str:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Manifest field {field} must be a non-empty build identifier")
    if isinstance(value, (int, str)):
        build = str(value).strip()
    else:
        raise ValueError(f"Manifest field {field} must be a non-empty build identifier")
    if not build:
        raise ValueError(f"Manifest field {field} must be a non-empty build identifier")
    return build


def _optional_bool(manifest: Mapping[str, Any], field: str, default: bool = False) -> bool:
    value = manifest.get(field, default)
    if not isinstance(value, bool):
        raise ValueError(f"Manifest field {field} must be a boolean")
    return value


def _optional_int(manifest: Mapping[str, Any], field: str, default: int = 0) -> int:
    value = manifest.get(field, default)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"Manifest field {field} must be an integer >= 0")
    return value


def _optional_string(manifest: Mapping[str, Any], field: str, default: str = "") -> str:
    value = manifest.get(field, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise ValueError(f"Manifest field {field} must be a string")
    return value


def _optional_string_list(manifest: Mapping[str, Any], *fields: str) -> List[str]:
    for field in fields:
        if field in manifest:
            value = manifest.get(field)
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                raise ValueError(f"Manifest field {field} must be a list of strings")
            return list(value)
    return []


def validate_update_manifest(manifest: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate and normalize a PharmacyOS update manifest."""
    latest_version = _require_string(manifest, "latest_version")
    latest_build = _coerce_build(manifest.get("latest_build"), "latest_build")
    release_notes = _optional_string_list(manifest, "release_notes", "whats_new")
    return {
        "latest_version": latest_version,
        "latest_build": latest_build,
        "mandatory": _optional_bool(manifest, "mandatory", False),
        "update_size_bytes": _optional_int(manifest, "update_size_bytes", 0),
        "download_url": _optional_string(manifest, "download_url", ""),
        "artifact_url": _optional_string(manifest, "artifact_url", ""),
        "frontend_artifact_url": _optional_string(manifest, "frontend_artifact_url", ""),
        "release_date": _optional_string(manifest, "release_date", ""),
        "release_notes": release_notes,
        "whats_new": release_notes,
    }


def fetch_update_manifest(manifest_url: str, timeout: float = MANIFEST_TIMEOUT_SECONDS) -> Dict[str, Any]:
    """Fetch and validate the remote update manifest.

    This function is intentionally side-effect free at import time; callers must
    invoke it from the update-check request path only.
    """
    if not manifest_url or not manifest_url.strip():
        raise ManifestUnavailable("Update manifest URL is not configured")
    try:
        request = urllib.request.Request(manifest_url.strip(), headers={"Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = getattr(response, "status", 200)
            if status < 200 or status >= 300:
                raise ManifestUnavailable(f"Manifest returned HTTP {status}")
            raw = response.read(MAX_MANIFEST_BYTES)
        parsed = json.loads(raw.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("Manifest must be a JSON object")
        return validate_update_manifest(parsed)
    except ManifestUnavailable:
        raise
    except Exception as exc:
        raise ManifestUnavailable(str(exc)) from exc


def _current_build_id(current_build: Any = None) -> str:
    return str(APP_BUILD if current_build is None or current_build == "" else current_build).strip()


def build_update_check_response(
    manifest: Mapping[str, Any],
    current_version: Any = None,
    current_build: Any = None,
) -> Dict[str, Any]:
    """Compare remote manifest build identity against the current build and return stable JSON."""
    normalized = validate_update_manifest(manifest)
    current_build_id = _current_build_id(current_build)
    latest_build = normalized["latest_build"]
    update_available = latest_build != current_build_id
    payload = {
        "status": "ok",
        "update_available": update_available,
        "current_version": str(APP_VERSION if current_version is None or current_version == "" else current_version),
        "latest_version": normalized["latest_version"],
        "current_build": current_build_id,
        "latest_build": latest_build,
        "message": "Update available" if update_available else "You are up to date",
        "release_notes": normalized["release_notes"],
        "whats_new": normalized["whats_new"],
    }
    artifact_url = normalized.get("artifact_url") or normalized.get("download_url")
    frontend_artifact_url = normalized.get("frontend_artifact_url") or artifact_url
    if artifact_url:
        payload["artifact_url"] = artifact_url
    if frontend_artifact_url:
        payload["frontend_artifact_url"] = frontend_artifact_url
    return payload


def build_update_check_fallback(reason: str = "", current_version: Any = None, current_build: Any = None) -> Dict[str, Any]:
    return {
        "status": "unavailable",
        "update_available": False,
        "current_version": str(APP_VERSION if current_version is None or current_version == "" else current_version),
        "latest_version": str(APP_VERSION),
        "current_build": _current_build_id(current_build),
        "latest_build": _current_build_id(current_build),
        "message": "Update check unavailable",
        "release_notes": [],
        "whats_new": [],
        "fallback": True,
        "reason": reason,
    }
