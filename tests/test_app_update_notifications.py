import os
import unittest

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")


class AppUpdateNotificationTest(unittest.IsolatedAsyncioTestCase):
    def test_importing_server_does_not_fetch_manifest(self):
        import importlib
        from unittest.mock import patch
        import server

        with patch("app_update_service.urllib.request.urlopen") as urlopen:
            importlib.reload(server)

        urlopen.assert_not_called()

    async def test_app_version_endpoint(self):
        import server
        from app_version import APP_BUILD, APP_CHANNEL, APP_VERSION

        payload = await server.app_version_endpoint()

        self.assertEqual(payload["version"], APP_VERSION)
        self.assertEqual(payload["build"], APP_BUILD)
        self.assertEqual(payload["channel"], APP_CHANNEL)
        self.assertEqual(payload["runtime_mode"], server.RUNTIME_MODE)
        self.assertEqual(payload["local_mode"], server.LOCAL_MODE)

    async def test_update_available(self):
        import server
        from app_version import APP_BUILD, APP_VERSION

        original_fetch = server.fetch_update_manifest
        original_url = os.environ.get("PHARMACYOS_UPDATE_MANIFEST_URL")
        manifest = {
            "latest_version": "1.1.0",
            "latest_build": APP_BUILD + 1,
            "mandatory": False,
            "update_size_bytes": 50331648,
            "download_url": "https://updates.example.test/pharmacyos.exe",
            "release_date": "2026-06-30",
            "whats_new": ["Improved update notifications"],
        }
        try:
            os.environ["PHARMACYOS_UPDATE_MANIFEST_URL"] = "https://updates.example.test/manifest.json"
            server.fetch_update_manifest = lambda url: manifest
            payload = await server.app_update_check()
        finally:
            server.fetch_update_manifest = original_fetch
            if original_url is None:
                os.environ.pop("PHARMACYOS_UPDATE_MANIFEST_URL", None)
            else:
                os.environ["PHARMACYOS_UPDATE_MANIFEST_URL"] = original_url

        self.assertEqual(payload, {
            "status": "ok",
            "update_available": True,
            "current_version": APP_VERSION,
            "latest_version": "1.1.0",
            "current_build": APP_BUILD,
            "latest_build": APP_BUILD + 1,
            "message": "Update available",
            "release_notes": ["Improved update notifications"],
        })

    async def test_no_update(self):
        import server
        from app_version import APP_BUILD, APP_VERSION

        original_fetch = server.fetch_update_manifest
        try:
            server.fetch_update_manifest = lambda url: {
                "latest_version": APP_VERSION,
                "latest_build": APP_BUILD,
                "mandatory": False,
                "update_size_bytes": 0,
                "download_url": "https://updates.example.test/pharmacyos.exe",
                "release_date": "2026-06-30",
                "whats_new": [],
            }
            payload = await server.app_update_check()
        finally:
            server.fetch_update_manifest = original_fetch

        self.assertEqual(payload, {
            "status": "ok",
            "update_available": False,
            "message": "You are up to date",
        })

    async def test_offline_manifest_returns_200_payload(self):
        import server
        from app_update_service import ManifestUnavailable

        original_fetch = server.fetch_update_manifest
        try:
            def offline(url):
                raise ManifestUnavailable("offline")
            server.fetch_update_manifest = offline
            payload = await server.app_update_check()
        finally:
            server.fetch_update_manifest = original_fetch

        self.assertEqual(payload, {
            "status": "unavailable",
            "update_available": False,
            "message": "Update check unavailable",
            "fallback": True,
        })

    def test_malformed_manifest(self):
        from app_update_service import validate_update_manifest

        with self.assertRaises(ValueError):
            validate_update_manifest({"latest_version": "1.1.0", "latest_build": {}})

    def test_size_conversion(self):
        from app_update_service import format_size_label

        self.assertEqual(format_size_label(1024), "1 KB")
        self.assertEqual(format_size_label(50331648), "48 MB")
        self.assertEqual(format_size_label(1073741824), "1 GB")


class AppUpdateStartTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import server

        self.original_local_mode = server.LOCAL_MODE
        self.original_updater_script = os.environ.get("PHARMACYOS_UPDATER_SCRIPT")
        self.original_github_token = os.environ.get("GITHUB_TOKEN")
        server._update_last_started_at = None
        server._update_last_started_monotonic = None

    def tearDown(self):
        import server

        server.LOCAL_MODE = self.original_local_mode
        server._update_last_started_at = None
        server._update_last_started_monotonic = None
        if self.original_updater_script is None:
            os.environ.pop("PHARMACYOS_UPDATER_SCRIPT", None)
        else:
            os.environ["PHARMACYOS_UPDATER_SCRIPT"] = self.original_updater_script
        if self.original_github_token is None:
            os.environ.pop("GITHUB_TOKEN", None)
        else:
            os.environ["GITHUB_TOKEN"] = self.original_github_token

    async def test_local_mode_start_update_returns_started_true_when_script_exists(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        import server

        with tempfile.TemporaryDirectory() as tmpdir:
            script = Path(tmpdir) / "Update-PharmacyOS.bat"
            script.write_text("@echo off\n", encoding="utf-8")
            os.environ["PHARMACYOS_UPDATER_SCRIPT"] = str(script)
            server.LOCAL_MODE = True

            with patch.object(server.subprocess, "Popen") as popen:
                payload = await server.app_start_update()

        self.assertEqual(payload, {"status": "started"})
        popen.assert_called_once()

    async def test_cloud_mode_start_update_is_rejected(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        from fastapi import HTTPException
        import server

        with tempfile.TemporaryDirectory() as tmpdir:
            script = Path(tmpdir) / "Update-PharmacyOS.bat"
            script.write_text("@echo off\n", encoding="utf-8")
            os.environ["PHARMACYOS_UPDATER_SCRIPT"] = str(script)
            server.LOCAL_MODE = False

            with patch.object(server.subprocess, "Popen") as popen:
                with self.assertRaises(HTTPException) as ctx:
                    await server.app_start_update()

        self.assertEqual(ctx.exception.status_code, 403)
        self.assertEqual(ctx.exception.detail, "Self-update is only available in local desktop mode.")
        popen.assert_not_called()

    async def test_missing_updater_script_returns_safe_error(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        from fastapi import HTTPException
        import server

        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ["PHARMACYOS_UPDATER_SCRIPT"] = str(Path(tmpdir) / "missing.bat")
            server.LOCAL_MODE = True

            with patch.object(server.subprocess, "Popen") as popen:
                with self.assertRaises(HTTPException) as ctx:
                    await server.app_start_update()

        self.assertEqual(ctx.exception.status_code, 404)
        self.assertEqual(ctx.exception.detail, "Updater script was not found.")
        popen.assert_not_called()

    async def test_duplicate_start_within_guard_window_does_not_launch_again(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        import server

        with tempfile.TemporaryDirectory() as tmpdir:
            script = Path(tmpdir) / "Update-PharmacyOS.bat"
            script.write_text("@echo off\n", encoding="utf-8")
            os.environ["PHARMACYOS_UPDATER_SCRIPT"] = str(script)
            server.LOCAL_MODE = True

            with patch.object(server.subprocess, "Popen") as popen:
                first = await server.app_start_update()
                second = await server.app_start_update()

        self.assertEqual(first, {"status": "started"})
        self.assertEqual(second, {"status": "already_started", "message": "Update already in progress."})
        popen.assert_called_once()

    async def test_github_token_is_not_returned_or_logged(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        import server

        secret = "ghp_secret_token_for_update_test"
        with tempfile.TemporaryDirectory() as tmpdir:
            script = Path(tmpdir) / "Update-PharmacyOS.bat"
            script.write_text("@echo off\n", encoding="utf-8")
            os.environ["PHARMACYOS_UPDATER_SCRIPT"] = str(script)
            os.environ["GITHUB_TOKEN"] = secret
            server.LOCAL_MODE = True

            with patch.object(server.subprocess, "Popen"), self.assertLogs(server.logger.name, level="INFO") as logs:
                server.logger.info("Starting test log capture")
                payload = await server.app_start_update()

        self.assertNotIn(secret, str(payload))
        self.assertNotIn(secret, "\n".join(logs.output))
