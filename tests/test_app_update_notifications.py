import os
import unittest

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")


class AppUpdateNotificationTest(unittest.IsolatedAsyncioTestCase):
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

        self.assertTrue(payload["update_available"])
        self.assertEqual(payload["current_version"], APP_VERSION)
        self.assertEqual(payload["current_build"], APP_BUILD)
        self.assertEqual(payload["latest_version"], "1.1.0")
        self.assertEqual(payload["latest_build"], APP_BUILD + 1)
        self.assertEqual(payload["update_size_label"], "48 MB")

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
            "update_available": False,
            "current_version": APP_VERSION,
            "current_build": APP_BUILD,
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

        self.assertEqual(payload, {"update_available": False, "message": "Update check unavailable"})

    def test_malformed_manifest(self):
        from app_update_service import validate_update_manifest

        with self.assertRaises(ValueError):
            validate_update_manifest({"latest_version": "1.1.0", "latest_build": "2"})

    def test_size_conversion(self):
        from app_update_service import format_size_label

        self.assertEqual(format_size_label(1024), "1 KB")
        self.assertEqual(format_size_label(50331648), "48 MB")
        self.assertEqual(format_size_label(1073741824), "1 GB")
