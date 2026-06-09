import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from fastapi import HTTPException

from server import (
    PASSWORD_MAX_AGE_DAYS,
    TenantAwareCollection,
    _current_demo,
    _current_tenant,
    _otp_hash,
    _password_expired,
    _request_active,
    _tenant_filter,
    _validate_password_strength,
)


class TenantIsolationTest(unittest.IsolatedAsyncioTestCase):
    def test_tenant_filter_always_combines_owner_scope(self):
        self.assertEqual(_tenant_filter({}, "demo_shop"), {"tenant_id": "demo_shop"})
        self.assertEqual(
            _tenant_filter({"id": "record-1"}, "real_shop"),
            {"$and": [{"tenant_id": "real_shop"}, {"id": "record-1"}]},
        )

    def test_demo_read_is_scoped_to_demo_tenant(self):
        collection = Mock()
        scoped = TenantAwareCollection(collection, "medicines")
        active = _request_active.set(True)
        tenant = _current_tenant.set("demo_shop")
        demo = _current_demo.set(True)
        try:
            scoped.find({"category": "OTC"})
            collection.find.assert_called_once_with(
                {"$and": [{"tenant_id": "demo_shop"}, {"category": "OTC"}]}
            )
        finally:
            _current_demo.reset(demo)
            _current_tenant.reset(tenant)
            _request_active.reset(active)

    async def test_demo_writes_are_rejected_before_database_call(self):
        collection = AsyncMock()
        scoped = TenantAwareCollection(collection, "medicines")
        active = _request_active.set(True)
        tenant = _current_tenant.set("demo_shop")
        demo = _current_demo.set(True)
        try:
            with self.assertRaises(HTTPException) as raised:
                await scoped.insert_one({"id": "unsafe"})
            self.assertEqual(raised.exception.status_code, 403)
            collection.insert_one.assert_not_awaited()
        finally:
            _current_demo.reset(demo)
            _current_tenant.reset(tenant)
            _request_active.reset(active)

    async def test_real_write_is_stamped_with_current_tenant(self):
        collection = AsyncMock()
        scoped = TenantAwareCollection(collection, "medicines")
        active = _request_active.set(True)
        tenant = _current_tenant.set("real_shop")
        demo = _current_demo.set(False)
        try:
            await scoped.insert_one({"id": "safe"})
            collection.insert_one.assert_awaited_once_with({"id": "safe", "tenant_id": "real_shop"})
        finally:
            _current_demo.reset(demo)
            _current_tenant.reset(tenant)
            _request_active.reset(active)


class PasswordSecurityTest(unittest.TestCase):
    def test_password_expires_after_six_month_window(self):
        now = datetime(2026, 6, 9, tzinfo=timezone.utc)
        fresh = {"password_changed_at": (now - timedelta(days=PASSWORD_MAX_AGE_DAYS)).isoformat()}
        expired = {"password_changed_at": (now - timedelta(days=PASSWORD_MAX_AGE_DAYS + 1)).isoformat()}
        self.assertFalse(_password_expired(fresh, now))
        self.assertTrue(_password_expired(expired, now))

    def test_otp_hash_is_deterministic_without_storing_plaintext(self):
        hashed = _otp_hash("admin@example.com", "123456")
        self.assertEqual(hashed, _otp_hash("admin@example.com", "123456"))
        self.assertNotIn("123456", hashed)

    def test_weak_password_is_rejected(self):
        with self.assertRaises(HTTPException) as raised:
            _validate_password_strength("weak")
        self.assertEqual(raised.exception.status_code, 422)
        _validate_password_strength("StrongPass123")


if __name__ == "__main__":
    unittest.main()
