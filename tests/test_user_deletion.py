import copy
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from fastapi import HTTPException, Response

from server import UserLogin, delete_user, hash_password, login, require_role


class InMemoryUsers:
    def __init__(self, users):
        self.users = [copy.deepcopy(user) for user in users]
        self.delete_queries = []

    @classmethod
    def _matches(cls, user, query):
        if "$and" in query:
            return all(cls._matches(user, condition) for condition in query["$and"])
        if "$or" in query:
            return any(cls._matches(user, condition) for condition in query["$or"])
        for key, value in query.items():
            if isinstance(value, dict) and "$ne" in value:
                if user.get(key) == value["$ne"]:
                    return False
            elif user.get(key) != value:
                return False
        return True

    async def find_one(self, query, *args, **kwargs):
        return next(
            (copy.deepcopy(user) for user in self.users if self._matches(user, query)),
            None,
        )

    async def count_documents(self, query):
        return sum(self._matches(user, query) for user in self.users)

    async def delete_one(self, query):
        self.delete_queries.append(copy.deepcopy(query))
        for index, user in enumerate(self.users):
            if self._matches(user, query):
                self.users.pop(index)
                return SimpleNamespace(deleted_count=1)
        return SimpleNamespace(deleted_count=0)


class UserDeletionTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.password = "StrongPass123"
        password_hash = hash_password(self.password)
        self.admin = {
            "id": "admin-1",
            "email": "admin@example.com",
            "password_hash": password_hash,
            "name": "Admin",
            "role": "admin",
            "tenant_id": "shop-1",
            "is_demo": False,
        }
        self.cashier = {
            "id": "cashier-1",
            "email": "cashier@example.com",
            "password_hash": password_hash,
            "name": "Cashier",
            "role": "cashier",
            "tenant_id": "shop-1",
            "is_demo": False,
        }

    async def test_admin_can_delete_another_user_in_same_tenant_and_user_cannot_login(self):
        collection = InMemoryUsers([self.admin, self.cashier])
        with patch("server.raw_db", SimpleNamespace(users=collection)):
            self.assertEqual(await delete_user(self.cashier["id"], self.admin), {"ok": True})
            with self.assertRaises(HTTPException) as raised:
                await login(
                    UserLogin(email=self.cashier["email"], password=self.password),
                    Response(),
                )

        self.assertEqual(raised.exception.status_code, 401)
        self.assertEqual(raised.exception.detail, "Invalid email or password")
        self.assertEqual(
            collection.delete_queries,
            [{"id": self.cashier["id"], "tenant_id": self.admin["tenant_id"]}],
        )

    async def test_real_login_returns_normal_auth_structure(self):
        collection = InMemoryUsers([self.admin])
        with patch("server.raw_db", SimpleNamespace(users=collection)):
            result = await login(UserLogin(email=self.admin["email"], password=self.password), Response())

        self.assertEqual(result["id"], self.admin["id"])
        self.assertEqual(result["tenant_id"], "shop-1")
        self.assertFalse(result["is_demo"])
        self.assertTrue(result["token"])

    async def test_normal_login_rejects_demo_identity(self):
        demo_user = {
            **self.admin, "id": "demo-user", "email": "demo@pharmacy.com", "tenant_id": "demo_shop",
            "shop_id": "demo_shop", "is_demo": True, "active": True,
        }
        collection = InMemoryUsers([demo_user])
        with patch("server.raw_db", SimpleNamespace(users=collection)):
            with self.assertRaises(HTTPException) as raised:
                await login(UserLogin(identifier="demo@pharmacy.com", password=self.password), Response())

        self.assertEqual(raised.exception.status_code, 401)

    async def test_demo_button_endpoint_returns_normal_auth_structure(self):
        from unittest.mock import AsyncMock
        from server import demo_login

        demo_user = {
            **self.admin, "id": "demo-user", "email": "demo@pharmacy.com", "tenant_id": "demo_shop",
            "shop_id": "demo_shop", "is_demo": True, "active": True,
        }
        collection = InMemoryUsers([demo_user])
        with patch("server._seed_demo_data", AsyncMock()) as seed, patch("server.raw_db", SimpleNamespace(users=collection)):
            result = await demo_login(Response())

        seed.assert_awaited_once()
        self.assertEqual(result["tenant_id"], "demo_shop")
        self.assertTrue(result["is_demo"])
        self.assertTrue(result["token"])

    async def test_admin_cannot_delete_self(self):
        collection = InMemoryUsers([self.admin])
        with patch("server.raw_db", SimpleNamespace(users=collection)):
            with self.assertRaises(HTTPException) as raised:
                await delete_user(self.admin["id"], self.admin)

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail, "Cannot delete currently logged in user")
        self.assertEqual(collection.delete_queries, [])

    async def test_admin_cannot_delete_last_admin(self):
        stale_admin_session = {**self.admin, "id": "admin-session"}
        collection = InMemoryUsers([self.admin])
        with patch("server.raw_db", SimpleNamespace(users=collection)):
            with self.assertRaises(HTTPException) as raised:
                await delete_user(self.admin["id"], stale_admin_session)

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail, "Cannot delete last admin user")
        self.assertEqual(collection.delete_queries, [])

    async def test_admin_cannot_delete_user_from_another_tenant(self):
        other_user = {**self.cashier, "id": "other-user", "tenant_id": "shop-2"}
        collection = InMemoryUsers([self.admin, other_user])
        with patch("server.raw_db", SimpleNamespace(users=collection)):
            with self.assertRaises(HTTPException) as raised:
                await delete_user(other_user["id"], self.admin)

        self.assertEqual(raised.exception.status_code, 404)
        self.assertEqual(raised.exception.detail, "User not found")
        self.assertEqual(collection.delete_queries, [])

    async def test_protected_demo_user_cannot_be_deleted(self):
        demo_user = {**self.cashier, "id": "demo-user", "is_demo": True}
        collection = InMemoryUsers([self.admin, demo_user])
        with patch("server.raw_db", SimpleNamespace(users=collection)):
            with self.assertRaises(HTTPException) as raised:
                await delete_user(demo_user["id"], self.admin)

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail, "Cannot delete protected demo user")
        self.assertEqual(collection.delete_queries, [])

    async def test_non_admin_role_is_rejected(self):
        dependency = require_role("admin")
        with self.assertRaises(HTTPException) as raised:
            await dependency(self.cashier)

        self.assertEqual(raised.exception.status_code, 403)
        self.assertEqual(raised.exception.detail, "Insufficient permissions")


if __name__ == "__main__":
    unittest.main()
