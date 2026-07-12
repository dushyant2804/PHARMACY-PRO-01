from dotenv import load_dotenv
from pathlib import Path
import time
ROOT_DIR = Path(__file__).parent
_STARTUP_TIMING_ORIGIN = time.perf_counter()
_STARTUP_TIMING_BUFFER = []
_STARTUP_TIMING_ACTIVE = True

def _startup_elapsed_seconds() -> float:
    return time.perf_counter() - _STARTUP_TIMING_ORIGIN

def _format_startup_timing(label, duration=None, **extra):
    suffix = []
    if duration is not None:
        suffix.append(f"duration={duration:.3f}s")
    suffix.extend(f"{key}={value}" for key, value in extra.items())
    detail = f" ({', '.join(suffix)})" if suffix else ""
    return f"[{_startup_elapsed_seconds():.1f}s] {label}{detail}"

def _record_startup_timing(label, duration=None, **extra):
    message = _format_startup_timing(label, duration, **extra)
    logger_obj = globals().get("logger")
    if logger_obj is None:
        _STARTUP_TIMING_BUFFER.append(message)
    else:
        logger_obj.info("STARTUP_TIMING %s", message)

def _flush_startup_timing_buffer():
    logger_obj = globals().get("logger")
    if logger_obj is None:
        return
    while _STARTUP_TIMING_BUFFER:
        logger_obj.info("STARTUP_TIMING %s", _STARTUP_TIMING_BUFFER.pop(0))

_load_config_started = time.perf_counter()
load_dotenv(ROOT_DIR / '.env')
_record_startup_timing("Load configuration", time.perf_counter() - _load_config_started, root=ROOT_DIR)

import os
import subprocess
import copy
import uuid
import logging
import math
import re
import bcrypt
import jwt
import asyncio
import threading
import hashlib
import hmac
import secrets
import smtplib
import json
import csv
import io
import base64
import urllib.request
import urllib.parse
import zipfile
from contextvars import ContextVar
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta, date
from calendar import monthrange
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Literal
from collections import Counter, defaultdict, deque

from fastapi import FastAPI, APIRouter, HTTPException, Depends, Request, Response, Query, Body
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from fastapi.encoders import jsonable_encoder
from fastapi.security import HTTPBearer
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.staticfiles import StaticFiles
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError
from local_database import LocalSQLiteDatabase
from bson import BSON
from pydantic import BaseModel, Field, EmailStr, ConfigDict, field_validator, model_validator
from starlette.datastructures import UploadFile

from version_config import VERSION_METADATA, get_version_metadata
from app_version import APP_BUILD as BACKEND_APP_BUILD, APP_CHANNEL, APP_VERSION as BACKEND_APP_VERSION
from app_update_service import ManifestUnavailable, build_update_check_fallback, build_update_check_response, fetch_update_manifest


# Runtime database mode
# CLOUD_MODE preserves the existing Render + MongoDB Atlas behavior.
# LOCAL_MODE uses a SQLite-backed local adapter that exposes the same async
# collection methods used by the existing API routes, so route contracts and
# business logic remain unchanged.
RUNTIME_MODE = os.environ.get("PHARMACYOS_MODE", os.environ.get("RUNTIME_MODE", "CLOUD_MODE")).upper()
LOCAL_MODE = RUNTIME_MODE == "LOCAL_MODE"
CLOUD_MODE = not LOCAL_MODE
LOCAL_DB_PATH = Path(os.environ.get("LOCAL_DB_PATH", ROOT_DIR / "local_data" / "pharmacyos.sqlite3"))
BACKUP_DIR = Path(os.environ.get("BACKUP_DIR", ROOT_DIR / "backups")).resolve()
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
SYNC_QUEUE_COLLECTION = "sync_queue"
ATLAS_BACKUP_MONGO_URL = os.environ.get("ATLAS_BACKUP_MONGO_URL", "")
ATLAS_BACKUP_DB_NAME = os.environ.get("ATLAS_BACKUP_DB_NAME", os.environ.get("DB_NAME", "pharmacyos_local_backups"))
GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_PATH = Path(os.environ.get("GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_PATH", os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", ROOT_DIR / "local_data" / "google_drive_service_account.json"))).expanduser()
GOOGLE_DRIVE_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")
GOOGLE_DRIVE_TOKEN_URI = "https://oauth2.googleapis.com/token"
BACKUP_ENCRYPTION_KEY = os.environ.get("BACKUP_ENCRYPTION_KEY", os.environ.get("JWT_SECRET", ""))
BACKUP_COLLECTIONS = [
    "medicines", "invoices", "purchase_orders", "distributors", "customers",
    "customer_transactions", "distributor_transactions", "purchase_returns",
    "stock_adjustments", "daily_closings", "daily_sales", "expenses",
    "regular_patients", "settings",
]

LOCAL_TO_CLOUD_SYNC_TABLES = [
    "users",
    "medicines",
    "invoices",
    "invoice_items",
    "purchase_orders",
    "purchase_returns",
    "customers",
    "distributors",
    "customer_transactions",
    "distributor_transactions",
    "stock_adjustments",
    "settings",
]
LOCAL_TO_CLOUD_SYNC_STATUS_ID = "local_to_cloud"


LOCAL_PERFORMANCE_MAX_RECENT = int(os.environ.get("LOCAL_PERFORMANCE_MAX_RECENT", "200"))
LOCAL_SLOW_REQUEST_MS = int(os.environ.get("LOCAL_SLOW_REQUEST_MS", "500"))

DEFAULT_UPDATER_SCRIPT = r"D:\pharmacy-app-v2\Update-PharmacyOS.bat"
UPDATE_START_GUARD_SECONDS = 120
_update_start_lock = threading.Lock()
_update_last_started_at = None
_update_last_started_monotonic = None


def _now_ms() -> float:
    return asyncio.get_running_loop().time() * 1000


class _StepTimer:
    def __init__(self, label: str):
        self.label = label
        self.started = _now_ms()
        self.last = self.started
        self.steps = []

    def mark(self, name: str) -> None:
        now = _now_ms()
        self.steps.append((name, round(now - self.last, 2)))
        self.last = now

    def total(self) -> float:
        return round(_now_ms() - self.started, 2)

    def log(self, **extra) -> None:
        total = self.total()
        lines = [self.label, *(f"{name} = {duration_ms} ms" for name, duration_ms in self.steps), f"total = {total} ms"]
        if extra:
            lines.extend(f"{key} = {value}" for key, value in extra.items())
        logger.info("\n".join(lines))

    def largest_step(self):
        return max(self.steps, key=lambda item: item[1], default=("none", 0.0))
LOCAL_SUMMARY_CACHE_TTL_SECONDS = float(os.environ.get("LOCAL_SUMMARY_CACHE_TTL_SECONDS", "10"))
LOCAL_BUSY_PATH_PREFIXES = (
    "/api/invoices",
    "/api/purchase-orders",
    "/api/stock-adjustments",
    "/api/ledger/customer/",
    "/api/ledger/distributor/",
    "/api/daily-closings",
)
LOCAL_BUSY_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
def _resolve_frontend_build_dir(env: Optional[Dict[str, str]] = None, root_dir: Path = ROOT_DIR) -> Path:
    """Resolve the local desktop frontend directory without changing cloud API behavior.

    Explicit environment overrides win first. Otherwise prefer the sibling
    frontend project used by the desktop launcher before the legacy in-repo
    backend/frontend locations.
    """
    env = env or os.environ
    if env.get("FRONTEND_BUILD_DIR"):
        return Path(env["FRONTEND_BUILD_DIR"]).expanduser().resolve()
    if env.get("FRONTEND_DIST_DIR"):
        return Path(env["FRONTEND_DIST_DIR"]).expanduser().resolve()

    candidates = [
        root_dir.parent / "frontend" / "dist",
        root_dir.parent / "frontend" / "build",
        root_dir / "frontend" / "dist",
        root_dir / "frontend" / "build",
    ]
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.exists():
            return resolved
    return candidates[-1].resolve()


FRONTEND_BUILD_DIR = _resolve_frontend_build_dir()

LOCAL_IMPORT_PATHS = {
    "/api/local/import/dry-run",
    "/api/local/import/confirm",
    "/api/local-mode/import/dry-run",
    "/api/local-mode/import/confirm",
}


def _allowed_cors_origins() -> List[str]:
    origins = {
        "https://pharmacy-pro-01-frontend.onrender.com",
        "http://localhost",
        "http://localhost:3000",
        "http://127.0.0.1",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    }
    for env_name in ("FRONTEND_URL", "FRONTEND_ORIGIN", "RENDER_FRONTEND_ORIGIN"):
        value = os.environ.get(env_name, "")
        origins.update(origin.strip().rstrip("/") for origin in value.split(",") if origin.strip())
    return sorted(origins)


class LocalImportRequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path in LOCAL_IMPORT_PATHS and request.method.upper() in {"OPTIONS", "POST"}:
            auth_header = request.headers.get("Authorization", "")
            cookie_auth = bool(request.cookies.get("access_token"))
            logger.info(
                "Local import request received: method=%s path=%s origin=%s auth=%s",
                request.method.upper(),
                request.url.path,
                request.headers.get("origin", "<missing>"),
                "present" if auth_header or cookie_auth else "missing",
            )
            try:
                return await call_next(request)
            except Exception:
                logger.exception(
                    "Local import request failed: method=%s path=%s origin=%s auth=%s",
                    request.method.upper(),
                    request.url.path,
                    request.headers.get("origin", "<missing>"),
                    "present" if auth_header or cookie_auth else "missing",
                )
                raise
        return await call_next(request)

LOCAL_FIRST_IMPORT_COLLECTIONS = [
    # Auth, roles, tenant bootstrap data. Password hashes are copied verbatim.
    "users", "roles", "pending_signups", "password_reset_requests",
    # Application settings and counters.
    "settings", "counters",
    # Inventory, stock, and day-to-day business data.
    "medicines", "stock_adjustments", "distributors", "customers",
    "purchase_orders", "purchase_returns", "invoices",
    "customer_transactions", "distributor_transactions",
    # Reports and supporting datasets.
    "daily_closings", "daily_sales", "daily_summary", "expenses",
    "regular_patients", "doctor_history", "historical_sales",
]

if LOCAL_MODE:
    mongo_url = os.environ.get("MONGO_URL", "")
    client = None
    _db_init_started = time.perf_counter()
    try:
        raw_db = LocalSQLiteDatabase(LOCAL_DB_PATH)
    except Exception as exc:
        _record_startup_timing("Connect SQLite failed", time.perf_counter() - _db_init_started, path=LOCAL_DB_PATH)
        raise RuntimeError(f"LOCAL_MODE database could not be opened at {LOCAL_DB_PATH}: {exc}") from exc
    _record_startup_timing("Connect SQLite", time.perf_counter() - _db_init_started, path=LOCAL_DB_PATH)
else:
    mongo_url = os.environ['MONGO_URL']
    _db_init_started = time.perf_counter()
    client = AsyncIOMotorClient(mongo_url)
    raw_db = client[os.environ['DB_NAME']]
    _record_startup_timing("Create Mongo client", time.perf_counter() - _db_init_started, database=os.environ['DB_NAME'])
UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", ROOT_DIR / "uploads")).resolve()
BRANDING_UPLOAD_DIR = UPLOAD_DIR / "branding"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

REAL_TENANT_ID = os.environ.get("REAL_TENANT_ID", "real_shop")
DEMO_TENANT_ID = "demo_shop"
DEMO_USER_ID = "demo-user"
SAFE_DEMO_EMAIL = "demo@pharmacyos.local"
UNSAFE_DEFAULT_EMAILS = {"admin@gmail.com", "admin@pharmacy.com"}
UNSAFE_DEFAULT_PASSWORDS = {"admin@123", "admin123"}
SYSTEM_ACCOUNT_MARKERS = (
    "is_demo", "system_seeded", "is_system_seeded", "protected", "is_protected",
    "is_default", "default_account",
)
BUSINESS_COLLECTIONS = {
    "counters", "customer_transactions", "customers", "daily_closings", "daily_sales",
    "daily_summary", "distributor_transactions", "distributors",
    "doctor_history", "expenses", "historical_sales", "invoices",
    "medicines", "purchase_orders", "purchase_returns", "stock_adjustments",
    "regular_patients", "settings",
}
_request_active = ContextVar("request_active", default=False)
_current_tenant = ContextVar("current_tenant", default=None)
_current_demo = ContextVar("current_demo", default=False)


def _tenant_filter(query: Optional[dict], tenant_id: str) -> dict:
    query = dict(query or {})
    if not query:
        return {"tenant_id": tenant_id}
    return {"$and": [{"tenant_id": tenant_id}, query]}


def _canonicalize_user_tenant(user: dict) -> dict:
    """Return an auth-safe user whose demo identity can never inherit another shop."""
    result = dict(user)
    if not result.get("id") and result.get("_id"):
        result["id"] = str(result["_id"])
    if not result.get("tenant_id"):
        result["tenant_id"] = result.get("shop_id") or REAL_TENANT_ID
    if not result.get("shop_id"):
        result["shop_id"] = result["tenant_id"]
    if result.get("id") == DEMO_USER_ID or result.get("is_demo") or result.get("tenant_id") == DEMO_TENANT_ID:
        result["id"] = DEMO_USER_ID
        result["tenant_id"] = DEMO_TENANT_ID
        result["shop_id"] = DEMO_TENANT_ID
        result["is_demo"] = True
        result["active"] = True
    return result


class TenantAwareCollection:
    """Motor collection proxy that enforces tenant scope and demo read-only rules."""

    def __init__(self, collection, name: str):
        self._collection = collection
        self._name = name

    def _scope(self, query=None):
        if not _request_active.get():
            return query or {}
        tenant_id = _current_tenant.get()
        if not tenant_id:
            raise HTTPException(status_code=401, detail="Not authenticated")
        return _tenant_filter(query, tenant_id)

    def _write_guard(self):
        if _request_active.get() and _current_demo.get():
            raise HTTPException(status_code=403, detail="Demo account is read-only")

    def _owned(self, document: dict) -> dict:
        result = dict(document)
        if _request_active.get():
            tenant_id = _current_tenant.get()
            if not tenant_id:
                raise HTTPException(status_code=401, detail="Not authenticated")
            result["tenant_id"] = tenant_id
            result["shop_id"] = tenant_id
        return result

    def find(self, query=None, *args, **kwargs):
        return self._collection.find(self._scope(query), *args, **kwargs)

    async def find_one(self, query=None, *args, **kwargs):
        return await self._collection.find_one(self._scope(query), *args, **kwargs)

    async def count_documents(self, query, *args, **kwargs):
        return await self._collection.count_documents(self._scope(query), *args, **kwargs)

    def aggregate(self, pipeline, *args, **kwargs):
        scoped_pipeline = list(pipeline or [])
        if _request_active.get():
            scoped_pipeline.insert(0, {"$match": self._scope({})})
        return self._collection.aggregate(scoped_pipeline, *args, **kwargs)

    async def distinct(self, key, query=None, *args, **kwargs):
        return await self._collection.distinct(key, self._scope(query), *args, **kwargs)

    async def insert_one(self, document, *args, **kwargs):
        self._write_guard()
        return await self._collection.insert_one(self._owned(document), *args, **kwargs)

    async def insert_many(self, documents, *args, **kwargs):
        self._write_guard()
        return await self._collection.insert_many([self._owned(d) for d in documents], *args, **kwargs)

    def _owned_update(self, update):
        if _request_active.get() and isinstance(update, list):
            tenant_id = _current_tenant.get()
            return [*update, {"$set": {"tenant_id": tenant_id, "shop_id": tenant_id}}]
        if not _request_active.get() or not isinstance(update, dict) or any(not str(k).startswith("$") for k in update):
            return update
        result = dict(update)
        result["$setOnInsert"] = {**result.get("$setOnInsert", {}), "tenant_id": _current_tenant.get(), "shop_id": _current_tenant.get()}
        return result

    async def update_one(self, query, update, *args, **kwargs):
        self._write_guard()
        return await self._collection.update_one(self._scope(query), self._owned_update(update), *args, **kwargs)

    async def update_many(self, query, update, *args, **kwargs):
        self._write_guard()
        return await self._collection.update_many(self._scope(query), self._owned_update(update), *args, **kwargs)

    async def find_one_and_update(self, query, update, *args, **kwargs):
        self._write_guard()
        return await self._collection.find_one_and_update(self._scope(query), self._owned_update(update), *args, **kwargs)

    async def replace_one(self, query, replacement, *args, **kwargs):
        self._write_guard()
        return await self._collection.replace_one(self._scope(query), self._owned(replacement), *args, **kwargs)

    async def delete_one(self, query, *args, **kwargs):
        self._write_guard()
        return await self._collection.delete_one(self._scope(query), *args, **kwargs)

    async def delete_many(self, query, *args, **kwargs):
        self._write_guard()
        return await self._collection.delete_many(self._scope(query), *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._collection, name)


class TenantAwareDatabase:
    def __init__(self, database):
        self._database = database

    def __getattr__(self, name):
        collection = getattr(self._database, name)
        return TenantAwareCollection(collection, name) if name in BUSINESS_COLLECTIONS else collection

    def __getitem__(self, name):
        collection = self._database[name]
        return TenantAwareCollection(collection, name) if name in BUSINESS_COLLECTIONS else collection


db = TenantAwareDatabase(raw_db)

_routes_started = time.perf_counter()
app = FastAPI(title="Pharmacy Management API")
_record_startup_timing("Create FastAPI app", time.perf_counter() - _routes_started)
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")
_record_startup_timing("Mount uploads route")

@app.get("/")
def home():
    if LOCAL_MODE and (FRONTEND_BUILD_DIR / "index.html").exists():
        return FileResponse(str(FRONTEND_BUILD_DIR / "index.html"))
    return {"message": "Pharmacy backend is running"}


def _backend_readiness_payload() -> dict:
    return {
        "status": "ok",
        "system_stable": True,
        "mode": "LOCAL_MODE" if LOCAL_MODE else "CLOUD_MODE",
        "ready": True,
    }


@app.get("/api/health")
async def health():
    return _backend_readiness_payload()


api_router = APIRouter(prefix="/api")


@app.middleware("http")
async def local_mode_request_timing(request: Request, call_next):
    global _local_busy_request_count
    start = asyncio.get_running_loop().time()
    path = request.url.path
    method = request.method.upper()
    cacheable = LOCAL_MODE and method == "GET" and (path == "/api/dashboard/summary" or path.startswith("/api/reports/"))
    cache_key = f"{method}:{path}?{request.url.query}"
    if cacheable:
        now = asyncio.get_running_loop().time()
        cached = _local_summary_cache.get(cache_key)
        if cached and now - cached[0] <= LOCAL_SUMMARY_CACHE_TTL_SECONDS:
            body, status_code, headers, media_type = cached[1]
            return Response(content=body, status_code=status_code, headers=headers, media_type=media_type)
    busy = _is_local_busy_path(method, path)
    if LOCAL_MODE and busy:
        _local_busy_request_count += 1
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        if cacheable and status_code == 200:
            body = b""
            async for chunk in response.body_iterator:
                body += chunk
            headers = dict(response.headers)
            headers.pop("content-length", None)
            _local_summary_cache[cache_key] = (asyncio.get_running_loop().time(), (body, status_code, headers, response.media_type))
            return Response(content=body, status_code=status_code, headers=headers, media_type=response.media_type)
        if LOCAL_MODE and busy and 200 <= status_code < 400:
            _invalidate_local_summary_cache()
        return response
    finally:
        duration_ms = round((asyncio.get_running_loop().time() - start) * 1000, 2)
        if LOCAL_MODE:
            record = {
                "method": method, "path": path, "status_code": status_code,
                "duration_ms": duration_ms, "at": datetime.now(timezone.utc).isoformat(),
            }
            _local_recent_requests.append(record)
            logger.info("LOCAL_MODE request %s %s completed status=%s duration_ms=%.2f", method, path, status_code, duration_ms)
            if duration_ms > LOCAL_SLOW_REQUEST_MS:
                logger.warning("LOCAL_MODE slow request %s %s status=%s duration_ms=%.2f", method, path, status_code, duration_ms)
        if LOCAL_MODE and busy:
            _local_busy_request_count = max(0, _local_busy_request_count - 1)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = exc.errors()

    if request.url.path == "/api/purchase-returns" and request.method.upper() == "POST":
        missing_fields = []
        for error in errors:
            if error.get("type") != "missing":
                continue

            location = error.get("loc") or []
            if len(location) >= 2 and location[0] == "body":
                missing_fields.append(str(location[-1]))

        if missing_fields:
            field_list = ", ".join(dict.fromkeys(missing_fields))
            return JSONResponse(
                status_code=422,
                content=jsonable_encoder(
                    {
                        "detail": errors,
                        "message": f"Missing field: {field_list}",
                        "missing_fields": list(dict.fromkeys(missing_fields)),
                    }
                ),
            )

    return JSONResponse(
        status_code=422,
        content=jsonable_encoder({"detail": errors}),
    )

JWT_ALGORITHM = "HS256"
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development").lower()
JWT_SECRET = os.environ.get("JWT_SECRET")
if not JWT_SECRET:
    if ENVIRONMENT in {"production", "prod"}:
        raise RuntimeError("JWT_SECRET environment variable must be set in production")
    JWT_SECRET = "dev-only-change-me"

logger = logging.getLogger("pharmacy")
logging.basicConfig(level=logging.INFO)
_flush_startup_timing_buffer()
_record_startup_timing("Logger initialized")
_background_tasks = set()
_local_recent_requests = deque(maxlen=LOCAL_PERFORMANCE_MAX_RECENT)
_local_summary_cache: Dict[str, Tuple[float, Any]] = {}
_local_busy_request_count = 0
_local_backup_sync_lock = asyncio.Lock()


def _is_local_busy_path(method: str, path: str) -> bool:
    return method.upper() in LOCAL_BUSY_METHODS and any(path.startswith(prefix) for prefix in LOCAL_BUSY_PATH_PREFIXES)


def _local_request_busy() -> bool:
    return LOCAL_MODE and _local_busy_request_count > 0


async def _time_startup_awaitable(label: str, awaitable, **extra):
    started = time.perf_counter()
    try:
        return await awaitable
    finally:
        if _STARTUP_TIMING_ACTIVE:
            _record_startup_timing(label, time.perf_counter() - started, **extra)


def _time_startup_sync(label: str, func, *args, **kwargs):
    started = time.perf_counter()
    try:
        return func(*args, **kwargs)
    finally:
        if _STARTUP_TIMING_ACTIVE:
            _record_startup_timing(label, time.perf_counter() - started)


def _local_summary_cache_key(name: str, *args, **kwargs) -> str:
    return json.dumps([name, args, sorted(kwargs.items())], default=str, sort_keys=True)


async def _local_cached_summary(name: str, producer, *args, **kwargs):
    if not LOCAL_MODE:
        return await producer(*args, **kwargs)
    key = _local_summary_cache_key(name, *args, **kwargs)
    now = asyncio.get_running_loop().time()
    cached = _local_summary_cache.get(key)
    if cached and now - cached[0] <= LOCAL_SUMMARY_CACHE_TTL_SECONDS:
        return copy.deepcopy(cached[1])
    value = await producer(*args, **kwargs)
    _local_summary_cache[key] = (now, copy.deepcopy(value))
    return value


def _invalidate_local_summary_cache() -> None:
    if LOCAL_MODE:
        _local_summary_cache.clear()

EXPIRY_WARNING_DAYS = 90
DASHBOARD_RECENTLY_EXPIRED_DAYS = 90
LOW_STOCK_STATUSES = {"low_stock", "reordered", "abandoned", "restocked"}
PASSWORD_MAX_AGE_DAYS = 183
PASSWORD_RESET_ATTEMPTS = 5
PASSWORD_RESET_TTL_MINUTES = 10
FORGOT_PASSWORD_RATE_LIMIT = 5
FORGOT_PASSWORD_WINDOW_MINUTES = 15
SIGNUP_OTP_TTL_MINUTES = 10
SIGNUP_OTP_ATTEMPTS = 5
# Compatibility aliases; version_config.py is the single source of release metadata.
APP_VERSION = VERSION_METADATA["latest_version"]
APP_BUILD = VERSION_METADATA["latest_build"]
APP_UPDATE_MESSAGE = "PharmacyOS {version} is ready to install.".format(version=VERSION_METADATA["full_version"])
APP_RELEASE_NOTES = VERSION_METADATA["release_notes"]


def _password_expired(user: dict, now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(timezone.utc)
    changed_at = user.get("password_changed_at") or user.get("created_at")
    if not changed_at:
        return True
    try:
        changed = datetime.fromisoformat(str(changed_at).replace("Z", "+00:00"))
        if changed.tzinfo is None:
            changed = changed.replace(tzinfo=timezone.utc)
        return now - changed > timedelta(days=PASSWORD_MAX_AGE_DAYS)
    except (TypeError, ValueError):
        return True


def _validate_password_strength(password: str) -> None:
    if len(password) < 10 or not any(c.islower() for c in password) or not any(c.isupper() for c in password) or not any(c.isdigit() for c in password):
        raise HTTPException(
            status_code=422,
            detail="Password must be at least 10 characters and include uppercase, lowercase, and a number",
        )


def _otp_hash(email: str, otp: str) -> str:
    return hmac.new(JWT_SECRET.encode(), f"{email.lower()}:{otp}".encode(), hashlib.sha256).hexdigest()


@app.middleware("http")
async def tenant_security_context(request: Request, call_next):
    active_token = _request_active.set(True)
    tenant_token = _current_tenant.set(None)
    demo_token = _current_demo.set(False)
    try:
        token = request.cookies.get("access_token")
        auth = request.headers.get("Authorization", "")
        if not token and auth.startswith("Bearer "):
            token = auth[7:]
        if token:
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
                user_id = payload.get("sub")
                token_is_demo = bool(payload.get("is_demo")) or user_id == DEMO_USER_ID
                query = ({"id": DEMO_USER_ID, "tenant_id": DEMO_TENANT_ID, "is_demo": True}
                         if token_is_demo else {"id": user_id, "tenant_id": {"$ne": DEMO_TENANT_ID}, "is_demo": {"$ne": True}})
                user = await raw_db.users.find_one(query)
                if user:
                    user = _canonicalize_user_tenant(user)
                    _current_tenant.set(DEMO_TENANT_ID if token_is_demo else user.get("tenant_id"))
                    _current_demo.set(token_is_demo or bool(user.get("is_demo")))
            except jwt.InvalidTokenError:
                pass
        if _current_demo.get() and request.method.upper() not in {"GET", "HEAD", "OPTIONS"} and request.url.path not in {"/api/auth/login", "/api/auth/demo-login", "/api/auth/logout"}:
            return JSONResponse(status_code=403, content={"detail": "Demo account is read-only"})
        return await call_next(request)
    finally:
        _current_demo.reset(demo_token)
        _current_tenant.reset(tenant_token)
        _request_active.reset(active_token)


def parse_expiry_date(expiry):
    if not expiry:
        return None

    value = str(expiry).strip()

    try:
        if "/" in value:
            parts = value.split("/")

            if len(parts) != 2:
                return None

            month = int(parts[0])
            year_text = parts[1].strip()
            year = int(year_text)

            if len(year_text) <= 2:
                year += 2000

            last_day = monthrange(year, month)[1]

            return datetime(year, month, last_day).date()

        return datetime.fromisoformat(value).date()
    except Exception:
        return None


STOCK_QUANTITY_FIELDS = {
    "purchased_units", "purchased_quantity", "quantity_units",
    "sold_units", "sold_quantity", "free_quantity", "free_qty", "free_units",
    "purchase_return_units", "purchase_return_quantity", "returned_quantity", "returned_units",
    "available_stock", "current_stock", "total_stock", "stock_adjustment_units",
}
INVENTORY_MONEY_FIELDS = {
    "purchase_price", "mrp", "cost_value", "mrp_value",
    "total_cost_value", "total_mrp_value", "valuation_total", "valuation_totals",
}
STANDARD_MEDICINE_CATEGORIES = {"OTC", "H", "H1", "X", "NRX", "G"}


def round_qty(value) -> float:
    """Normalize medicine quantities to one decimal and eliminate negative zero."""
    rounded = round(float(value or 0), 1)
    return 0.0 if abs(rounded) < 0.05 else rounded


def _normalize_inventory_quantities(value):
    """Round stock quantity fields before returning inventory data through APIs."""
    if isinstance(value, list):
        return [_normalize_inventory_quantities(item) for item in value]
    if not isinstance(value, dict):
        return value
    return {
        key: (
            round_qty(item) if key in STOCK_QUANTITY_FIELDS
            else round(float(item or 0), 2) if key in INVENTORY_MONEY_FIELDS and isinstance(item, (int, float))
            else _normalize_inventory_quantities(item)
        )
        for key, item in value.items()
    }


def _inventory_category(value) -> dict:
    """Keep the stored category while exposing a stable badge value."""
    category = str(value or "OTC").strip() or "OTC"
    normalized = category.upper().replace(" ", "")
    if normalized == "NRX":
        normalized = "NRx"
    return {
        "category": category,
        "category_code": normalized if normalized.upper() in STANDARD_MEDICINE_CATEGORIES else category,
        "category_status": normalized if normalized.upper() in STANDARD_MEDICINE_CATEGORIES else "other",
    }


def _inventory_stock_status(medicine: dict, today: Optional[date] = None) -> str:
    stock = _available_stock(medicine)
    if stock <= 0:
        return "sold_out"
    if expiry_details(medicine.get("expiry_date"), today or datetime.now(timezone.utc).date())["expiry_status"] == "expired":
        return "expired"
    threshold = medicine.get("low_stock_threshold")
    if threshold is None:
        return "healthy"
    threshold = max(0.0, float(threshold))
    if stock <= max(1.0, threshold * 0.25):
        return "critical"
    if stock <= threshold:
        return "low_stock"
    return "healthy"


def _stock_quantity(medicine: dict, canonical_key: str, *fallback_keys: str) -> float:
    """Read a stock quantity while supporting older/imported field names."""
    for key in (canonical_key, *fallback_keys):
        if key in medicine and medicine.get(key) is not None:
            return round_qty(medicine.get(key))
    return 0.0


def _purchased_stock(medicine: dict) -> float:
    if "purchased_units" in medicine and medicine.get("purchased_units") is not None:
        return round_qty(medicine.get("purchased_units"))
    return round_qty(_stock_quantity(medicine, "purchased_quantity", "quantity") + _stock_quantity(
        medicine, "free_quantity", "free_qty", "free_units"
    ))


def _stock_adjustment_stock(medicine: dict) -> float:
    return _stock_quantity(medicine, "stock_adjustment_units")


def _purchase_return_stock(medicine: dict) -> float:
    return _stock_quantity(
        medicine,
        "purchase_return_units",
        "purchase_return_quantity",
        "returned_quantity",
        "returned_units",
    )


def _available_stock(medicine: dict) -> float:
    return round_qty(max(
        0.0,
        _purchased_stock(medicine)
        + _stock_adjustment_stock(medicine)
        - _stock_quantity(medicine, "sold_units", "sold_quantity")
        - _purchase_return_stock(medicine),
    ))


def _expiry_risk_values(expired: float, expiring_30: float, expiring_90: float) -> dict:
    """Round expiry buckets first so the API total always equals their displayed sum."""
    values = {
        "expired_value_at_risk": _round_ledger_money(expired),
        "expiring_30_value_at_risk": _round_ledger_money(expiring_30),
        "expiring_90_value_at_risk": _round_ledger_money(expiring_90),
    }
    values["expiry_value_at_risk"] = _round_ledger_money(sum(values.values()))
    return values


def _safe_legacy_sold_stock(medicine: dict) -> float:
    """Retain legacy/manual sold stock only when it agrees with a stored stock snapshot."""
    sold = _stock_quantity(medicine, "sold_units", "sold_quantity")
    if sold <= 0:
        return 0.0
    snapshot = next(
        (medicine.get(key) for key in ("current_stock", "available_qty", "available_stock", "quantity_units")
         if medicine.get(key) is not None),
        None,
    )
    if snapshot is None:
        return sold
    expected = round_qty(
        _purchased_stock(medicine) + _stock_adjustment_stock(medicine)
        - sold - _purchase_return_stock(medicine)
    )
    return sold if round_qty(snapshot) == expected else 0.0


def _mongo_first_stock_value(*field_names: str) -> dict:
    expression = 0
    for field_name in reversed(field_names):
        expression = {"$ifNull": [f"${field_name}", expression]}
    return expression


def _mongo_available_stock_expression() -> dict:
    """Mongo expression equivalent of _available_stock for atomic stock writes."""
    purchased = {
        "$ifNull": [
            "$purchased_units",
            {
                "$add": [
                    _mongo_first_stock_value("purchased_quantity", "quantity"),
                    _mongo_first_stock_value("free_quantity", "free_qty", "free_units"),
                ]
            },
        ]
    }
    sold = _mongo_first_stock_value("sold_units", "sold_quantity")
    returned = _mongo_first_stock_value(
        "purchase_return_units", "purchase_return_quantity", "returned_quantity", "returned_units"
    )
    adjustment = _mongo_first_stock_value("stock_adjustment_units")
    return {"$subtract": [{"$subtract": [{"$add": [purchased, adjustment]}, sold]}, returned]}


async def _set_rounded_stock_delta(
    medicine_id: str, field: str, delta: float, session=None, require_available: bool = False
):
    """Atomically replace a stock quantity with its one-decimal calculated value."""
    medicine = await db.medicines.find_one({"id": medicine_id}, {"_id": 0}, session=session)
    if not medicine:
        return None

    fallback_fields = {
        "purchased_units": ("purchased_quantity", "quantity"),
        "sold_units": ("sold_quantity",),
        "purchase_return_units": ("purchase_return_quantity", "returned_quantity", "returned_units"),
    }.get(field, ())
    current = _stock_quantity(medicine, field, *fallback_fields)
    raw_current = next(
        (float(medicine.get(key) or 0) for key in (field, *fallback_fields) if medicine.get(key) is not None),
        0.0,
    )
    delta = round_qty(delta)
    updated_value = round_qty(current + delta)
    refreshed = {**medicine, field: updated_value}
    available = _available_stock(refreshed)
    status = _return_status(refreshed)
    query = {
        "id": medicine_id,
        "$expr": {"$eq": [_mongo_first_stock_value(field, *fallback_fields), raw_current]},
    }
    if require_available:
        query["$expr"] = {
            "$and": [
                query["$expr"],
                {"$gte": [_mongo_available_stock_expression(), delta]},
            ]
        }
    return await db.medicines.update_one(
        query,
        {"$set": {
            field: updated_value,
            "available_stock": available,
            "quantity_units": available,
            "return_status": status,
            "status": status,
        }},
        session=session,
    )


def _return_status(medicine: dict) -> str:
    purchased = round_qty(_purchased_stock(medicine) + _stock_adjustment_stock(medicine))
    sold = _stock_quantity(medicine, "sold_units", "sold_quantity")
    returned = _purchase_return_stock(medicine)
    available = _available_stock(medicine)

    # A purchase return takes priority over sold-out.  Compare it with stock
    # that was not already sold so mixed sold/returned batches are classified
    # as Returned rather than Sold Out.
    remaining_non_sold = max(0.0, purchased - sold)
    if returned > 0 and returned >= remaining_non_sold:
        return "Returned"
    if returned > 0:
        return "Partially Returned"
    if available <= 0:
        return "Sold Out"
    return "Not Returned"


def expiry_details(expiry, today):
    expiry_date = parse_expiry_date(expiry)

    details = {
        "expiry_status": "safe",
        "days_to_expiry": None,
        "days_expired": None,
        "expired_days_ago": None,
    }

    if not expiry_date:
        return details

    days_left = (expiry_date - today).days

    if days_left < 0:
        days_expired = abs(days_left)
        details["expiry_status"] = "expired"
        details["days_expired"] = days_expired
        details["expired_days_ago"] = days_expired
    else:
        details["days_to_expiry"] = days_left

        if days_left <= EXPIRY_WARNING_DAYS:
            details["expiry_status"] = "warning"

    return details


def _low_stock_status(medicine: dict) -> str:
    """Return the dashboard workflow status without confusing it with batch status."""
    status = medicine.get("low_stock_status")
    return status if status in LOW_STOCK_STATUSES else "low_stock"


def parse_iso_date(value):
    if not value:
        return None

    try:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text).date()
    except Exception:
        return None


def _patient_invalid_filter() -> dict:
    return {
        "$or": [
            {"phone": {"$exists": False}},
            {"phone": None},
            {"phone": ""},
            {"phone": {"$regex": r"^\s+$"}},
            {"name": {"$exists": False}},
            {"name": None},
            {"name": ""},
            {"name": {"$regex": r"^\s+$"}},
        ]
    }


def _patient_alert_item(patient: dict, today):
    if not _has_patient_identity(patient):
        return None

    last_refill = parse_iso_date(patient.get("last_refill_date"))
    if not last_refill:
        return None

    try:
        duration_days = int(patient.get("duration_days") or 0)
    except (TypeError, ValueError):
        return None

    if duration_days <= 0:
        return None

    days_since_refill = (today - last_refill).days
    days_until_due = duration_days - days_since_refill

    if days_until_due > 0:
        return None

    next_refill_date = last_refill + timedelta(days=duration_days)

    return {
        **patient,
        "duration_days": duration_days,
        "last_refill_date": patient.get("last_refill_date"),
        "next_refill_date": next_refill_date.isoformat(),
        "days_since_refill": days_since_refill,
        "days_overdue": abs(days_until_due),
    }


async def _get_patient_alerts(today=None) -> list:
    today = today or datetime.now(timezone.utc).date()
    patients = await db.regular_patients.find({}, {"_id": 0}).to_list(2000)

    alerts = []
    for patient in patients:
        alert = _patient_alert_item(patient, today)
        if alert:
            alerts.append(alert)

    alerts.sort(
        key=lambda item: (
            -int(item.get("days_overdue") or 0),
            str(item.get("name") or "").lower(),
        )
    )
    return alerts


def _patient_medicine_matches(patient: dict, medicine: dict) -> bool:
    """Match explicit inventory references first, then legacy name/batch snapshots."""
    patient_refs = {
        str(patient.get(key) or "").strip().lower()
        for key in ("medicine_id", "medicine_key")
        if patient.get(key)
    }
    medicine_refs = {
        str(medicine.get(key) or "").strip().lower()
        for key in ("id", "medicine_key")
        if medicine.get(key)
    }
    if patient_refs and patient_refs & medicine_refs:
        return True
    if str(patient.get("medicine_name") or "").strip().lower() != str(medicine.get("name") or "").strip().lower():
        return False
    selected_batch = str(patient.get("batch") or "").strip().lower()
    return not selected_batch or selected_batch == str(medicine.get("batch_no") or "").strip().lower()


async def _link_patient_medicine(data: dict) -> dict:
    """Enrich patient tracking with an inventory snapshot without changing stock."""
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(5000)
    matches = [medicine for medicine in medicines if _patient_medicine_matches(data, medicine)]
    if not matches:
        return data
    medicine = sorted(matches, key=_fifo_expiry_key)[0]
    linked = dict(data)
    linked.update({
        "medicine_id": medicine.get("id") or linked.get("medicine_id"),
        "medicine_key": medicine.get("medicine_key") or linked.get("medicine_key"),
        "medicine_name": medicine.get("name") or linked.get("medicine_name"),
        "batch": medicine.get("batch_no") or linked.get("batch"),
        "expiry": medicine.get("expiry_date") or linked.get("expiry"),
        "current_mrp": round(float(medicine["mrp"]), 2) if medicine.get("mrp") is not None else linked.get("current_mrp"),
    })
    return linked


async def _get_patient_stock_alerts() -> list:
    patients = await db.regular_patients.find({}, {"_id": 0}).to_list(2000)
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(5000)
    alerts = []
    for medicine in medicines:
        status = _inventory_stock_status(medicine)
        if status not in {"low_stock", "critical", "sold_out"}:
            continue
        affected = [patient for patient in patients if _has_patient_identity(patient) and _patient_medicine_matches(patient, medicine)]
        if affected:
            alerts.append({
                "medicine_id": medicine.get("id"),
                "medicine_key": medicine.get("medicine_key"),
                "medicine_name": medicine.get("name"),
                "batch": medicine.get("batch_no"),
                "stock_status": status,
                "available_stock": round_qty(_available_stock(medicine)),
                "affected_patient_count": len(affected),
                "patient_names": sorted({patient["name"] for patient in affected}),
            })
    return alerts

def normalize_phone_number(phone: str) -> str:
    if not phone:
        return ""

    phone = str(phone).strip()
    phone = phone.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")

    # Already +91
    if phone.startswith("+91"):
        return phone

    # 91XXXXXXXXXX
    if phone.startswith("91") and len(phone) == 12:
        return "+" + phone

    # 10 digit Indian mobile
    if len(phone) == 10:
        return "+91" + phone

    return phone

async def normalize_existing_phone_numbers():
    collections = [
        db.patients,
        db.customers,
        db.distributors,
        db.regular_patients,
    ]

    for collection in collections:
        async for item in collection.find({"phone": {"$exists": True}}):
            old_phone = item.get("phone")
            new_phone = normalize_phone_number(old_phone)

            if old_phone != new_phone:
                await collection.update_one(
                    {"_id": item["_id"]},
                    {
                        "$set": {
                            "phone": new_phone
                        }
                    }
                )


# ---------------- Auth helpers ----------------
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def _auth_identifier_matches(user: dict, identifier: str) -> bool:
    needle = str(identifier or "").strip().lower()
    return any(str(user.get(field) or "").strip().lower() == needle for field in ("email", "mobile", "identifier"))


async def _find_local_auth_user_by_identifier(identifier: str) -> Optional[dict]:
    users = await raw_db.users.find({}).to_list(None)
    for user in users:
        user = _canonicalize_user_tenant(user)
        if user.get("id") == DEMO_USER_ID or user.get("tenant_id") == DEMO_TENANT_ID or user.get("is_demo") is True:
            continue
        if _auth_identifier_matches(user, identifier):
            return user
    return None


async def _find_local_auth_user_by_id(user_id: str, token_is_demo: bool = False) -> Optional[dict]:
    users = await raw_db.users.find({}).to_list(None)
    for user in users:
        user = _canonicalize_user_tenant(user)
        if token_is_demo:
            if user.get("id") == DEMO_USER_ID and user.get("tenant_id") == DEMO_TENANT_ID and user.get("is_demo") is True:
                return user
            continue
        if user.get("is_demo") is True or user.get("tenant_id") == DEMO_TENANT_ID:
            continue
        if str(user.get("id") or "") == str(user_id) or str(user.get("_id") or "") == str(user_id):
            return user
    return None


def create_access_token(user_id: str, email: str, role: str, tenant_id: Optional[str] = None, is_demo: bool = False) -> str:
    payload = {
        "sub": user_id, "email": email, "role": role, "tenant_id": tenant_id, "is_demo": is_demo,
        "exp": datetime.now(timezone.utc) + timedelta(hours=12),
        "type": "access",
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


async def get_current_user(request: Request) -> dict:
    token = request.cookies.get("access_token")
    if not token:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id = payload["sub"]
        token_is_demo = bool(payload.get("is_demo")) or user_id == DEMO_USER_ID
        if LOCAL_MODE:
            user = await _find_local_auth_user_by_id(user_id, token_is_demo=token_is_demo)
            if user:
                user.pop("password_hash", None)
                user.pop("reset_otp_hash", None)
        else:
            query = ({"id": DEMO_USER_ID, "tenant_id": DEMO_TENANT_ID, "is_demo": True}
                     if token_is_demo else {"id": user_id, "tenant_id": {"$ne": DEMO_TENANT_ID}, "is_demo": {"$ne": True}})
            user = await raw_db.users.find_one(query, {"_id": 0, "password_hash": 0, "reset_otp_hash": 0})
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        user = _canonicalize_user_tenant(user)
        if user.get("is_demo") and request.method.upper() not in {"GET", "HEAD", "OPTIONS"} and request.url.path not in {"/api/auth/logout"}:
            raise HTTPException(status_code=403, detail="Demo account is read-only")
        if _password_expired(user) and request.url.path not in {"/api/auth/change-password", "/api/auth/logout", "/api/auth/me"}:
            raise HTTPException(status_code=403, detail="Password expired; change password to continue", headers={"X-Password-Expired": "true"})
        user["password_expired"] = _password_expired(user)
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


def require_role(*roles: str):
    async def _dep(user: dict = Depends(get_current_user)):
        if user.get("role") not in roles and user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user
    return _dep


# ---------------- Models ----------------
class LocalImportConfirmRequest(BaseModel):
    overwrite_local: bool = False


class UserRegister(BaseModel):
    email: EmailStr
    password: str
    name: str


class UserCreateByAdmin(UserRegister):
    role: Literal["admin", "cashier", "pharmacist"] = "cashier"


class UserLogin(BaseModel):
    email: Optional[EmailStr] = None
    mobile: Optional[str] = None
    identifier: Optional[str] = None
    password: str

    @model_validator(mode="after")
    def require_identifier(self):
        if not (self.email or self.mobile or self.identifier):
            raise ValueError("email, mobile, or identifier is required")
        return self


class SignupRequest(BaseModel):
    email: Optional[EmailStr] = None
    mobile: Optional[str] = None
    password: str
    pharmacy_name: str
    owner_name: str
    gst: str = ""
    contact: str
    address: str
    state: str
    pincode: str
    method: Optional[Literal["email", "mobile"]] = None

    @model_validator(mode="after")
    def validate_signup_method(self):
        method = self.method or ("email" if self.email else "mobile")
        if method == "email" and not self.email:
            raise ValueError("email is required for email OTP signup")
        if method == "mobile" and not self.mobile:
            raise ValueError("mobile is required for mobile OTP signup")
        self.method = method
        return self


class SignupVerify(BaseModel):
    verification_id: str
    otp: str


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    email: EmailStr
    otp: str
    new_password: str


class Medicine(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    batch_no: str
    expiry_date: str  # YYYY-MM-DD
    manufacturer: str
    distributor: str
    distributor_id: Optional[str] = None

    purchase_price: float  # per unit
    mrp: float  # per unit
    pack_size: str = ""

    # 🔥 BASE STOCK (ONLY INITIAL PURCHASE)
    purchased_units: float= 0

    category: str = "OTC"
    gst_rate: float = 12.0
    barcode: Optional[str] = None
    low_stock_threshold: int = 10
    low_stock_status: Literal["low_stock", "reordered", "abandoned", "restocked"] = "low_stock"

    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
class MedicineCreate(BaseModel):
    name: str
    batch_no: str
    expiry_date: str
    manufacturer: str
    distributor: str
    distributor_id: Optional[str] = None

    purchase_price: float
    mrp: float
    pack_size: str = ""

    # 🔥 INITIAL STOCK ENTRY ONLY (purchase time)
    purchased_units: float= 0

    category: str = "OTC"
    gst_rate: float = 12.0
    barcode: Optional[str] = None
    low_stock_threshold: int = 10

    auto_ledger: bool = True


class LowStockStatusUpdate(BaseModel):
    status: Literal["low_stock", "reordered", "abandoned", "restocked"]


class LowStockThresholdUpdate(BaseModel):
    threshold: int

    @model_validator(mode="before")
    @classmethod
    def _accept_legacy_threshold_name(cls, data):
        if isinstance(data, dict) and "threshold" not in data and "low_stock_threshold" in data:
            return {**data, "threshold": data["low_stock_threshold"]}
        return data


class LowStockThresholdUnlock(BaseModel):
    privacy_password: str


class PrivacyPasswordUpdate(BaseModel):
    privacy_password: str

    @field_validator("privacy_password")
    @classmethod
    def _privacy_password_not_blank(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("Privacy password is required")
        return value
    
# Keep migration-only types separate so they can be removed from the normal
# workflow without changing the permanent adjustment types. Existing records
# may contain legacy labels and remain readable through the history endpoints.
PERMANENT_STOCK_ADJUSTMENT_TYPES = ("damaged", "expired", "correction")
MIGRATION_ONLY_STOCK_ADJUSTMENT_TYPES = ("opening_reconciliation",)
STOCK_ADJUSTMENT_TYPES = Literal[
    "damaged",
    "expired",
    "correction",
    "opening_reconciliation",
]


class StockAdjustmentCreate(BaseModel):
    adjustment_date: str
    medicine_id: str
    medicine_name: Optional[str] = None
    batch_no: Optional[str] = None
    adjustment_type: STOCK_ADJUSTMENT_TYPES
    quantity: float
    notes: str = ""
    reference_number: str = ""

    @field_validator("adjustment_date")
    @classmethod
    def require_valid_adjustment_date(cls, value):
        if not parse_iso_date(value):
            raise ValueError("adjustment_date must be a valid ISO date")
        return value

    @field_validator("medicine_id", "medicine_name", "batch_no", "notes", "reference_number", mode="before")
    @classmethod
    def trim_adjustment_strings(cls, value):
        return value.strip() if isinstance(value, str) else value

    @field_validator("quantity")
    @classmethod
    def require_nonzero_finite_quantity(cls, value):
        value = round_qty(value)
        if not math.isfinite(value) or value == 0:
            raise ValueError("quantity must be a non-zero finite number")
        return value


class RegularPatient(BaseModel):
    name: str
    age: int
    phone: str
    address: Optional[str] = None

    medicine_name: str
    medicine_id: Optional[str] = None
    medicine_key: Optional[str] = None
    current_mrp: Optional[float] = None
    batch: Optional[str] = None
    expiry: Optional[str] = None
    dosage: Optional[str] = None
    frequency: Optional[str] = None
    duration: Optional[str] = None
    duration_days: int
    last_refill_date: str

    condition: str = ""

    @field_validator(
        "name", "phone", "medicine_name", "last_refill_date", "condition",
        "medicine_id", "medicine_key", "batch", "expiry", "dosage", "frequency",
        "duration", mode="before"
    )
    @classmethod
    def trim_string_fields(cls, value):
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("current_mrp")
    @classmethod
    def normalize_current_mrp(cls, value):
        return None if value is None else round(float(value), 2)

    @field_validator("address", mode="before")
    @classmethod
    def trim_optional_string_fields(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            trimmed = value.strip()
            return trimmed or None
        return value

    @field_validator("name", "phone")
    @classmethod
    def require_name_and_phone(cls, value, info):
        if not value:
            raise ValueError(f"Patient {info.field_name} is required")
        return value

    @field_validator("age")
    @classmethod
    def require_valid_age(cls, value):
        if value < 0 or value > 130:
            raise ValueError("Patient age must be between 0 and 130")
        return value

    @field_validator("duration_days")
    @classmethod
    def require_positive_duration(cls, value):
        if value <= 0:
            raise ValueError("Patient duration_days must be greater than 0")
        return value

    @field_validator("last_refill_date")
    @classmethod
    def require_valid_refill_date(cls, value):
        if not parse_iso_date(value):
            raise ValueError("Patient last_refill_date must be a valid ISO date")
        return value


class Distributor(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    phone: str = ""
    email: str = ""
    address: str = ""
    gstin: str = ""
    opening_balance: float = 0.0
    opening_balance_date: Optional[str] = None
    opening_balance_invoice_number: Optional[str] = None
    opening_balance_bill_number: Optional[str] = None
    opening_balance_reference_number: Optional[str] = None
    opening_balance_notes: Optional[str] = None
    distributor_status: Literal["active", "inactive", "return_heavy"] = "active"
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class Customer(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    phone: str = ""
    email: str = ""
    gstin: str = ""
    address: str = ""
    customer_type: Literal["walk_in", "regular", "clinic", "hospital", "staff", "other"] = "regular"
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class InvoiceItem(BaseModel):
    medicine_id: str
    name: str
    batch_no: str
    expiry_date: str
    quantity: int
    unit_type: Literal["unit", "box"] = "unit"
    units_per_box: int = 1
    mrp: float  # per unit
    discount_pct: float = 0.0
    gst_rate: float = 12.0
    category: str = "OTC"


class InvoiceCreate(BaseModel):
    customer_id: Optional[str] = None
    customer_name: str = "Walk-in"
    customer_phone: str = ""
    customer_gstin: str = ""
    referring_doctor: str = ""
    items: List[InvoiceItem]
    payment_mode: Literal["cash", "upi", "card", "credit", "mixed"] = "cash"
    paid_amount: float = 0.0
    bill_discount_amount: float = 0.0
    bill_discount_pct: float = 0.0
    notes: str = ""


class PaymentCreate(BaseModel):
    amount: float
    mode: str = "cash"
    notes: str = ""
    date: Optional[str] = None
    receipt_number: Optional[str] = None
    invoice_number: Optional[str] = None
    bill_number: Optional[str] = None
    reference_number: Optional[str] = None
    payment_mode: Optional[str] = None

    @field_validator(
        "mode",
        "notes",
        "date",
        "receipt_number",
        "invoice_number",
        "bill_number",
        "reference_number",
        "payment_mode",
        mode="before",
    )
    @classmethod
    def trim_transaction_strings(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip()
        return value


class DistributorTransactionUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    receipt_no: Optional[str] = None
    receipt_number: Optional[str] = None
    invoice_no: Optional[str] = None
    invoice_number: Optional[str] = None
    bill_no: Optional[str] = None
    bill_number: Optional[str] = None
    reference_no: Optional[str] = None
    reference_number: Optional[str] = None
    reference: Optional[str] = None
    payment_mode: Optional[str] = None
    notes: Optional[str] = None
    opening_balance_date: Optional[str] = None
    date: Optional[str] = None
    transaction_date: Optional[str] = None

    @field_validator(
        "receipt_no",
        "receipt_number",
        "invoice_no",
        "invoice_number",
        "bill_no",
        "bill_number",
        "reference_no",
        "reference_number",
        "reference",
        "payment_mode",
        "notes",
        "opening_balance_date",
        "date",
        "transaction_date",
        mode="before",
    )
    @classmethod
    def trim_editable_strings(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("opening_balance_date", "date", "transaction_date")
    @classmethod
    def require_valid_opening_balance_date(cls, value):
        if value and not parse_iso_date(value):
            raise ValueError("date fields must be valid ISO dates")
        return value

    @model_validator(mode="after")
    def require_matching_opening_balance_date_aliases(self):
        submitted_dates = [
            parse_iso_date(value)
            for value in (self.opening_balance_date, self.date, self.transaction_date)
            if value
        ]
        if submitted_dates and any(value != submitted_dates[0] for value in submitted_dates):
            raise ValueError("opening balance date aliases must match")
        return self


class DistributorOpeningBalanceDateUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    opening_balance_date: str

    @field_validator("opening_balance_date", mode="before")
    @classmethod
    def trim_opening_balance_date(cls, value):
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("opening_balance_date")
    @classmethod
    def require_valid_opening_balance_date(cls, value):
        if not parse_iso_date(value):
            raise ValueError("opening_balance_date must be a valid ISO date")
        return value


class HistoricalSaleCreate(BaseModel):
    date: str
    cash_amount: float = 0
    upi_amount: float = 0
    pending_amount: float = 0
    notes: Optional[str] = None

class ExpenseCreate(BaseModel):
    date: str
    category: str
    amount: float
    notes: Optional[str] = None


DAILY_CLOSING_AMOUNT_FIELDS = (
    "cash_sales",
    "upi_sales",
    "card_sales",
    "credit_sales",
    "expenses",
    "expected_total",
    "counted_cash",
    "opening_cash",
)


class DailyClosingCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    closing_date: str
    cash_sales: float = 0
    upi_sales: float = 0
    card_sales: float = 0
    credit_sales: float = 0
    expenses: float = 0
    expected_total: float = 0
    counted_cash: float
    notes: str = ""
    closing_notes: str = ""
    opening_cash: float = 0
    locked: bool = False

    @field_validator("closing_date")
    @classmethod
    def require_valid_closing_date(cls, value):
        if not parse_iso_date(value):
            raise ValueError("closing_date must be a valid ISO date")
        return value

    @field_validator(*DAILY_CLOSING_AMOUNT_FIELDS)
    @classmethod
    def require_valid_amounts(cls, value, info):
        if not math.isfinite(value):
            raise ValueError(f"{info.field_name} must be finite")
        if info.field_name not in {"expected_total"} and value < 0:
            raise ValueError(f"{info.field_name} cannot be negative")
        return value


class DailyClosingUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    closing_date: Optional[str] = None
    cash_sales: Optional[float] = None
    upi_sales: Optional[float] = None
    card_sales: Optional[float] = None
    credit_sales: Optional[float] = None
    expenses: Optional[float] = None
    expected_total: Optional[float] = None
    counted_cash: Optional[float] = None
    notes: Optional[str] = None
    closing_notes: Optional[str] = None
    opening_cash: Optional[float] = None
    locked: Optional[bool] = None

    @field_validator("closing_date")
    @classmethod
    def require_valid_closing_date(cls, value):
        if value is not None and not parse_iso_date(value):
            raise ValueError("closing_date must be a valid ISO date")
        return value

    @field_validator(*DAILY_CLOSING_AMOUNT_FIELDS)
    @classmethod
    def require_valid_amounts(cls, value, info):
        if value is None:
            return value
        if not math.isfinite(value):
            raise ValueError(f"{info.field_name} must be finite")
        if info.field_name not in {"expected_total"} and value < 0:
            raise ValueError(f"{info.field_name} cannot be negative")
        return value


class PurchaseReturnCreate(BaseModel):
    return_date: str
    distributor: str
    distributor_id: str = ""
    medicine_name: str
    medicine_key: str = ""
    medicine_id: str = ""
    batch_number: str
    expiry_date: str
    return_quantity: float
    purchase_rate: float
    reason: Literal["Expired", "Damaged", "Wrong Item", "Other"]
    notes: str = ""
    adjust_distributor_ledger: bool = False

    @model_validator(mode="before")
    @classmethod
    def normalize_frontend_payload(cls, data):
        if not isinstance(data, dict):
            return data

        normalized = dict(data)

        alias_fields = {
            "distributor": ("distributor_id",),
            "batch_number": ("batch", "batch_no"),
            "expiry_date": ("expiry",),
            "return_quantity": ("quantity",),
            "purchase_rate": ("rate",),
        }

        for field_name, aliases in alias_fields.items():
            if normalized.get(field_name) not in (None, ""):
                continue
            for alias in aliases:
                if normalized.get(alias) not in (None, ""):
                    normalized[field_name] = normalized[alias]
                    break

        if normalized.get("medicine_key") in (None, "") and normalized.get("medicine_id") not in (None, ""):
            normalized["medicine_key"] = normalized["medicine_id"]

        reason = normalized.get("reason")
        if isinstance(reason, str):
            reason_lookup = {
                "expired": "Expired",
                "damaged": "Damaged",
                "wrong item": "Wrong Item",
                "wrong_item": "Wrong Item",
                "other": "Other",
            }
            normalized["reason"] = reason_lookup.get(reason.strip().lower(), reason.strip())

        return normalized

    @field_validator(
        "return_date",
        "distributor",
        "medicine_name",
        "batch_number",
        "expiry_date",
        "reason",
        mode="before",
    )
    @classmethod
    def trim_required_strings(cls, value):
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("notes", mode="before")
    @classmethod
    def trim_notes(cls, value):
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator(
        "return_date",
        "distributor",
        "medicine_name",
        "batch_number",
        "expiry_date",
        "reason",
    )
    @classmethod
    def require_strings(cls, value, info):
        if not value:
            raise ValueError(f"{info.field_name} is required")
        return value

    @field_validator("return_date")
    @classmethod
    def require_valid_return_date(cls, value):
        if not parse_iso_date(value):
            raise ValueError("return_date must be a valid ISO date")
        return value

    @field_validator("expiry_date")
    @classmethod
    def require_valid_expiry_date(cls, value):
        if not parse_expiry_date(value):
            raise ValueError("expiry_date must be a valid ISO date or MM/YY value")
        return value

    @field_validator("return_quantity")
    @classmethod
    def require_positive_return_quantity(cls, value):
        if value <= 0:
            raise ValueError("return_quantity must be greater than zero")
        return value

    @field_validator("purchase_rate")
    @classmethod
    def require_positive_purchase_rate(cls, value):
        if value <= 0:
            raise ValueError("purchase_rate must be greater than zero")
        return value


PURCHASE_RETURN_UPDATE_DISPLAY_FIELDS = {
    "id",
    "created_at",
    "updated_at",
    "created_by",
    "updated_by",
    "deleted_at",
    "deleted_by",
    "distributor_name",
    "medicine_name",
    "medicine_key",
    "medicine_id",
    "batch_label",
    "status",
    "total_amount",
    "return_amount",
    "ledger_adjusted",
    "ledger_transaction_id",
    "linked_transaction_id",
    "settlement_status",
    "settled_by_po",
    "settled_at",
    "settlement_reference",
    "settled_return_value",
    "po_adjustment_id",
    "po_adjusted_at",
}


class PurchaseReturnUpdate(BaseModel):
    # Purchase Return edit forms often post back table/display fields with the
    # editable values.  Keep this schema permissive and explicitly strip known
    # read-only helpers so PATCH and PUT share one safe validation path.
    model_config = ConfigDict(extra="ignore")

    return_date: Optional[str] = None
    reason: Optional[Literal["Expired", "Damaged", "Wrong Item", "Other"]] = None
    return_quantity: Optional[float] = None
    purchase_rate: Optional[float] = None
    expiry_date: Optional[str] = None
    notes: Optional[str] = None
    distributor: Optional[str] = None
    distributor_id: Optional[str] = None
    adjust_distributor_ledger: Optional[bool] = None

    @model_validator(mode="before")
    @classmethod
    def normalize_frontend_payload(cls, data):
        if not isinstance(data, dict):
            return data

        normalized = {
            key: value
            for key, value in dict(data).items()
            if key not in PURCHASE_RETURN_UPDATE_DISPLAY_FIELDS
        }
        alias_fields = {
            "batch_number": ("batch", "batch_no", "batch_label"),
            "expiry_date": ("expiry",),
            "return_quantity": ("quantity",),
            "purchase_rate": ("rate",),
        }
        for field_name, aliases in alias_fields.items():
            if normalized.get(field_name) not in (None, ""):
                continue
            for alias in aliases:
                if normalized.get(alias) not in (None, ""):
                    normalized[field_name] = normalized[alias]
                    break

        reason = normalized.get("reason")
        if isinstance(reason, str):
            reason_lookup = {
                "expired": "Expired",
                "damaged": "Damaged",
                "wrong item": "Wrong Item",
                "wrong_item": "Wrong Item",
                "other": "Other",
            }
            normalized["reason"] = reason_lookup.get(reason.strip().lower(), reason.strip())

        return normalized

    @field_validator("return_date")
    @classmethod
    def validate_return_date(cls, value):
        if value is not None and not parse_iso_date(value):
            raise ValueError("return_date must be a valid ISO date")
        return value

    @field_validator("expiry_date")
    @classmethod
    def validate_expiry_date(cls, value):
        if value is not None and value != "" and not parse_expiry_date(value):
            raise ValueError("expiry_date must be a valid MM/YY value")
        return value

    @field_validator("return_quantity")
    @classmethod
    def validate_return_quantity(cls, value):
        if value is not None and value <= 0:
            raise ValueError("return_quantity must be greater than zero")
        return value

    @field_validator("purchase_rate")
    @classmethod
    def validate_purchase_rate(cls, value):
        if value is not None and value <= 0:
            raise ValueError("purchase_rate must be greater than zero")
        return value

    @field_validator("notes", mode="before")
    @classmethod
    def normalize_notes(cls, value):
        return value.strip() if isinstance(value, str) else value
    
# ---------------- Startup stability tracking ----------------
# Defaults are stable for direct function/test calls before the FastAPI startup
# event schedules deferred maintenance. Startup flips these flags while tenant
# backfills, index creation, and purchase-return repairs are in progress.
_STARTUP_STABILITY = {
    "maintenance_running": False,
    "tenant_initialization_complete": True,
    "purchase_return_recalculation_complete": True,
    "indexing_complete": True,
}


def _mark_startup_maintenance_started() -> None:
    _STARTUP_STABILITY.update({
        "maintenance_running": True,
        "tenant_initialization_complete": False,
        "purchase_return_recalculation_complete": LOCAL_MODE,
        "indexing_complete": False,
    })


def _mark_startup_maintenance_finished() -> None:
    _STARTUP_STABILITY["maintenance_running"] = False


def _system_stable() -> bool:
    return (
        not _STARTUP_STABILITY["maintenance_running"]
        and _STARTUP_STABILITY["tenant_initialization_complete"]
        and _STARTUP_STABILITY["purchase_return_recalculation_complete"]
        and _STARTUP_STABILITY["indexing_complete"]
    )


# ---------------- Startup ----------------
async def _backfill_tenant_data(now_iso: str) -> None:
    await raw_db.users.update_many(
        {"tenant_id": {"$exists": False}, "$or": [{"is_demo": True}, {"id": DEMO_USER_ID}]},
        {"$set": {"tenant_id": DEMO_TENANT_ID, "shop_id": DEMO_TENANT_ID, "is_demo": True, "password_changed_at": now_iso}},
    )
    await raw_db.users.update_many(
        {"tenant_id": {"$exists": False}},
        {"$set": {"tenant_id": REAL_TENANT_ID, "is_demo": False, "password_changed_at": now_iso}},
    )
    await raw_db.users.update_many(
        {"password_changed_at": {"$exists": False}},
        {"$set": {"password_changed_at": now_iso}},
    )
    await raw_db.users.update_many({"shop_id": {"$exists": False}}, [{"$set": {"shop_id": "$tenant_id"}}])
    for collection_name in BUSINESS_COLLECTIONS:
        collection = raw_db[collection_name]
        await collection.update_many(
            {"tenant_id": {"$exists": False}},
            {"$set": {"tenant_id": REAL_TENANT_ID}},
        )
        await collection.update_many({"shop_id": {"$exists": False}}, [{"$set": {"shop_id": "$tenant_id"}}])
        await collection.create_index("tenant_id")


async def _resolve_demo_email() -> str:
    """Choose a demo-only email without ever taking an address owned by a real user."""
    configured = os.environ.get("DEMO_EMAIL", SAFE_DEMO_EMAIL).strip().lower() or SAFE_DEMO_EMAIL
    admin_email = os.environ.get("ADMIN_EMAIL", "").strip().lower()
    candidate = SAFE_DEMO_EMAIL if admin_email and configured == admin_email else configured
    conflict = await raw_db.users.find_one({
        "email": candidate,
        "id": {"$ne": DEMO_USER_ID},
        "is_demo": {"$ne": True},
        "tenant_id": {"$ne": DEMO_TENANT_ID},
    })
    if conflict:
        candidate = SAFE_DEMO_EMAIL
    safe_conflict = await raw_db.users.find_one({
        "email": candidate,
        "id": {"$ne": DEMO_USER_ID},
        "is_demo": {"$ne": True},
        "tenant_id": {"$ne": DEMO_TENANT_ID},
    })
    if safe_conflict:
        raise RuntimeError(f"Safe demo email {candidate} is already owned by a real user")
    return candidate


def _system_account_marker_query() -> List[dict]:
    return [{marker: True} for marker in SYSTEM_ACCOUNT_MARKERS] + [
        {"id": DEMO_USER_ID},
        {"name": "Administrator", "role": "admin"},
        {"name": {"$in": ["Demo Administrator", "Demo Pharmacist"]}},
    ]


def _unsafe_seeded_user_query(tenant_id: str) -> dict:
    return {
        "tenant_id": tenant_id,
        "email": {"$in": sorted(UNSAFE_DEFAULT_EMAILS)},
        "$or": _system_account_marker_query(),
    }


async def _cleanup_unsafe_real_users() -> None:
    """Remove clearly seeded unsafe defaults from the real tenant, preserving its last admin."""
    candidates = await raw_db.users.find(_unsafe_seeded_user_query(REAL_TENANT_ID)).to_list(1000)
    admin_count = await raw_db.users.count_documents({"tenant_id": REAL_TENANT_ID, "role": "admin"})
    for candidate in candidates:
        if candidate.get("role") == "admin" and admin_count <= 1:
            logger.warning("Preserving the only real-tenant admin despite unsafe seeded markers: %s", candidate.get("email"))
            continue
        result = await raw_db.users.delete_one({"id": candidate["id"], "tenant_id": REAL_TENANT_ID})
        if result.deleted_count and candidate.get("role") == "admin":
            admin_count -= 1


async def _seed_admin_if_enabled(now_iso: str) -> None:
    if os.environ.get("SEED_ADMIN", "").strip().lower() != "true":
        return

    admin_email = os.environ.get("ADMIN_EMAIL", "").strip().lower()
    admin_password = os.environ.get("ADMIN_PASSWORD", "")
    if not admin_email or not admin_password:
        raise RuntimeError("SEED_ADMIN=true requires explicit ADMIN_EMAIL and ADMIN_PASSWORD")
    if admin_email in UNSAFE_DEFAULT_EMAILS or admin_password in UNSAFE_DEFAULT_PASSWORDS:
        raise RuntimeError("Refusing to seed an unsafe default admin credential")
    try:
        _validate_password_strength(admin_password)
    except HTTPException as exc:
        raise RuntimeError(f"ADMIN_PASSWORD does not meet password strength requirements: {exc.detail}") from exc

    existing = await raw_db.users.find_one({"email": admin_email})
    if existing:
        if existing.get("tenant_id") != REAL_TENANT_ID:
            raise RuntimeError("ADMIN_EMAIL is already assigned outside REAL_TENANT_ID")
        return
    await raw_db.users.insert_one({
        "id": str(uuid.uuid4()), "email": admin_email, "password_hash": hash_password(admin_password),
        "name": "Administrator", "role": "admin", "tenant_id": REAL_TENANT_ID, "shop_id": REAL_TENANT_ID,
        "is_demo": False, "system_seeded": True, "active": True,
        "created_at": now_iso, "password_changed_at": now_iso,
    })
    logger.info("Seeded explicitly configured admin user: %s", admin_email)


async def _cleanup_demo_users() -> None:
    """Remove only explicitly identifiable demo identities from non-demo tenants."""
    await raw_db.users.delete_many({
        "tenant_id": {"$ne": DEMO_TENANT_ID},
        "$or": [{"is_demo": True}, {"id": DEMO_USER_ID}],
    })
    await raw_db.users.delete_many({
        "tenant_id": DEMO_TENANT_ID,
        "is_demo": True,
        "id": {"$ne": DEMO_USER_ID},
    })


async def _seed_demo_data(now_iso: str) -> None:
    demo_email = await _resolve_demo_email()
    demo_password = os.environ.get("DEMO_PASSWORD", "DemoAccess123")
    await _cleanup_demo_users()
    await raw_db.users.replace_one(
        {"id": DEMO_USER_ID, "tenant_id": DEMO_TENANT_ID, "is_demo": True},
        {
            "id": DEMO_USER_ID, "email": demo_email, "name": "Demo Pharmacist", "role": "admin",
            "password_hash": hash_password(demo_password), "password_changed_at": now_iso,
            "tenant_id": DEMO_TENANT_ID, "shop_id": DEMO_TENANT_ID, "is_demo": True, "active": True,
            "created_at": now_iso,
        },
        upsert=True,
    )
    demo_documents = {
        "distributors": [{"id": "demo-dist-1", "name": "Demo Health Distributors", "phone": "555-0101", "email": "orders@example.invalid", "created_at": now_iso}],
        "medicines": [
            {"id": "demo-med-1", "medicine_key": "paracetamol 500mg::DEMO-B1", "name": "Paracetamol 500mg", "batch_no": "DEMO-B1", "expiry_date": "2027-12-31", "manufacturer": "Demo Labs", "distributor": "Demo Health Distributors", "distributor_id": "demo-dist-1", "purchase_price": 1.0, "mrp": 2.0, "purchased_units": 250, "sold_units": 25, "category": "OTC", "gst_rate": 5, "created_at": now_iso},
            {"id": "demo-med-2", "medicine_key": "vitamin c::DEMO-B2", "name": "Vitamin C", "batch_no": "DEMO-B2", "expiry_date": "2028-06-30", "manufacturer": "Demo Labs", "distributor": "Demo Health Distributors", "distributor_id": "demo-dist-1", "purchase_price": 2.0, "mrp": 4.0, "purchased_units": 100, "sold_units": 10, "category": "Supplements", "gst_rate": 12, "created_at": now_iso},
        ],
        "customers": [{"id": "demo-customer-1", "name": "Demo Customer", "phone": "555-0110", "email": "customer@example.invalid", "created_at": now_iso}],
        "purchase_orders": [{"id": "demo-po-1", "po_number": "DEMO-PO-001", "distributor_id": "demo-dist-1", "distributor_name": "Demo Health Distributors", "invoice_ref": "DEMO-SUP-001", "items": [{"name": "Paracetamol 500mg", "batch_no": "DEMO-B1", "quantity": 250, "free_quantity": 0, "purchase_price": 1.0, "mrp": 2.0, "gst_rate": 5}], "sub_total": 250, "grand_total": 262.5, "created_at": now_iso}],
        "invoices": [{"id": "demo-invoice-1", "invoice_number": "DEMO-INV-001", "customer_id": "demo-customer-1", "customer_name": "Demo Customer", "items": [{"medicine_id": "demo-med-1", "name": "Paracetamol 500mg", "quantity": 5, "price": 2.0}], "subtotal": 10, "grand_total": 10, "total": 10, "created_at": now_iso}],
        "customer_transactions": [{"id": "demo-customer-txn-1", "customer_id": "demo-customer-1", "type": "sale", "amount": 10, "reference": "DEMO-INV-001", "created_at": now_iso}],
        "distributor_transactions": [{"id": "demo-dist-txn-1", "distributor_id": "demo-dist-1", "type": "purchase", "amount": 262.5, "reference": "DEMO-PO-001", "created_at": now_iso}],
        "expenses": [{"id": "demo-expense-1", "category": "Utilities", "amount": 25, "description": "Demo electricity bill", "created_at": now_iso}],
        "daily_summary": [{"id": "demo-summary-1", "date": datetime.now(timezone.utc).date().isoformat(), "total_sales": 10, "cash": 10, "upi": 0, "pending": 0, "expenses": 25, "created_at": now_iso}],
        "daily_sales": [{"id": "demo-sale-1", "medicine_id": "demo-med-1", "medicine_name": "Paracetamol 500mg", "quantity": 5, "unit_type": "unit", "total_amount": 10, "customer_name": "Demo Customer", "payment_status": "paid", "sale_date": datetime.now(timezone.utc).date().isoformat(), "created_at": now_iso}],
        "settings": [{"id": "demo-settings-main", "key": "main", "business_name": "Demo Pharmacy", "business_address": "Demo shop — isolated sample data", "business_phone": "555-0100", "business_gstin": "", "signature_b64": ""}],
    }
    for collection_name, documents in demo_documents.items():
        collection = raw_db[collection_name]
        for document in documents:
            owned = {**document, "tenant_id": DEMO_TENANT_ID, "shop_id": DEMO_TENANT_ID}
            await collection.replace_one({"id": document["id"], "tenant_id": DEMO_TENANT_ID}, owned, upsert=True)


async def _run_deferred_startup_maintenance(now_iso: str) -> None:
    """Run startup repairs/indexing after the API is already accepting requests."""
    global _STARTUP_TIMING_ACTIVE
    maintenance_started = time.perf_counter()
    _mark_startup_maintenance_started()
    _record_startup_timing("Background startup maintenance started")
    try:
        await _time_startup_awaitable("Tenant initialization/backfill", _backfill_tenant_data(now_iso))
        _STARTUP_STABILITY["tenant_initialization_complete"] = True
        await _time_startup_awaitable("Tenant/user repair cleanup", _cleanup_unsafe_real_users())
        await _time_startup_awaitable("Index creation users.email", raw_db.users.create_index("email", unique=True))
        await _time_startup_awaitable("Index creation users.mobile", raw_db.users.create_index("mobile", unique=True, sparse=True))
        await _time_startup_awaitable("Index creation password_reset_requests.created_at", raw_db.password_reset_requests.create_index("created_at", expireAfterSeconds=FORGOT_PASSWORD_WINDOW_MINUTES * 60))
        await _time_startup_awaitable("Index creation pending_signups.expires_at", raw_db.pending_signups.create_index("expires_at", expireAfterSeconds=0))
        for collection_name, indexes in {
            "medicines": ["name", "batch_no", "manufacturer", "barcode"], "invoices": ["created_at"],
            "purchase_returns": ["return_date", "distributor", "medicine_name", "reason", "ledger_adjusted", "po_adjustment_id"],
        }.items():
            for index in indexes:
                await _time_startup_awaitable(f"Index creation {collection_name}.{index}", raw_db[collection_name].create_index(index))
        for collection_name, date_field in {
            "invoices": "created_at", "purchase_orders": "created_at",
            "customer_transactions": "created_at", "purchase_returns": "return_date",
        }.items():
            await _time_startup_awaitable(f"Index creation {collection_name}.tenant_id_{date_field}", raw_db[collection_name].create_index([("tenant_id", 1), (date_field, -1)]))
        await _time_startup_awaitable("Index creation purchase_returns.tenant_distributor_po", raw_db.purchase_returns.create_index([("tenant_id", 1), ("distributor_id", 1), ("po_adjustment_id", 1)]))
        await _time_startup_awaitable("Index creation daily_closings.tenant_closing_date", raw_db.daily_closings.create_index([("tenant_id", 1), ("closing_date", 1)], unique=True))
        _STARTUP_STABILITY["indexing_complete"] = True
        await _time_startup_awaitable("Seed admin", _seed_admin_if_enabled(now_iso))
        if LOCAL_MODE:
            logger.info(
                "Skipping heavy LOCAL_MODE startup maintenance: purchase-return stock recalculation, "
                "inventory rebuild, and dashboard/cache rebuild are manual admin maintenance actions."
            )
            _STARTUP_STABILITY["purchase_return_recalculation_complete"] = True
            _record_startup_timing("Purchase return recalculation all tenants", status="skipped in LOCAL_MODE startup; run manually from admin maintenance")
            _record_startup_timing("Inventory rebuild", status="skipped in LOCAL_MODE startup; run manually from admin maintenance")
            _record_startup_timing("Dashboard rebuild", status="skipped in LOCAL_MODE startup; run manually from admin maintenance")
            _record_startup_timing("Cache warm-up", status="skipped in LOCAL_MODE startup; run manually from admin maintenance")
        else:
            await _time_startup_awaitable("Purchase return recalculation all tenants", _run_startup_purchase_return_stock_recalculation())
            _STARTUP_STABILITY["purchase_return_recalculation_complete"] = True
            _record_startup_timing("Inventory rebuild", status="not scheduled during startup")
            _record_startup_timing("Cache warm-up", status="not scheduled during startup")
    except Exception:
        logger.exception("Deferred startup maintenance failed")
    finally:
        _mark_startup_maintenance_finished()
        _record_startup_timing("Background startup maintenance complete", time.perf_counter() - maintenance_started)
        _STARTUP_TIMING_ACTIVE = False


def _start_deferred_startup_maintenance(now_iso: str) -> None:
    started = time.perf_counter()
    if LOCAL_MODE:
        task = asyncio.create_task(_run_deferred_startup_maintenance(now_iso))
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)
        _record_startup_timing("Background startup maintenance task scheduled", time.perf_counter() - started, mode="asyncio")
        return

    def runner() -> None:
        asyncio.run(_run_deferred_startup_maintenance(now_iso))

    thread = threading.Thread(
        target=runner,
        name="pharmacyos-startup-maintenance",
        daemon=True,
    )
    thread.start()
    _record_startup_timing("Background startup maintenance task scheduled", time.perf_counter() - started, mode="thread")


@app.on_event("startup")
async def startup():
    await normalize_existing_phone_numbers()
    startup_started = time.perf_counter()
    _record_startup_timing("FastAPI startup event started")
    now_iso = datetime.now(timezone.utc).isoformat()
    # Keep the lightweight demo identity available for local/demo login, but defer
    # tenant-wide repairs, indexing, and stock recalculation so health is immediate.
    await _time_startup_awaitable("Seed demo data", _seed_demo_data(now_iso))
    _time_startup_sync("Schedule deferred startup maintenance", _start_deferred_startup_maintenance, now_iso)
    if LOCAL_MODE:
        backup_started = time.perf_counter()
        backup_task = asyncio.create_task(_scheduled_local_backup_loop())
        _background_tasks.add(backup_task)
        backup_task.add_done_callback(_background_tasks.discard)
        _record_startup_timing("Backup scheduler task scheduled", time.perf_counter() - backup_started)
        idle_sync_started = time.perf_counter()
        idle_sync_task = asyncio.create_task(_local_idle_backup_sync_loop())
        _background_tasks.add(idle_sync_task)
        idle_sync_task.add_done_callback(_background_tasks.discard)
        _record_startup_timing("Idle backup sync task scheduled", time.perf_counter() - idle_sync_started)
        logger.info("Local PharmacyOS server running at http://localhost:8000")
    _record_startup_timing("FastAPI startup event complete", time.perf_counter() - startup_started)


@app.on_event("shutdown")
async def shutdown():
    if LOCAL_MODE:
        await _create_and_sync_backup("app_exit")
    if client is not None:
        client.close()


# ---------------- Auth routes ----------------
async def _create_user(payload: UserRegister, role: str, tenant_id: Optional[str] = None) -> dict:
    email = payload.email.lower()
    existing = await raw_db.users.find_one({"email": email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    _validate_password_strength(payload.password)
    now_iso = datetime.now(timezone.utc).isoformat()
    user = {
        "id": str(uuid.uuid4()), "email": email, "password_hash": hash_password(payload.password),
        "name": payload.name, "role": role, "tenant_id": tenant_id or f"shop_{uuid.uuid4().hex}",
        "is_demo": False, "created_at": now_iso, "password_changed_at": now_iso,
    }
    user["shop_id"] = user["tenant_id"]
    await raw_db.users.insert_one(user)
    return {key: user[key] for key in ("id", "email", "name", "role", "tenant_id", "shop_id", "is_demo")}


async def _send_password_reset_email(email: str, otp: str) -> bool:
    host = os.environ.get("SMTP_HOST")
    sender = os.environ.get("SMTP_FROM")
    if not host or not sender:
        return False
    message = EmailMessage()
    message["Subject"] = "Pharmacy Pro password reset code"
    message["From"] = sender
    message["To"] = email
    message.set_content(f"Your password reset code expires in 10 minutes: {otp}")

    def send():
        port = int(os.environ.get("SMTP_PORT", "587"))
        with smtplib.SMTP(host, port, timeout=10) as smtp:
            if os.environ.get("SMTP_STARTTLS", "true").lower() == "true":
                smtp.starttls()
            username = os.environ.get("SMTP_USERNAME")
            password = os.environ.get("SMTP_PASSWORD")
            if username and password:
                smtp.login(username, password)
            smtp.send_message(message)

    loop = asyncio.get_running_loop()

    await loop.run_in_executor(
        None,
        send
    )
    return True


def _signup_identifier(payload: SignupRequest) -> str:
    return str(payload.email if payload.method == "email" else payload.mobile).strip().lower()


async def _send_signup_otp(method: str, identifier: str, otp: str) -> bool:
    if method == "email":
        host = os.environ.get("SMTP_HOST")
        sender = os.environ.get("SMTP_FROM")
        if not host or not sender:
            return False
        message = EmailMessage()
        message["Subject"] = "Pharmacy Pro signup verification code"
        message["From"] = sender
        message["To"] = identifier
        message.set_content(f"Your Pharmacy Pro signup code expires in 10 minutes: {otp}")

        def send_email():
            with smtplib.SMTP(host, int(os.environ.get("SMTP_PORT", "587")), timeout=10) as smtp:
                if os.environ.get("SMTP_STARTTLS", "true").lower() == "true":
                    smtp.starttls()
                if os.environ.get("SMTP_USERNAME") and os.environ.get("SMTP_PASSWORD"):
                    smtp.login(os.environ["SMTP_USERNAME"], os.environ["SMTP_PASSWORD"])
                smtp.send_message(message)

        loop = asyncio.get_running_loop()

        await loop.run_in_executor(
            None,
            send_email
        )
        return True

    webhook = os.environ.get("SMS_OTP_WEBHOOK_URL")
    if not webhook:
        return False

    def send_sms():
        body = json.dumps({"mobile": identifier, "otp": otp, "purpose": "signup"}).encode()
        request = urllib.request.Request(webhook, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=10) as response:
            return 200 <= response.status < 300

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, send_sms)
    return bool(result)


def _set_update_metadata_no_cache_headers(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"


def _log_update_metadata_check(label: str, metadata: dict, response: Response) -> None:
    logger.info(
        "%s: current_version=%s current_build=%s latest_version=%s latest_build=%s release_timestamp=%s update_available=%s cache_control=%s",
        label,
        metadata["current_version"],
        metadata["current_build"],
        metadata["latest_version"],
        metadata["latest_build"],
        metadata["release_timestamp"],
        metadata["update_available"],
        response.headers.get("Cache-Control"),
    )


@api_router.get("/version")
@api_router.get("/version.json")
async def version(
    response: Response,
    current_version: Optional[str] = Query(default=None),
    current_build: Optional[str] = Query(default=None),
):
    _set_update_metadata_no_cache_headers(response)
    metadata = get_version_metadata(current_version=current_version, current_build=current_build)
    _log_update_metadata_check("Version metadata check", metadata, response)
    return metadata


@api_router.get("/updates/check")
@api_router.get("/update-check")
async def check_updates(
    response: Response,
    current_version: Optional[str] = Query(default=None),
    current_build: Optional[str] = Query(default=None),
):
    _set_update_metadata_no_cache_headers(response)
    manifest_url = os.environ.get("PHARMACYOS_UPDATE_MANIFEST_URL", "")
    try:
        loop = asyncio.get_running_loop()
        manifest = await loop.run_in_executor(None, fetch_update_manifest, manifest_url)
        payload = build_update_check_response(manifest, current_version=current_version, current_build=current_build)
        logger.info(
            "Update check: current_version=%s current_build=%s latest_version=%s latest_build=%s update_available=%s",
            payload.get("current_version"),
            payload.get("current_build"),
            payload.get("latest_version"),
            payload.get("latest_build"),
            payload.get("update_available"),
        )
        return payload
    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"
        logger.warning("Update metadata check failed: %s", reason)
        return build_update_check_fallback(reason=reason, current_version=current_version, current_build=current_build)


@api_router.get("/app/version")
async def app_version_endpoint():
    return {
        "version": BACKEND_APP_VERSION,
        "build": BACKEND_APP_BUILD,
        "channel": APP_CHANNEL,
        "runtime_mode": RUNTIME_MODE,
        "local_mode": LOCAL_MODE,
    }


@api_router.get("/app/update-check")
async def app_update_check():
    manifest_url = os.environ.get("PHARMACYOS_UPDATE_MANIFEST_URL", "")
    try:
        loop = asyncio.get_running_loop()
        manifest = await loop.run_in_executor(None, fetch_update_manifest, manifest_url)
        return build_update_check_response(manifest)
    except ManifestUnavailable as exc:
        logger.warning("Update check unavailable: %s", exc)
        return build_update_check_fallback(reason=str(exc))


def _updater_script_path() -> Path:
    configured_path = os.environ.get("PHARMACYOS_UPDATER_SCRIPT") or DEFAULT_UPDATER_SCRIPT
    return Path(configured_path).expanduser()


def _launch_updater_script(script_path: Path) -> None:
    script = str(script_path)
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "DETACHED_PROCESS", 0)
        subprocess.Popen(
            ["cmd.exe", "/c", "start", "", script],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            creationflags=creationflags,
        )
        return

    subprocess.Popen(
        [script],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        start_new_session=True,
    )


def _update_status_payload() -> Dict[str, Any]:
    now = time.monotonic()
    with _update_start_lock:
        in_progress = (
            _update_last_started_monotonic is not None
            and now - _update_last_started_monotonic < UPDATE_START_GUARD_SECONDS
        )
        last_started_at = _update_last_started_at.isoformat() if _update_last_started_at else None
    return {
        "update_in_progress": in_progress,
        "last_started_at": last_started_at,
        "message": "Update already in progress." if in_progress else "No update in progress.",
    }


@api_router.post("/app/start-update")
async def app_start_update():
    global _update_last_started_at, _update_last_started_monotonic

    if not LOCAL_MODE:
        raise HTTPException(status_code=403, detail="Self-update is only available in local desktop mode.")

    script_path = _updater_script_path()
    if not script_path.is_file():
        raise HTTPException(status_code=404, detail="Updater script was not found.")

    now_monotonic = time.monotonic()
    with _update_start_lock:
        if (
            _update_last_started_monotonic is not None
            and now_monotonic - _update_last_started_monotonic < UPDATE_START_GUARD_SECONDS
        ):
            return {"status": "already_started", "message": "Update already in progress."}

        try:
            _launch_updater_script(script_path)
        except Exception as exc:
            logger.exception("Failed to start PharmacyOS updater script")
            raise HTTPException(status_code=500, detail="Updater script could not be started.") from exc

        _update_last_started_monotonic = now_monotonic
        _update_last_started_at = datetime.now(timezone.utc)

    return {"status": "started"}


@api_router.get("/app/update-status")
async def app_update_status():
    return _update_status_payload()


@api_router.post("/auth/signup/request-otp")
async def request_signup_otp(payload: SignupRequest):
    _validate_password_strength(payload.password)
    identifier = _signup_identifier(payload)
    existing_query = {"email": identifier} if payload.method == "email" else {"mobile": identifier}
    if await raw_db.users.find_one(existing_query):
        raise HTTPException(status_code=400, detail=f"{payload.method.title()} already registered")

    verification_id = str(uuid.uuid4())
    otp = f"{secrets.randbelow(1_000_000):06d}"
    now = datetime.now(timezone.utc)
    pending = {
        "id": verification_id,
        "method": payload.method,
        "identifier": identifier,
        "otp_hash": _otp_hash(identifier, otp),
        "attempts": 0,
        "expires_at": now + timedelta(minutes=SIGNUP_OTP_TTL_MINUTES),
        "created_at": now,
        "payload": payload.model_dump(mode="json", exclude={"password"}),
        "password_hash": hash_password(payload.password),
    }
    await raw_db.pending_signups.replace_one({"identifier": identifier}, pending, upsert=True)
    delivered = await _send_signup_otp(payload.method, identifier, otp)
    response = {"verification_id": verification_id, "method": payload.method, "delivered": delivered, "expires_in_seconds": SIGNUP_OTP_TTL_MINUTES * 60}
    if not delivered and ENVIRONMENT not in {"production", "prod"}:
        response["debug_otp"] = otp
    return response


@api_router.post("/auth/signup/verify")
async def verify_signup_otp(payload: SignupVerify):
    pending = await raw_db.pending_signups.find_one({"id": payload.verification_id})
    now = datetime.now(timezone.utc)
    if not pending or pending.get("attempts", 0) >= SIGNUP_OTP_ATTEMPTS or pending.get("expires_at") < now:
        raise HTTPException(status_code=400, detail="OTP verification expired or invalid")
    if not hmac.compare_digest(pending["otp_hash"], _otp_hash(pending["identifier"], payload.otp)):
        await raw_db.pending_signups.update_one({"id": payload.verification_id}, {"$inc": {"attempts": 1}})
        raise HTTPException(status_code=400, detail="Invalid OTP")

    signup = pending["payload"]
    method = pending["method"]
    tenant_id = f"shop_{uuid.uuid4().hex}"
    identifier = pending["identifier"]
    email = identifier if method == "email" else f"mobile-{hashlib.sha256(identifier.encode()).hexdigest()[:20]}@mobile.pharmacy.invalid"
    if await raw_db.users.find_one({"$or": [{"email": email}, {"mobile": identifier}]}):
        raise HTTPException(status_code=400, detail="Account already registered")
    user = {
        "id": str(uuid.uuid4()), "email": email, "mobile": signup.get("mobile"),
        "password_hash": pending["password_hash"], "name": signup["owner_name"], "role": "admin",
        "tenant_id": tenant_id, "shop_id": tenant_id, "is_demo": False, "active": True, "verified_at": now.isoformat(),
        "created_at": now.isoformat(), "password_changed_at": now.isoformat(),
    }
    pharmacy = {
        "key": "main", "business_name": signup["pharmacy_name"], "owner_name": signup["owner_name"],
        "business_gstin": signup.get("gst", ""), "business_phone": signup["contact"],
        "business_address": signup["address"], "state": signup["state"], "pincode": signup["pincode"],
        "tenant_id": tenant_id, "shop_id": tenant_id, "created_at": now.isoformat(),
    }
    await raw_db.users.insert_one(user)
    await raw_db.settings.insert_one(pharmacy)
    await raw_db.pending_signups.delete_one({"id": payload.verification_id})
    return {"id": user["id"], "email": user["email"], "mobile": user.get("mobile"), "name": user["name"], "role": user["role"], "tenant_id": tenant_id, "shop_id": tenant_id, "verified": True}


@api_router.post("/auth/register")
async def register(payload: UserRegister):
    return await _create_user(payload, "cashier")


def _login_response(user: dict, response: Response) -> dict:
    user = _canonicalize_user_tenant(user)
    token = create_access_token(
        user["id"], user.get("email", user.get("mobile", "")), user["role"],
        tenant_id=user["tenant_id"], is_demo=bool(user.get("is_demo")),
    )
    response.set_cookie("access_token", token, httponly=True, samesite="lax", secure=ENVIRONMENT in {"production", "prod"}, max_age=43200, path="/")
    return {
        "id": user["id"], "email": user.get("email"), "mobile": user.get("mobile"), "name": user["name"], "role": user["role"],
        "tenant_id": user["tenant_id"], "shop_id": user.get("shop_id", user["tenant_id"]), "is_demo": bool(user.get("is_demo")),
        "password_expired": _password_expired(user), "token": token,
    }


@api_router.post("/auth/login")
async def login(payload: UserLogin, response: Response):
    identifier = str(payload.email or payload.mobile or payload.identifier or "").strip().lower()
    if LOCAL_MODE:
        user = await _find_local_auth_user_by_identifier(identifier)
    else:
        user = await raw_db.users.find_one({
            "$and": [
                {"$or": [{"email": identifier}, {"mobile": identifier}]},
                {"id": {"$ne": DEMO_USER_ID}},
                {"tenant_id": {"$ne": DEMO_TENANT_ID}},
                {"is_demo": {"$ne": True}},
            ]
        })
    if not user or user.get("active", True) is False or not user.get("password_hash") or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return _login_response(user, response)


@api_router.post("/auth/demo-login")
async def demo_login(response: Response):
    now_iso = datetime.now(timezone.utc).isoformat()
    await _seed_demo_data(now_iso)
    user = await raw_db.users.find_one({"id": DEMO_USER_ID, "tenant_id": DEMO_TENANT_ID, "is_demo": True, "active": True})
    if not user:
        raise HTTPException(status_code=503, detail="Demo account is unavailable")
    return _login_response(user, response)


@api_router.post("/auth/logout")
async def logout(response: Response):
    response.delete_cookie("access_token", path="/")
    return {"ok": True}


@api_router.get("/auth/me")
async def me(user: dict = Depends(get_current_user)):
    return user


@api_router.get("/auth/users")
async def list_users(user: dict = Depends(require_role("admin"))):
    if user.get("is_demo") or user.get("tenant_id") == DEMO_TENANT_ID:
        return []
    query = {
        "tenant_id": user["tenant_id"],
        "id": {"$ne": DEMO_USER_ID},
        "is_demo": {"$ne": True},
        "$nor": [_unsafe_seeded_user_query(user["tenant_id"])],
    }
    return await raw_db.users.find(query, {"_id": 0, "password_hash": 0, "reset_otp_hash": 0}).to_list(1000)


@api_router.post("/auth/users")
async def create_user_by_admin(payload: UserCreateByAdmin, user: dict = Depends(require_role("admin"))):
    return await _create_user(payload, payload.role, user["tenant_id"])


@api_router.delete("/users/{user_id}")
async def delete_user(user_id: str, user: dict = Depends(require_role("admin"))):
    if user_id == user["id"]:
        raise HTTPException(status_code=400, detail="Cannot delete currently logged in user")

    tenant_id = user["tenant_id"]
    target = await raw_db.users.find_one({"id": user_id, "tenant_id": tenant_id})
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    if target.get("is_demo"):
        raise HTTPException(status_code=400, detail="Cannot delete protected demo user")
    if target.get("role") == "admin":
        admin_count = await raw_db.users.count_documents({"tenant_id": tenant_id, "role": "admin"})
        if admin_count <= 1:
            raise HTTPException(status_code=400, detail="Cannot delete last admin user")

    result = await raw_db.users.delete_one({"id": user_id, "tenant_id": tenant_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {"ok": True}


@api_router.post("/auth/change-password")
async def change_password(payload: ChangePasswordRequest, user: dict = Depends(get_current_user)):
    stored = await raw_db.users.find_one({"id": user["id"]})
    if not stored or not verify_password(payload.old_password, stored["password_hash"]):
        raise HTTPException(status_code=400, detail="Old password is incorrect")
    _validate_password_strength(payload.new_password)
    if verify_password(payload.new_password, stored["password_hash"]):
        raise HTTPException(status_code=422, detail="New password must be different from old password")
    changed_at = datetime.now(timezone.utc).isoformat()
    await raw_db.users.update_one({"id": user["id"]}, {"$set": {"password_hash": hash_password(payload.new_password), "password_changed_at": changed_at}, "$unset": {"reset_otp_hash": "", "reset_otp_expires_at": "", "reset_otp_attempts": ""}})
    return {"ok": True, "password_changed_at": changed_at}


@api_router.post("/auth/forgot-password")
async def forgot_password(payload: ForgotPasswordRequest, request: Request):
    email = payload.email.lower()
    now = datetime.now(timezone.utc)
    client_key = request.client.host if request.client else "unknown"
    recent = await raw_db.password_reset_requests.count_documents({"client_key": client_key, "created_at": {"$gt": now - timedelta(minutes=FORGOT_PASSWORD_WINDOW_MINUTES)}})
    if recent >= FORGOT_PASSWORD_RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Too many password reset requests; try again later")
    await raw_db.password_reset_requests.insert_one({"client_key": client_key, "created_at": now})
    user = None if _current_demo.get() else await raw_db.users.find_one({"email": email, "is_demo": {"$ne": True}})
    delivered = False
    if user:
        otp = f"{secrets.randbelow(1_000_000):06d}"
        await raw_db.users.update_one({"id": user["id"]}, {"$set": {"reset_otp_hash": _otp_hash(email, otp), "reset_otp_expires_at": now + timedelta(minutes=PASSWORD_RESET_TTL_MINUTES), "reset_otp_attempts": 0}})
        try:
            delivered = await _send_password_reset_email(email, otp)
        except Exception as exc:
            logger.warning("Password reset email delivery failed: %s", type(exc).__name__)
    result = {"message": "If the email is registered, a reset code will be sent."}
    smtp_configured = bool(os.environ.get("SMTP_HOST") and os.environ.get("SMTP_FROM"))
    result["delivery_configured"] = smtp_configured
    if not smtp_configured:
        result["todo"] = "Configure SMTP_HOST, SMTP_PORT, SMTP_FROM, and optional SMTP_USERNAME/SMTP_PASSWORD."
    return result


@api_router.post("/auth/reset-password")
async def reset_password(payload: ResetPasswordRequest):
    if _current_demo.get():
        raise HTTPException(status_code=403, detail="Demo account cannot reset passwords")
    email = payload.email.lower()
    user = await raw_db.users.find_one({"email": email, "is_demo": {"$ne": True}})
    generic_error = HTTPException(status_code=400, detail="Invalid or expired reset code")
    if not user or not user.get("reset_otp_hash") or int(user.get("reset_otp_attempts", 0)) >= PASSWORD_RESET_ATTEMPTS:
        raise generic_error
    expiry = user.get("reset_otp_expires_at")
    if isinstance(expiry, datetime) and expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    if not expiry or expiry < datetime.now(timezone.utc):
        raise generic_error
    if not hmac.compare_digest(user["reset_otp_hash"], _otp_hash(email, payload.otp)):
        await raw_db.users.update_one({"id": user["id"]}, {"$inc": {"reset_otp_attempts": 1}})
        raise generic_error
    _validate_password_strength(payload.new_password)
    changed_at = datetime.now(timezone.utc).isoformat()
    await raw_db.users.update_one({"id": user["id"]}, {"$set": {"password_hash": hash_password(payload.new_password), "password_changed_at": changed_at}, "$unset": {"reset_otp_hash": "", "reset_otp_expires_at": "", "reset_otp_attempts": ""}})
    return {"ok": True, "password_changed_at": changed_at}


@api_router.post("/settings/privacy-password")
@api_router.patch("/settings/privacy-password")
@api_router.put("/settings/privacy-password")
async def set_privacy_password(
    payload: PrivacyPasswordUpdate,
    user: dict = Depends(require_role("admin")),
):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        await db.settings.update_one(
            _privacy_settings_filter(user),
            {
                "$set": {
                    "privacy_password_hash": hash_password(payload.privacy_password),
                    "updated_at": now_iso,
                    "updated_by": user.get("id") or user.get("email"),
                },
                "$setOnInsert": {
                    "key": "privacy_password",
                    "created_at": now_iso,
                },
            },
            upsert=True,
        )
    except PyMongoError as exc:
        logger.exception("Failed to save privacy password setting")
        raise HTTPException(status_code=503, detail="Unable to save privacy password") from exc
    return {"ok": True, "privacy_password_configured": True, "updated_at": now_iso}


# ---------------- Medicines ----------------
def _medicine_identity_filter(medicine_id: str) -> dict:
    return {
        "$or": [
            {"id": medicine_id},
            {"medicine_key": medicine_id},
        ]
    }


def canonical_medicine_key(
    medicine_name: str,
    batch_no: str,
    distributor_id: Optional[str] = None,
    expiry_date: Optional[str] = None,
    pack_size: Optional[str] = None,
) -> str:
    """Return the one canonical identity for a physical medicine batch.

    Inventory reconstruction, purchase orders, invoices, purchase returns, and
    medicines must all join on this key only. Price, MRP, generated document
    ids, and legacy name-only fallbacks are intentionally excluded because they
    are mutable commercial attributes, not batch identity.
    """

    def normalize(value, *, upper: bool = False) -> str:
        normalized = str(value or "").strip()
        normalized = normalized.upper() if upper else normalized.casefold()
        return normalized or "-"

    return "::".join([
        normalize(medicine_name),
        normalize(distributor_id),
        normalize(batch_no, upper=True),
        normalize(expiry_date),
        normalize(pack_size),
    ])


def _stock_lot_key(
    medicine_name: str,
    batch_no: str,
    distributor_id: Optional[str] = None,
    distributor_name: Optional[str] = None,
    expiry_date: Optional[str] = None,
    pack_size: Optional[str] = None,
    purchase_rate=None,
    mrp=None,
) -> str:
    """Backward-compatible wrapper for the canonical medicine batch key."""
    return canonical_medicine_key(medicine_name, batch_no, distributor_id, expiry_date, pack_size)


def _threshold_response(medicine: dict) -> dict:
    return {
        "medicine_id": medicine.get("id") or medicine.get("medicine_key"),
        "low_stock_threshold": medicine.get("low_stock_threshold"),
        "threshold_locked": bool(medicine.get("threshold_locked")),
        "threshold_unlocked": bool(medicine.get("threshold_unlocked")),
        "threshold_updated_at": medicine.get("threshold_updated_at"),
        "threshold_set_by": medicine.get("threshold_set_by"),
    }


def _privacy_settings_filter(user: dict) -> dict:
    # TenantAwareCollection injects tenant_id/shop_id automatically for settings.
    # Keep this selector neutral so MongoDB upserts do not see tenant_id twice.
    return {"key": "privacy_password"}


async def _privacy_password_hash(user: dict) -> Optional[str]:
    settings = await db.settings.find_one(_privacy_settings_filter(user), {"_id": 0})
    return (settings or {}).get("privacy_password_hash")


def _fifo_expiry_key(batch: dict) -> tuple:
    """Sort valid expiry dates first and keep malformed dates deterministic."""
    expiry = parse_expiry_date(batch.get("expiry_date"))
    return (
        expiry is None,
        expiry or date.max,
        str(batch.get("id") or batch.get("medicine_key") or ""),
    )


def _compact_billing_stock_summary(batches: List[dict]) -> dict:
    """Return the live, compact medicine stock shape used by quick billing."""
    ordered = sorted(batches, key=_fifo_expiry_key)
    sellable = [batch for batch in ordered if _available_stock(batch) > 0]
    representative = (sellable or ordered)[0]
    thresholds = [
        int(batch.get("low_stock_threshold"))
        for batch in batches
        if batch.get("low_stock_threshold") is not None
    ]

    return {
        "medicine_id": representative.get("id") or representative.get("medicine_key"),
        "name": representative.get("name"),
        "available_qty": round_qty(sum(_available_stock(batch) for batch in batches)),
        "nearest_expiry": representative.get("expiry_date"),
        "low_stock_threshold": max(thresholds, default=10),
        "batch_count": len(sellable),
        "mrp": representative.get("mrp"),
        "gst": representative.get("gst_rate"),
    }


@api_router.get("/medicines/search")
async def search_medicines_for_billing(
    q: str = "",
    limit: int = 20,
    user: dict = Depends(get_current_user),
):
    """Fast autocomplete with a compact, live stock summary for billing."""
    limit = min(max(limit, 1), 100)
    term = q.strip()
    query = {}
    if term:
        prefix = f"^{re.escape(term)}"
        query = {
            "$or": [
                {"name": {"$regex": prefix, "$options": "i"}},
                {"batch_no": {"$regex": prefix, "$options": "i"}},
                {"manufacturer": {"$regex": prefix, "$options": "i"}},
                {"barcode": {"$regex": prefix, "$options": "i"}},
            ]
        }

    matches = await db.medicines.find(
        query,
        {"_id": 0, "name": 1},
    ).sort("name", 1).to_list(limit * 10)
    names = list(dict.fromkeys(row.get("name") for row in matches if row.get("name")))[:limit]
    if not names:
        return []

    batches = await db.medicines.find(
        {"name": {"$in": names}},
        {
            "_id": 0,
            "id": 1,
            "medicine_key": 1,
            "name": 1,
            "expiry_date": 1,
            "purchased_units": 1,
            "purchased_quantity": 1,
            "quantity": 1,
            "free_quantity": 1,
            "free_qty": 1,
            "free_units": 1,
            "sold_units": 1,
            "sold_quantity": 1,
            "purchase_return_units": 1,
            "stock_adjustment_units": 1,
            "purchase_return_quantity": 1,
            "returned_quantity": 1,
            "returned_units": 1,
            "low_stock_threshold": 1,
            "mrp": 1,
            "gst_rate": 1,
        },
    ).to_list(5000)

    grouped = defaultdict(list)
    for batch in batches:
        grouped[batch.get("name")].append(batch)

    return [
        _compact_billing_stock_summary(grouped[name])
        for name in names
        if grouped[name]
    ]


@api_router.get("/medicines")
async def list_medicines(
    search: Optional[str] = None,
    category: Optional[str] = None,
    distributor: Optional[str] = None,
    manufacturer: Optional[str] = None,
    batch_no: Optional[str] = None,
    sort_by: str = "name",
    user: dict = Depends(get_current_user),
):

    from datetime import datetime, timezone

    today = datetime.now(
        timezone.utc
    ).date()

    q = {}

    if search:
        search_pattern = re.escape(search.strip())
        q["$or"] = [
            {field: {"$regex": search_pattern, "$options": "i"}}
            for field in ("name", "batch_no", "manufacturer", "barcode")
        ]
        q["$or"].append({"category": {"$regex": search_pattern, "$options": "i"}})

    if category:
        q["category"] = category

    if distributor:
        q["distributor"] = distributor

    if manufacturer:
        q["manufacturer"] = manufacturer

    if batch_no:
        q["batch_no"] = batch_no

    items = await db.medicines.find(
        q,
        {"_id": 0}
    ).to_list(5000)

    def normalized_medicine_key(name, batch_no, distributor_id=None):
        if not name or not batch_no:
            return None

        return canonical_medicine_key(
            name,
            batch_no,
            distributor_id,
        )

    medicine_lookup_keys = set()

    for m in items:

        medicine_key = m.get("medicine_key")

        if medicine_key:

            medicine_lookup_keys.add(medicine_key)

        normalized_key = normalized_medicine_key(
            m.get("name"),
            m.get("batch_no")
        )

        if normalized_key:

            medicine_lookup_keys.add(normalized_key)

    purchase_order_distributors = {}

    if medicine_lookup_keys:

        purchase_orders = await db.purchase_orders.find(
            {},
            {
                "_id": 0,
                "distributor_id": 1,
                "distributor_name": 1,
                "items.name": 1,
                "items.batch_no": 1,
                "items.medicine_key": 1,
            }
        ).to_list(10000)

        for po in purchase_orders:

            po_distributor_name = po.get("distributor_name")

            if not po.get("distributor_id") and not po_distributor_name:

                continue

            distributor_details = {
                "distributor_id": po.get("distributor_id"),
                "distributor_name": po_distributor_name,
                "distributor": po_distributor_name,
            }

            for item in po.get("items", []):

                item_keys = []

                item_medicine_key = item.get("medicine_key")

                if item_medicine_key:

                    item_keys.append(item_medicine_key)

                normalized_item_key = normalized_medicine_key(
                    item.get("name"),
                    item.get("batch_no"),
                    po.get("distributor_id"),
                )

                if normalized_item_key:

                    item_keys.append(normalized_item_key)

                for item_key in item_keys:

                    if item_key not in medicine_lookup_keys:

                        continue

                    if item_key in purchase_order_distributors:

                        continue

                    purchase_order_distributors[item_key] = distributor_details

    distributor_ids = {
        m.get("distributor_id")
        for m in items
        if m.get("distributor_id")
    }

    distributor_names = {}

    if distributor_ids:

        distributor_rows = await db.distributors.find(
            {"id": {"$in": list(distributor_ids)}},
            {"_id": 0, "id": 1, "name": 1}
        ).to_list(5000)

        distributor_names = {
            d.get("id"): d.get("name")
            for d in distributor_rows
            if d.get("id")
        }

    grouped = {}

    for m in items:

        purchased = _purchased_stock(m)
        sold = _stock_quantity(m, "sold_units", "sold_quantity")
        returned = _purchase_return_stock(m)
        qty = _available_stock(m)
        return_status = _return_status(m)

        m["quantity_units"] = qty
        m["available_stock"] = qty
        m["return_status"] = return_status
        m["status"] = return_status
        m["stock_status"] = _inventory_stock_status(m, today)
        m["low_stock_status"] = _low_stock_status(m)
        m.update(_threshold_response(m))
        m.update(_inventory_category(m.get("category")))

        # EXPIRY WARNING SYSTEM

        expiry_info = expiry_details(
            m.get("expiry_date"),
            today
        )

        expiry_status = expiry_info["expiry_status"]
        days_to_expiry = expiry_info["days_to_expiry"]
        days_expired = expiry_info["days_expired"]
        expired_days_ago = expiry_info["expired_days_ago"]

        name = (
            m.get("name") or ""
        ).strip().upper()

        if name not in grouped:

            grouped[name] = {

                "id":
                    m.get("id") or m.get("medicine_key"),

                "medicine_id":
                    m.get("id"),

                "medicine_key":
                    m.get("medicine_key"),

                "low_stock_threshold":
                    m.get("low_stock_threshold"),

                "low_stock_status":
                    _low_stock_status(m),

                "threshold_locked":
                    bool(m.get("threshold_locked")),

                "threshold_unlocked":
                    bool(m.get("threshold_unlocked")),

                "threshold_updated_at":
                    m.get("threshold_updated_at"),

                "threshold_set_by":
                    m.get("threshold_set_by"),

                "sold_units":
                    0,

                "purchased_units":
                    0,

                "purchase_return_units":
                    0,

                "stock_adjustment_units":
                    0,

                "return_status":
                    return_status,

                "status":
                    return_status,

                "name":
                    m.get("name"),

                "manufacturer":
                    m.get("manufacturer"),

                "category":
                    m.get("category"),

                "category_code":
                    m.get("category_code"),

                "category_status":
                    m.get("category_status"),

                "mrp":
                    m.get("mrp"),

                "purchase_price":
                    m.get("purchase_price"),

                "gst_rate":
                    m.get("gst_rate"),

                "actual_cost":
                  round(
                    float(m.get("purchase_price") or 0)
                    * (1 + float(m.get("gst_rate") or 0) / 100),
                    2
                  ),

                "total_stock":
                   0,

                "expiry_status":
                    "safe",

                "batches":
                    [],
            }

        grouped[name]["total_stock"] += qty
        grouped[name]["purchase_return_units"] += returned
        grouped[name]["stock_adjustment_units"] += _stock_adjustment_stock(m)
        grouped[name]["return_status"] = _return_status({
            "purchased_units": grouped[name].get("purchased_units", 0) + purchased,
            "stock_adjustment_units": grouped[name]["stock_adjustment_units"],
            "sold_units": grouped[name].get("sold_units", 0) + sold,
            "purchase_return_units": grouped[name]["purchase_return_units"],
        })
        grouped[name]["status"] = grouped[name]["return_status"]
        grouped[name]["purchased_units"] = grouped[name].get("purchased_units", 0) + purchased
        grouped[name]["sold_units"] = grouped[name].get("sold_units", 0) + sold

        # UPGRADE GROUP WARNING LEVEL

        current = grouped[name][
            "expiry_status"
        ]

        priority = {
            "safe": 0,
            "warning": 1,
            "expired": 2,
        }

        if (
            priority[expiry_status]
            >
            priority[current]
        ):

            grouped[name][
                "expiry_status"
            ] = expiry_status

        distributor_id = m.get("distributor_id")

        distributor_name = (
            m.get("distributor_name")
            or m.get("distributor")
            or distributor_names.get(distributor_id)
        )

        distributor_value = (
            m.get("distributor")
            or m.get("distributor_name")
            or distributor_names.get(distributor_id)
        )

        normalized_key = normalized_medicine_key(
            m.get("name"),
            m.get("batch_no")
        )

        po_distributor = (
            purchase_order_distributors.get(m.get("medicine_key"))
            or purchase_order_distributors.get(normalized_key)
            or {}
        )

        if po_distributor:

            distributor_id = (
                distributor_id
                or po_distributor.get("distributor_id")
            )

            distributor_name = (
                distributor_name
                or po_distributor.get("distributor_name")
            )

            distributor_value = (
                distributor_value
                or po_distributor.get("distributor")
            )

        grouped[name]["batches"].append({

            "id":
                m.get("id") or m.get("medicine_key"),

            "medicine_id":
                m.get("id"),

            "medicine_key":
                m.get("medicine_key"),

            "low_stock_threshold":
                m.get("low_stock_threshold"),

            "low_stock_status":
                _low_stock_status(m),

            "threshold_locked":
                bool(m.get("threshold_locked")),

            "threshold_unlocked":
                bool(m.get("threshold_unlocked")),

            "threshold_updated_at":
                m.get("threshold_updated_at"),

            "threshold_set_by":
                m.get("threshold_set_by"),

            "batch_no":
                m.get("batch_no"),

            "expiry_date":
                m.get("expiry_date"),

            "expiry_status":
                expiry_status,

            "days_to_expiry":
                days_to_expiry,

            "days_expired":
                days_expired,

            "expired_days_ago":
                expired_days_ago,

            "quantity_units":
                qty,

            "available_stock":
                qty,

            "purchased_units":
                purchased,

            "sold_units":
                sold,

            "purchase_return_units":
                returned,

            "stock_adjustment_units":
                _stock_adjustment_stock(m),

            "return_status":
                return_status,

            "status":
                return_status,

            "stock_status":
                m.get("stock_status"),

            "category":
                m.get("category"),

            "category_code":
                m.get("category_code"),

            "category_status":
                m.get("category_status"),

            "pack_size":
                m.get("pack_size"),

            "purchase_price":
                m.get("purchase_price"),

            "gst_rate":
                m.get("gst_rate"),

            "actual_cost":
              round(
                float(m.get("purchase_price") or 0)
                * (1 + float(m.get("gst_rate") or 0) / 100),
                2
              ),

            "mrp":
              m.get("mrp"),

            "distributor_id":
                distributor_id,

            "distributor_name":
                distributor_name,

            "distributor":
                distributor_value,
        })

    result = list(
        grouped.values()
    )

    for item in result:
        merged_batches = {}
        for batch in item["batches"]:
            lot_key = _stock_lot_key(
                batch.get("name") or item.get("name"),
                batch.get("batch_no"),
                batch.get("distributor_id"),
                batch.get("distributor_name") or batch.get("distributor"),
                batch.get("expiry_date"),
                batch.get("pack_size"),
                batch.get("purchase_price"),
                batch.get("mrp"),
            )
            if lot_key not in merged_batches:
                merged_batches[lot_key] = dict(batch)
                continue
            merged = merged_batches[lot_key]
            for field in (
                "quantity_units",
                "available_stock",
                "purchased_units",
                "sold_units",
                "purchase_return_units",
                "stock_adjustment_units",
            ):
                merged[field] = round_qty(
                    _safe_float(merged.get(field)) + _safe_float(batch.get(field))
                )
            merged["return_status"] = _return_status(merged)
            merged["status"] = merged["return_status"]
        item["batches"] = list(merged_batches.values())
        distributors = {}
        for batch in item["batches"]:
            distributor_key = str(batch.get("distributor_id") or batch.get("distributor_name") or batch.get("distributor") or "").strip().casefold()
            if distributor_key not in distributors:
                distributors[distributor_key] = {
                    "distributor_id": batch.get("distributor_id"),
                    "distributor_name": batch.get("distributor_name"),
                    "distributor": batch.get("distributor"),
                    "total_stock": 0,
                    "available_stock": 0,
                    "quantity_units": 0,
                    "batches": [],
                }
            distributors[distributor_key]["total_stock"] = round_qty(distributors[distributor_key]["total_stock"] + _safe_float(batch.get("available_stock")))
            distributors[distributor_key]["available_stock"] = distributors[distributor_key]["total_stock"]
            distributors[distributor_key]["quantity_units"] = distributors[distributor_key]["total_stock"]
            distributors[distributor_key]["batches"].append(batch)
        item["distributors"] = list(distributors.values())
        aggregate_stock = {
            **item,
            "purchased_units": item.get("total_stock"),
            "sold_units": 0,
            "purchase_return_units": 0,
            "stock_adjustment_units": 0,
        }
        item["stock_status"] = _inventory_stock_status(aggregate_stock, today)
        if item["total_stock"] > 0 and any(
            batch.get("stock_status") == "expired" and batch.get("available_stock", 0) > 0
            for batch in item["batches"]
        ):
            item["stock_status"] = "expired"
        item["current_stock"] = item["total_stock"]
        item["available_qty"] = item["total_stock"]
        
        actual_cost = round(
          float(item.get("purchase_price") or 0) *
          (1 + float(item.get("gst_rate") or 0) / 100),
          2
        )

        item["cost_value"] = round(
           actual_cost * item["total_stock"],
           2
        )
        item["mrp_value"] = round(float(item.get("mrp") or 0) * item["total_stock"], 2)

    result.sort(
        key=lambda x: (
            x.get(sort_by)
            or ""
        )
    )

    return _normalize_inventory_quantities(result)


@api_router.put("/medicines/{medicine_id}/low-stock-status")
async def update_low_stock_status(
    medicine_id: str,
    payload: LowStockStatusUpdate,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    """Persist workflow state without changing any stock quantity fields."""
    medicine_filter = _medicine_identity_filter(medicine_id)
    result = await db.medicines.update_one(
        medicine_filter,
        {"$set": {"low_stock_status": payload.status}},
    )
    if not result.matched_count:
        raise HTTPException(status_code=404, detail="Medicine not found")

    return {
        "medicine_id": medicine_id,
        "low_stock_status": payload.status,
        "status": payload.status,
    }


@api_router.put("/medicines/{medicine_id}/threshold")
@api_router.patch("/inventory/{medicine_id}/low-stock-threshold")
async def update_low_stock_threshold(
    medicine_id: str,
    payload: LowStockThresholdUpdate,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    medicine_filter = _medicine_identity_filter(medicine_id)
    existing = await db.medicines.find_one(medicine_filter, {"_id": 0})
    if not existing:
        raise HTTPException(404, "Medicine not found")

    locked = bool(existing.get("threshold_locked"))
    unlocked = bool(existing.get("threshold_unlocked"))
    if locked and not unlocked:
        raise HTTPException(status_code=403, detail="Low stock threshold is locked; admin privacy unlock required")
    if locked and unlocked and user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only admins can edit unlocked low stock thresholds")

    now_iso = datetime.now(timezone.utc).isoformat()
    result = await db.medicines.update_one(
        medicine_filter,
        {
            "$set": {
                "low_stock_threshold": int(payload.threshold),
                "threshold_locked": True,
                "threshold_unlocked": False,
                "threshold_updated_at": now_iso,
                "threshold_set_by": user.get("id") or user.get("email"),
            }
        },
    )
    if result.matched_count == 0:
        raise HTTPException(404, "Medicine not found")

    updated = await db.medicines.find_one(medicine_filter, {"_id": 0})
    return {"message": "threshold updated", **_threshold_response(updated)}


async def update_threshold(
    medicine_id: str,
    payload: dict,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    threshold = payload.get("threshold", payload.get("low_stock_threshold"))
    return await update_low_stock_threshold(
        medicine_id,
        LowStockThresholdUpdate(threshold=threshold),
        user=user,
    )


@api_router.post("/inventory/{medicine_id}/low-stock-threshold/unlock")
async def unlock_low_stock_threshold(
    medicine_id: str,
    payload: LowStockThresholdUnlock,
    user: dict = Depends(require_role("admin")),
):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    stored_hash = await _privacy_password_hash(user)
    if not stored_hash:
        raise HTTPException(status_code=400, detail="Privacy password is not configured")
    if not verify_password(payload.privacy_password, stored_hash):
        raise HTTPException(status_code=403, detail="Invalid privacy password")

    medicine_filter = _medicine_identity_filter(medicine_id)
    result = await db.medicines.update_one(
        medicine_filter,
        {"$set": {"threshold_unlocked": True}},
    )
    if result.matched_count == 0:
        raise HTTPException(404, "Medicine not found")

    updated = await db.medicines.find_one(medicine_filter, {"_id": 0})
    return {"message": "threshold unlocked", **_threshold_response(updated)}

def _manual_sold_capacity(batch: dict) -> float:
    purchased = max(0.0, _purchased_stock(batch) + _stock_adjustment_stock(batch))
    returned = max(0.0, _purchase_return_stock(batch))
    return round_qty(max(0.0, purchased - returned))


def _manual_sold_allocations(batches: List[dict], requested_sold: float) -> List[Tuple[dict, float]]:
    """Return a canonical FIFO sold allocation for one medicine's batches.

    Rebuilding the complete allocation (rather than applying the delta to one
    batch) also makes reductions naturally unwind from the newest FIFO batches
    first. Purchase-returned units are excluded from each batch's sellable
    capacity.
    """
    if not math.isfinite(requested_sold) or requested_sold < 0:
        raise HTTPException(status_code=400, detail="Sold quantity must be a non-negative number")

    def fifo_key(batch: dict):
        expiry = parse_expiry_date(batch.get("expiry_date"))
        return (
            expiry is None,
            expiry or date.max,
            str(batch.get("created_at") or ""),
            str(batch.get("id") or batch.get("medicine_key") or ""),
        )

    ordered = sorted(batches, key=fifo_key)
    requested_sold = round_qty(requested_sold)
    total_capacity = round_qty(sum(_manual_sold_capacity(batch) for batch in ordered))
    if requested_sold > total_capacity + 1e-9:
        raise HTTPException(
            status_code=400,
            detail=f"Sold quantity cannot exceed sellable stock ({total_capacity:g})",
        )

    remaining = requested_sold
    allocations = []
    for batch in ordered:
        capacity = _manual_sold_capacity(batch)
        sold = round_qty(min(capacity, remaining))
        allocations.append((batch, sold))
        remaining = round_qty(max(0.0, remaining - sold))

    return allocations


def _manual_sold_derivatives(batch: dict, sold_units: float, today: date) -> dict:
    sold_units = round_qty(sold_units)
    refreshed = {**batch, "sold_units": sold_units}
    available = _available_stock(refreshed)
    status = _return_status(refreshed)
    return {
        "sold_units": sold_units,
        "available_stock": available,
        "quantity_units": available,
        "return_status": status,
        "status": status,
        **expiry_details(refreshed.get("expiry_date"), today),
    }


async def _set_manual_sold_allocations(allocations: List[Tuple[dict, float]], session=None) -> List[dict]:
    today = datetime.now(timezone.utc).date()
    updated_batches = []
    for batch, sold_units in allocations:
        derivatives = _manual_sold_derivatives(batch, sold_units, today)
        await db.medicines.update_one(
            _medicine_identity_filter(batch.get("id") or batch.get("medicine_key")),
            {"$set": derivatives},
            session=session,
        )
        updated_batches.append({**batch, **derivatives})
    return updated_batches


@api_router.put("/medicines/{medicine_id}/sold")
async def update_sold_units(
    medicine_id: str,
    payload: dict,
    user: dict = Depends(require_role("admin", "pharmacist"))
):
    medicine = await db.medicines.find_one(_medicine_identity_filter(medicine_id), {"_id": 0})
    if not medicine:
        raise HTTPException(404, "Medicine not found")

    try:
        requested_sold = float(payload["sold_units"])
    except (KeyError, TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Sold quantity must be a non-negative number")

    batches = await db.medicines.find(
        {"name": medicine.get("name")},
        {"_id": 0},
    ).to_list(5000)
    allocations = _manual_sold_allocations(batches, requested_sold)

    async def transaction_operation(session):
        return await _set_manual_sold_allocations(allocations, session=session)

    async def fallback_operation():
        try:
            return await _set_manual_sold_allocations(allocations)
        except Exception:
            original_allocations = [
                (batch, _stock_quantity(batch, "sold_units", "sold_quantity"))
                for batch, _ in allocations
            ]
            await _set_manual_sold_allocations(original_allocations)
            raise

    updated_batches = await _run_with_transaction(transaction_operation, fallback_operation)

    requested_sold = round_qty(requested_sold)
    total_available = round_qty(sum(_available_stock(batch) for batch in updated_batches))
    total_purchased = round_qty(sum(_purchased_stock(batch) for batch in updated_batches))
    total_returned = round_qty(sum(_purchase_return_stock(batch) for batch in updated_batches))
    aggregate_status = _return_status({
        "purchased_units": total_purchased,
        "sold_units": requested_sold,
        "purchase_return_units": total_returned,
    })

    return _normalize_inventory_quantities({
        "message": "sold qty updated",
        "sold_units": requested_sold,
        "available_stock": total_available,
        "quantity_units": total_available,
        "return_status": aggregate_status,
        "status": aggregate_status,
        "batches": updated_batches,
    })


def _stock_adjustment_derivatives(medicine: dict, adjustment_units: float, today: date) -> dict:
    adjustment_units = round_qty(adjustment_units)
    refreshed = {**medicine, "stock_adjustment_units": adjustment_units}
    available = _available_stock(refreshed)
    status = _return_status(refreshed)
    threshold = refreshed.get("low_stock_threshold")
    is_low_stock = threshold is not None and available <= float(threshold)
    return {
        "stock_adjustment_units": adjustment_units,
        "available_stock": available,
        "quantity_units": available,
        "is_low_stock": is_low_stock,
        "low_stock": is_low_stock,
        "return_status": status,
        "status": status,
        **expiry_details(refreshed.get("expiry_date"), today),
    }


async def _apply_stock_adjustment_delta(medicine: dict, delta: float, session=None) -> dict:
    delta = round_qty(delta)
    available = _available_stock(medicine)
    if delta < 0 and abs(delta) > available + 1e-9:
        raise HTTPException(
            status_code=400,
            detail=f"Adjustment cannot reduce stock below zero; available stock is {available:g}",
        )

    current_adjustment = _stock_adjustment_stock(medicine)
    derivatives = _stock_adjustment_derivatives(
        medicine,
        round_qty(current_adjustment + delta),
        datetime.now(timezone.utc).date(),
    )
    query = _medicine_identity_filter(medicine.get("id") or medicine.get("medicine_key"))
    query["$expr"] = {
        "$and": [
            {"$eq": [_mongo_first_stock_value("stock_adjustment_units"), current_adjustment]},
            {"$eq": [_mongo_available_stock_expression(), available]},
        ]
    }
    result = await db.medicines.update_one(query, {"$set": derivatives}, session=session)
    if not result or result.modified_count != 1:
        raise HTTPException(status_code=409, detail="Medicine stock changed; retry the adjustment")
    return {**medicine, **derivatives}


def _stock_adjustment_public(adjustment: dict) -> dict:
    return {key: value for key, value in adjustment.items() if key not in {"_id", "tenant_id", "shop_id"}}


def _stock_adjustment_query(
    start: Optional[str] = None,
    end: Optional[str] = None,
    adjustment_type: Optional[str] = None,
    medicine_id: Optional[str] = None,
    search: Optional[str] = None,
) -> dict:
    query = {}
    if start or end:
        query["adjustment_date"] = {}
        if start:
            query["adjustment_date"]["$gte"] = start
        if end:
            query["adjustment_date"]["$lte"] = end
    if adjustment_type:
        query["adjustment_type"] = adjustment_type
    if medicine_id:
        query["medicine_id"] = medicine_id
    if search:
        pattern = re.escape(search.strip())
        query["$or"] = [
            {field: {"$regex": pattern, "$options": "i"}}
            for field in ("medicine_name", "batch_no", "reference_number", "notes")
        ]
    return query


@api_router.post("/stock-adjustments")
async def create_stock_adjustment(
    payload: StockAdjustmentCreate,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    medicine = await db.medicines.find_one(_medicine_identity_filter(payload.medicine_id), {"_id": 0})
    if not medicine:
        raise HTTPException(status_code=404, detail="Medicine batch not found")
    if payload.medicine_name and payload.medicine_name.casefold() != str(medicine.get("name") or "").casefold():
        raise HTTPException(status_code=400, detail="medicine_name does not match the selected batch")
    if payload.batch_no and payload.batch_no.casefold() != str(medicine.get("batch_no") or "").casefold():
        raise HTTPException(status_code=400, detail="batch_no does not match the selected batch")

    current_stock = _available_stock(medicine)
    adjustment_quantity = round_qty(payload.quantity)
    adjustment = {
        "id": str(uuid.uuid4()),
        "adjustment_date": payload.adjustment_date,
        "medicine_id": medicine.get("id") or payload.medicine_id,
        "medicine_name": medicine.get("name") or payload.medicine_name or "",
        "batch_no": medicine.get("batch_no") or payload.batch_no or "",
        "adjustment_type": payload.adjustment_type,
        # Quantity is intentionally signed: positive adds stock, negative
        # removes stock.
        "quantity": adjustment_quantity,
        "notes": payload.notes,
        "reference_number": payload.reference_number,
        "created_by": user.get("name") or user.get("email") or user.get("id") or "Unknown",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    async def write(session=None):
        await _apply_stock_adjustment_delta(medicine, adjustment["quantity"], session=session)
        await db.stock_adjustments.insert_one(adjustment, session=session)
        return {
            **_stock_adjustment_public(adjustment),
            "current_stock": current_stock,
            "adjustment_quantity": adjustment_quantity,
            "resulting_stock": round_qty(current_stock + adjustment_quantity),
        }

    async def transaction_operation(session):
        return await write(session=session)

    async def fallback_operation():
        try:
            return await write()
        except Exception:
            refreshed = await db.medicines.find_one(_medicine_identity_filter(payload.medicine_id), {"_id": 0})
            if refreshed and _stock_adjustment_stock(refreshed) == round_qty(
                _stock_adjustment_stock(medicine) + adjustment["quantity"]
            ):
                await _apply_stock_adjustment_delta(refreshed, -adjustment["quantity"])
            raise

    return await _run_with_transaction(transaction_operation, fallback_operation)


@api_router.get("/stock-adjustments/summary")
async def stock_adjustment_summary(
    start: Optional[str] = None,
    end: Optional[str] = None,
    adjustment_type: Optional[str] = None,
    medicine_id: Optional[str] = None,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    items = await db.stock_adjustments.find(
        _stock_adjustment_query(start, end, adjustment_type, medicine_id),
        {"_id": 0, "adjustment_type": 1, "quantity": 1},
    ).to_list(100000)
    by_type = defaultdict(lambda: {"count": 0, "net_quantity": 0.0})
    positive = negative = 0.0
    for item in items:
        quantity = round_qty(item.get("quantity"))
        bucket = by_type[item.get("adjustment_type") or "Other"]
        bucket["count"] += 1
        bucket["net_quantity"] = round_qty(bucket["net_quantity"] + quantity)
        if quantity > 0:
            positive = round_qty(positive + quantity)
        else:
            negative = round_qty(negative + quantity)
    return {
        "total_adjustments": len(items),
        "net_quantity": round_qty(positive + negative),
        "positive_quantity": positive,
        "negative_quantity": negative,
        "by_type": dict(by_type),
    }


@api_router.get("/stock-adjustments")
async def list_stock_adjustments(
    start: Optional[str] = None,
    end: Optional[str] = None,
    adjustment_type: Optional[str] = None,
    medicine_id: Optional[str] = None,
    search: Optional[str] = None,
    page: int = 1,
    page_size: int = 25,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    page = max(page, 1)
    page_size = min(max(page_size, 1), 100)
    query = _stock_adjustment_query(start, end, adjustment_type, medicine_id, search)
    total = await db.stock_adjustments.count_documents(query)
    items = await db.stock_adjustments.find(query, {"_id": 0}).sort(
        [("adjustment_date", -1), ("created_at", -1)]
    ).skip((page - 1) * page_size).limit(page_size).to_list(page_size)
    return {
        "items": [_stock_adjustment_public(item) for item in items],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@api_router.post("/historical-sales")
async def create_historical_sale(
    payload: HistoricalSaleCreate,
    user: dict = Depends(require_role("admin", "pharmacist"))
):
    data = payload.model_dump()

    total = (
        float(data.get("cash_amount") or 0)
        + float(data.get("upi_amount") or 0)
        + float(data.get("pending_amount") or 0)
    )

    sale = {
        "id": str(uuid.uuid4()),
        "date": data["date"],
        "cash_amount": data["cash_amount"],
        "upi_amount": data["upi_amount"],
        "pending_amount": data["pending_amount"],
        "total_amount": total,
        "notes": data.get("notes", ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    await db.historical_sales.insert_one(sale)

    return sale


@api_router.post("/expenses")
async def create_expense(
    payload: ExpenseCreate,
    user: dict = Depends(require_role("admin", "pharmacist"))
):
    data = payload.model_dump()

    expense = {
        "id": str(uuid.uuid4()),
        "date": data["date"],
        "category": data["category"],
        "amount": data["amount"],
        "notes": data.get("notes", ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    await db.expenses.insert_one(expense)

    return expense


@api_router.get("/s/monthly-summary")
async def monthly_summary(
    month: str,
    user: dict = Depends(get_current_user)
):
    # month format: YYYY-MM

    live_sales = await db.daily_sales.find(
        {"sale_date": {"$regex": f"^{month}"}},
        {"_id": 0}
    ).to_list(5000)

    historical_sales = await db.historical_sales.find(
        {"date": {"$regex": f"^{month}"}},
        {"_id": 0}
    ).to_list(5000)

    expenses = await db.expenses.find(
        {"date": {"$regex": f"^{month}"}},
        {"_id": 0}
    ).to_list(5000)

    live_total = sum(
        s.get("total_amount", 0)
        for s in live_sales
    )

    historical_total = sum(
        s.get("total_amount", 0)
        for s in historical_sales
    )

    sales_total = live_total + historical_total

    expense_total = sum(
        e.get("amount", 0)
        for e in expenses
    )

    estimated_profit = sales_total - expense_total

    return {
        "month": month,
        "sales": round(sales_total, 2),
        "expenses": round(expense_total, 2),
        "estimated_profit": round(estimated_profit, 2),
        "live_sales_count": len(live_sales),
        "historical_sales_count": len(historical_sales),
        "expense_count": len(expenses),
    }

    
@api_router.post("/medicines")
async def create_medicine(
    payload: MedicineCreate,
    user: dict = Depends(require_role("admin", "pharmacist"))
):
    data = payload.model_dump()
    data.pop("auto_ledger", None)

    med = Medicine(
        **data,
        purchased_units=round_qty(data.get("purchased_units", 0)),
    )

    medicine = _normalize_inventory_quantities(med.model_dump())
    await db.medicines.insert_one(medicine)
    return medicine

@api_router.get("/medicines/lookup/{barcode}")
async def lookup_barcode(barcode: str, user: dict = Depends(get_current_user)):
    med = await db.medicines.find_one({"barcode": barcode}, {"_id": 0})
    if not med:
        raise HTTPException(status_code=404, detail="Not found")
    return _normalize_inventory_quantities(med)


@api_router.post("/patients")
async def add_patient(
    payload: RegularPatient,
    user: dict = Depends(get_current_user)
):
    data = await _link_patient_medicine(payload.model_dump())
    data["phone"] = normalize_phone_number(data.get("phone"))
    await db.regular_patients.insert_one(data)
    return {"success": True}

@api_router.post("/patients/{phone}/refill")
async def refill_patient(
    phone: str,
    payload: dict,
    user: dict = Depends(get_current_user)
):
    phone = phone.strip()
    if not phone:
        raise HTTPException(status_code=400, detail="Patient phone is required")

    date = payload.get("date")
    medicines = payload.get("medicines", [])

    if not date:
        raise HTTPException(status_code=400, detail="Refill date is required")

    patient = await db.regular_patients.find_one({"phone": phone})
    if not patient:
        raise HTTPException(status_code=404, detail="Patient not found")

    update_data = {
        "last_refill_date": date,
        "last_refill_medicines": medicines
    }

    # OPTIONAL: keep history
    await db.regular_patients.update_one(
        {"phone": phone},
        {
            "$set": update_data,
            "$push": {
                "refill_history": {
                    "date": date,
                    "medicines": medicines,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            }
        }
    )

    return {
        "success": True,
        "message": "Refill updated"
    }

@api_router.put("/patients/{phone}")
async def update_patient(
    phone: str,
    payload: RegularPatient,
    user: dict = Depends(get_current_user)
):
    phone = phone.strip()
    if not phone:
        raise HTTPException(status_code=400, detail="Patient phone is required")

    data = await _link_patient_medicine(payload.model_dump())
    data["phone"] = normalize_phone_number(data.get("phone"))
    result = await db.regular_patients.update_one(
        {"phone": phone},
        {
            "$set": data
        }
    )

    if result.matched_count == 0:
        raise HTTPException(
            status_code=404,
            detail="Patient not found"
        )

    return {
        "success": True
    }

def _has_patient_identity(patient: dict) -> bool:
    has_name = bool(str(patient.get("name") or "").strip())
    has_phone = bool(str(patient.get("phone") or "").strip())
    return has_name and has_phone


@api_router.get("/patients")
async def list_patients(user: dict = Depends(get_current_user)):
    items = await db.regular_patients.find({}, {"_id": 0}).to_list(2000)
    return [patient for patient in items if _has_patient_identity(patient)]


@api_router.get("/patients/alerts")
async def patient_alerts(user: dict = Depends(get_current_user)):
    return await _get_patient_alerts()


@api_router.get("/patients/stock-alerts")
async def patient_stock_alerts(user: dict = Depends(get_current_user)):
    return await _get_patient_stock_alerts()


@api_router.delete("/patients")
async def delete_invalid_patient(
    user: dict = Depends(get_current_user)
):
    result = await db.regular_patients.delete_many(_patient_invalid_filter())

    return {
        "success": True,
        "deleted_count": result.deleted_count,
    }


@api_router.delete("/patients/{phone}")
async def delete_patient(
    phone: str,
    user: dict = Depends(get_current_user)
):
    phone = phone.strip()
    if not phone:
        raise HTTPException(status_code=400, detail="Patient phone is required")

    result = await db.regular_patients.delete_one(
        {"phone": phone}
    )

    if result.deleted_count == 0:
        raise HTTPException(
            status_code=404,
            detail="Patient not found"
        )

    return {
        "success": True
    }

from datetime import datetime, timezone

@api_router.post("/patients/contacted/{phone}")
async def mark_contacted(
    phone: str,
    user: dict = Depends(get_current_user)
):
    await db.regular_patients.update_one(
        {"phone": phone},
        {
            "$set": {
                "last_contacted": datetime.now(timezone.utc).isoformat()
            }
        }
    )
    return {"success": True}


@api_router.put("/medicines/{med_id}")
async def update_medicine(
    med_id: str,
    payload: MedicineCreate,
    user: dict = Depends(require_role("admin", "pharmacist"))
):
    data = payload.model_dump()
    data.pop("auto_ledger", None)
    data["purchased_units"] = round_qty(data.get("purchased_units", 0))

    res = await db.medicines.update_one(
        {"id": med_id},
        {"$set": data}
    )

    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail="Medicine not found")

    return _normalize_inventory_quantities(await db.medicines.find_one({"id": med_id}, {"_id": 0}))


@api_router.delete("/medicines/{med_id}")
async def delete_medicine(med_id: str, user: dict = Depends(require_role("admin"))):
    result = await db.medicines.delete_one(_medicine_identity_filter(med_id))

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Medicine not found")

    return {"ok": True}


# ---------------- Distributors ----------------
@api_router.get("/distributors")
async def list_distributors(
    search: Optional[str] = None,
    status: Optional[Literal["active", "inactive", "return_heavy"]] = None,
    user: dict = Depends(get_current_user),
):
    distributors = await db.distributors.find({}, {"_id": 0}).sort("name", 1).to_list(1000)
    distributor_ids = [d.get("id") for d in distributors if d.get("id")]
    transactions_by_distributor = defaultdict(list)

    if distributor_ids:
        transactions = await db.distributor_transactions.find(
            {"distributor_id": {"$in": distributor_ids}},
            {"_id": 0},
        ).to_list(10000)
        for txn in transactions:
            transactions_by_distributor[txn.get("distributor_id")].append(txn)

    for distributor in distributors:
        opening_balance_date = _distributor_opening_balance_date(distributor)
        if opening_balance_date:
            distributor["opening_balance_date"] = opening_balance_date

        distributor_transactions = _distributor_opening_balance_deduped_transactions(
            distributor,
            transactions_by_distributor.get(distributor.get("id"), []),
        )
        current_balance = _current_distributor_balance(distributor, distributor_transactions)
        distributor["current_balance"] = current_balance
        distributor["outstanding_balance"] = current_balance
        distributor["total_payable"] = _round_ledger_money(max(current_balance, 0))
        distributor["total_receivable_from_distributors"] = _round_ledger_money(abs(min(current_balance, 0)))
        distributor["net_distributor_balance"] = current_balance
        distributor["distributor_status"] = distributor.get("distributor_status") or "active"
        purchases = [
            txn for txn in distributor_transactions
            if txn.get("type") in {"purchase", "sale", "opening_balance"}
        ]
        distributor["total_purchases"] = _round_ledger_money(
            sum(_safe_float(txn.get("amount")) for txn in purchases)
        )
        actual_payments = _round_ledger_money(sum(
            _safe_float(txn.get("amount"))
            for txn in distributor_transactions
            if txn.get("type") == "payment"
        ))
        # Reconcile paid/adjusted to the payable side only. Credits beyond the
        # distributor's purchases are receivables, not additional payments.
        paid_adjusted = _round_ledger_money(distributor["total_purchases"] - distributor["total_payable"])
        distributor["actual_payments"] = actual_payments
        distributor["total_paid"] = paid_adjusted
        distributor["total_paid_adjusted"] = paid_adjusted
        purchase_dates = [
            _distributor_transaction_date(txn).isoformat()
            for txn in purchases
            if _distributor_transaction_date(txn)
        ]
        distributor["last_purchase_date"] = max(purchase_dates, default=None)

    if search:
        needle = search.strip().lower()
        distributors = [
            distributor for distributor in distributors
            if any(needle in str(distributor.get(field, "")).lower() for field in ("name", "phone", "gstin"))
        ]
    if status:
        distributors = [d for d in distributors if d["distributor_status"] == status]
    return distributors


@api_router.post("/distributors")
async def create_distributor(d: Distributor, user: dict = Depends(require_role("admin", "pharmacist"))):
    data = d.model_dump()

    data["phone"] = normalize_phone_number(data.get("phone"))

    await db.distributors.insert_one(data)

    return data

@api_router.put("/distributors/{did}")
async def update_distributor(did: str, d: Distributor, user: dict = Depends(require_role("admin", "pharmacist"))):
    existing = await db.distributors.find_one({"id": did}, {"_id": 0}) or {}
    data = d.model_dump()

    data["phone"] = normalize_phone_number(data.get("phone"))

    data["id"] = did

    if "created_at" not in d.model_fields_set and existing.get("created_at"):
        data["created_at"] = existing["created_at"]

    if (
        "opening_balance_date" not in d.model_fields_set
        and existing.get("opening_balance_date")
    ):
        data["opening_balance_date"] = existing["opening_balance_date"]

    await db.distributors.update_one({"id": did}, {"$set": data})

    if "opening_balance_date" in d.model_fields_set:
        await _sync_distributor_opening_balance_transaction_date(data, data["opening_balance_date"])

    return data


@api_router.patch("/distributors/{did}/opening-balance-date")
async def update_distributor_opening_balance_date(
    did: str,
    payload: DistributorOpeningBalanceDateUpdate,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    updated_dist = await db.distributors.find_one_and_update(
        {"id": did},
        {"$set": {"opening_balance_date": payload.opening_balance_date}},
        projection={"_id": 0},
        return_document=ReturnDocument.AFTER,
    )
    if not updated_dist:
        raise HTTPException(status_code=404, detail="Distributor not found")

    matched_transaction = await _sync_distributor_opening_balance_transaction_date(
        updated_dist,
        payload.opening_balance_date,
    )

    return {
        **updated_dist,
        "opening_balance_date": payload.opening_balance_date,
        "opening_balance_transaction_updated": matched_transaction is not None,
    }


@api_router.delete("/distributors/{did}")
async def delete_distributor(did: str, user: dict = Depends(require_role("admin"))):
    await db.distributors.delete_one({"id": did})
    return {"ok": True}


# ---------------- Customers ----------------
@api_router.get("/customers")
async def list_customers(search: Optional[str] = None, user: dict = Depends(get_current_user)):
    query = {}
    if search and search.strip():
        term = {"$regex": re.escape(search.strip()), "$options": "i"}
        query = {"$or": [{field: term} for field in ("name", "phone", "email", "gstin")]}
    customers = await db.customers.find(query, {"_id": 0}).sort("name", 1).to_list(1000)
    customer_ids = [customer["id"] for customer in customers]
    txns = await db.customer_transactions.find(
        {"customer_id": {"$in": customer_ids}}, {"_id": 0}
    ).to_list(10000) if customer_ids else []
    invoices = await db.invoices.find(
        {"customer_id": {"$in": customer_ids}}, {"_id": 0, "customer_id": 1, "total": 1, "paid_amount": 1, "created_at": 1}
    ).to_list(10000) if customer_ids else []
    by_customer = defaultdict(list)
    invoice_by_customer = defaultdict(list)
    for txn in txns:
        by_customer[txn.get("customer_id")].append(txn)
    for invoice in invoices:
        invoice_by_customer[invoice.get("customer_id")].append(invoice)
    results = []
    for customer in customers:
        own_txns = by_customer[customer["id"]]
        own_invoices = invoice_by_customer[customer["id"]]
        linked_refs = {
            str(value) for invoice in own_invoices
            for value in (invoice.get("id"), invoice.get("invoice_no"), invoice.get("invoice_number"))
            if value
        }
        invoice_sales = sum(_safe_float(i.get("total", i.get("grand_total", 0))) for i in own_invoices)
        unlinked_ledger_sales = sum(
            _safe_float(t.get("amount")) for t in own_txns
            if t.get("type") == "sale"
            and str(t.get("invoice_id") or t.get("invoice_number") or t.get("reference") or "") not in linked_refs
        )
        total_sales = _round_ledger_money(invoice_sales + unlinked_ledger_sales)
        total_paid = round(sum(float(i.get("paid_amount", 0) or 0) for i in own_invoices) + sum(float(t.get("amount", 0) or 0) for t in own_txns if t.get("type") == "payment"), 2)
        opening_balance = _safe_float(customer.get("opening_balance", customer.get("legacy_balance", 0)))
        balance = _round_ledger_money(total_sales - total_paid + opening_balance)
        status = "cleared" if balance <= 0 else ("partial" if total_paid > 0 else "due")
        results.append({**customer, "customer_type": customer.get("customer_type") or "regular",
            "current_balance": balance, "receivable_balance": balance, "total_sales": total_sales,
            "total_paid": total_paid, "last_sale_date": max((i.get("created_at") for i in own_invoices if i.get("created_at")), default=None),
            "payment_status": status})
    return results


@api_router.post("/customers")
async def create_customer(c: Customer, user: dict = Depends(get_current_user)):
    data = c.model_dump()

    data["phone"] = normalize_phone_number(data.get("phone"))

    await db.customers.insert_one(data)

    return data

@api_router.put("/customers/{cid}")
async def update_customer(cid: str, c: Customer, user: dict = Depends(get_current_user)):
    data = c.model_dump()
    data["phone"] = normalize_phone_number(data.get("phone"))
    data["id"] = cid
    await db.customers.update_one({"id": cid}, {"$set": data})
    return data


@api_router.delete("/customers/{cid}")
async def delete_customer(cid: str, user: dict = Depends(require_role("admin"))):
    await db.customers.delete_one({"id": cid})
    return {"ok": True}


# ---------------- Invoices ----------------
async def _existing_document_max_sequence(prefix: str, today: str) -> int:
    collection, field = {
        "INV": (db.invoices, "invoice_no"),
        "PO": (db.purchase_orders, "po_no"),
    }[prefix]

    docs = await collection.find(
        {
            field: {
                "$regex": f"^{prefix}-{today}-\\d+$"
            }
        },
        {
            "_id": 0,
            field: 1
        }
    ).to_list(None)

    max_sequence = 0
    for doc in docs:
        try:
            max_sequence = max(
                max_sequence,
                int(str(doc.get(field, "")).rsplit("-", 1)[-1])
            )
        except ValueError:
            continue

    return max_sequence


async def _next_document_no(prefix: str) -> str:
    today = datetime.now(timezone.utc).strftime("%y%m%d")
    existing_max = await _existing_document_max_sequence(prefix, today)
    counter_id = f"{_current_tenant.get() or REAL_TENANT_ID}:{prefix}-{today}"

    if LOCAL_MODE:
        # Cloud-to-local import intentionally reads MongoDB documents with
        # ``{"_id": 0}``, so imported counter rows can be present in the
        # SQLite JSON collection table without their Mongo ``_id``.  Resolve
        # counters by the canonical id first, then by the JSON fields retained
        # by import, and safely create the row if neither shape exists.
        counter = (
            await db.counters.find_one({"_id": counter_id})
            or await db.counters.find_one({"id": counter_id})
            or await db.counters.find_one({"prefix": prefix, "date": today})
        )
        next_sequence = max(int((counter or {}).get("seq") or 0), existing_max) + 1
        update_query = {"_id": counter.get("_id")} if counter and counter.get("_id") else {"_id": counter_id}
        if counter and not counter.get("_id"):
            update_query = {"id": counter["id"]} if counter.get("id") else {"prefix": prefix, "date": today}
        counter_update = {
            "seq": next_sequence,
            "prefix": prefix,
            "date": today,
        }
        if not counter:
            counter_update["_id"] = counter_id
        await db.counters.update_one(
            update_query,
            {"$set": counter_update},
            upsert=True,
        )
        return f"{prefix}-{today}-{next_sequence:04d}"

    counter = await db.counters.find_one_and_update(
        {"_id": counter_id},
        [
            {
                "$set": {
                    "seq": {
                        "$add": [
                            {
                                "$max": [
                                    {"$ifNull": ["$seq", 0]},
                                    existing_max,
                                ]
                            },
                            1,
                        ]
                    },
                    "prefix": prefix,
                    "date": today,
                }
            }
        ],
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return f"{prefix}-{today}-{int(counter['seq']):04d}"


async def _next_invoice_no() -> str:
    return await _next_document_no("INV")


async def _next_po_no() -> str:
    return await _next_document_no("PO")

def _is_transaction_unsupported(exc: Exception) -> bool:
    message = str(exc).lower()
    details = getattr(exc, "details", None) or {}
    code_name = str(details.get("codeName", "")).lower()

    unsupported_markers = (
        "transaction numbers are only allowed",
        "transactions are not supported",
        "transaction is not supported",
        "multi-document transactions are not supported",
        "sessions are not supported",
        "session is not supported",
        "cannot use sessions",
        "replica set member or mongos",
        "not a replica set member",
    )

    if any(marker in message for marker in unsupported_markers):
        return True

    return (
        "transaction" in message
        and "not supported" in message
        and code_name in {"illegaloperation", "operationnotsupportedintransaction"}
    )


async def _run_with_transaction(operation, fallback):
    if client is None:
        return await fallback()
    try:
        async with await client.start_session() as session:
            async with session.start_transaction():
                return await operation(session)
    except PyMongoError as exc:
        if _is_transaction_unsupported(exc):
            logger.warning(
                "MongoDB transactions are not available; using validation-first stock fallback"
            )
            return await fallback()
        raise


async def _build_fifo_stock_plan(
    medicine_name: str,
    qty: float,
    session=None
):

    qty = round_qty(qty)
    if qty <= 0:
        raise HTTPException(
            status_code=400,
            detail=f"Requested quantity for {medicine_name} must be greater than zero",
        )

    batches = await db.medicines.find(
        {
            "name": medicine_name
        },
        {
            "_id": 0
        },
        session=session
    ).to_list(5000)
    batches.sort(key=_fifo_expiry_key)

    available_qty = round_qty(sum(_available_stock(batch) for batch in batches))
    if qty > available_qty:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Requested quantity ({qty:g}) for {medicine_name} exceeds "
                f"available stock ({available_qty:g})"
            ),
        )

    remaining = qty
    plan = []

    for batch in batches:

        available = _available_stock(batch)

        if available <= 0:
            continue

        deduct = min(
            available,
            remaining
        )

        plan.append({
            "medicine_id": batch["id"],
            "medicine_key": batch.get("medicine_key"),
            "batch_no": batch.get("batch_no"),
            "medicine_name": medicine_name,
            "deduct": round_qty(deduct),
            "sold_units_before": _stock_quantity(batch, "sold_units", "sold_quantity"),
        })

        remaining = round_qty(remaining - deduct)

        if remaining <= 0:
            break

    if remaining > 0:
        # Defensive guard for malformed stock rows; normal overselling is rejected above.
        raise HTTPException(
            status_code=400,
            detail=(
                f"Requested quantity ({qty:g}) for {medicine_name} exceeds "
                f"available stock ({available_qty:g})"
            ),
        )

    return plan


async def _apply_fifo_stock_plan(
    plan: List[dict],
    session=None,
    applied=None
):
    applied_steps = []

    for step in plan:

        result = await _set_rounded_stock_delta(
            step["medicine_id"], "sold_units", step["deduct"],
            session=session, require_available=True,
        )

        if not result or result.modified_count != 1:
            raise HTTPException(
                status_code=409,
                detail=f"Stock changed while processing {step['medicine_name']}. Please retry."
            )

        applied_steps.append(step)
        if applied is not None:
            applied.append(step)

    return applied_steps


async def _apply_fifo_stock_requests(
    stock_requests: Dict[str, float],
    session=None,
    applied=None
):
    applied_steps = []

    for medicine_name, qty in stock_requests.items():
        plan = await _build_fifo_stock_plan(
            medicine_name,
            qty,
            session=session
        )
        applied_steps.extend(
            await _apply_fifo_stock_plan(
                plan,
                session=session,
                applied=applied
            )
        )

    return applied_steps


async def _restore_fifo_stock(applied: List[dict]):
    for step in reversed(applied):
        result = await _set_rounded_stock_delta(
            step["medicine_id"], "sold_units", -step["deduct"]
        )

        if not result or result.modified_count != 1:
            logger.error(
                "Could not automatically restore %s units for medicine %s without "
                "making sold_units negative; manual stock reconciliation may be needed",
                step["deduct"],
                step["medicine_id"],
            )


def _stock_deductions_from_steps(steps: List[dict]) -> List[dict]:
    return [
        {
            "medicine_id": step["medicine_id"],
            "medicine_name": step.get("medicine_name", ""),
            "deduct": round_qty(step.get("deduct", 0)),
            **({"batch_no": step.get("batch_no")} if step.get("batch_no") else {}),
            **({"medicine_key": step.get("medicine_key")} if step.get("medicine_key") else {}),
        }
        for step in steps
        if round_qty(step.get("deduct", 0)) > 0
    ]


def _stock_deductions_from_daily_sale(sale: dict) -> List[dict]:
    deductions = sale.get("stock_deductions") or []
    if deductions:
        return _stock_deductions_from_steps(deductions)

    return [
        {
            "medicine_id": sale["medicine_id"],
            "medicine_name": sale.get("medicine_name", ""),
            "deduct": round_qty(
                sale.get(
                    "units_dispensed",
                    sale.get("quantity", 0)
                )
            ),
            "legacy_fallback": True,
        }
    ]


async def _restore_daily_sale_stock(
    deductions: List[dict],
    session=None,
    restored=None
):
    for step in reversed(deductions):
        deduct = round_qty(step.get("deduct", 0))
        if deduct <= 0:
            continue

        result = await _set_rounded_stock_delta(
            step["medicine_id"], "sold_units", -deduct, session=session
        )

        if not result or result.modified_count != 1:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Could not restore daily-sale stock for "
                    f"{step.get('medicine_name') or step['medicine_id']}. "
                    "Manual reconciliation may be needed."
                )
            )

        if restored is not None:
            restored.append(step)


async def _reapply_daily_sale_stock(restored: List[dict]):
    for step in reversed(restored):
        deduct = round_qty(step.get("deduct", 0))
        if deduct <= 0:
            continue

        await _set_rounded_stock_delta(
            step["medicine_id"], "sold_units", deduct, require_available=True
        )


INVOICE_PAYMENT_MODES = {"cash", "upi", "card", "credit", "mixed"}
INTERNAL_INVOICE_FIELDS = {
    "purchase_cost", "profit", "estimated_profit", "margin", "margin_percentage",
}
INVOICE_MONEY_FIELDS = {
    "subtotal", "gst_total", "gst", "bill_discount", "discount", "grand_total",
    "total", "paid_amount", "paid", "due_amount", "due", *INTERNAL_INVOICE_FIELDS,
}

def _round_invoice_money(value) -> float:
    return float(Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def _invoice_paid_amount(payment_mode: str, submitted_paid, total) -> float:
    if payment_mode in {"credit", "mixed"}:
        return _round_invoice_money(submitted_paid)
    return _round_invoice_money(submitted_paid or total)

def _invoice_user_can_view_internal(user: Optional[dict]) -> bool:
    """Keep profit intelligence admin-only until an explicit role permission exists."""
    return bool(user and str(user.get("role", "")).lower() == "admin")


def _strip_internal_invoice_fields(value):
    """Recursively sanitize anything used for customer, print, PDF, or share output."""
    if isinstance(value, dict):
        return {
            key: _strip_internal_invoice_fields(item)
            for key, item in value.items()
            if key not in INTERNAL_INVOICE_FIELDS
        }
    if isinstance(value, list):
        return [_strip_internal_invoice_fields(item) for item in value]
    return value


def _normalize_invoice(invoice: dict, include_internal: bool = False) -> dict:
    """Normalize legacy combined customer/payment invoices without breaking stored data."""
    result = dict(invoice)
    mode = str(result.get("payment_mode") or "").strip().lower()
    customer = str(result.get("customer_name") or "").strip()
    if mode not in INVOICE_PAYMENT_MODES:
        legacy = mode or str(result.get("payment") or "").strip().lower()
        mode = legacy if legacy in INVOICE_PAYMENT_MODES else "cash"
    if customer.lower() in INVOICE_PAYMENT_MODES:
        # Older invoices sometimes put the mode in customer_name. Preserve identity separately.
        customer = str(result.get("customer") or "Walk-in").strip() or "Walk-in"
    result["payment_mode"] = mode
    result["customer_name"] = customer or "Walk-in"
    for field in INVOICE_MONEY_FIELDS:
        if field in result:
            result[field] = _round_invoice_money(result[field])
    result["items"] = [{**item, **{field: _round_invoice_money(item[field]) for field in ("line_total", "gst_amount", "net_amount", "purchase_cost", "estimated_profit", "margin_percentage") if field in item}} for item in result.get("items", [])]
    if not include_internal:
        result = _strip_internal_invoice_fields(result)
    return _ensure_action_aliases(result, alias_id_fields=("invoice_id",))


def _ensure_action_aliases(row: dict, *, alias_id_fields: Tuple[str, ...] = ()) -> dict:
    """Add non-destructive stable aliases used by table row actions."""
    result = dict(row or {})
    stable_id = result.get("id") or result.get("_id")
    if result.get("id") in (None, "") and stable_id not in (None, ""):
        result["id"] = str(stable_id)
    for alias in alias_id_fields:
        if result.get(alias) in (None, "") and stable_id not in (None, ""):
            result[alias] = str(stable_id)
    return result


async def reduce_fifo_stock(
    medicine_name: str,
    qty: float,
    session=None
):
    plan = await _build_fifo_stock_plan(
        medicine_name,
        qty,
        session=session
    )
    return await _apply_fifo_stock_plan(
        plan,
        session=session
    )


@api_router.post("/invoices")
async def create_invoice(
    payload: InvoiceCreate,
    user: dict = Depends(get_current_user)
):

    subtotal = 0.0
    gst_total = 0.0
    items_out = []
    line_total_raw = 0.0
    stock_requests = defaultdict(float)

    for item in payload.items:

        med = await db.medicines.find_one({
            "name": item.name
        })

        if not med:
            raise HTTPException(
                status_code=400,
                detail=f"Medicine not found: {item.name}"
            )

        upb = max(
            int(
                med.get("units_per_box")
                or item.units_per_box
                or 1
            ),
            1
        )

        units_needed = round_qty(item.quantity * (
            upb if item.unit_type == "box" else 1
        ))

        stock_requests[item.name] = round_qty(stock_requests[item.name] + units_needed)

        unit_price = item.mrp * (
            upb if item.unit_type == "box" else 1
        )

        line_base = unit_price * item.quantity

        line_discount = (
            line_base *
            (item.discount_pct / 100.0)
        )

        taxable = line_base - line_discount

        line_total_raw += taxable

        items_out.append({
            **item.model_dump(),
            "units_per_box": upb,
            "units_dispensed": units_needed,
            "line_total": _round_invoice_money(taxable),
            "purchase_cost": _round_invoice_money(float(med.get("purchase_price", 0) or 0) * units_needed),
        })

    bill_disc = float(
        payload.bill_discount_amount or 0.0
    )

    if not bill_disc and payload.bill_discount_pct:

        bill_disc = (
            line_total_raw *
            (
                float(
                    payload.bill_discount_pct
                ) / 100.0
            )
        )

    bill_disc = min(
        bill_disc,
        line_total_raw
    )

    after_disc = max(
        line_total_raw - bill_disc,
        0.0
    )

    final_items = []

    for it, raw in zip(
        items_out,
        [i["line_total"] for i in items_out]
    ):

        share = (
            (raw / line_total_raw)
            if line_total_raw else 0
        )

        item_after = raw - bill_disc * share

        gst_amount = (
            item_after -
            (
                item_after /
                (
                    1 +
                    it["gst_rate"] / 100.0
                )
            )
        )

        net = item_after - gst_amount

        gst_total += gst_amount
        subtotal += net

        final_items.append({
            **it,
            "gst_amount": round(gst_amount, 2),
            "net_amount": _round_invoice_money(net),
            "estimated_profit": _round_invoice_money(item_after - it["purchase_cost"]),
            "margin_percentage": _round_invoice_money(((item_after - it["purchase_cost"]) / item_after * 100) if item_after else 0),
        })

    total = round(
        subtotal + gst_total,
        2
    )

    paid = _invoice_paid_amount(payload.payment_mode, payload.paid_amount, total)

    invoice = {

        "id": str(uuid.uuid4()),

        "invoice_no": await _next_invoice_no(),

        "customer_id": payload.customer_id,

        "customer_name": payload.customer_name,

        "customer_phone": payload.customer_phone,

        "customer_gstin": payload.customer_gstin,

        "referring_doctor": (
            payload.referring_doctor.strip()
            if payload.referring_doctor
            else ""
        ),

        "items": final_items,

        "subtotal": round(subtotal, 2),

        "gst_total": round(gst_total, 2),

        "bill_discount": round(bill_disc, 2),

        "total": total,

        "payment_mode": payload.payment_mode,

        "paid_amount": _round_invoice_money(paid),

        "due_amount": round(
            total - paid,
            2
        ),

        "purchase_cost": _round_invoice_money(sum(item["purchase_cost"] for item in final_items)),

        "estimated_profit": _round_invoice_money(total - sum(item["purchase_cost"] for item in final_items)),

        "margin_percentage": _round_invoice_money(((total - sum(item["purchase_cost"] for item in final_items)) / total * 100) if total else 0),

        "notes": payload.notes,

        "created_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "created_by": user.get(
            "name",
            ""
        ),
    }

    async def write_invoice_records(session=None, rollback_log=None):
        doctor_name = invoice["referring_doctor"]

        if doctor_name:

            if rollback_log is not None:
                previous_doctor = await db.doctor_history.find_one(
                    {
                        "name": doctor_name
                    }
                )
                rollback_log.append((
                    "doctor_history",
                    doctor_name,
                    previous_doctor,
                ))

            await db.doctor_history.update_one(

                {
                    "name": doctor_name
                },

                {
                    "$inc": {
                        "count": 1
                    },

                    "$set": {
                        "last_used": invoice["created_at"]
                    }
                },

                upsert=True,
                session=session,
            )

        if (
            payload.customer_id
            and invoice["due_amount"] > 0
        ):

            customer_txn = {

                "id": str(uuid.uuid4()),

                "customer_id": payload.customer_id,

                "type": "sale",

                "amount": invoice["due_amount"],

                "reference": invoice["invoice_no"],
                "reference_number": invoice["invoice_no"],
                "invoice_number": invoice["invoice_no"],
                "invoice_id": invoice["id"],
                "payment_mode": invoice["payment_mode"],

                "notes": "Credit sale",

                "created_at": invoice["created_at"],
            }

            await db.customer_transactions.insert_one(
                customer_txn,
                session=session
            )

            if rollback_log is not None:
                rollback_log.append((
                    "customer_transaction",
                    customer_txn["id"],
                ))

        await db.invoices.insert_one(
            invoice,
            session=session
        )

        if rollback_log is not None:
            rollback_log.append((
                "invoice",
                invoice["id"],
            ))


    async def rollback_invoice_records(rollback_log):
        for action in reversed(rollback_log):
            if action[0] == "invoice":
                await db.invoices.delete_one({
                    "id": action[1]
                })
            elif action[0] == "customer_transaction":
                await db.customer_transactions.delete_one({
                    "id": action[1]
                })
            elif action[0] == "doctor_history":
                _, doctor_name, previous_doctor = action
                if previous_doctor:
                    await db.doctor_history.replace_one(
                        {
                            "_id": previous_doctor["_id"]
                        },
                        previous_doctor,
                        upsert=True
                    )
                else:
                    await db.doctor_history.delete_one({
                        "name": doctor_name
                    })

    async def transaction_operation(session):
        applied = await _apply_fifo_stock_requests(
            stock_requests,
            session=session
        )
        invoice["stock_deductions"] = _stock_deductions_from_steps(applied)
        await write_invoice_records(session=session)
        return invoice

    async def fallback_operation():
        applied = []

        try:
            await _apply_fifo_stock_requests(
                stock_requests,
                applied=applied
            )
            invoice["stock_deductions"] = _stock_deductions_from_steps(applied)
            rollback_log = []
            await write_invoice_records(rollback_log=rollback_log)
        except Exception:
            try:
                if "rollback_log" in locals() and rollback_log:
                    await rollback_invoice_records(rollback_log)
            finally:
                if applied:
                    await _restore_fifo_stock(applied)
            raise

        return invoice

    created = await _run_with_transaction(
        transaction_operation,
        fallback_operation
    )
    return _normalize_invoice(created, include_internal=_invoice_user_can_view_internal(user))
    
@api_router.get("/invoices")
async def list_invoices(
    start: Optional[str] = None,
    end: Optional[str] = None,
    search: Optional[str] = None,
    invoice_number: Optional[str] = None,
    customer_name: Optional[str] = None,
    phone: Optional[str] = None,
    payment_mode: Optional[str] = None,
    user: dict = Depends(get_current_user),
):
    import re

    q = {}
    if start or end:
        q["created_at"] = {}
        if start:
            q["created_at"]["$gte"] = start
        if end:
            q["created_at"]["$lte"] = end
    filters = []
    if search:
        term = {"$regex": re.escape(search.strip()), "$options": "i"}
        filters.append({"$or": [{"invoice_no": term}, {"invoice_number": term}, {"customer_name": term}, {"customer_phone": term}]})
    if invoice_number:
        term = {"$regex": re.escape(invoice_number.strip()), "$options": "i"}
        filters.append({"$or": [{"invoice_no": term}, {"invoice_number": term}]})
    for field, value in (("customer_name", customer_name), ("customer_phone", phone)):
        if value:
            filters.append({field: {"$regex": re.escape(value.strip()), "$options": "i"}})
    if payment_mode:
        filters.append({"payment_mode": payment_mode.strip().lower()})
    if filters:
        q = {"$and": [q, *filters]} if q else (filters[0] if len(filters) == 1 else {"$and": filters})
    invoices = await db.invoices.find(q, {"_id": 0}).sort("created_at", -1).to_list(2000)
    return [_normalize_invoice(inv, include_internal=_invoice_user_can_view_internal(user)) for inv in invoices]


@api_router.get("/invoices/{inv_id}")
async def get_invoice(inv_id: str, user: dict = Depends(get_current_user)):
    inv = await db.invoices.find_one({"id": inv_id}, {"_id": 0})
    if not inv:
        raise HTTPException(status_code=404, detail="Invoice not found")
    return _normalize_invoice(inv, include_internal=_invoice_user_can_view_internal(user))


@api_router.get("/invoices/{inv_id}/share")
async def get_invoice_share_payload(inv_id: str, user: dict = Depends(get_current_user)):
    """Return customer-safe invoice data; delivery is deliberately left to the client."""
    inv = await db.invoices.find_one({"id": inv_id}, {"_id": 0})
    if not inv:
        raise HTTPException(status_code=404, detail="Invoice not found")
    return {"invoice": _normalize_invoice(inv, include_internal=False)}


# ---------------- Ledgers ----------------
DISTRIBUTOR_PURCHASE_MODES = {"cash", "upi", "credit"}


def _transaction_payment_mode(payload: PaymentCreate) -> str:
    mode = (payload.payment_mode or payload.mode or "cash").strip().lower()
    return mode


def _validate_distributor_purchase_mode(payload: PaymentCreate) -> str:
    submitted_modes = [payload.mode, payload.payment_mode]
    invalid_modes = {
        str(mode).strip().lower()
        for mode in submitted_modes
        if mode and str(mode).strip().lower() not in DISTRIBUTOR_PURCHASE_MODES
    }
    if invalid_modes:
        raise HTTPException(
            status_code=400,
            detail="Purchase payment mode must be cash, upi, or credit",
        )

    payment_mode = _transaction_payment_mode(payload)
    if payment_mode not in DISTRIBUTOR_PURCHASE_MODES:
        raise HTTPException(
            status_code=400,
            detail="Purchase payment mode must be cash, upi, or credit",
        )
    return payment_mode


def _distributor_transaction_metadata(payload: PaymentCreate) -> dict:
    payment_mode = _transaction_payment_mode(payload)
    return {
        "receipt_number": payload.receipt_number,
        "invoice_number": payload.invoice_number,
        "bill_number": payload.bill_number,
        "reference_number": payload.reference_number,
        "payment_mode": payment_mode,
        "mode": payment_mode,
        "notes": payload.notes,
    }


OPENING_BALANCE_DATE_EDIT_FIELDS = {"opening_balance_date", "date", "transaction_date"}

OPENING_BALANCE_METADATA_FIELD_ALIASES = {
    "invoice_no": "invoice_number",
    "invoice_number": "invoice_number",
    "bill_no": "bill_number",
    "bill_number": "bill_number",
    "receipt_no": "receipt_number",
    "receipt_number": "receipt_number",
    "reference_no": "reference_number",
    "reference_number": "reference_number",
    "reference": "reference_number",
    "notes": "notes",
}

OPENING_BALANCE_EDITABLE_FIELDS = (
    set(OPENING_BALANCE_METADATA_FIELD_ALIASES)
    | set(OPENING_BALANCE_METADATA_FIELD_ALIASES.values())
    | {"opening_balance_date"}
)

OPENING_BALANCE_NORMALIZED_EDITABLE_FIELDS = (
    set(OPENING_BALANCE_METADATA_FIELD_ALIASES.values())
    | {"opening_balance_date"}
)

DISTRIBUTOR_TRANSACTION_EDITABLE_FIELDS = {
    "receipt_number",
    "invoice_number",
    "bill_number",
    "reference_number",
    "payment_mode",
    "notes",
}


def _distributor_transaction_update_date(changes: dict) -> Optional[str]:
    for field_name in ("opening_balance_date", "date", "transaction_date"):
        value = changes.get(field_name)
        if value:
            return value
    return None


def _normalize_opening_balance_update_changes(changes: dict) -> dict:
    normalized = {}
    for field_name, value in changes.items():
        if field_name in OPENING_BALANCE_DATE_EDIT_FIELDS:
            continue

        normalized_field_name = OPENING_BALANCE_METADATA_FIELD_ALIASES.get(field_name, field_name)
        if (
            normalized_field_name in normalized
            and normalized[normalized_field_name] != value
        ):
            raise HTTPException(
                status_code=400,
                detail=f"Opening balance {normalized_field_name} aliases must match",
            )
        normalized[normalized_field_name] = value

    opening_balance_date = _distributor_transaction_update_date(changes)
    if opening_balance_date:
        normalized["opening_balance_date"] = opening_balance_date
    return normalized


def _strip_normal_transaction_date_changes(changes: dict) -> dict:
    return {
        field_name: value
        for field_name, value in changes.items()
        if field_name not in OPENING_BALANCE_DATE_EDIT_FIELDS
    }


def _opening_balance_transaction_id(distributor_id: Optional[str]) -> str:
    return f"opening-balance-{distributor_id}"


def _is_opening_balance_transaction(txn: dict, distributor_id: Optional[str] = None) -> bool:
    txn_id = str(txn.get("id") or "")
    if distributor_id and txn_id == _opening_balance_transaction_id(distributor_id):
        return True

    txn_type = str(txn.get("type") or "").strip().lower()
    display_type = str(txn.get("display_type") or txn.get("subtype") or "").strip().lower()
    notes = str(txn.get("notes") or "").strip().lower()
    reference = str(txn.get("reference_number") or "").strip().lower()

    return (
        txn_type == "opening_balance"
        or display_type == "opening balance"
        or (txn_type in {"purchase", "payment"} and (notes == "opening balance" or reference == "opening balance"))
    )


def _has_opening_balance_metadata(txn: dict) -> bool:
    if not isinstance(txn, dict):
        return False
    metadata_values = (
        txn.get("reference_number"),
        txn.get("reference"),
        txn.get("notes"),
        txn.get("subtype"),
        txn.get("display_type"),
        txn.get("source"),
    )
    return any(str(value or "").strip().lower() in {
        "opening balance",
        "opening_balance",
        "opening-balance",
    } for value in metadata_values)


DISTRIBUTOR_OPENING_BALANCE_DATE_FIELDS = (
    "opening_balance_date",
    "opening_date",
    "balance_date",
    "transaction_date",
    "date",
)


def _first_present_field(source: dict, field_names: Tuple[str, ...]):
    for field_name in field_names:
        value = source.get(field_name)
        if value:
            return value
    return None


def _distributor_opening_balance_date(distributor: dict):
    return _first_present_field(distributor, DISTRIBUTOR_OPENING_BALANCE_DATE_FIELDS)


def _opening_balance_amount_matches(txn: dict, distributor: dict) -> bool:
    try:
        txn_amount = round(float(txn.get("amount", 0) or 0), 2)
        opening_balance = round(float(distributor.get("opening_balance", 0) or 0), 2)
        return txn_amount == opening_balance
    except (TypeError, ValueError):
        return False


def _normalized_ledger_reference(value) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _ledger_reference_values(source: dict, field_names: Tuple[str, ...]) -> Set[str]:
    if not isinstance(source, dict):
        return set()
    return {
        _normalized_ledger_reference(source.get(field_name))
        for field_name in field_names
        if _normalized_ledger_reference(source.get(field_name))
    }


def _opening_balance_reference_values(distributor: dict, opening_txn: Optional[dict] = None) -> Set[str]:
    values = _ledger_reference_values(
        distributor,
        (
            "opening_balance_invoice_number",
            "opening_balance_bill_number",
            "opening_balance_reference_number",
            "opening_balance_receipt_number",
        ),
    )
    values.update(_ledger_reference_values(
        opening_txn or {},
        (
            "invoice_number",
            "invoice_no",
            "bill_number",
            "bill_no",
            "reference_number",
            "reference_no",
            "reference",
            "receipt_number",
        ),
    ))
    return values


def _transaction_reference_values(txn: dict) -> Set[str]:
    return _ledger_reference_values(
        txn,
        (
            "invoice_number",
            "invoice_no",
            "bill_number",
            "bill_no",
            "reference_number",
            "reference_no",
            "reference",
            "receipt_number",
        ),
    )


def _opening_balance_duplicate_reference_matches(
    txn: dict,
    distributor: dict,
    opening_txn: Optional[dict] = None,
) -> bool:
    opening_refs = _opening_balance_reference_values(distributor, opening_txn)
    txn_refs = _transaction_reference_values(txn)
    return not opening_refs or bool(opening_refs & txn_refs)


def _is_duplicate_opening_balance_row(txn: dict, distributor: dict, opening_txn: Optional[dict] = None) -> bool:
    """Detect legacy synthetic purchase/payment rows that duplicate the distributor opening balance."""
    if not isinstance(txn, dict):
        return False

    txn_type = str(txn.get("type") or "").strip().lower()
    if txn_type not in {"purchase", "payment"}:
        return False
    if txn.get("is_opening_balance"):
        return False
    if not _has_opening_balance_metadata(txn):
        return False
    if not _opening_balance_amount_matches(txn, distributor):
        return False
    if not _opening_balance_duplicate_reference_matches(txn, distributor, opening_txn):
        return False

    opening_date = _distributor_transaction_date(
        opening_txn or _opening_balance_transaction(distributor)
    )
    txn_date = _distributor_transaction_date(txn)
    return bool(opening_date and txn_date and opening_date == txn_date)




def _ledger_amount_key(value) -> float:
    return _round_ledger_money(_safe_float(value))


def _ledger_reference_match_values(source: dict) -> Set[str]:
    generic_opening_refs = {"opening balance", "opening_balance", "opening-balance"}
    return {
        value
        for value in _ledger_reference_values(
            source,
            (
                "invoice_no",
                "invoice_number",
                "bill_no",
                "bill_number",
                "reference_no",
                "reference_number",
                "reference",
                "receipt_invoice_no",
                "receipt_number",
            ),
        )
        if value not in generic_opening_refs
    }


def _ledger_amount_detail_key(txn: dict) -> Tuple[float, float, float]:
    return (
        _ledger_amount_key(txn.get("bill_amount")),
        _ledger_amount_key(txn.get("paid_amount")),
        _ledger_amount_key(txn.get("due_amount")),
    )


def _dedupe_distributor_opening_balance_rows(transactions: List[dict], distributor_id: Optional[str]) -> List[dict]:
    """Remove normal rows that duplicate an opening-balance bill before balances are calculated."""
    opening_ref_keys: Set[Tuple[str, str, float, object]] = set()
    opening_fallback_keys: Set[Tuple[str, float, object, Tuple[float, float, float]]] = set()

    for txn in transactions:
        if not _is_explicit_opening_balance_transaction(txn, distributor_id):
            continue
        txn_distributor_id = str(txn.get("distributor_id") or distributor_id or "")
        txn_date = _distributor_transaction_date(txn)
        amount = _ledger_amount_key(txn.get("amount"))
        refs = _ledger_reference_match_values(txn)
        for ref in refs:
            opening_ref_keys.add((txn_distributor_id, ref, amount, txn_date))
        if not refs:
            opening_fallback_keys.add((txn_distributor_id, amount, txn_date, _ledger_amount_detail_key(txn)))

    if not opening_ref_keys and not opening_fallback_keys:
        return transactions

    deduped = []
    for txn in transactions:
        if _is_explicit_opening_balance_transaction(txn, distributor_id):
            deduped.append(txn)
            continue

        txn_type = str(txn.get("type") or "").strip().lower()
        if txn_type not in {"purchase", "sale", "payment"}:
            deduped.append(txn)
            continue

        txn_distributor_id = str(txn.get("distributor_id") or distributor_id or "")
        txn_date = _distributor_transaction_date(txn)
        amount = _ledger_amount_key(txn.get("amount"))
        refs = _ledger_reference_match_values(txn)
        ref_match = any((txn_distributor_id, ref, amount, txn_date) in opening_ref_keys for ref in refs)
        fallback_match = (
            not refs
            and (txn_distributor_id, amount, txn_date, _ledger_amount_detail_key(txn)) in opening_fallback_keys
        )

        # Actual payments can share amount/date/reference with an opening balance; only legacy
        # payment rows explicitly labelled as opening balance should be suppressed.
        if txn_type == "payment" and not _has_opening_balance_metadata(txn):
            deduped.append(txn)
            continue

        if ref_match or fallback_match:
            continue
        deduped.append(txn)

    return deduped

def _is_explicit_opening_balance_transaction(txn: dict, distributor_id: Optional[str] = None) -> bool:
    if not isinstance(txn, dict):
        return False
    txn_id = str(txn.get("id") or "")
    txn_type = str(txn.get("type") or "").strip().lower()
    return (
        bool(distributor_id and txn_id == _opening_balance_transaction_id(distributor_id))
        or txn_type == "opening_balance"
        or bool(txn.get("is_opening_balance"))
    )


def _opening_balance_row_duplicates_manual_ledger_transaction(
    opening_txn: dict,
    candidate: dict,
    distributor_id: Optional[str] = None,
) -> bool:
    """Prefer a manually entered debit over an opening-balance copy of that debit."""
    if not _is_explicit_opening_balance_transaction(opening_txn, distributor_id):
        return False
    if not _has_opening_balance_metadata(opening_txn):
        return False
    if _is_explicit_opening_balance_transaction(candidate, distributor_id):
        return False
    if _has_opening_balance_metadata(candidate):
        return False
    if str(candidate.get("type") or "").strip().lower() not in {"purchase", "sale"}:
        return False
    if _ledger_amount_key(opening_txn.get("amount")) != _ledger_amount_key(candidate.get("amount")):
        return False

    opening_date = _distributor_transaction_date(opening_txn)
    candidate_date = _distributor_transaction_date(candidate)
    if not opening_date or opening_date != candidate_date:
        return False

    opening_refs = _ledger_reference_match_values(opening_txn)
    candidate_refs = _ledger_reference_match_values(candidate)
    return not opening_refs or not candidate_refs or bool(opening_refs & candidate_refs)


async def _find_distributor_opening_balance_transaction(distributor: dict):
    distributor_id = distributor.get("id")
    if not distributor_id:
        return None

    txns = await db.distributor_transactions.find(
        {"distributor_id": distributor_id},
    ).sort("created_at", 1).to_list(1000)

    candidates = [
        txn for txn in txns
        if _is_opening_balance_transaction(txn, distributor_id)
    ]
    if not candidates:
        return None

    amount_matches = [
        txn for txn in candidates
        if _opening_balance_amount_matches(txn, distributor)
    ]
    return (amount_matches or candidates)[0]


async def _sync_distributor_opening_balance_transaction_date(
    distributor: dict,
    opening_balance_date: str,
):
    matched_transaction = await _find_distributor_opening_balance_transaction(distributor)
    if not matched_transaction:
        return None

    update_filter = {"distributor_id": distributor.get("id")}
    if matched_transaction.get("id"):
        update_filter["id"] = matched_transaction.get("id")
    else:
        update_filter["_id"] = matched_transaction.get("_id")

    updated = await db.distributor_transactions.find_one_and_update(
        update_filter,
        {
            "$set": {
                "opening_balance_date": opening_balance_date,
                "transaction_date": opening_balance_date,
                "date": opening_balance_date,
            }
        },
        projection={"_id": 0},
        return_document=ReturnDocument.AFTER,
    )
    return updated


def _opening_balance_transaction_date(txn: dict, distributor: dict):
    return (
        _first_present_field(txn, DISTRIBUTOR_OPENING_BALANCE_DATE_FIELDS)
        or _distributor_opening_balance_date(distributor)
        or txn.get("created_at")
        or distributor.get("created_at")
        or datetime.now(timezone.utc).isoformat()
    )


def _opening_balance_transaction(distributor: dict) -> dict:
    opening_balance = _safe_float(distributor.get("opening_balance", 0))

    transaction_date = _opening_balance_transaction_date({}, distributor)

    return {
        "id": _opening_balance_transaction_id(distributor.get("id")),
        "distributor_id": distributor.get("id"),
        "type": "opening_balance",
        "subtype": "Opening Balance",
        "display_type": "Opening Balance",
        "amount": opening_balance,
        "invoice_number": distributor.get("opening_balance_invoice_number"),
        "bill_number": distributor.get("opening_balance_bill_number"),
        "receipt_number": distributor.get("opening_balance_receipt_number"),
        "reference_number": distributor.get("opening_balance_reference_number") or "Opening Balance",
        "notes": distributor.get("opening_balance_notes") or "Opening Balance",
        "created_at": transaction_date,
        "opening_balance_date": transaction_date,
        "transaction_date": transaction_date,
        "date": transaction_date,
        "is_opening_balance": True,
        "is_system_generated": True,
        "is_synthetic": True,
        "source": "opening_balance",
        "backend_row_source": "opening_balance",
        "transaction_id": None,
        "can_edit": False,
        "can_delete": False,
        "running_balance": round(opening_balance, 2),
    }


def _normalize_opening_balance_transaction(txn: dict, distributor: dict) -> dict:
    transaction_date = _opening_balance_transaction_date(txn, distributor)
    normalized = {
        **txn,
        "id": txn.get("id") or _opening_balance_transaction_id(distributor.get("id")),
        "distributor_id": txn.get("distributor_id") or distributor.get("id"),
        "type": "opening_balance",
        "subtype": "Opening Balance",
        "display_type": "Opening Balance",
        "amount": _safe_float(txn.get("amount", distributor.get("opening_balance", 0))),
        "reference_number": txn.get("reference_number") or "Opening Balance",
        "notes": txn.get("notes") or "Opening Balance",
        "created_at": transaction_date,
        "opening_balance_date": transaction_date,
        "transaction_date": transaction_date,
        "date": transaction_date,
        "is_opening_balance": True,
        "is_synthetic": False,
        "backend_row_source": "distributor_transactions",
        "source": txn.get("source") or "distributor_transactions",
        "transaction_id": _ledger_row_persisted_id(txn),
        "can_edit": bool(_ledger_row_persisted_id(txn)),
        "can_delete": False,
    }
    return normalized


def _parse_ledger_transaction_date(value: Optional[str]):
    if not value:
        return None

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except (TypeError, ValueError, OverflowError):
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except (TypeError, ValueError, OverflowError):
            return None


def _distributor_transaction_date(txn: dict):
    if not isinstance(txn, dict):
        return None
    return _parse_ledger_transaction_date(
        txn.get("transaction_date")
        or txn.get("date")
        or txn.get("created_at")
    )


def _financial_year_for_date(value) -> str:
    start_year = value.year if value.month >= 4 else value.year - 1
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def _current_financial_year() -> str:
    return _financial_year_for_date(datetime.now(timezone.utc).date())


def _normalize_distributor_ledger_financial_year(financial_year: Optional[str]) -> Optional[str]:
    """Normalize all-year sentinels while preserving strict FY validation."""
    if financial_year is None:
        return None

    normalized = financial_year.strip()
    if not normalized or normalized.casefold() == "all":
        return None
    return normalized


def _financial_year_date_range(financial_year: str):
    try:
        start_text, end_text = financial_year.split("-", 1)
        start_year = int(start_text)
        end_year_suffix = int(end_text)
    except (AttributeError, ValueError):
        raise HTTPException(
            status_code=400,
            detail="financial_year must use YYYY-YY format, for example 2025-26",
        )

    if (
        len(start_text) != 4
        or len(end_text) != 2
        or (start_year + 1) % 100 != end_year_suffix
    ):
        raise HTTPException(
            status_code=400,
            detail="financial_year must use YYYY-YY format, for example 2025-26",
        )

    return (
        datetime(start_year, 4, 1).date(),
        datetime(start_year + 1, 3, 31).date(),
    )


def _available_financial_years(transactions: List[dict]) -> List[str]:
    years = {
        _financial_year_for_date(txn_date)
        for txn in transactions
        if (txn_date := _distributor_transaction_date(txn))
    }
    return sorted(years, key=lambda year: int(year.split("-", 1)[0]), reverse=True)


def _filter_transactions_by_financial_year(
    transactions: List[dict],
    financial_year: str,
) -> List[dict]:
    start_date, end_date = _financial_year_date_range(financial_year)
    return [
        txn
        for txn in transactions
        if (txn_date := _distributor_transaction_date(txn))
        and start_date <= txn_date <= end_date
    ]


def _previous_financial_year(financial_year: str) -> str:
    start_year, _end_year = _financial_year_date_range(financial_year)
    previous_start_year = start_year.year - 1
    return f"{previous_start_year}-{str(previous_start_year + 1)[-2:]}"


def _next_financial_year(financial_year: str) -> str:
    start_year, _end_year = _financial_year_date_range(financial_year)
    next_start_year = start_year.year + 1
    return f"{next_start_year}-{str(next_start_year + 1)[-2:]}"


def _distributor_balance_until_date(
    transactions: List[dict],
    end_date,
    inclusive: bool = True,
) -> float:
    balance = 0.0

    for txn in transactions:
        txn_date = _distributor_transaction_date(txn)
        if not txn_date:
            continue

        if inclusive:
            include_transaction = txn_date <= end_date
        else:
            include_transaction = txn_date < end_date

        if include_transaction:
            balance, _bucket = _apply_distributor_transaction(balance, txn)

    return round(balance, 2)


def _distributor_financial_year_metadata(
    transactions: List[dict],
    financial_year: Optional[str],
) -> dict:
    if not financial_year:
        return {
            "brought_forward_balance": None,
            "brought_forward_from_financial_year": None,
            "balance_till_date": None,
            "carried_forward_balance": None,
            "carried_forward_to_financial_year": None,
            "is_financial_year_closed": None,
        }

    start_date, end_date = _financial_year_date_range(financial_year)
    today = datetime.now(timezone.utc).date()
    is_closed = end_date < today

    metadata = {
        "brought_forward_balance": _distributor_balance_until_date(
            transactions,
            start_date,
            inclusive=False,
        ),
        "brought_forward_from_financial_year": _previous_financial_year(financial_year),
        "balance_till_date": None,
        "carried_forward_balance": None,
        "carried_forward_to_financial_year": None,
        "is_financial_year_closed": is_closed,
    }

    if is_closed:
        metadata["carried_forward_balance"] = _distributor_balance_until_date(
            transactions,
            end_date,
            inclusive=True,
        )
        metadata["carried_forward_to_financial_year"] = _next_financial_year(financial_year)
    else:
        till_date = min(today, end_date)
        metadata["balance_till_date"] = _distributor_balance_until_date(
            transactions,
            till_date,
            inclusive=True,
        )

    return metadata


def _ledger_sort_datetime(value):
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)

    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        parsed_date = _parse_ledger_transaction_date(value)
        if parsed_date:
            return datetime.combine(parsed_date, datetime.min.time(), tzinfo=timezone.utc)
        return datetime.min.replace(tzinfo=timezone.utc)


def _distributor_fifo_sort_key(txn: dict):
    if not isinstance(txn, dict):
        txn = {}
    transaction_date_value = (
        txn.get("transaction_date")
        or txn.get("date")
        or txn.get("created_at")
    )
    return (
        _ledger_sort_datetime(transaction_date_value),
        _ledger_sort_datetime(txn.get("created_at")),
        str(txn.get("id") or ""),
    )


def _distributor_bill_reference(txn: dict) -> str:
    if not isinstance(txn, dict):
        return ""
    for field_name in (
        "invoice_no",
        "invoice_number",
        "bill_no",
        "bill_number",
        "reference_no",
        "reference_number",
        "reference",
    ):
        value = txn.get(field_name)
        if value not in (None, ""):
            return str(value)
    return str(txn.get("id") or "")


def _is_fifo_purchase_bill(txn: dict, distributor_id: str) -> bool:
    if not isinstance(txn, dict):
        return False
    txn_type = txn.get("type")
    if txn_type not in {"purchase", "sale", "opening_balance"}:
        return False

    if _is_opening_balance_transaction(txn, distributor_id):
        normalized_type = _normalize_opening_balance_transaction(
            txn,
            {"id": distributor_id, "opening_balance": txn.get("amount", 0)},
        ).get("type")
        return normalized_type in {"purchase", "sale", "opening_balance"}

    return True


def _is_fifo_credit_transaction(txn: dict) -> bool:
    if not isinstance(txn, dict):
        return False
    if txn.get("type") == "purchase_return":
        return True
    return txn.get("type") not in {"purchase", "sale", "opening_balance"}


def _safe_float(value, default: float = 0.0) -> float:
    if value in (None, ""):
        return default

    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return number if math.isfinite(number) else default


def _round_ledger_money(value: float) -> float:
    """Round ledger currency with the same decimal-safe rule used by POs."""
    return _money_float(_to_decimal(value))


def _serializable_transaction_id(value) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value)


def _json_safe_ledger_value(value):
    if isinstance(value, Decimal):
        return _round_ledger_money(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_safe_ledger_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_ledger_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe_ledger_value(item) for key, item in value.items()}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _json_safe_ledger_transaction(txn: dict, include_items: bool = False) -> dict:
    safe_txn = _json_safe_ledger_value(txn if isinstance(txn, dict) else {})
    if not include_items:
        safe_txn.pop("items", None)
    for money_field in ("amount", "running_balance", "bill_amount", "paid_amount", "due_amount"):
        if money_field in safe_txn:
            safe_txn[money_field] = _round_ledger_money(safe_txn[money_field])
    safe_txn = _ensure_action_aliases(safe_txn, alias_id_fields=("transaction_id",))
    for id_field in ("id", "transaction_id", "distributor_id", "purchase_order_id", "sale_id"):
        if safe_txn.get(id_field) not in (None, ""):
            safe_txn[id_field] = str(safe_txn[id_field])
    return safe_txn


def _fifo_debug_enabled(distributor_id: str) -> bool:
    debug_distributor_id = os.environ.get("DISTRIBUTOR_FIFO_DEBUG_LEDGER_ID")
    return debug_distributor_id in {"*", str(distributor_id)}


def _build_distributor_fifo_metadata(
    transactions: List[dict],
    distributor_id: str,
) -> Dict[str, dict]:
    metadata_by_id: Dict[str, dict] = {}
    unpaid_bills: List[dict] = []
    pending_credits: List[dict] = []
    allocation_sequence: List[dict] = []
    debug_enabled = _fifo_debug_enabled(distributor_id)

    try:
        sorted_transactions = sorted(transactions, key=_distributor_fifo_sort_key)
    except Exception:
        logger.exception(
            "Failed to sort distributor FIFO metadata rows; returning ledger without FIFO metadata. distributor_id=%s",
            distributor_id,
        )
        return metadata_by_id

    def record_debug_row(
        txn: Optional[dict],
        txn_id: Optional[str],
        stage: str,
        sequence_no: Optional[int] = None,
    ):
        if not debug_enabled:
            return
        try:
            safe_txn = txn if isinstance(txn, dict) else {}
            snapshot = metadata_by_id.get(txn_id or "", {}) if txn_id else {}
            logger.info(
                "Distributor FIFO debug distributor_id=%s stage=%s sequence=%s transaction_id=%s type=%s amount=%s paid_amount=%s due_amount=%s bill_status=%s allocation_sequence=%s",
                distributor_id,
                stage,
                sequence_no,
                txn_id,
                safe_txn.get("type"),
                _round_ledger_money(safe_txn.get("amount", 0)),
                snapshot.get("paid_amount"),
                snapshot.get("due_amount"),
                snapshot.get("bill_status"),
                allocation_sequence,
            )
        except Exception:
            logger.exception(
                "Failed to write distributor FIFO debug row. distributor_id=%s transaction_id=%s",
                distributor_id,
                txn_id,
            )

    def allocate_credit(credit: dict):
        remaining_credit = _round_ledger_money(credit.get("remaining_amount"))
        if remaining_credit <= 0:
            credit["remaining_amount"] = 0.0
            return

        credit_id = _serializable_transaction_id(credit.get("transaction_id"))
        allocations = metadata_by_id.setdefault(credit_id, {"adjusted_against": []}).setdefault(
            "adjusted_against",
            [],
        ) if credit_id else []

        for bill in unpaid_bills:
            if remaining_credit <= 0:
                break

            bill_due_amount = _round_ledger_money(bill.get("due_amount"))
            if bill_due_amount <= 0:
                continue

            allocated_amount = _round_ledger_money(min(remaining_credit, bill_due_amount))
            if allocated_amount <= 0:
                continue

            bill["paid_amount"] = _round_ledger_money(bill.get("paid_amount") + allocated_amount)
            bill["due_amount"] = _round_ledger_money(bill.get("due_amount") - allocated_amount)
            remaining_credit = _round_ledger_money(remaining_credit - allocated_amount)
            credit["remaining_amount"] = remaining_credit

            allocation = {
                "invoice_no": str(bill.get("invoice_no") or bill.get("transaction_id") or ""),
                "transaction_id": _serializable_transaction_id(bill.get("transaction_id")),
                "amount": allocated_amount,
            }
            allocations.append(allocation)
            allocation_sequence.append({
                "credit_transaction_id": credit_id,
                "bill_transaction_id": allocation["transaction_id"],
                "amount": allocated_amount,
            })

            bill_transaction_id = _serializable_transaction_id(bill.get("transaction_id"))
            if not bill_transaction_id:
                continue
            bill_metadata = metadata_by_id.get(bill_transaction_id, {})
            bill_metadata.update({
                "bill_amount": _round_ledger_money(bill.get("bill_amount")),
                "paid_amount": _round_ledger_money(bill.get("paid_amount")),
                "due_amount": _round_ledger_money(bill.get("due_amount")),
            })
            metadata_by_id[bill_transaction_id] = bill_metadata

        if credit_id:
            metadata_by_id[credit_id] = {"adjusted_against": allocations}

    for sequence_no, txn in enumerate(sorted_transactions, start=1):
        txn_id = _serializable_transaction_id(txn.get("id") if isinstance(txn, dict) else None)
        txn_type = txn.get("type") if isinstance(txn, dict) else None

        try:
            if not txn_id:
                continue

            amount = _round_ledger_money(txn.get("amount", 0))
            if amount <= 0:
                if _is_fifo_credit_transaction(txn):
                    metadata_by_id[txn_id] = {"adjusted_against": []}
                    record_debug_row(txn, txn_id, "skipped_empty_credit", sequence_no)
                continue

            if _is_fifo_purchase_bill(txn, distributor_id):
                bill = {
                    "transaction_id": txn_id,
                    "invoice_no": _distributor_bill_reference(txn),
                    "bill_amount": amount,
                    "paid_amount": 0.0,
                    "due_amount": amount,
                }
                unpaid_bills.append(bill)
                metadata_by_id[txn_id] = {
                    "bill_amount": amount,
                    "paid_amount": 0.0,
                    "due_amount": amount,
                    "bill_status": "later_due",
                }

                for credit in pending_credits:
                    allocate_credit(credit)
                pending_credits = [
                    credit
                    for credit in pending_credits
                    if _round_ledger_money(credit.get("remaining_amount")) > 0
                ]
                record_debug_row(txn, txn_id, "bill_processed", sequence_no)
                continue

            if not _is_fifo_credit_transaction(txn):
                continue

            credit = {
                "transaction_id": txn_id,
                "remaining_amount": amount,
            }
            metadata_by_id[txn_id] = {"adjusted_against": []}
            allocate_credit(credit)
            if _round_ledger_money(credit.get("remaining_amount")) > 0:
                pending_credits.append(credit)
            record_debug_row(txn, txn_id, "credit_processed", sequence_no)
        except Exception:
            logger.exception(
                "Skipping distributor FIFO metadata for malformed row. distributor_id=%s transaction_id=%s row_type=%s helper_section=allocation",
                distributor_id,
                txn_id,
                txn_type,
            )
            if txn_id and _is_fifo_credit_transaction(txn if isinstance(txn, dict) else {}):
                metadata_by_id.setdefault(txn_id, {"adjusted_against": []})
            continue

    oldest_due_transaction_id = None
    for bill in unpaid_bills:
        try:
            bill_transaction_id = _serializable_transaction_id(bill.get("transaction_id"))
            if not bill_transaction_id:
                continue
            bill_metadata = metadata_by_id.get(bill_transaction_id, {})
            due_amount = _round_ledger_money(bill_metadata.get("due_amount", bill.get("due_amount")))
            if due_amount <= 0:
                bill_status = "cleared"
            elif oldest_due_transaction_id is None:
                bill_status = "oldest_due"
                oldest_due_transaction_id = bill_transaction_id
            else:
                bill_status = "later_due"

            bill_metadata.update({
                "bill_amount": _round_ledger_money(bill.get("bill_amount")),
                "paid_amount": _round_ledger_money(bill_metadata.get("paid_amount", bill.get("paid_amount"))),
                "due_amount": due_amount,
                "bill_status": bill_status,
            })
            metadata_by_id[bill_transaction_id] = bill_metadata
            record_debug_row(
                {
                    "id": bill_transaction_id,
                    "type": "purchase",
                    "amount": bill.get("bill_amount"),
                },
                bill_transaction_id,
                "bill_status_finalized",
            )
        except Exception:
            logger.exception(
                "Skipping distributor FIFO bill status metadata. distributor_id=%s transaction_id=%s helper_section=bill_status",
                distributor_id,
                bill.get("transaction_id") if isinstance(bill, dict) else None,
            )
            continue

    if debug_enabled:
        logger.info(
            "Distributor FIFO debug completed distributor_id=%s allocation_sequence=%s oldest_due_transaction_id=%s",
            distributor_id,
            allocation_sequence,
            oldest_due_transaction_id,
        )

    return _json_safe_ledger_value(metadata_by_id)

def _distributor_fifo_metadata_transactions(
    transactions: List[dict],
    financial_year: Optional[str],
) -> List[dict]:
    if not financial_year:
        return list(transactions)

    _start_date, end_date = _financial_year_date_range(financial_year)
    return [
        txn
        for txn in transactions
        if (txn_date := _distributor_transaction_date(txn)) and txn_date <= end_date
    ]

def _apply_distributor_transaction(balance: float, txn: dict) -> Tuple[float, str]:
    amount = _safe_float(txn.get("amount", 0) if isinstance(txn, dict) else 0)
    txn_type = txn.get("type") if isinstance(txn, dict) else None

    if txn_type in ["purchase", "sale", "opening_balance"]:
        return balance + amount, "purchase"

    if txn_type == "purchase_return":
        return balance - amount, "adjustment"

    return balance - amount, "payment"


def _distributor_opening_balance_deduped_transactions(
    distributor: dict,
    transactions: List[dict],
) -> List[dict]:
    """Return transactions without an opening-balance copy of a manual ledger debit."""
    distributor_id = str(distributor.get("id") or "")
    opening_txn = None
    opening_balance_covered_by_manual_txn = False
    non_opening_txns = []
    manual_txns = [
        txn for txn in transactions
        if not _is_explicit_opening_balance_transaction(txn, distributor_id)
    ]

    for txn in transactions:
        if _is_explicit_opening_balance_transaction(txn, distributor_id):
            if any(
                _opening_balance_row_duplicates_manual_ledger_transaction(
                    txn, manual_txn, distributor_id
                )
                for manual_txn in manual_txns
            ):
                opening_balance_covered_by_manual_txn = True
                continue
            if opening_txn is None:
                opening_txn = _normalize_opening_balance_transaction(txn, distributor)
            continue
        if _is_duplicate_opening_balance_row(txn, distributor, opening_txn):
            continue
        non_opening_txns.append(txn)

    deduped = []
    if (
        not opening_balance_covered_by_manual_txn
        and (opening_txn is not None or _safe_float(distributor.get("opening_balance", 0)) != 0)
    ):
        deduped.append(opening_txn or _opening_balance_transaction(distributor))
    deduped.extend(non_opening_txns)
    return _dedupe_distributor_opening_balance_rows(deduped, distributor_id)


def _normalize_ledger_invoice_identity_value(value) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _ledger_invoice_identity_variants(value) -> Set[str]:
    """Return comparable invoice identities, including legacy prefix-stripped forms."""
    normalized = _normalize_ledger_invoice_identity_value(value)
    if not normalized:
        return set()

    variants = {normalized}
    # Legacy imports sometimes store distributor invoice series as a leading
    # parenthesized prefix, e.g. "(Q) 4557", while the paired ledger row stores
    # only "4557".  Keep the original identity and add only this deterministic
    # alternate form so unrelated invoices with different explicit refs remain
    # separate.
    without_series = re.sub(r"^\([^)]+\)\s*", "", normalized).strip()
    if without_series and without_series != normalized:
        variants.add(without_series)
    # Supplier invoices are commonly imported with a descriptive prefix in
    # one source and as the bare invoice number in another. Preserve the raw
    # normalized identity while adding the narrowly stripped canonical form.
    without_invoice_prefix = re.sub(
        r"^(?:invoice|inv|i\s*[\.\-]?\s*n\s*[\.\-]?)\s*[:#.\-]?\s*",
        "",
        normalized,
        flags=re.IGNORECASE,
    ).strip()
    if without_invoice_prefix and without_invoice_prefix != normalized:
        variants.add(without_invoice_prefix)
    return variants


def _purchase_invoice_reference_values(txn: dict) -> Set[str]:
    if not isinstance(txn, dict):
        return set()
    refs = set()
    for field_name in (
        "invoice_no",
        "invoice_number",
        "invoice_ref",
        "bill_no",
        "bill_number",
        "reference_no",
        "reference_number",
        "reference",
        "receipt_invoice_no",
        "po_no",
    ):
        refs.update(_ledger_invoice_identity_variants(txn.get(field_name)))
    return refs


def _purchase_invoice_identity_keys(txn: dict, distributor_id: Optional[str] = None) -> List[tuple]:
    """Return robust identity keys for distributor purchase/invoice ledger rows."""
    if not isinstance(txn, dict):
        return []
    txn_type = str(txn.get("type") or "").strip().lower()
    if txn_type not in {"purchase", "sale"}:
        return []
    txn_distributor_id = str(distributor_id or txn.get("distributor_id") or "").strip()
    txn_date = _distributor_transaction_date(txn)
    amount = _ledger_amount_key(txn.get("amount"))
    keys = [
        (txn_distributor_id, "ref", ref, txn_date, amount)
        for ref in sorted(_purchase_invoice_reference_values(txn))
    ]
    purchase_order_id = _normalize_ledger_invoice_identity_value(
        txn.get("purchase_order_id") or txn.get("po_id")
    )
    if purchase_order_id:
        keys.append((txn_distributor_id, "po", purchase_order_id))
    return keys


def _purchase_invoice_row_score(txn: dict) -> Tuple[int, int, int, int]:
    """Prefer richer canonical persisted purchase rows when duplicate invoices exist."""
    if not isinstance(txn, dict):
        return (0, 0, 0, 0)
    has_po_id = int(bool(txn.get("purchase_order_id") or txn.get("po_id")))
    is_po_source = int(txn.get("source") == "purchase_order")
    metadata_fields = (
        "items", "bill_amount", "paid_amount", "due_amount", "invoice_number",
        "invoice_no", "bill_number", "bill_no", "reference_number", "reference", "notes",
    )
    metadata_count = sum(
        1 for field_name in metadata_fields if txn.get(field_name) not in (None, "", [])
    )
    # Keep deterministic order for ties by preserving the existing first row.
    return (has_po_id, is_po_source, metadata_count, len(str(txn.get("id") or "")))


def _canonical_purchase_invoice_display_row(existing: dict, candidate: dict) -> dict:
    """Pick the richer duplicate invoice row without combining duplicate amounts."""
    existing_is_po = existing.get("source") == "purchase_order"
    candidate_is_po = candidate.get("source") == "purchase_order"
    if (
        existing_is_po
        and not candidate_is_po
        and _ledger_amount_key(existing.get("amount")) != _ledger_amount_key(candidate.get("amount"))
    ):
        canonical = dict(existing)
        canonical["amount"] = candidate.get("amount")
        return canonical

    if _purchase_invoice_row_score(candidate) <= _purchase_invoice_row_score(existing):
        return existing

    canonical = dict(candidate)
    # Keep the ledger amount already selected for display when a PO-linked row
    # has richer metadata but a different stored amount.
    if _ledger_amount_key(existing.get("amount")) != _ledger_amount_key(candidate.get("amount")):
        canonical["amount"] = existing.get("amount")
    return canonical




def _dedupe_distributor_purchase_invoice_rows(transactions: List[dict], distributor_id: Optional[str] = None) -> List[dict]:
    """Keep one purchase ledger row per distributor invoice identity; never dedupe payments."""
    selected_by_group: Dict[int, dict] = {}
    key_to_group: Dict[tuple, int] = {}
    group_order: List[Tuple[str, object]] = []
    next_group = 0

    for txn in transactions:
        # Persisted rows are accounting records, not display artifacts. Never
        # collapse two distributor_transactions rows against each other.
        if not (
            txn.get("is_synthetic")
            and txn.get("backend_row_source") == "purchase_orders"
        ):
            group_order.append(("row", txn))
            continue

        keys = _purchase_invoice_identity_keys(txn, distributor_id)
        if not keys:
            group_order.append(("row", txn))
            continue

        matching_groups = {key_to_group[key] for key in keys if key in key_to_group}
        if matching_groups:
            group_id = min(matching_groups)
            # Merge aliases if a richer multi-reference row bridges previously separate keys.
            for other_group in sorted(matching_groups - {group_id}):
                old_txn = selected_by_group.pop(other_group, None)
                if (
                    old_txn
                    and _purchase_invoice_row_score(old_txn)
                    > _purchase_invoice_row_score(selected_by_group[group_id])
                ):
                    selected_by_group[group_id] = _canonical_purchase_invoice_display_row(
                        selected_by_group[group_id],
                        old_txn,
                    )
                for key, mapped_group in list(key_to_group.items()):
                    if mapped_group == other_group:
                        key_to_group[key] = group_id
            selected_by_group[group_id] = _canonical_purchase_invoice_display_row(
                selected_by_group[group_id],
                txn,
            )
        else:
            group_id = next_group
            next_group += 1
            selected_by_group[group_id] = txn
            group_order.append(("group", group_id))

        for key in keys:
            key_to_group[key] = group_id

    deduped = []
    emitted_groups = set()
    for marker_type, value in group_order:
        if marker_type == "row":
            deduped.append(value)
            continue
        group_id = key_to_group.get(value, value)
        if group_id in emitted_groups or group_id not in selected_by_group:
            continue
        emitted_groups.add(group_id)
        deduped.append(selected_by_group[group_id])
    return deduped


def _final_distributor_ledger_rows(
    transactions: List[dict],
    distributor_id: str,
) -> List[dict]:
    """Return the authoritative post-dedupe rows for display and accounting."""
    return _dedupe_distributor_purchase_invoice_rows(transactions, distributor_id)


def _ledger_row_persisted_id(txn: dict) -> Optional[str]:
    if not isinstance(txn, dict):
        return None
    for field_name in ("_id", "id"):
        value = txn.get(field_name)
        if value not in (None, ""):
            return str(value)
    return None


def _annotate_distributor_transaction_source(txn: dict) -> dict:
    annotated = dict(txn)
    annotated.setdefault("backend_row_source", "distributor_transactions")
    annotated.setdefault("source", "distributor_transactions")
    annotated.setdefault("is_synthetic", False)
    persisted_id = _ledger_row_persisted_id(txn)
    if persisted_id:
        annotated.setdefault("transaction_id", persisted_id)
    if txn.get("_id") is not None:
        annotated.setdefault("transaction_object_id", str(txn.get("_id")))
    is_opening_txn = _is_opening_balance_transaction(annotated, annotated.get("distributor_id"))
    annotated.setdefault("can_edit", bool(persisted_id))
    annotated.setdefault("can_delete", bool(persisted_id and not is_opening_txn))
    return annotated



def _current_distributor_balance(distributor: dict, transactions: List[dict]) -> float:
    balance = 0.0

    for txn in _distributor_opening_balance_deduped_transactions(distributor, transactions):
        balance, _bucket = _apply_distributor_transaction(balance, txn)

    return round(balance, 2)


def _distributor_identity_values(distributor: dict, requested_id: Optional[str] = None) -> Set[str]:
    values = set()
    for field in ("_id", "_legacy_object_id", "id", "distributor_id"):
        value = distributor.get(field)
        if value is not None and str(value).strip():
            values.add(str(value).strip())
    if requested_id and str(requested_id).strip():
        values.add(str(requested_id).strip())
    return values


def _belongs_to_distributor(row: dict, identity_values: Set[str], names: Set[str]) -> bool:
    linked_values = {
        str(row.get(field)).strip()
        for field in ("distributor_id", "distributor", "distributorId")
        if row.get(field) is not None and str(row.get(field)).strip()
    }
    if linked_values & identity_values:
        return True
    # Names are only a legacy fallback when no usable identifier is present.
    if linked_values:
        return False
    row_names = {
        str(row.get(field)).strip().casefold()
        for field in ("distributor_name", "name")
        if row.get(field) is not None and str(row.get(field)).strip()
    }
    return bool(row_names & names)





async def _canonical_distributor_ledger_transactions(
    distributor: dict,
    requested_id: Optional[str] = None,
    raw_transactions: Optional[List[dict]] = None,
    purchase_orders: Optional[List[dict]] = None,
) -> List[dict]:
    """Build the single canonical transaction set used for distributor accounting.

    This keeps the accounting transaction stream intact for existing balance
    logic. The distributor ledger endpoint applies its own display-only
    purchase invoice dedupe after this function returns.
    """
    did = str(distributor.get("id") or requested_id or "")
    identity_values = _distributor_identity_values(distributor, requested_id)
    names = {
        str(distributor.get(field)).strip().casefold()
        for field in ("name", "distributor_name")
        if distributor.get(field) and str(distributor.get(field)).strip()
    }

    if raw_transactions is None:
        raw_transactions = await db.distributor_transactions.find({}, {"_id": 0}).to_list(10000)
    # Purchase Orders are inventory documents only. Do not project purchase_orders
    # into Distributor Ledger; payable balances must be based solely on persisted
    # distributor_transactions plus opening-balance normalization.
    txns = [
        _annotate_distributor_transaction_source(txn)
        for txn in raw_transactions
        if _belongs_to_distributor(txn, identity_values, names)
    ]

    canonical = _distributor_opening_balance_deduped_transactions(distributor, txns)
    canonical.sort(key=_distributor_fifo_sort_key)
    return canonical


def _debug_purchase_invoice_identity(txn: dict) -> List[str]:
    return sorted(_purchase_invoice_reference_values(txn))


def _debug_purchase_invoice_dedupe_key(txn: dict, distributor_id: Optional[str] = None) -> List[str]:
    return [repr(key) for key in _purchase_invoice_identity_keys(txn, distributor_id)]


def _with_distributor_ledger_debug_fields(txn: dict, distributor_id: Optional[str] = None) -> dict:
    debugged = dict(txn if isinstance(txn, dict) else {})
    source = debugged.get("backend_row_source") or debugged.get("source") or "distributor_transactions"
    debugged["_debug_source"] = source
    debugged["_debug_is_synthetic"] = bool(debugged.get("is_synthetic"))
    debugged["_debug_transaction_id"] = _serializable_transaction_id(
        debugged.get("transaction_id") or debugged.get("id") or debugged.get("_id")
    )
    debugged["_debug_purchase_order_id"] = _serializable_transaction_id(
        debugged.get("purchase_order_id") or debugged.get("po_id") or debugged.get("matched_purchase_order_id")
    )
    debugged["_debug_invoice_identity"] = _debug_purchase_invoice_identity(debugged)
    debugged["_debug_dedupe_key"] = _debug_purchase_invoice_dedupe_key(debugged, distributor_id)
    skip_reason = debugged.get("synthetic_purchase_order_skip_reason") or debugged.get("_debug_skip_reason")
    if skip_reason:
        debugged["_debug_skip_reason"] = skip_reason
    return debugged


FORENSIC_DISTRIBUTOR_NAMES = (
    "ABHI ENTERPRISES",
    "ARORA MEDICOSE",
    "BALAJI PHARMA",
    "KAPIL MEDICOSE",
    "KISSAN MEDICAL AGENCY",
    "MIDHA DISTRIBUTORS",
    "R K PHARMA",
    "VISHAL SURGICAL",
)


def _forensic_row_identity(txn: dict) -> str:
    return "|".join(str(txn.get(field) or "") for field in (
        "backend_row_source", "source", "transaction_id", "id",
        "purchase_order_id", "purchase_order_object_id",
    ))


def _forensic_row(txn: dict, distributor_id: Optional[str] = None) -> dict:
    row = _with_distributor_ledger_debug_fields(txn, distributor_id)
    row_date = _distributor_transaction_date(row)
    invoice_variants = _debug_purchase_invoice_identity(row)
    return _json_safe_ledger_transaction({
        "date": row_date.isoformat() if row_date else None,
        "type": row.get("type"),
        "amount": row.get("amount"),
        "invoice_ref": (
            row.get("invoice_number") or row.get("invoice_no") or row.get("invoice_ref")
            or row.get("bill_number") or row.get("bill_no") or row.get("reference_number")
            or row.get("reference")
        ),
        "source": row.get("backend_row_source") or row.get("source"),
        "is_synthetic": bool(row.get("is_synthetic")),
        "transaction_id": _serializable_transaction_id(row.get("transaction_id") or row.get("id")),
        "purchase_order_id": _serializable_transaction_id(
            row.get("purchase_order_id") or row.get("po_id") or row.get("matched_purchase_order_id")
        ),
        "opening_balance": bool(row.get("is_opening_balance") or _is_explicit_opening_balance_transaction(row, distributor_id)),
        "generated_synthetic": bool(row.get("is_system_generated") or row.get("is_synthetic")),
        "dedupe_key": _debug_purchase_invoice_dedupe_key(row, distributor_id),
        "invoice_identity_variants": invoice_variants,
        "raw_id": _serializable_transaction_id(row.get("id")),
        "skip_reason": row.get("synthetic_purchase_order_skip_reason") or row.get("_debug_skip_reason"),
    }, include_items=False)


def _forensic_duplicate_fingerprint(row: dict) -> tuple:
    if row.get("dedupe_key"):
        return ("purchase_identity", tuple(row.get("dedupe_key") or []))
    return (
        row.get("type"),
        row.get("date"),
        _round_ledger_money(row.get("amount")),
        tuple(row.get("invoice_identity_variants") or []),
        row.get("invoice_ref"),
    )


def _forensic_removed_reason(before: dict, after_rows: List[dict], distributor_id: str) -> dict:
    after_keys = {
        key
        for row in after_rows
        for key in _purchase_invoice_identity_keys(row, distributor_id)
    }
    before_keys = set(_purchase_invoice_identity_keys(before, distributor_id))
    if before_keys & after_keys:
        return {
            "rule": "display purchase invoice dedupe",
            "duplicate_reason": "shared distributor/invoice/date/amount or purchase_order_id identity with retained row",
            "correctness": "needs human review against live source documents",
        }
    if _is_duplicate_opening_balance_row(before, {"id": distributor_id}, None):
        return {
            "rule": "opening balance duplicate suppression",
            "duplicate_reason": "row was labelled as opening balance and matched opening-balance amount/reference/date",
            "correctness": "correct if this is a legacy opening-balance mirror row",
        }
    return {
        "rule": "unknown/non-display removal",
        "duplicate_reason": "row is absent after canonicalization but no forensic rule matched",
        "correctness": "incorrect until proven otherwise",
    }


def _admin_debug_distributor_ledger_row(txn: dict, distributor_id: Optional[str] = None) -> dict:
    row = _with_distributor_ledger_debug_fields(txn, distributor_id)
    row_date = _distributor_transaction_date(row)
    return _json_safe_ledger_transaction({
        "date": row_date.isoformat() if row_date else None,
        "type": row.get("type"),
        "amount": row.get("amount"),
        "invoice_no": row.get("invoice_no") or row.get("invoice_number"),
        "invoice_ref": row.get("invoice_ref") or row.get("invoice_number"),
        "bill_no": row.get("bill_no") or row.get("bill_number"),
        "reference_no": row.get("reference_no") or row.get("reference_number") or row.get("reference"),
        "source": row.get("backend_row_source") or row.get("source"),
        "is_synthetic": bool(row.get("is_synthetic")),
        "transaction_id": _serializable_transaction_id(row.get("transaction_id") or row.get("id")),
        "purchase_order_id": _serializable_transaction_id(
            row.get("purchase_order_id") or row.get("po_id") or row.get("matched_purchase_order_id")
        ),
        "dedupe_key": _debug_purchase_invoice_dedupe_key(row, distributor_id),
        "opening_balance": bool(
            row.get("is_opening_balance")
            or _is_explicit_opening_balance_transaction(row, distributor_id)
        ),
        "created_at": row.get("created_at"),
        "skip_reason": row.get("synthetic_purchase_order_skip_reason") or row.get("_debug_skip_reason"),
    }, include_items=False)


async def _admin_distributor_ledger_debug_report(distributor_id: str) -> dict:
    dist = await db.distributors.find_one({"id": distributor_id}, {"_id": 0})
    if not dist:
        candidates = await db.distributors.find({}).to_list(5000)
        dist = next((row for row in candidates if distributor_id in _distributor_identity_values(row)), None)
    if not dist:
        raise HTTPException(status_code=404, detail="Distributor not found")
    if dist.get("_id") is not None:
        dist["_legacy_object_id"] = str(dist.pop("_id"))

    identity_values = _distributor_identity_values(dist, distributor_id)
    names = {
        str(dist.get(field)).strip().casefold()
        for field in ("name", "distributor_name")
        if dist.get(field) and str(dist.get(field)).strip()
    }
    raw_transactions = await db.distributor_transactions.find({}, {"_id": 0}).to_list(10000)
    raw_purchase_orders = (
        await db.purchase_orders.find({}, {"_id": 0}).to_list(10000)
        if hasattr(db, "purchase_orders")
        else []
    )
    matching_transactions = [
        txn for txn in raw_transactions
        if _belongs_to_distributor(txn, identity_values, names)
    ]
    matching_purchase_orders = [
        po for po in raw_purchase_orders
        if _belongs_to_distributor(po, identity_values, names)
    ]

    canonical_rows = await _canonical_distributor_ledger_transactions(
        dist,
        distributor_id,
        raw_transactions=raw_transactions,
        purchase_orders=raw_purchase_orders,
    )
    final_rows = _dedupe_distributor_purchase_invoice_rows(
        canonical_rows,
        str(dist.get("id") or distributor_id),
    )
    final_identities = {_forensic_row_identity(row) for row in final_rows}
    removed_rows = [
        row for row in canonical_rows
        if _forensic_row_identity(row) not in final_identities
    ]

    synthetic_po_rows = [
        row for row in canonical_rows
        if row.get("backend_row_source") == "purchase_orders" and row.get("is_synthetic")
    ]

    return {
        "distributor": {
            "id": dist.get("id") or distributor_id,
            "name": dist.get("name") or dist.get("distributor_name"),
        },
        "counts": {
            "raw_distributor_transactions": len(matching_transactions),
            "raw_purchase_orders": len(matching_purchase_orders),
            "synthetic_po_rows_generated": len(synthetic_po_rows),
            "rows_removed_by_dedupe": len(removed_rows),
            "final_rows_returned": len(final_rows),
        },
        "rows_before_dedupe": [
            _admin_debug_distributor_ledger_row(row, str(dist.get("id") or distributor_id))
            for row in canonical_rows
        ],
        "synthetic_po_rows_generated": [
            _admin_debug_distributor_ledger_row(row, str(dist.get("id") or distributor_id))
            for row in synthetic_po_rows
        ],
        "rows_removed_by_dedupe": [
            _admin_debug_distributor_ledger_row(row, str(dist.get("id") or distributor_id))
            for row in removed_rows
        ],
        "final_rows_returned": [
            _admin_debug_distributor_ledger_row(row, str(dist.get("id") or distributor_id))
            for row in final_rows
        ],
    }


@api_router.get("/admin/distributor-ledger-debug/{distributor_id}")
async def admin_distributor_ledger_debug(
    distributor_id: str,
    user: dict = Depends(require_role("admin")),
):
    return await _admin_distributor_ledger_debug_report(distributor_id)


async def _distributor_ledger_forensic_audit_for_dist(dist: dict, requested_id: Optional[str] = None) -> dict:
    did = str(dist.get("id") or requested_id or "")
    before = await _canonical_distributor_ledger_transactions(
        dist, requested_id
    )
    after = _dedupe_distributor_purchase_invoice_rows(before, did)
    before_report = [_forensic_row(row, did) for row in before]
    after_report = [_forensic_row(row, did) for row in after]
    after_identities = {_forensic_row_identity(row) for row in after}

    removed = []
    for row in before:
        if _forensic_row_identity(row) in after_identities:
            continue
        report_row = _forensic_row(row, did)
        report_row["removal_analysis"] = _forensic_removed_reason(row, after, did)
        removed.append(report_row)

    grouped: Dict[tuple, List[dict]] = defaultdict(list)
    for row in after_report:
        grouped[_forensic_duplicate_fingerprint(row)].append(row)
    surviving_duplicates = [
        {
            "fingerprint": [str(part) for part in fingerprint],
            "rows": rows,
            "explanation": (
                "purchase rows survived because they do not share a purchase invoice dedupe key"
                if fingerprint and fingerprint[0] == "purchase_identity"
                else "non-purchase rows survive because display dedupe intentionally only collapses purchase/sale invoice identities"
            ),
        }
        for fingerprint, rows in grouped.items()
        if len(rows) > 1
    ]

    return {
        "distributor": {"id": did, "name": dist.get("name") or dist.get("distributor_name")},
        "before_dedupe": before_report,
        "after_dedupe": after_report,
        "removed_rows": removed,
        "surviving_duplicate_pairs": surviving_duplicates,
        "counts": {
            "before": len(before_report),
            "after": len(after_report),
            "removed": len(removed),
            "surviving_duplicate_groups": len(surviving_duplicates),
        },
    }


@api_router.get("/ledger/distributor-forensic-audit")
async def distributor_ledger_forensic_audit(user: dict = Depends(require_role("admin"))):
    distributors = await db.distributors.find({}, {"_id": 0}).to_list(10000)
    wanted = {name.casefold() for name in FORENSIC_DISTRIBUTOR_NAMES}
    reports = []
    for dist in distributors:
        if str(dist.get("name") or dist.get("distributor_name") or "").strip().casefold() in wanted:
            reports.append(await _distributor_ledger_forensic_audit_for_dist(dist, dist.get("id")))
    return {"distributors": reports}


def _distributor_ledger_totals(transactions: List[dict]) -> dict:
    balance = 0.0
    totals = {"total_purchases": 0.0, "total_paid": 0.0, "total_adjustments": 0.0, "balance": 0.0}
    for txn in transactions:
        balance, bucket = _apply_distributor_transaction(balance, txn)
        amount = _safe_float(txn.get("amount", 0) if isinstance(txn, dict) else 0)
        if bucket == "purchase":
            totals["total_purchases"] += amount
        elif bucket == "adjustment":
            totals["total_adjustments"] += amount
        else:
            totals["total_paid"] += amount
    totals["balance"] = balance
    return {key: _round_ledger_money(value) for key, value in totals.items()}

@api_router.get("/ledger/distributor/{did}")
async def distributor_ledger(
    did: str,
    financial_year: Optional[str] = None,
    search: Optional[str] = None,
    invoice_number: Optional[str] = None,
    reference_number: Optional[str] = None,
    payment_mode: Optional[str] = None,
    transaction_type: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    amount: Optional[float] = None,
    user: dict = Depends(get_current_user),
):
    financial_year = _normalize_distributor_ledger_financial_year(financial_year)

    dist = await db.distributors.find_one({"id": did}, {"_id": 0})
    if not dist:
        candidates = await db.distributors.find({}).to_list(5000)
        dist = next((row for row in candidates if did in _distributor_identity_values(row)), None)
    if not dist:
        raise HTTPException(status_code=404, detail="Distributor not found")
    if dist.get("_id") is not None:
        dist["_legacy_object_id"] = str(dist.pop("_id"))

    opening_balance_date = _distributor_opening_balance_date(dist)
    if opening_balance_date:
        dist["opening_balance_date"] = opening_balance_date

    # Materialize persisted distributor ledger rows only, then make the
    # post-dedupe rows the sole source for display, running balances, period
    # totals, and financial-year carry metadata.
    canonical_txns = await _canonical_distributor_ledger_transactions(
            dist,
            did,
        )
    ledger_txns = _final_distributor_ledger_rows(
        canonical_txns,
        str(dist.get("id") or did),
    )
    available_financial_years = _available_financial_years(ledger_txns)
    financial_year_metadata = _distributor_financial_year_metadata(
        ledger_txns,
        financial_year,
    )
    display_txns = (
        _filter_transactions_by_financial_year(ledger_txns, financial_year)
        if financial_year
        else list(ledger_txns)
    )
    display_txn_ids = {_forensic_row_identity(txn) for txn in display_txns}
    try:
        fifo_metadata_by_id = _build_distributor_fifo_metadata(
            _distributor_fifo_metadata_transactions(ledger_txns, financial_year),
            did,
        )
    except Exception:
        logger.exception(
            "Failed to build distributor FIFO metadata; returning ledger without FIFO metadata. distributor_id=%s",
            did,
        )
        fifo_metadata_by_id = {}

    balance = 0.0
    running = []
    total_purchases = 0.0
    total_paid = 0.0
    total_adjustments = 0.0

    for txn in ledger_txns:
        balance, bucket = _apply_distributor_transaction(balance, txn)
        txn_amount = _safe_float(txn.get("amount", 0) if isinstance(txn, dict) else 0)

        if _forensic_row_identity(txn) not in display_txn_ids:
            continue

        if bucket == "purchase":
            total_purchases += txn_amount
        elif bucket == "adjustment":
            total_adjustments += txn_amount
        else:
            total_paid += txn_amount

        txn_id = _serializable_transaction_id(txn.get("id") if isinstance(txn, dict) else None)
        running.append(_json_safe_ledger_transaction({
            **(txn if isinstance(txn, dict) else {}),
            **fifo_metadata_by_id.get(txn_id, {}),
            "running_balance": round(balance, 2),
            "settlement_status": (
                "settled" if fifo_metadata_by_id.get(txn_id, {}).get("due_amount") == 0
                else "partially_settled" if _safe_float(fifo_metadata_by_id.get(txn_id, {}).get("paid_amount")) > 0
                else "unpaid"
            ) if bucket == "purchase" else None,
        }, include_items=True))

    def matches_filters(txn: dict) -> bool:
        txn_date = _distributor_transaction_date(txn)
        searchable = " ".join(str(txn.get(field, "")) for field in (
            "invoice_number", "invoice_no", "bill_number", "bill_no",
            "reference_number", "reference_no", "reference", "payment_mode", "mode", "type",
        )).lower()
        return (
            (not search or search.strip().lower() in searchable)
            and (not invoice_number or invoice_number.lower() in " ".join(str(txn.get(f, "")).lower() for f in ("invoice_number", "invoice_no", "bill_number", "bill_no")))
            and (not reference_number or reference_number.lower() in " ".join(str(txn.get(f, "")).lower() for f in ("reference_number", "reference_no", "reference")))
            and (not payment_mode or payment_mode.lower() == str(txn.get("payment_mode") or txn.get("mode") or "").lower())
            and (not transaction_type or transaction_type.lower() == str(txn.get("type") or "").lower())
            and (not date_from or (txn_date is not None and txn_date >= date_from))
            and (not date_to or (txn_date is not None and txn_date <= date_to))
            and (amount is None or _round_ledger_money(txn.get("amount")) == _round_ledger_money(amount))
        )

    filtered_running = [txn for txn in running if matches_filters(txn)]
    selected_period_net_balance = _round_ledger_money(total_purchases - total_paid - total_adjustments)
    response = {
        "distributor": dist,
        "transactions": filtered_running,
        "balance": round(balance, 2),
        "total_purchases": round(total_purchases, 2),
        "total_paid": round(total_paid, 2),
        "total_adjustments": round(total_adjustments, 2),
        "balance_for_selected_period": selected_period_net_balance,
        "net_balance_for_selected_period": selected_period_net_balance,
        "payable_for_selected_period": _round_ledger_money(max(selected_period_net_balance, 0)),
        "receivable_for_selected_period": _round_ledger_money(abs(min(selected_period_net_balance, 0))),
        "available_financial_years": available_financial_years,
        "current_financial_year": _current_financial_year(),
        **financial_year_metadata,
    }
    return response


def _distributor_transaction_edit_user(user: dict) -> str:
    return str(
        user.get("id")
        or user.get("email")
        or user.get("name")
        or "unknown"
    )


def _distributor_transaction_old_value(txn: dict, field_name: str):
    if field_name == "payment_mode":
        return txn.get("payment_mode", txn.get("mode"))
    return txn.get(field_name)


@api_router.patch("/distributor-transactions/{transaction_id}")
async def update_distributor_transaction(
    transaction_id: str,
    payload: DistributorTransactionUpdate,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    changes = payload.model_dump(exclude_unset=True)
    if not changes:
        raise HTTPException(
            status_code=400,
            detail="At least one editable transaction field is required",
        )

    if transaction_id.startswith("opening-balance-"):
        changes = _normalize_opening_balance_update_changes(changes)
        if not changes:
            raise HTTPException(
                status_code=400,
                detail="At least one editable transaction field is required",
            )
        if not set(changes).issubset(OPENING_BALANCE_NORMALIZED_EDITABLE_FIELDS):
            raise HTTPException(
                status_code=400,
                detail="Opening balance can only edit invoice/bill/receipt number, notes/reference, and opening balance date",
            )

        distributor_id = transaction_id.removeprefix("opening-balance-")
        dist = await db.distributors.find_one({"id": distributor_id}, {"_id": 0})
        if not dist:
            raise HTTPException(status_code=404, detail="Transaction not found")

        old_values = {
            "invoice_number": dist.get("opening_balance_invoice_number"),
            "bill_number": dist.get("opening_balance_bill_number"),
            "receipt_number": dist.get("opening_balance_receipt_number"),
            "reference_number": dist.get("opening_balance_reference_number"),
            "notes": dist.get("opening_balance_notes"),
            "opening_balance_date": dist.get("opening_balance_date"),
        }
        old_values = {field_name: old_values.get(field_name) for field_name in changes}

        edited_at = datetime.now(timezone.utc).isoformat()
        edited_by = _distributor_transaction_edit_user(user)
        set_values = {
            f"opening_balance_{field_name}": value
            for field_name, value in changes.items()
            if field_name != "opening_balance_date"
        }
        if "opening_balance_date" in changes:
            set_values["opening_balance_date"] = changes["opening_balance_date"]
        set_values["opening_balance_edited_at"] = edited_at
        set_values["opening_balance_edited_by"] = edited_by

        updated_dist = await db.distributors.find_one_and_update(
            {"id": distributor_id},
            {
                "$set": set_values,
                "$push": {
                    "opening_balance_edit_history": {
                        "edited_at": edited_at,
                        "edited_by": edited_by,
                        "old_values": old_values,
                        "new_values": dict(changes),
                    }
                },
            },
            projection={"_id": 0},
            return_document=ReturnDocument.AFTER,
        )

        if not updated_dist:
            raise HTTPException(status_code=404, detail="Transaction not found")

        if "opening_balance_date" in changes:
            await _sync_distributor_opening_balance_transaction_date(
                updated_dist,
                changes["opening_balance_date"],
            )

        opening_txn = _opening_balance_transaction(updated_dist)
        opening_txn["edited_at"] = updated_dist.get("opening_balance_edited_at")
        opening_txn["edited_by"] = updated_dist.get("opening_balance_edited_by")
        opening_txn["edit_history"] = updated_dist.get("opening_balance_edit_history", [])
        return opening_txn

    txn = await db.distributor_transactions.find_one(
        {"id": transaction_id},
        {"_id": 0},
    )
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")

    is_opening_txn = _is_opening_balance_transaction(txn, txn.get("distributor_id"))
    if is_opening_txn:
        changes = _normalize_opening_balance_update_changes(changes)
        if not changes:
            raise HTTPException(
                status_code=400,
                detail="At least one editable transaction field is required",
            )
        if not set(changes).issubset(OPENING_BALANCE_NORMALIZED_EDITABLE_FIELDS):
            raise HTTPException(
                status_code=400,
                detail="Opening balance can only edit invoice/bill/receipt number, notes/reference, and opening balance date",
            )
    else:
        changes = _strip_normal_transaction_date_changes(changes)
        if not changes:
            raise HTTPException(
                status_code=400,
                detail="At least one editable transaction field is required",
            )
        if not set(changes).issubset(DISTRIBUTOR_TRANSACTION_EDITABLE_FIELDS):
            raise HTTPException(
                status_code=400,
                detail="Distributor transactions can only edit invoice/bill/receipt/reference number, payment mode, and notes",
            )

    old_values = {
        field_name: _distributor_transaction_old_value(txn, field_name)
        for field_name in changes
    }
    new_values = dict(changes)

    edited_at = datetime.now(timezone.utc).isoformat()
    edited_by = _distributor_transaction_edit_user(user)

    set_values = {
        field_name: value
        for field_name, value in changes.items()
        if field_name != "payment_mode"
    }
    if "payment_mode" in changes:
        set_values["payment_mode"] = changes["payment_mode"]
        set_values["mode"] = changes["payment_mode"]

    if is_opening_txn and "opening_balance_date" in changes:
        set_values["transaction_date"] = changes["opening_balance_date"]
        set_values["date"] = changes["opening_balance_date"]

    set_values["edited_at"] = edited_at
    set_values["edited_by"] = edited_by

    updated = await db.distributor_transactions.find_one_and_update(
        {"id": transaction_id},
        {
            "$set": set_values,
            "$push": {
                "edit_history": {
                    "edited_at": edited_at,
                    "edited_by": edited_by,
                    "old_values": old_values,
                    "new_values": new_values,
                }
            },
        },
        projection={"_id": 0},
        return_document=ReturnDocument.AFTER,
    )

    if not updated:
        raise HTTPException(status_code=404, detail="Transaction not found")

    if is_opening_txn:
        if "opening_balance_date" in changes:
            await db.distributors.update_one(
                {"id": updated.get("distributor_id")},
                {"$set": {"opening_balance_date": changes["opening_balance_date"]}},
            )

        distributor = await db.distributors.find_one(
            {"id": updated.get("distributor_id")},
            {"_id": 0},
        ) or {}
        return _normalize_opening_balance_transaction(updated, distributor)

    return updated


@api_router.delete("/ledger/distributor/{did}/transaction/{txn_id}")
async def delete_distributor_txn(
    did: str,
    txn_id: str,
    user: dict = Depends(require_role("admin", "pharmacist"))
):
    if txn_id == _opening_balance_transaction_id(did):
        raise HTTPException(status_code=400, detail="Opening balance cannot be deleted")

    txn = await db.distributor_transactions.find_one(
        {"id": txn_id, "distributor_id": did},
        {"_id": 0},
    )
    if txn and _is_opening_balance_transaction(txn, did):
        raise HTTPException(status_code=400, detail="Opening balance cannot be deleted")

    result = await db.distributor_transactions.delete_one({
        "id": txn_id,
        "distributor_id": did
    })

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Transaction not found")

    return {"ok": True}

    
@api_router.post("/ledger/distributor/{did}/purchase")
async def add_purchase(
    did: str,
    p: PaymentCreate,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    payment_mode = _validate_distributor_purchase_mode(p)

    txn_date = p.date or datetime.now(timezone.utc).isoformat()
    purchase_txn = {
        "id": str(uuid.uuid4()),
        "distributor_id": did,
        "type": "purchase",
        "amount": p.amount,
        "created_at": txn_date,
        "entry_source": "distributor_ledger",
        **_distributor_transaction_metadata(p),
    }

    inserted_transactions = [purchase_txn]
    auto_payment_txn = None

    if payment_mode in {"cash", "upi"}:
        auto_payment_txn = {
            "id": str(uuid.uuid4()),
            "distributor_id": did,
            "type": "payment",
            "amount": p.amount,
            "created_at": txn_date,
            "mode": payment_mode,
            "payment_mode": payment_mode,
            "notes": f"Auto payment for {payment_mode} purchase",
            "linked_transaction_id": purchase_txn["id"],
            "originating_purchase_transaction_id": purchase_txn["id"],
            "is_auto_generated": True,
            "receipt_number": p.receipt_number,
            "invoice_number": p.invoice_number,
            "bill_number": p.bill_number,
            "reference_number": p.reference_number,
        }
        inserted_transactions.append(auto_payment_txn)

    await db.distributor_transactions.insert_many(inserted_transactions)

    for txn in inserted_transactions:
        txn.pop("_id", None)

    if auto_payment_txn:
        return {
            **purchase_txn,
            "auto_payment_transaction": auto_payment_txn,
            "transactions": inserted_transactions,
        }

    return purchase_txn


@api_router.post("/ledger/distributor/{did}/payment")
async def add_dist_payment(
    did: str,
    p: PaymentCreate,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    txn = {
        "id": str(uuid.uuid4()),
        "distributor_id": did,
        "type": "payment",
        "amount": p.amount,
        "created_at": p.date or datetime.now(timezone.utc).isoformat(),
        "entry_source": "distributor_ledger",
        **_distributor_transaction_metadata(p),
    }
    await db.distributor_transactions.insert_one(txn)
    txn.pop("_id", None)
    return txn


# ---------------- Purchase Returns / Expiry Returns ----------------
def _purchase_return_query(
    start: Optional[str] = None,
    end: Optional[str] = None,
    distributor: Optional[str] = None,
    medicine: Optional[str] = None,
    reason: Optional[str] = None,
    ledger_adjusted: Optional[bool] = None,
    search: Optional[str] = None,
) -> dict:
    query = {}

    if start or end:
        query["return_date"] = {}
        if start:
            query["return_date"]["$gte"] = start
        if end:
            query["return_date"]["$lte"] = end

    if distributor:
        query["distributor"] = {"$regex": distributor, "$options": "i"}

    if medicine:
        query["medicine_name"] = {"$regex": medicine, "$options": "i"}

    if reason:
        query["reason"] = reason

    if ledger_adjusted is not None:
        query["ledger_adjusted"] = ledger_adjusted

    if search:
        query["$or"] = [
            {"distributor": {"$regex": search, "$options": "i"}},
            {"medicine_name": {"$regex": search, "$options": "i"}},
            {"batch_number": {"$regex": search, "$options": "i"}},
            {"reason": {"$regex": search, "$options": "i"}},
            {"notes": {"$regex": search, "$options": "i"}},
        ]

    query["deleted_at"] = {"$exists": False}
    query["voided_at"] = {"$exists": False}
    query["settlement_status"] = {"$ne": "deleted"}
    return query


def _available_batch_stock(medicine: dict) -> float:
    return _available_stock(medicine)


def _return_public(return_doc: dict) -> dict:
    public = {key: value for key, value in return_doc.items() if key != "_id"}
    return _normalized_purchase_return_money(public)


def _normalized_purchase_return_money(return_doc: dict) -> dict:
    """Expose accounting values consistently without making legacy records unreadable."""
    normalized = dict(return_doc)
    for field in ("purchase_rate", "gst_rate", "return_amount", "settled_return_value"):
        if normalized.get(field) is not None:
            normalized[field] = _money_float(_to_decimal(normalized[field]))
    if normalized.get("return_amount") is None:
        normalized["return_amount"] = _purchase_return_credit(normalized)
    normalized = _ensure_action_aliases(normalized)
    status, adjustment_type, _bucket = _purchase_return_business_status(normalized)
    normalized["status"] = status
    normalized["adjustment_type"] = adjustment_type
    return normalized


def _purchase_return_settlement_status(return_doc: dict) -> str:
    if return_doc.get("deleted_at") or return_doc.get("voided_at"):
        return "deleted"
    if return_doc.get("settlement_status"):
        return return_doc["settlement_status"]
    if return_doc.get("po_adjustment_id") or return_doc.get("settled_by_po"):
        return "settled_by_po"
    if return_doc.get("ledger_adjusted") or return_doc.get("adjust_distributor_ledger"):
        return "ledger_adjusted"
    return "unsettled"




def _purchase_return_business_status(return_doc: dict) -> Tuple[str, str, str]:
    settlement = _purchase_return_settlement_status(return_doc)
    if settlement == "deleted":
        return "Deleted / Voided", "deleted", "deleted"
    if settlement == "settled_by_po":
        return "Adjusted in Purchase", "purchase_adjustment", "adjusted_in_purchase"
    if return_doc.get("ledger_adjusted") or return_doc.get("adjust_distributor_ledger"):
        return "Ledger Adjusted", "ledger_adjustment", "ledger_adjusted"
    return "Credit Pending / Recorded Only", "recorded_only", "pending_credit"


def _purchase_return_report_status_label(return_doc: dict) -> str:
    if return_doc.get("adjust_ledger") is True or return_doc.get("ledger_adjusted") is True:
        return "Ledger Adjusted"
    if (
        return_doc.get("adjusted_in_purchase") is True
        or return_doc.get("consumed_in_po") is True
        or bool(return_doc.get("po_adjustment_id"))
        or bool(return_doc.get("settled_by_po"))
    ):
        return "Adjusted in Purchase"
    return "Credit Pending"


def _purchase_return_analytics_row(return_doc: dict) -> dict:
    quantity = _safe_float(return_doc.get("return_quantity"))
    purchase_rate = _safe_float(return_doc.get("purchase_rate"))
    medicine_name = return_doc.get("medicine_name") or return_doc.get("medicine") or return_doc.get("name") or "Unknown"
    distributor_name = return_doc.get("distributor_name") or return_doc.get("distributor") or return_doc.get("supplier_name") or "Unknown"
    return_value = _money_float(_to_decimal(quantity) * _to_decimal(purchase_rate))
    status_label = _purchase_return_report_status_label(return_doc)
    return {
        "medicine_name": medicine_name,
        "distributor_name": distributor_name,
        "return_quantity": round_qty(quantity),
        "purchase_rate": _money_float(_to_decimal(purchase_rate)),
        "return_value": return_value,
        "return_date": return_doc.get("return_date") or return_doc.get("date") or return_doc.get("created_at"),
        "status_label": status_label,
        # Backward-compatible aliases for existing report consumers.
        "medicine": medicine_name,
        "distributor": distributor_name,
        "returned_qty": round_qty(quantity),
        "status": status_label if status_label != "Credit Pending" else "Credit Pending / Recorded Only",
    }

async def _find_purchase_return_medicine(payload: PurchaseReturnCreate, session=None) -> dict:
    batch_filter = {"batch_no": payload.batch_number}
    lookup_filters = [
        {**batch_filter, "name": payload.medicine_name},
    ]

    if payload.medicine_key:
        lookup_filters.append({**batch_filter, "medicine_key": payload.medicine_key})

    if payload.medicine_id:
        lookup_filters.append({"id": payload.medicine_id})

    candidates = await db.medicines.find(
        {"$or": lookup_filters},
        {"_id": 0},
        session=session,
    ).to_list(10000)

    distributor_id = _normalized_stock_match_value(payload.distributor_id or payload.distributor)
    distributor_name = _normalized_stock_match_value(payload.distributor)
    if distributor_id or distributor_name:
        distributor_matches = [
            medicine for medicine in candidates
            if (
                distributor_id
                and _normalized_stock_match_value(medicine.get("distributor_id")) == distributor_id
            )
            or (
                distributor_name
                and _normalized_stock_match_value(
                    medicine.get("distributor_name") or medicine.get("distributor")
                ) == distributor_name
            )
        ]
        if distributor_matches:
            candidates = distributor_matches

    if payload.expiry_date:
        expiry = _normalized_stock_expiry(payload.expiry_date)
        expiry_matches = [
            medicine for medicine in candidates
            if _normalized_stock_expiry(medicine.get("expiry_date")) == expiry
        ]
        if expiry_matches:
            candidates = expiry_matches

    if payload.purchase_rate not in (None, ""):
        rate_matches = [
            medicine for medicine in candidates
            if _round_money(_to_decimal(medicine.get("purchase_price"))) == _round_money(_to_decimal(payload.purchase_rate))
        ]
        if rate_matches:
            candidates = rate_matches

    medicine = candidates[0] if len(candidates) == 1 else None

    if not medicine:
        raise HTTPException(
            status_code=400,
            detail="Medicine stock lot not found or ambiguous",
        )

    return medicine



def _normalize_purchase_return_expiry(value: Optional[str]) -> Optional[str]:
    if value in (None, ""):
        return value
    parsed = parse_expiry_date(value)
    return parsed.isoformat() if parsed else value

def _purchase_return_ledger_transaction(return_doc: dict) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "distributor_id": return_doc.get("distributor_id"),
        "type": "purchase_return",
        "subtype": "Purchase Return / Expiry Return",
        "direction": "credit_adjustment",
        "amount": _money_float(_to_decimal(return_doc["return_amount"])),
        "reference": return_doc["id"],
        "return_id": return_doc["id"],
        "notes": (
            f"{return_doc.get('medicine_name', '')}, "
            f"Batch {return_doc.get('batch_number', '')}, "
            f"Qty {return_doc.get('return_quantity', 0)}, "
            f"Reason: {return_doc.get('reason', '')}"
        ),
        "created_at": return_doc.get("return_date") or return_doc.get("created_at"),
        "transaction_date": return_doc.get("return_date") or return_doc.get("created_at"),
        "date": return_doc.get("return_date") or return_doc.get("created_at"),
    }


def _normalized_stock_match_value(value) -> str:
    return str(value or "").strip().casefold()


def _normalized_stock_expiry(value) -> str:
    parsed = parse_expiry_date(value)
    return parsed.isoformat() if parsed else _normalized_stock_match_value(value)


def _purchase_return_quantity(return_doc: dict) -> float:
    return _stock_quantity(
        return_doc,
        "return_quantity",
        "quantity",
        "returned_quantity",
        "returned_units",
        "purchase_return_units",
    )


def _match_purchase_return_medicine(
    return_doc: dict, medicines: List[dict], tenant_id: Optional[str] = None
) -> Optional[dict]:
    """Safely resolve a legacy purchase return to one unambiguous medicine batch."""
    return_id = return_doc.get("id")

    def unique_match(candidates: List[dict], method: str) -> Optional[dict]:
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            logger.warning(
                "Purchase return stock repair ambiguous match tenant/shop=%s return_id=%s method=%s candidates=%s",
                tenant_id, return_id, method, [item.get("id") or item.get("medicine_key") for item in candidates],
            )
        return None

    medicine_id = return_doc.get("medicine_id")
    if medicine_id:
        id_matches = [medicine for medicine in medicines if medicine.get("id") == medicine_id]
        match = unique_match(id_matches, "medicine_id")
        if match:
            return match
        if len(id_matches) > 1:
            return None

    medicine_key = _normalized_stock_match_value(return_doc.get("medicine_key"))
    if medicine_key:
        key_matches = [
            medicine for medicine in medicines
            if _normalized_stock_match_value(medicine.get("medicine_key")) == medicine_key
        ]
        match = unique_match(key_matches, "medicine_key")
        if match:
            return match
        if len(key_matches) > 1:
            return None
        matches = list(medicines)
    else:
        matches = list(medicines)

    name = _normalized_stock_match_value(return_doc.get("medicine_name") or return_doc.get("name"))
    batch = _normalized_stock_match_value(return_doc.get("batch_number") or return_doc.get("batch_no"))
    if not name or not batch:
        return None

    matches = [
        medicine for medicine in matches
        if _normalized_stock_match_value(medicine.get("name") or medicine.get("medicine_name")) == name
        and _normalized_stock_match_value(medicine.get("batch_no") or medicine.get("batch_number")) == batch
    ]

    expiry = return_doc.get("expiry_date")
    if expiry:
        matches = [
            medicine for medicine in matches
            if _normalized_stock_expiry(medicine.get("expiry_date")) == _normalized_stock_expiry(expiry)
        ]

    distributor_id = return_doc.get("distributor_id")
    if distributor_id:
        matches = [medicine for medicine in matches if medicine.get("distributor_id") == distributor_id]

    return unique_match(matches, "name/batch/expiry/distributor")


def _inventory_derivatives(medicine: dict, purchase_return_units: float) -> dict:
    purchase_return_units = round_qty(purchase_return_units)
    refreshed = {**medicine, "purchase_return_units": purchase_return_units}
    available = _available_stock(refreshed)
    status = _return_status(refreshed)
    return {
        "purchase_return_units": purchase_return_units,
        "available_stock": available,
        "quantity_units": available,
        "return_status": status,
        "status": status,
    }


def _rebuild_dashboard_summaries(tenant_id: Optional[str]) -> None:
    started = time.perf_counter()
    try:
        # Dashboard summaries are calculated live from medicines and transactions.
        # Refreshing medicine derivatives rebuilds the inventory portion immediately.
        logger.info("Dashboard inventory summaries rebuilt tenant/shop=%s", tenant_id)
    finally:
        if _STARTUP_TIMING_ACTIVE:
            _record_startup_timing("Dashboard rebuild", time.perf_counter() - started, tenant_id=tenant_id)


def _invalidate_inventory_dashboard_cache(tenant_id: Optional[str]) -> None:
    started = time.perf_counter()
    try:
        # There is currently no application cache to clear. Keep this hook for
        # future inventory/dashboard cache providers.
        logger.info("Inventory/dashboard cache invalidated tenant/shop=%s (no cache configured)", tenant_id)
    finally:
        if _STARTUP_TIMING_ACTIVE:
            _record_startup_timing("Cache invalidation/warm-up", time.perf_counter() - started, tenant_id=tenant_id)


async def recalculate_purchase_return_stock(tenant_id: Optional[str] = None) -> dict:
    """Backfill return quantities and refresh inventory/dashboard-derived fields."""
    tenant_id = tenant_id or _current_tenant.get()
    logger.info("Purchase return stock recalculation started tenant/shop=%s", tenant_id)
    medicines = await _time_startup_awaitable(
        f"Purchase return load medicines tenant={tenant_id}",
        db.medicines.find({}, {"_id": 0}).to_list(None),
        tenant_id=tenant_id,
    )
    purchase_returns = await _time_startup_awaitable(
        f"Purchase return load returns tenant={tenant_id}",
        db.purchase_returns.find({}, {"_id": 0}).to_list(None),
        tenant_id=tenant_id,
    )
    totals = defaultdict(float)
    unmatched_return_ids = []
    matched_returns = 0

    for return_doc in purchase_returns:
        if return_doc.get("voided_at") or return_doc.get("deleted_at") or return_doc.get("settlement_status") == "deleted":
            continue
        medicine = _match_purchase_return_medicine(return_doc, medicines, tenant_id)
        if not medicine:
            unmatched_return_ids.append(return_doc.get("id"))
            logger.warning(
                "Purchase return stock repair medicine skipped tenant/shop=%s return_id=%s",
                tenant_id, return_doc.get("id"),
            )
            continue
        medicine_identity = medicine.get("id") or medicine.get("medicine_key")
        if not medicine_identity:
            unmatched_return_ids.append(return_doc.get("id"))
            logger.warning(
                "Purchase return stock repair medicine skipped tenant/shop=%s return_id=%s reason=missing_identity",
                tenant_id, return_doc.get("id"),
            )
            continue
        identity = ("id" if medicine.get("id") else "medicine_key", medicine_identity)
        totals[identity] += _purchase_return_quantity(return_doc)
        matched_returns += 1
        logger.info(
            "Purchase return stock repair medicine matched tenant/shop=%s return_id=%s medicine=%s",
            tenant_id, return_doc.get("id"), medicine_identity,
        )

    # Refresh only derived inventory fields. Purchase/sale quantities, FIFO,
    # billing, return documents, and ledger records remain untouched.
    update_started = time.perf_counter()
    for medicine in medicines:
        identity = (
            "id" if medicine.get("id") else "medicine_key",
            medicine.get("id") or medicine.get("medicine_key"),
        )
        if not identity[1]:
            logger.warning("Purchase return stock repair medicine skipped tenant/shop=%s reason=missing_identity", tenant_id)
            continue
        await db.medicines.update_one(
            {identity[0]: identity[1]},
            {"$set": _inventory_derivatives(medicine, totals.get(identity, 0.0))},
        )
    if _STARTUP_TIMING_ACTIVE:
        _record_startup_timing("Purchase return medicine updates", time.perf_counter() - update_started, tenant_id=tenant_id, medicines=len(medicines))

    # Dashboard stock summaries read medicines live, so refreshing every
    # medicine above rebuilds their source data. No persisted stock summary or
    # cache exists in this application.
    _rebuild_dashboard_summaries(tenant_id)
    _invalidate_inventory_dashboard_cache(tenant_id)
    logger.info("Purchase return stock recalculation completed tenant/shop=%s", tenant_id)
    return {
        "ok": True,
        "medicines_scanned": len(medicines),
        "returns_scanned": len(purchase_returns),
        "medicines_updated": len(totals),
        "matched_returns": matched_returns,
        "unmatched_returns": unmatched_return_ids,
    }


async def _run_startup_purchase_return_stock_recalculation() -> None:
    """Run one isolated stock repair per tenant after startup without failing startup."""
    recalculation_started = time.perf_counter()
    try:
        tenant_ids = set(await _time_startup_awaitable("Purchase return tenants scan medicines", raw_db.medicines.distinct("tenant_id")))
        tenant_ids.update(await _time_startup_awaitable("Purchase return tenants scan returns", raw_db.purchase_returns.distinct("tenant_id")))
        for tenant_id in sorted(item for item in tenant_ids if item):
            active_token = _request_active.set(True)
            tenant_token = _current_tenant.set(tenant_id)
            demo_token = _current_demo.set(False)
            try:
                await _time_startup_awaitable(f"Purchase return recalculation tenant={tenant_id}", recalculate_purchase_return_stock(tenant_id), tenant_id=tenant_id)
            except Exception:
                logger.exception(
                    "Purchase return stock recalculation failed tenant/shop=%s",
                    tenant_id,
                )
            finally:
                _current_demo.reset(demo_token)
                _current_tenant.reset(tenant_token)
                _request_active.reset(active_token)
        logger.info("Purchase return stock recalculation completed")
    except Exception:
        logger.exception("Purchase return stock recalculation failed during startup")
    finally:
        _record_startup_timing("Purchase return recalculation complete", time.perf_counter() - recalculation_started)


def _invoice_item_quantity(row: dict) -> float:
    return round_qty(row.get("deduct", row.get("quantity", row.get("units_dispensed", row.get("quantity_units", 0)))))


def _invoice_row_matches_medicine_batch(row: dict, medicine: dict, *, conservative_missing_batch: bool = True) -> bool:
    row_id = str(row.get("medicine_id") or "").strip()
    med_id = str(medicine.get("id") or "").strip()
    row_key = _normalized_stock_match_value(row.get("medicine_key"))
    med_key = _normalized_stock_match_value(medicine.get("medicine_key"))
    row_batch = _normalized_stock_match_value(row.get("batch_no") or row.get("batch_number"))
    med_batch = _normalized_stock_match_value(medicine.get("batch_no") or medicine.get("batch_number"))

    identity_matches = bool((row_id and med_id and row_id == med_id) or (row_key and med_key and row_key == med_key))
    if not identity_matches:
        return False
    if row_batch and med_batch:
        return row_batch == med_batch
    # If a real medicine id matches, missing legacy batch metadata may still be the
    # only invoice evidence available. Legacy inventory rows without an id are more
    # ambiguous, so callers can require an explicit batch match to avoid protecting
    # manual sold_units with broad invoice item fallbacks.
    return conservative_missing_batch and bool(row_id and med_id and row_id == med_id)


def _invoice_item_can_conservatively_match_medicine(row: dict, medicine: dict) -> bool:
    return bool(str(row.get("medicine_id") or "").strip() and str(medicine.get("id") or "").strip())


async def _invoice_backed_sold_units_for_medicine_batch(medicine: dict) -> float:
    total = 0.0
    invoices_collection = getattr(db, "invoices", None)
    if invoices_collection is None:
        return 0.0
    async for invoice in invoices_collection.find({}):
        deductions = invoice.get("stock_deductions") or []
        for row in deductions:
            quantity = _invoice_item_quantity(row)
            if quantity > 0 and _invoice_row_matches_medicine_batch(row, medicine):
                total += quantity
        if deductions:
            continue
        for row in invoice.get("items") or []:
            quantity = _invoice_item_quantity(row)
            conservative = _invoice_item_can_conservatively_match_medicine(row, medicine)
            if quantity > 0 and _invoice_row_matches_medicine_batch(row, medicine, conservative_missing_batch=conservative):
                total += quantity
    return round_qty(total)


def _stale_sold_units_repair_row(medicine: dict, invoice_backed_sold_units: float) -> dict:
    sold_units = _stock_quantity(medicine, "sold_units", "sold_quantity")
    current_stock = _available_stock(medicine)
    without_stale = {**medicine, "sold_units": 0, "sold_quantity": 0}
    return {
        "medicine_id": medicine.get("id") or medicine.get("medicine_key"),
        "medicine_name": medicine.get("name") or medicine.get("medicine_name") or "",
        "batch_no": medicine.get("batch_no") or medicine.get("batch_number") or "",
        "expiry": medicine.get("expiry_date") or medicine.get("expiry") or "",
        "purchased_units": _purchased_stock(medicine),
        "sold_units": sold_units,
        "purchase_return_units": _purchase_return_stock(medicine),
        "stock_adjustment_units": _stock_adjustment_stock(medicine),
        "current_stock": current_stock,
        "available_stock": current_stock,
        "invoice_backed_sold_units": invoice_backed_sold_units,
        "stale_sold_units": sold_units,
        "calculated_stock_if_sold_units_removed": _available_stock(without_stale),
        "reason": "No invoice-backed deduction found",
    }




@api_router.post("/admin/recalculate-purchase-return-stock")
async def recalculate_purchase_return_stock_endpoint(
    user: dict = Depends(require_role("admin")),
):
    return await recalculate_purchase_return_stock(user.get("tenant_id") or user.get("shop_id"))


@api_router.post("/admin/run-stock-repair")
async def run_stock_repair(user: dict = Depends(require_role("admin"))):
    result = await recalculate_purchase_return_stock(user.get("tenant_id") or user.get("shop_id"))
    return {
        "success": True,
        "updated_medicines": result["medicines_updated"],
        "matched_returns": result["matched_returns"],
        "unmatched_returns": result["unmatched_returns"],
    }


async def _deduct_purchase_return_stock(medicine_id: str, quantity: float, session=None):
    result = await _set_rounded_stock_delta(
        medicine_id, "purchase_return_units", quantity,
        session=session, require_available=True,
    )
    if not result or result.modified_count != 1:
        raise HTTPException(status_code=409, detail="Return quantity exceeds available stock for this batch")


@api_router.post("/purchase-returns")
async def create_purchase_return(
    payload: PurchaseReturnCreate,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    medicine = await _find_purchase_return_medicine(payload)
    available_stock = _available_batch_stock(medicine)

    if payload.return_quantity > available_stock:
        raise HTTPException(
            status_code=400,
            detail="Return quantity cannot exceed available stock in that batch",
        )

    distributor_lookup = [
        {"id": payload.distributor},
        {"name": payload.distributor},
    ]
    if payload.distributor_id:
        distributor_lookup.append({"id": payload.distributor_id})

    distributor = await db.distributors.find_one(
        {"$or": distributor_lookup},
        {"_id": 0},
    )

    distributor_id = (
        distributor.get("id")
        if distributor
        else medicine.get("distributor_id")
    )
    distributor_name = (
        distributor.get("name")
        if distributor
        else payload.distributor
    )

    return_amount = _purchase_return_credit({
        "return_quantity": payload.return_quantity,
        "purchase_rate": payload.purchase_rate,
    })
    now = datetime.now(timezone.utc).isoformat()

    purchase_return = {
        "id": str(uuid.uuid4()),
        "return_date": payload.return_date,
        "distributor": distributor_name,
        "distributor_id": distributor_id,
        "medicine_id": medicine.get("id"),
        "medicine_key": medicine.get("medicine_key"),
        "medicine_name": payload.medicine_name,
        "batch_number": payload.batch_number,
        "expiry_date": payload.expiry_date,
        "return_quantity": payload.return_quantity,
        "purchase_rate": _money_float(_to_decimal(payload.purchase_rate)),
        "gst_rate": _money_float(_to_decimal(medicine.get("gst_rate"))),
        "return_amount": return_amount,
        "reason": payload.reason,
        "notes": payload.notes,
        "adjust_distributor_ledger": payload.adjust_distributor_ledger,
        "ledger_adjusted": payload.adjust_distributor_ledger,
        "ledger_transaction_id": None,
        "settlement_status": "ledger_adjusted" if payload.adjust_distributor_ledger else "unsettled",
        "settled_by_po": None,
        "settled_at": now if payload.adjust_distributor_ledger else None,
        "settlement_reference": None,
        "settled_return_value": return_amount if payload.adjust_distributor_ledger else 0.0,
        "created_at": now,
        "created_by": user.get("name", ""),
    }

    ledger_txn = None
    if payload.adjust_distributor_ledger:
        if not distributor_id:
            raise HTTPException(
                status_code=400,
                detail="Distributor ledger adjustment requires a matched distributor",
            )
        ledger_txn = _purchase_return_ledger_transaction(purchase_return)
        purchase_return["ledger_transaction_id"] = ledger_txn["id"]

    async def write_records(session=None):
        current_medicine = await _find_purchase_return_medicine(payload, session=session)
        await _deduct_purchase_return_stock(
            current_medicine["id"],
            payload.return_quantity,
            session=session,
        )
        await db.purchase_returns.insert_one(purchase_return, session=session)
        if ledger_txn:
            await db.distributor_transactions.insert_one(ledger_txn, session=session)
        return _return_public(purchase_return)

    async def transaction_operation(session):
        return await write_records(session=session)

    async def fallback_operation():
        stock_deducted = False
        try:
            current_medicine = await _find_purchase_return_medicine(payload)
            await _deduct_purchase_return_stock(
                current_medicine["id"],
                payload.return_quantity,
            )
            stock_deducted = True
            await db.purchase_returns.insert_one(purchase_return)
            if ledger_txn:
                await db.distributor_transactions.insert_one(ledger_txn)
        except Exception:
            if stock_deducted:
                await _set_rounded_stock_delta(
                    medicine.get("id"), "purchase_return_units", -payload.return_quantity
                )
            raise

        return _return_public(purchase_return)

    return await _run_with_transaction(
        transaction_operation,
        fallback_operation,
    )


async def _set_purchase_return_stock_delta(medicine_id: str, delta: float, session=None):
    """Apply a physical return delta; positive removes stock, negative restores it."""
    if delta > 0:
        return await _deduct_purchase_return_stock(medicine_id, delta, session=session)
    result = await _set_rounded_stock_delta(
        medicine_id, "purchase_return_units", delta, session=session
    )
    if not result or result.modified_count != 1:
        raise HTTPException(status_code=404, detail="Medicine batch not found")


async def _restore_purchase_return_stock_if_present(medicine_id: Optional[str], quantity: float, session=None) -> bool:
    if not medicine_id or not quantity:
        return False
    result = await _set_rounded_stock_delta(
        medicine_id, "purchase_return_units", -quantity, session=session
    )
    return bool(result and result.modified_count == 1)


async def _sync_purchase_return_ledger(old: dict, updated: dict, session=None):
    existing_id = old.get("ledger_transaction_id")
    should_adjust = bool(updated.get("adjust_distributor_ledger"))
    if should_adjust and not updated.get("distributor_id"):
        raise HTTPException(status_code=400, detail="Distributor ledger adjustment requires a matched distributor")

    if not should_adjust:
        if existing_id:
            await db.distributor_transactions.delete_one({"id": existing_id, "return_id": old["id"]}, session=session)
        return None

    transaction = _purchase_return_ledger_transaction(updated)
    if existing_id:
        transaction["id"] = existing_id
        result = await db.distributor_transactions.update_one(
            {"id": existing_id, "return_id": old["id"]},
            {"$set": transaction},
            session=session,
        )
        if not result or (getattr(result, "matched_count", result.modified_count) != 1 and result.modified_count != 1):
            await db.distributor_transactions.insert_one(transaction, session=session)
        return existing_id
    await db.distributor_transactions.insert_one(transaction, session=session)
    return transaction["id"]


@api_router.patch("/purchase-returns/{return_id}")
@api_router.put("/purchase-returns/{return_id}")
async def update_purchase_return(
    return_id: str,
    payload: PurchaseReturnUpdate,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    current = await db.purchase_returns.find_one({"id": return_id})
    if not current:
        raise HTTPException(status_code=404, detail="Purchase return not found")
    status = _purchase_return_settlement_status(current)
    if status in {"voided", "deleted"} or current.get("voided_at") or current.get("deleted_at"):
        raise HTTPException(status_code=409, detail="Deleted purchase returns cannot be edited")
    if status == "settled_by_po":
        raise HTTPException(status_code=409, detail="Purchase return is settled in a purchase order and cannot be edited")

    changes = payload.model_dump(exclude_unset=True)
    if not changes:
        return _return_public(current)
    if "expiry_date" in changes:
        changes["expiry_date"] = _normalize_purchase_return_expiry(changes["expiry_date"])
    updated = {**current, **changes}
    updated["ledger_adjusted"] = bool(updated.get("adjust_distributor_ledger"))
    updated["purchase_rate"] = _money_float(_to_decimal(updated["purchase_rate"]))
    if updated.get("gst_rate") is not None:
        updated["gst_rate"] = _money_float(_to_decimal(updated["gst_rate"]))
    updated["return_amount"] = _purchase_return_credit(updated)
    updated["settlement_status"] = "ledger_adjusted" if updated["ledger_adjusted"] else "unsettled"
    updated["settled_return_value"] = updated["return_amount"] if updated["ledger_adjusted"] else 0.0
    updated["updated_at"] = datetime.now(timezone.utc).isoformat()
    delta = float(updated["return_quantity"]) - float(current.get("return_quantity", 0) or 0)

    async def write(session=None):
        if delta:
            await _set_purchase_return_stock_delta(current["medicine_id"], delta, session=session)
        ledger_id = await _sync_purchase_return_ledger(current, updated, session=session)
        updated["ledger_transaction_id"] = ledger_id
        result = await db.purchase_returns.update_one(
            {"id": return_id, "$or": [{"po_adjustment_id": {"$exists": False}}, {"po_adjustment_id": None}]},
            {"$set": {key: value for key, value in updated.items() if key != "_id"}},
            session=session,
        )
        if not result or result.modified_count != 1:
            raise HTTPException(status_code=409, detail="Purchase return changed or was applied to a purchase order")
        return _return_public(updated)

    async def transaction_operation(session):
        return await write(session=session)

    async def fallback_operation():
        try:
            return await write()
        except Exception:
            if delta:
                await _set_rounded_stock_delta(current["medicine_id"], "purchase_return_units", -delta)
            raise

    return await _run_with_transaction(transaction_operation, fallback_operation)


@api_router.delete("/purchase-returns/{return_id}")
async def delete_purchase_return(
    return_id: str,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    current = await db.purchase_returns.find_one({"id": return_id})
    if not current:
        raise HTTPException(status_code=404, detail="Purchase return not found")
    status = _purchase_return_settlement_status(current)
    if status in {"voided", "deleted"} or current.get("voided_at") or current.get("deleted_at"):
        raise HTTPException(status_code=409, detail="Purchase return is already deleted")
    if status == "settled_by_po":
        raise HTTPException(status_code=409, detail="Purchase return is settled in a purchase order and cannot be deleted")

    quantity = float(current.get("return_quantity", 0) or 0)
    stock_restored = False

    async def write(session=None):
        nonlocal stock_restored
        deleted_at = datetime.now(timezone.utc).isoformat()
        if current.get("ledger_transaction_id"):
            await db.distributor_transactions.delete_one(
                {"id": current["ledger_transaction_id"], "return_id": return_id},
                session=session,
            )
        stock_restored = await _restore_purchase_return_stock_if_present(
            current.get("medicine_id"), quantity, session=session
        )
        stock_warning = None if stock_restored else "Stock restoration skipped because medicine was missing"
        result = await db.purchase_returns.update_one({
            "id": return_id, "$or": [{"po_adjustment_id": {"$exists": False}}, {"po_adjustment_id": None}]
        }, {"$set": {
            "deleted_at": deleted_at,
            "deleted_by": user.get("name") or user.get("id", ""),
            "settlement_status": "deleted",
            "ledger_adjusted": False,
            "adjust_distributor_ledger": False,
            "ledger_transaction_id": None,
            "settled_return_value": 0.0,
        }}, session=session)
        if result.modified_count != 1:
            raise HTTPException(status_code=409, detail="Purchase return changed or was applied to a purchase order")
        response = {"success": True, "message": "Purchase return deleted", "id": return_id}
        if stock_warning:
            response["warning"] = stock_warning
        return response

    async def transaction_operation(session):
        return await write(session=session)

    async def fallback_operation():
        try:
            return await write()
        except Exception:
            if stock_restored:
                await _set_rounded_stock_delta(current["medicine_id"], "purchase_return_units", quantity)
            raise

    return await _run_with_transaction(transaction_operation, fallback_operation)


@api_router.get("/purchase-returns")
async def list_purchase_returns(
    search: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    distributor: Optional[str] = None,
    medicine: Optional[str] = None,
    reason: Optional[str] = None,
    ledger_adjusted: Optional[bool] = None,
    page: int = 1,
    page_size: int = 25,
    user: dict = Depends(get_current_user),
):
    page = max(page, 1)
    page_size = min(max(page_size, 1), 100)
    query = _purchase_return_query(
        start=start,
        end=end,
        distributor=distributor,
        medicine=medicine,
        reason=reason,
        ledger_adjusted=ledger_adjusted,
        search=search,
    )

    total = await db.purchase_returns.count_documents(query)
    items = await db.purchase_returns.find(
        query,
        {"_id": 0},
    ).sort("return_date", -1).skip((page - 1) * page_size).limit(page_size).to_list(page_size)

    normalized_items = [_normalized_purchase_return_money(item) for item in items]
    return {
        "items": normalized_items,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
    }


@api_router.get("/reports/purchase-returns")
async def purchase_return_report(
    start: Optional[str] = None,
    end: Optional[str] = None,
    distributor: Optional[str] = None,
    medicine: Optional[str] = None,
    reason: Optional[str] = None,
    ledger_adjusted: Optional[bool] = None,
    user: dict = Depends(get_current_user),
):
    query = _purchase_return_query(
        start=start,
        end=end,
        distributor=distributor,
        medicine=medicine,
        reason=reason,
        ledger_adjusted=ledger_adjusted,
    )
    returns = await db.purchase_returns.find(query, {"_id": 0}).to_list(10000)
    returns = [item for item in returns if _purchase_return_settlement_status(item) != "deleted"]

    by_distributor = defaultdict(lambda: {"quantity": 0.0, "value": 0.0, "count": 0})
    by_medicine = defaultdict(lambda: {"quantity": 0.0, "value": 0.0, "count": 0})
    by_reason = defaultdict(lambda: {"quantity": 0.0, "value": 0.0, "count": 0})
    by_ledger_status = defaultdict(lambda: {"quantity": 0.0, "value": 0.0, "count": 0})
    analytics_rows = []

    total_quantity = 0.0
    total_value = 0.0
    settled_value = 0.0
    ledger_adjusted_value = 0.0
    pending_credit_value = 0.0
    adjusted_in_purchase_value = 0.0

    def add_summary(bucket: dict, key: str, quantity: float, value: float):
        bucket[key]["quantity"] += quantity
        bucket[key]["value"] += value
        bucket[key]["count"] += 1

    for item in returns:
        quantity = _safe_float(item.get("return_quantity"))
        value = _money_float(_to_decimal(quantity) * _to_decimal(_safe_float(item.get("purchase_rate"))))
        total_quantity += quantity
        total_value += value
        analytics_row = _purchase_return_analytics_row(item)
        status_label = analytics_row["status_label"]
        if status_label in {"Ledger Adjusted", "Adjusted in Purchase"}:
            settled_value += value
        if status_label == "Ledger Adjusted":
            ledger_adjusted_value += value
        elif status_label == "Adjusted in Purchase":
            adjusted_in_purchase_value += value
        elif status_label == "Credit Pending":
            pending_credit_value += value

        add_summary(by_distributor, analytics_row["distributor_name"], quantity, value)
        add_summary(by_medicine, analytics_row["medicine_name"], quantity, value)
        add_summary(by_reason, item.get("reason") or "Unknown", quantity, value)
        add_summary(by_ledger_status, status_label, quantity, value)
        analytics_rows.append(analytics_row)

    def finalize_summary(bucket: dict) -> List[dict]:
        output = []
        for key, values in bucket.items():
            output.append({
                "name": key,
                "quantity": round(values["quantity"], 2),
                "value": round(values["value"], 2),
                "count": values["count"],
            })
        output.sort(key=lambda row: str(row["name"]).lower())
        return output

    return {
        "start": start,
        "end": end,
        "total_returned_quantity": round_qty(total_quantity),
        "returned_quantity": round_qty(total_quantity),
        "total_return_value": round(total_value, 2),
        "ledger_adjusted_value": _money_float(_to_decimal(ledger_adjusted_value)),
        "pending_credit_value": _money_float(_to_decimal(pending_credit_value)),
        "adjusted_in_purchase_value": _money_float(_to_decimal(adjusted_in_purchase_value)),
        "summary_buckets": {
            "Ledger Adjusted Value": _money_float(_to_decimal(ledger_adjusted_value)),
            "Pending Credit Value": _money_float(_to_decimal(pending_credit_value)),
            "Adjusted in Purchase Value": _money_float(_to_decimal(adjusted_in_purchase_value)),
        },
        "settled_return_value": _money_float(_to_decimal(settled_value)),
        "unsettled_return_value": _money_float(_to_decimal(total_value) - _to_decimal(settled_value)),
        "return_count": len(returns),
        "returns": [_normalized_purchase_return_money(item) for item in returns],
        "purchase_returns": [_normalized_purchase_return_money(item) for item in returns],
        "returns_by_distributor": finalize_summary(by_distributor),
        "returns_by_medicine": finalize_summary(by_medicine),
        "purchase_return_analytics": analytics_rows,
        "medicine_wise_return_analytics": analytics_rows,
        "returns_by_reason": finalize_summary(by_reason),
        "ledger_adjusted_status": finalize_summary(by_ledger_status),
    }



def _customer_transaction_date(txn: dict):
    if not isinstance(txn, dict):
        return None
    parsed = _parse_ledger_transaction_date(
        txn.get("transaction_date")
        or txn.get("date")
        or txn.get("created_at")
    )
    if parsed:
        return parsed

    # Last-resort legacy fallback after the required transaction_date/date/created_at priority.
    txn_id = str(txn.get("id") or "")
    if len(txn_id) >= 10 and txn_id[4:5] == "-" and txn_id[7:8] == "-":
        return _parse_ledger_transaction_date(txn_id[:10])
    return None


def _customer_transaction_month(txn: dict) -> Optional[str]:
    parsed = _customer_transaction_date(txn)
    return parsed.strftime("%Y-%m") if parsed else None


def _customer_transaction_sort_key(txn: dict):
    parsed = _customer_transaction_date(txn)
    return (
        parsed or date.min,
        _ledger_sort_datetime(txn.get("created_at") if isinstance(txn, dict) else None),
        str(txn.get("id") if isinstance(txn, dict) else ""),
    )


def _apply_customer_transaction(balance: float, txn: dict) -> Tuple[float, str]:
    amount = _safe_float(txn.get("amount", 0) if isinstance(txn, dict) else 0)
    txn_type = str(txn.get("type") if isinstance(txn, dict) else "").strip().lower()

    if txn_type == "sale":
        return balance + amount, "sale"

    if txn_type == "payment":
        return balance - amount, "payment"

    return balance, "ignored"


def _customer_monthly_summary_from_transactions(transactions: List[dict]) -> List[dict]:
    monthly = defaultdict(lambda: {
        "month": "",
        "total_credit_sales": 0.0,
        "sales_added": 0.0,
        "total_payments_received": 0.0,
        "net_receivable_movement": 0.0,
        "closing_receivable_balance": 0.0,
        "transaction_count": 0,
    })

    valid_txns = []
    for txn in transactions:
        month = _customer_transaction_month(txn)
        if not month:
            continue
        valid_txns.append((month, txn))

    balance = 0.0
    for month, txn in sorted(valid_txns, key=lambda item: (item[0], _customer_transaction_sort_key(item[1]))):
        amount = _safe_float(txn.get("amount"))
        new_balance, bucket = _apply_customer_transaction(balance, txn)
        if bucket == "ignored":
            continue
        balance = _round_ledger_money(new_balance)
        row = monthly[month]
        row["month"] = month
        if bucket == "sale":
            row["total_credit_sales"] += amount
            row["sales_added"] += amount
            row["net_receivable_movement"] += amount
        elif bucket == "payment":
            row["total_payments_received"] += amount
            row["net_receivable_movement"] -= amount
        row["closing_receivable_balance"] = balance
        row["transaction_count"] += 1

    summaries = []
    money_keys = ("total_credit_sales", "sales_added", "total_payments_received", "net_receivable_movement", "closing_receivable_balance")
    for month in sorted(monthly):
        row = monthly[month]
        for key in money_keys:
            row[key] = _round_ledger_money(row[key])
        if all(row[key] == 0 for key in money_keys):
            continue
        summaries.append(dict(row))
    if transactions and not summaries:
        logger.warning("Customer monthly summary generation produced no non-zero rows from %s transactions", len(transactions))
    return summaries


async def _customer_monthly_summary_data(customer_id: Optional[str] = None):
    query = {"customer_id": customer_id} if customer_id else {}
    txns = await db.customer_transactions.find(query, {"_id": 0}).to_list(10000)
    try:
        summaries = _customer_monthly_summary_from_transactions(txns)
    except Exception:
        logger.exception("Customer monthly summary generation failed customer_id=%s", customer_id)
        summaries = []
    return {
        "customer_id": customer_id,
        "items": summaries,
        "total_credit_sales": _round_ledger_money(sum(row["total_credit_sales"] for row in summaries)),
        "sales_added": _round_ledger_money(sum(row["sales_added"] for row in summaries)),
        "total_payments_received": _round_ledger_money(sum(row["total_payments_received"] for row in summaries)),
        "net_receivable_movement": _round_ledger_money(sum(row["net_receivable_movement"] for row in summaries)),
    }

async def _distributor_monthly_summary_data(month: str, distributor_id: Optional[str] = None):
    try:
        datetime.strptime(month, "%Y-%m")
    except ValueError:
        raise HTTPException(status_code=400, detail="month must be in YYYY-MM format")

    distributor_query = {}
    if distributor_id:
        distributor_query["id"] = distributor_id

    distributors = await db.distributors.find(distributor_query, {"_id": 0}).to_list(1000)
    all_txns = await db.distributor_transactions.find({}, {"_id": 0}).to_list(10000)
    summaries = {}
    for distributor in distributors:
        did = distributor.get("id")
        identity_values = _distributor_identity_values(distributor, distributor_id)
        names = {
            str(distributor.get(field)).strip().casefold()
            for field in ("name", "distributor_name")
            if distributor.get(field) and str(distributor.get(field)).strip()
        }
        dist_txns = [txn for txn in all_txns if _belongs_to_distributor(txn, identity_values, names)]

        ledger_txns = _distributor_opening_balance_deduped_transactions(distributor, dist_txns)
        month_txns = [
            txn for txn in ledger_txns
            if (txn_date := _distributor_transaction_date(txn)) and txn_date.strftime("%Y-%m") == month
        ]

        summary = {
            "distributor_id": did,
            "distributor_name": distributor.get("name", ""),
            "month": month,
            "purchase_total": 0.0,
            "payment_total": 0.0,
            "adjustment_total": 0.0,
            "net_change": 0.0,
            "transaction_count": 0,
            "transactions": [],
        }

        for txn in month_txns:
            amount = _safe_float(txn.get("amount", 0) if isinstance(txn, dict) else 0)
            bucket = _apply_distributor_transaction(0, txn)[1]
            if bucket == "purchase":
                summary["purchase_total"] += amount
                summary["net_change"] += amount
            elif bucket == "adjustment":
                summary["adjustment_total"] += amount
                summary["net_change"] -= amount
            else:
                summary["payment_total"] += amount
                summary["net_change"] -= amount
            summary["transaction_count"] += 1
            summary["transactions"].append(_json_safe_ledger_transaction(txn))

        summaries[did] = summary

    items = []
    total_purchases = 0.0
    total_payments = 0.0
    total_adjustments = 0.0

    for summary in summaries.values():
        summary["purchase_total"] = round(summary["purchase_total"], 2)
        summary["payment_total"] = round(summary["payment_total"], 2)
        summary["adjustment_total"] = round(summary["adjustment_total"], 2)
        summary["net_change"] = round(summary["net_change"], 2)
        total_purchases += summary["purchase_total"]
        total_payments += summary["payment_total"]
        total_adjustments += summary["adjustment_total"]
        items.append(summary)

    items.sort(key=lambda item: str(item.get("distributor_name") or "").lower())

    return {
        "month": month,
        "distributor_id": distributor_id,
        "purchase_total": round(total_purchases, 2),
        "payment_total": round(total_payments, 2),
        "adjustment_total": round(total_adjustments, 2),
        "net_change": round(total_purchases - total_payments - total_adjustments, 2),
        "distributor_count": len(items),
        "items": items,
    }


async def repair_duplicate_distributor_opening_balance_rows(distributor_id: Optional[str] = None) -> dict:
    distributor_query = {"id": distributor_id} if distributor_id else {}
    distributors = await db.distributors.find(distributor_query, {"_id": 0}).to_list(5000)
    removed_ids = []

    for distributor in distributors:
        did = distributor.get("id")
        if not did:
            continue

        opening_txn = await _find_distributor_opening_balance_transaction(distributor)
        txns = await db.distributor_transactions.find(
            {"distributor_id": did},
            {"_id": 0},
        ).sort("created_at", 1).to_list(1000)
        duplicate_ids = [
            txn.get("id")
            for txn in txns
            if txn.get("id")
            and _is_duplicate_opening_balance_row(txn, distributor, opening_txn)
        ]
        if not duplicate_ids:
            continue

        await db.distributor_transactions.delete_many({
            "distributor_id": did,
            "id": {"$in": duplicate_ids},
        })
        removed_ids.extend(duplicate_ids)

    return {
        "success": True,
        "distributor_id": distributor_id,
        "removed_count": len(removed_ids),
        "removed_transaction_ids": removed_ids,
    }


@api_router.get("/distributors/monthly-summary")
async def distributor_monthly_summary(
    month: str,
    distributor_id: Optional[str] = None,
    user: dict = Depends(get_current_user),
):
    return await _distributor_monthly_summary_data(month, distributor_id)


@api_router.get("/ledger/distributor/{did}/monthly-summary")
async def distributor_ledger_monthly_summary(
    did: str,
    month: str,
    user: dict = Depends(get_current_user),
):
    return await _distributor_monthly_summary_data(month, did)


@api_router.post("/admin/repair-distributor-opening-balance-duplicates")
async def repair_distributor_opening_balance_duplicates_endpoint(
    distributor_id: Optional[str] = None,
    user: dict = Depends(require_role("admin")),
):
    return await repair_duplicate_distributor_opening_balance_rows(distributor_id)


@api_router.get("/ledger/customer/{cid}")
async def customer_ledger(cid: str, search: Optional[str] = None, invoice_number: Optional[str] = None,
    reference_number: Optional[str] = None, payment_mode: Optional[str] = None,
    transaction_type: Optional[str] = None, start: Optional[str] = None, end: Optional[str] = None,
    amount: Optional[float] = None, user: dict = Depends(get_current_user)):
    cust = await db.customers.find_one({"id": cid}, {"_id": 0})
    if not cust:
        raise HTTPException(status_code=404, detail="Customer not found")
    txns = await db.customer_transactions.find({"customer_id": cid}, {"_id": 0}).sort("created_at", 1).to_list(1000)
    balance = 0.0
    running = []
    for t in txns:
        if t.get("type") == "sale":
            balance += float(t.get("amount", 0) or 0)
        else:
            balance -= float(t.get("amount", 0) or 0)
        balance = round(balance, 2)
        if t.get("running_balance") != balance:
            await db.customer_transactions.update_one({"id": t["id"]}, {"$set": {"running_balance": balance, "amount": round(float(t.get("amount", 0) or 0), 2)}})
        running.append(_ensure_action_aliases(
            {**t, "amount": round(float(t.get("amount", 0) or 0), 2), "running_balance": balance},
            alias_id_fields=("transaction_id",),
        ))
    def matches(t):
        text = " ".join(str(t.get(k, "")) for k in ("invoice_number", "reference_number", "reference", "payment_mode", "mode", "type"))
        return (not search or search.lower() in text.lower()) and (not invoice_number or invoice_number.lower() in str(t.get("invoice_number") or t.get("reference") or "").lower()) and (not reference_number or reference_number.lower() in str(t.get("reference_number") or t.get("reference") or "").lower()) and (not payment_mode or payment_mode.lower() == str(t.get("payment_mode") or t.get("mode") or "").lower()) and (not transaction_type or transaction_type.lower() == str(t.get("type") or "").lower()) and (not start or str(t.get("created_at", "")) >= start) and (not end or str(t.get("created_at", "")) <= end) and (amount is None or round(float(t.get("amount", 0) or 0), 2) == round(amount, 2))
    running = [t for t in running if matches(t)]
    try:
        monthly_summary = _customer_monthly_summary_from_transactions(txns)
    except Exception:
        logger.exception("Customer ledger monthly summary generation failed customer_id=%s", cid)
        monthly_summary = []
    return {"customer": cust, "transactions": running, "balance": round(balance, 2), "monthly_summary": monthly_summary, "monthly_movement_summary": monthly_summary}


def _ledger_export_csv(owner_type: str, ledger: dict, start_date: Optional[date], end_date: Optional[date]) -> str:
    owner = ledger[owner_type]
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["Ledger Owner", owner.get("name") or owner.get("customer_name") or owner.get("distributor_name") or ""])
    writer.writerow(["Phone", owner.get("phone") or owner.get("mobile") or owner.get("contact_number") or ""])
    writer.writerow(["Date Range", f"{start_date.isoformat() if start_date else 'Beginning'} to {end_date.isoformat() if end_date else 'Present'}"])
    writer.writerow(["Current Balance", _round_ledger_money(ledger.get("balance", 0))])
    writer.writerow([])
    writer.writerow(["Transaction Date", "Transaction Type", "Reference/Invoice Number", "Mode", "Amount", "Running Balance", "Notes"])
    for txn in ledger.get("transactions", []):
        writer.writerow([
            txn.get("transaction_date") or txn.get("date") or txn.get("created_at") or "",
            txn.get("display_type") or txn.get("subtype") or txn.get("type") or "",
            _distributor_bill_reference(txn),
            txn.get("payment_mode") or txn.get("mode") or "",
            _round_ledger_money(txn.get("amount", 0)),
            _round_ledger_money(txn.get("running_balance", 0)),
            txn.get("notes") or "",
        ])
    return output.getvalue()


@api_router.get("/ledger/{ledger_type}/{owner_id}/export")
async def export_ledger(
    ledger_type: str,
    owner_id: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    user: dict = Depends(get_current_user),
):
    if ledger_type not in {"customer", "distributor"}:
        raise HTTPException(status_code=400, detail="Ledger type must be customer or distributor")
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must not be after end_date")

    if ledger_type == "customer":
        ledger = await customer_ledger(
            owner_id,
            start=start_date.isoformat() if start_date else None,
            end=f"{end_date.isoformat()}T23:59:59.999999" if end_date else None,
            user=user,
        )
    else:
        ledger = await distributor_ledger(
            owner_id,
            date_from=start_date,
            date_to=end_date,
            user=user,
        )

    csv_text = _ledger_export_csv(ledger_type, ledger, start_date, end_date)
    filename = f"{ledger_type}-ledger-{owner_id}.csv"
    return StreamingResponse(
        iter([csv_text]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@api_router.delete("/ledger/customer/{cid}/transaction/{txn_id}")
async def delete_customer_txn(
    cid: str,
    txn_id: str,
    user: dict = Depends(get_current_user)
):
    result = await db.customer_transactions.delete_one({
        "id": txn_id,
        "customer_id": cid
    })

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Transaction not found")

    return {"ok": True}


@api_router.post("/ledger/customer/{cid}/payment")
async def add_cust_payment(
    cid: str,
    p: PaymentCreate,
    user: dict = Depends(get_current_user)
):
    txn = {
        "id": str(uuid.uuid4()),
        "customer_id": cid,
        "type": "payment",
        "amount": round(p.amount, 2),
        "mode": p.mode,
        "payment_mode": _transaction_payment_mode(p),
        "invoice_number": p.invoice_number,
        "reference_number": p.reference_number or p.receipt_number,
        "notes": p.notes,
        "created_at": p.date or datetime.now(timezone.utc).isoformat(),
    }

    await db.customer_transactions.insert_one(txn)

    txn.pop("_id", None)

    return txn


@api_router.post("/ledger/customer/{cid}/sale")
async def add_customer_sale(
    cid: str,
    p: PaymentCreate,
    user: dict = Depends(get_current_user)
):
    txn = {
        "id": str(uuid.uuid4()),
        "customer_id": cid,
        "type": "sale",
        "amount": round(p.amount, 2),
        "mode": p.mode,
        "payment_mode": _transaction_payment_mode(p),
        "invoice_number": p.invoice_number,
        "reference_number": p.reference_number,
        "notes": p.notes,
        "created_at": p.date or datetime.now(timezone.utc).isoformat(),
    }

    await db.customer_transactions.insert_one(txn)

    txn.pop("_id", None)

    return txn



async def _dashboard_purchase_return_summary(start: Optional[str] = None, end: Optional[str] = None) -> dict:
    """Return compact dashboard purchase-return totals with count always present."""
    summary = {"return_records": 0, "units_returned": 0, "returned_value": 0}
    if not hasattr(db, "purchase_returns"):
        return summary

    query = _purchase_return_query(start=start, end=end)
    returns = await db.purchase_returns.find(query, {"_id": 0}).to_list(10000)
    for item in returns:
        if _purchase_return_settlement_status(item) == "deleted":
            continue
        quantity = _safe_float(item.get("return_quantity"))
        value = _safe_float(item.get("total_value"), None)
        if value is None:
            value = _money_float(_to_decimal(quantity) * _to_decimal(_safe_float(item.get("purchase_rate"))))
        summary["return_records"] += 1
        summary["units_returned"] += quantity
        summary["returned_value"] += value

    return {
        "return_records": int(summary["return_records"]),
        "units_returned": round_qty(summary["units_returned"]),
        "returned_value": _round_ledger_money(summary["returned_value"]),
    }


# ---------------- Dashboard & Reports ----------------
@api_router.get("/dashboard/summary")
async def dashboard_summary(
    start: Optional[str] = None,
    end: Optional[str] = None,
    user: dict = Depends(get_current_user),
):

    q = {}

    if start or end:
        q["created_at"] = {}
        if start:
            q["created_at"]["$gte"] = start
        if end:
            q["created_at"]["$lte"] = end

    today = datetime.now(timezone.utc).date()
    current_month = today.month
    current_year = today.year

    sales_today = 0
    sales_month = 0

    expenses_today = 0
    expenses_month = 0

    profit_today = 0
    profit_month = 0

    received_today = 0
    received_month = 0
    received_total = 0

    customer_outstanding_today = 0
    customer_outstanding_month = 0

    # SALES
    invoices = await db.invoices.find(q, {"_id": 0}).to_list(5000)

    total_sales = 0

    for i in invoices:
        amt = float(i.get("total", 0))
        total_sales += amt

        try:
            dt = datetime.fromisoformat(i["created_at"]).date()

            if dt == today:
                sales_today += amt

            if dt.month == current_month and dt.year == current_year:
                sales_month += amt

        except Exception:
            pass

    total_gst = sum(i.get("gst_total", 0) for i in invoices)
    total_discount = sum(i.get("bill_discount", 0) for i in invoices)

    # EXPENSES
    expenses = await db.expenses.find(q, {"_id": 0}).to_list(5000)

    total_expenses = 0

    for e in expenses:
        amt = float(e.get("amount", 0))
        total_expenses += amt

        try:
            dt = datetime.fromisoformat(e["created_at"]).date()

            if dt == today:
                expenses_today += amt

            if dt.month == current_month and dt.year == current_year:
                expenses_month += amt

        except Exception:
            pass

    profit = total_sales - total_expenses
    profit_today = sales_today - expenses_today
    profit_month = sales_month - expenses_month

    # STOCK
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(5000)

    stock_value = 0
    low_stock_items = []
    expiring_soon_items = []
    expired_items = []

    low_stock_by_name = {}
    for m in medicines:

        available = _available_stock(m)

        stock_value += available * float(m.get("purchase_price", 0))

        threshold = m.get("low_stock_threshold")

        name_key = str(m.get("name") or "").strip().upper()
        medicine_id = m.get("id") or m.get("medicine_key")
        item = low_stock_by_name.setdefault(name_key, {
            "medicine_id": medicine_id, "id": medicine_id, "_id": medicine_id,
            "name": m.get("name"), "qty": 0,
            "current_stock": 0, "available_qty": 0, "threshold": threshold,
            "status": _low_stock_status(m),
            "low_stock_status": _low_stock_status(m),
        })
        if threshold is not None:
            item["threshold"] = max(item["threshold"] or 0, threshold)
        item["qty"] = item["current_stock"] = item["available_qty"] = round_qty(item["qty"] + available)

        if available <= 0:
            continue

        expiry_info = expiry_details(
            m.get("expiry_date"),
            today
        )

        expiry_item = {
            "id": m.get("id"),
            "medicine_key": m.get("medicine_key"),
            "name": m.get("name"),
            "batch_no": m.get("batch_no", ""),
            "expiry_date": m.get("expiry_date"),
            "expiry_status": expiry_info["expiry_status"],
            "days_to_expiry": expiry_info["days_to_expiry"],
            "days_expired": expiry_info["days_expired"],
            "expired_days_ago": expiry_info["expired_days_ago"],
        }

        if (
            expiry_info["expiry_status"] == "expired"
            and expiry_info["days_expired"] <= DASHBOARD_RECENTLY_EXPIRED_DAYS
        ):
            expired_items.append(expiry_item)
        elif expiry_info["expiry_status"] == "warning":
            expiring_soon_items.append(expiry_item)

    low_stock_items = [
        item for item in low_stock_by_name.values()
        if item["threshold"] is not None and item["available_qty"] <= item["threshold"]
    ]

    # CUSTOMER OUTSTANDING
    customer_txns = await db.customer_transactions.find({}, {"_id": 0}).to_list(5000)

    customer_balances = defaultdict(float)

    for t in customer_txns:
        customer_id = t.get("customer_id") or "__unassigned__"

        if t.get("type") == "sale":
            amt = float(t.get("amount", 0))
            customer_balances[customer_id] += amt

            try:
                dt = datetime.fromisoformat(t["created_at"]).date()

                if dt == today:
                    customer_outstanding_today += amt

                if dt.month == current_month and dt.year == current_year:
                    customer_outstanding_month += amt

            except Exception:
                pass

        elif t.get("type") == "payment":
            amt = float(t.get("amount", 0))
            customer_balances[customer_id] -= amt
            received_total += amt

            try:
                dt = datetime.fromisoformat(t["created_at"]).date()

                if dt == today:
                    received_today += amt

                if dt.month == current_month and dt.year == current_year:
                    received_month += amt

            except Exception:
                pass

    customer_outstanding = sum(max(0.0, balance) for balance in customer_balances.values())

    # DISTRIBUTOR OUTSTANDING
    distributors = await db.distributors.find({}, {"_id": 0}).to_list(1000)

    distributor_outstanding = 0

    for d in distributors:

        txns = await db.distributor_transactions.find(
            {"distributor_id": d["id"]},
            {"_id": 0}
        ).to_list(1000)

        bal = _current_distributor_balance(d, txns)

        if bal > 0:
            distributor_outstanding += bal

    purchase_orders = await db.purchase_orders.find(
        {},
        {"_id": 0}
    ).to_list(5000)

    total_purchase_amount = sum(
        float(po.get("grand_total", 0))
        for po in purchase_orders
    )

    purchase_return_summary = await _dashboard_purchase_return_summary(start=start, end=end)

    patient_alerts = await _get_patient_alerts(today)

    return {
        "sales": round(total_sales, 2),
        "sales_total": round(total_sales, 2),
        "sales_month": round(sales_month, 2),
        "sales_this_month": round(sales_month, 2),
        "sales_today": round(sales_today, 2),

        "gst_collected": round(total_gst, 2),
        "discount_given": round(total_discount, 2),

        "expenses": round(total_expenses, 2),
        "expenses_total": round(total_expenses, 2),
        "expenses_month": round(expenses_month, 2),
        "expenses_this_month": round(expenses_month, 2),
        "expenses_today": round(expenses_today, 2),

        "profit": round(profit, 2),
        "profit_total": round(profit, 2),
        "profit_month": round(profit_month, 2),
        "profit_this_month": round(profit_month, 2),
        "profit_today": round(profit_today, 2),

        "stock_value": round(stock_value, 2),

        "total_purchase_amount": round(total_purchase_amount, 2),

        "customer_outstanding": round(customer_outstanding, 2),
        "customer_receivables": round(customer_outstanding, 2),
        "customer_outstanding_month": round(customer_outstanding_month, 2),
        "customer_outstanding_today": round(customer_outstanding_today, 2),

        "distributor_outstanding": round(distributor_outstanding, 2),
        "distributor_payables": round(distributor_outstanding, 2),
        # Backward-compatible combined value for older dashboard clients.
        "pending_payment": round(customer_outstanding + distributor_outstanding, 2),

        "amount_received": round(received_total, 2),
        "amount_received_month": round(received_month, 2),
        "amount_received_today": round(received_today, 2),

        "low_stock_count": len(low_stock_items),
        "low_stock_items": low_stock_items,
        "low_stock_medicines": low_stock_items,

        "expiring_soon_count": len(expiring_soon_items),
        "expiring_soon_items": expiring_soon_items,
        "expiring_soon": expiring_soon_items,

        "expired_count": len(expired_items),
        "expired_items": expired_items,
        "recently_expired_medicines": expired_items,

        "patient_alert_count": len(patient_alerts),
        "patient_alerts": patient_alerts,
        "patient_due_alerts_count": len(patient_alerts),
        "patient_due_alerts": patient_alerts,

        "purchase_return_summary": purchase_return_summary,
    }
    
@api_router.get("/reports/sales")
async def sales_report(start: Optional[str] = None, end: Optional[str] = None, user: dict = Depends(get_current_user)):
    q = {}
    if start or end:
        q["created_at"] = {}
        if start:
            q["created_at"]["$gte"] = start
        if end:
            q["created_at"]["$lte"] = end + "T23:59:59"
    invoices = await db.invoices.find(q, {"_id": 0}).to_list(5000)
    medicine_ids = {item.get("medicine_id") for invoice in invoices for item in invoice.get("items", []) if item.get("medicine_id")}
    medicines = await db.medicines.find({"id": {"$in": list(medicine_ids)}}, {"_id": 0, "id": 1, "purchase_price": 1}).to_list(len(medicine_ids) or 1)
    costs = {medicine.get("id"): _safe_float(medicine.get("purchase_price")) for medicine in medicines}
    total_sales = total_gst = total_discount = profit = 0.0
    daily, monthly, monthly_profit = defaultdict(float), defaultdict(float), defaultdict(float)
    payment_modes = defaultdict(float)
    medicine_sales = defaultdict(lambda: {"medicine": "Unknown", "units_sold": 0.0, "revenue": 0.0, "cost": 0.0})
    for invoice in invoices:
        invoice_total = _safe_float(invoice.get("total", invoice.get("grand_total", 0)))
        total_sales += invoice_total
        total_gst += _safe_float(invoice.get("gst_total", invoice.get("total_gst", 0)))
        total_discount += _safe_float(invoice.get("bill_discount", invoice.get("discount", 0)))
        day = str(invoice.get("created_at") or "")[:10]
        if day:
            daily[day] += invoice_total
            monthly[day[:7]] += invoice_total
        payment_modes[str(invoice.get("payment_mode") or invoice.get("payment_method") or "unknown").lower()] += invoice_total
        invoice_profit = 0.0
        for item in invoice.get("items", []):
            quantity = _safe_float(item.get("units_dispensed", item.get("quantity", 0)))
            revenue = _safe_float(item.get("line_total", item.get("net_amount", item.get("mrp", 0) * quantity)))
            # Current invoices persist the batch-aware purchase cost; legacy rows fall back to batch medicine cost.
            cost = _safe_float(item.get("purchase_cost"), -1)
            if cost < 0:
                unit_cost = _safe_float(item.get("purchase_rate", item.get("purchase_price", costs.get(item.get("medicine_id"), 0))))
                cost = unit_cost * quantity
            item_profit = revenue - cost
            profit += item_profit
            invoice_profit += item_profit
            med_key = item.get("medicine_id") or item.get("name") or item.get("medicine_name") or "Unknown"
            row = medicine_sales[med_key]
            row["medicine"] = item.get("name") or item.get("medicine_name") or row["medicine"]
            row["units_sold"] += quantity
            row["revenue"] += revenue
            row["cost"] += cost
        if day:
            monthly_profit[day[:7]] += invoice_profit
    def _rank_row(row: dict) -> dict:
        revenue = row["revenue"]; cost = row["cost"]; item_profit = revenue - cost
        return {"medicine": row["medicine"], "units_sold": round_qty(row["units_sold"]), "revenue": _round_ledger_money(revenue),
                "cost": _round_ledger_money(cost), "profit": _round_ledger_money(item_profit),
                "margin_percentage": _round_ledger_money((item_profit / revenue * 100) if revenue else 0)}
    ranked = [_rank_row(row) for row in medicine_sales.values()]
    return {
        "total_sales": _round_ledger_money(total_sales), "total_gst": _round_ledger_money(total_gst),
        "total_discount": _round_ledger_money(total_discount), "estimated_profit": _round_ledger_money(profit),
        "daily": [{"date": key, "total": _round_ledger_money(value)} for key, value in sorted(daily.items())],
        "monthly_sales_trend": [{"month": key, "sales": _round_ledger_money(value)} for key, value in sorted(monthly.items())],
        "monthly_profit_trend": [{"month": key, "profit": _round_ledger_money(value)} for key, value in sorted(monthly_profit.items())],
        "payment_mode_distribution": [{"mode": key, "amount": _round_ledger_money(value)} for key, value in sorted(payment_modes.items())],
        "top_revenue_medicines": sorted(ranked, key=lambda x: x["revenue"], reverse=True)[:20],
        "top_profit_medicines": sorted(ranked, key=lambda x: x["profit"], reverse=True)[:20],
        "average_bill_value": _round_ledger_money(total_sales / len(invoices)) if invoices else 0.0,
        "average_profit_per_invoice": _round_ledger_money(profit / len(invoices)) if invoices else 0.0,
        "highest_billing_day": ({"date": max(daily.items(), key=lambda x: x[1])[0], "sales": _round_ledger_money(max(daily.values()))} if daily else None),
        "invoice_count": len(invoices),
    }


@api_router.get("/reports/stock-valuation")
async def stock_valuation(user: dict = Depends(get_current_user)):
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(5000)
    cost_value = mrp_value = total_units = 0.0
    risk = {key: {"count": 0, "value_at_risk": 0.0} for key in ("expired", "expiring_30", "expiring_90", "safe")}
    today = datetime.now(timezone.utc).date()
    for medicine in medicines:
        available = _available_stock(medicine)
        cost = available * _safe_float(medicine.get("purchase_price"))
        total_units += available; cost_value += cost; mrp_value += available * _safe_float(medicine.get("mrp"))
        details = expiry_details(medicine.get("expiry_date"), today)
        days = details["days_to_expiry"]
        key = ("expired" if details["expiry_status"] == "expired" else "expiring_30" if days is not None and days <= 30
               else "expiring_90" if days is not None and days <= EXPIRY_WARNING_DAYS else "safe")
        if available > 0:
            risk[key]["count"] += 1; risk[key]["value_at_risk"] += cost
    expiry_risk_count = sum(risk[key]["count"] for key in ("expired", "expiring_30", "expiring_90"))
    expiry_values = _expiry_risk_values(
        risk["expired"]["value_at_risk"], risk["expiring_30"]["value_at_risk"], risk["expiring_90"]["value_at_risk"]
    )
    return {"total_items": len(medicines), "total_units": round_qty(total_units), "cost_value": _round_ledger_money(cost_value),
            "mrp_value": _round_ledger_money(mrp_value), "potential_profit": _round_ledger_money(mrp_value-cost_value),
            "expiry_risk_counts": {key: value["count"] for key, value in risk.items()},
            "expiry_values_at_risk": {key: _round_ledger_money(value["value_at_risk"]) for key, value in risk.items()},
            "expiry_risk_count": expiry_risk_count,
            "expired_count": risk["expired"]["count"], "expiring_30_count": risk["expiring_30"]["count"],
            "expiring_90_count": risk["expiring_90"]["count"], "safe_count": risk["safe"]["count"],
            **expiry_values, "total_expiry_value_at_risk": expiry_values["expiry_value_at_risk"]}


def _outstanding_aging(transactions: List[dict], charge_types: Set[str], credit_types: Set[str]) -> dict:
    today = datetime.now(timezone.utc).date()
    charges = []
    credits = 0.0
    for txn in sorted(transactions, key=_distributor_fifo_sort_key):
        amount = max(0.0, _safe_float(txn.get("amount")))
        txn_type = str(txn.get("type") or "").lower()
        if txn_type in charge_types:
            charges.append([_analytics_date(txn.get("transaction_date") or txn.get("date") or txn.get("created_at"), today), amount])
        elif txn_type in credit_types:
            credits += amount
    for charge in charges:
        applied = min(charge[1], credits); charge[1] -= applied; credits -= applied
    buckets = {"0-30": 0.0, "31-60": 0.0, "61-90": 0.0, "90+": 0.0}
    oldest = 0
    for charged_on, amount in charges:
        if amount <= 0: continue
        age = max(0, (today - charged_on).days); oldest = max(oldest, age)
        bucket = "0-30" if age <= 30 else "31-60" if age <= 60 else "61-90" if age <= 90 else "90+"
        buckets[bucket] += amount
    return {"buckets": {key: _round_ledger_money(value) for key, value in buckets.items()}, "oldest_due_days": oldest,
            "urgency": "critical" if oldest > 90 else "high" if oldest > 60 else "medium" if oldest > 30 else "normal"}


def _purchase_order_as_distributor_transaction(po: dict) -> dict:
    row = dict(po)
    row["type"] = "purchase"
    row["amount"] = _safe_float(po.get("grand_total", po.get("total", po.get("amount", 0))))
    row["transaction_date"] = po.get("po_date") or po.get("created_at") or po.get("date")
    row.setdefault("created_at", po.get("created_at") or po.get("po_date"))
    row.setdefault("purchase_order_id", po.get("id") or po.get("purchase_order_id"))
    row.setdefault("source", "purchase_order")
    row.setdefault("backend_row_source", "purchase_orders")
    row.setdefault("is_synthetic", True)
    return row


def _distributor_monthly_outstanding_movement(distributors: List[dict], distributor_txns: List[dict]) -> List[dict]:
    """Build month-end distributor payable movement from real distributor payable sources."""
    distributor_by_id = {str(d.get("id") or ""): d for d in distributors if isinstance(d, dict)}
    raw_txns_by_distributor = defaultdict(list)
    for txn in distributor_txns:
        if not isinstance(txn, dict):
            continue
        raw_txns_by_distributor[str(txn.get("distributor_id") or "")].append(txn)

    rows_by_month = defaultdict(lambda: {"purchases": 0.0, "payments": 0.0, "adjustments": 0.0})
    txns_by_month = defaultdict(list)
    running_balance_by_distributor = defaultdict(float)
    distributor_ids = set(distributor_by_id) | set(raw_txns_by_distributor)

    for distributor_id in sorted(distributor_ids):
        distributor = distributor_by_id.get(distributor_id, {"id": distributor_id})
        # Canonicalize each distributor's own ledger stream so legacy opening
        # balances are represented exactly as the Distributor Ledger does,
        # without ever consulting Customer Ledger transactions.
        raw_distributor_txns = raw_txns_by_distributor.get(distributor_id, [])
        if distributor_id in distributor_by_id:
            ledger_txns = _distributor_opening_balance_deduped_transactions(distributor, raw_distributor_txns)
            if raw_distributor_txns and not (distributor.get("created_at") or _distributor_opening_balance_date(distributor)):
                first_ledger_date = min(
                    (
                        parsed
                        for raw_txn in raw_distributor_txns
                        if (parsed := _analytics_date(raw_txn.get("transaction_date") or raw_txn.get("date") or raw_txn.get("created_at")))
                    ),
                    default=None,
                )
                if first_ledger_date:
                    for index, ledger_txn in enumerate(ledger_txns):
                        if str(ledger_txn.get("type") or "").lower() == "opening_balance":
                            dated_opening = ledger_txn.copy()
                            dated_opening["transaction_date"] = first_ledger_date.isoformat()
                            dated_opening["created_at"] = first_ledger_date.isoformat()
                            ledger_txns[index] = dated_opening
                            break
        else:
            ledger_txns = list(raw_distributor_txns)

        for txn in sorted(ledger_txns, key=_distributor_fifo_sort_key):
            month = _month_key(txn.get("transaction_date") or txn.get("date") or txn.get("created_at"))
            if not month:
                continue
            txns_by_month[month].append(txn)
            _balance, bucket = _apply_distributor_transaction(0, txn)
            amount = _safe_float(txn.get("amount"))
            rows_by_month[month]
            if bucket == "purchase":
                if str(txn.get("type") or "").lower() != "opening_balance":
                    rows_by_month[month]["purchases"] += amount
            elif bucket == "adjustment":
                rows_by_month[month]["adjustments"] += amount
            else:
                rows_by_month[month]["payments"] += amount

    if not rows_by_month:
        logger.info(
            "Distributor outstanding movement generated distributors_processed=%s movement_records=%s chart_dataset=%s",
            len(distributor_ids),
            0,
            [],
        )
        return []

    movement = []
    previous_closing = 0.0
    for month in sorted(rows_by_month):
        for txn in sorted(txns_by_month.get(month, []), key=_distributor_fifo_sort_key):
            distributor_id = str(txn.get("distributor_id") or "")
            running_balance_by_distributor[distributor_id], _bucket = _apply_distributor_transaction(
                running_balance_by_distributor[distributor_id],
                txn,
            )
        row = rows_by_month[month]
        closing_payable = _round_ledger_money(sum(max(balance, 0.0) for balance in running_balance_by_distributor.values()))
        net_movement = _round_ledger_money(closing_payable - previous_closing)
        movement.append({
            "month": month,
            "purchases": _round_ledger_money(row["purchases"]),
            "payments": _round_ledger_money(row["payments"]),
            "adjustments": _round_ledger_money(row["adjustments"]),
            "opening_distributor_payable": _round_ledger_money(previous_closing),
            "closing_distributor_payable": closing_payable,
            "outstanding_payable": closing_payable,
            "net_movement": net_movement,
            "outstanding_increase": _round_ledger_money(max(net_movement, 0.0)),
            "outstanding_decrease": _round_ledger_money(abs(min(net_movement, 0.0))),
        })
        previous_closing = closing_payable

    logger.info(
        "Distributor outstanding movement generated distributors_processed=%s movement_records=%s chart_dataset=%s",
        len(distributor_ids),
        len(movement),
        movement,
    )
    return movement


@api_router.get("/reports/outstanding")
async def outstanding_report(user: dict = Depends(get_current_user)):
    customers = await db.customers.find({}, {"_id": 0}).to_list(1000)
    distributors = await db.distributors.find({}, {"_id": 0}).to_list(1000)
    customer_txns = await db.customer_transactions.find({}, {"_id": 0}).to_list(10000)
    distributor_txns = await db.distributor_transactions.find({}, {"_id": 0}).to_list(10000)
    customer_grouped, distributor_grouped = defaultdict(list), defaultdict(list)
    for txn in customer_txns: customer_grouped[txn.get("customer_id")].append(txn)
    for txn in distributor_txns: distributor_grouped[txn.get("distributor_id")].append(txn)
    cust_out, dist_out = [], []
    customer_aging = {key: 0.0 for key in ("0-30", "31-60", "61-90", "90+")}; distributor_aging = dict(customer_aging)
    for customer in customers:
        aging = _outstanding_aging(customer_grouped[customer.get("id")], {"sale"}, {"payment", "credit", "credit_adjustment"})
        balance = sum(aging["buckets"].values())
        if balance > 0:
            last_payment = max((_analytics_date(t.get("transaction_date") or t.get("date") or t.get("created_at")) for t in customer_grouped[customer.get("id")] if str(t.get("type")).lower() == "payment"), default=None)
            cust_out.append({"id": customer["id"], "name": customer["name"], "customer": customer["name"], "phone": customer.get("phone", ""), "balance": _round_ledger_money(balance), "outstanding": _round_ledger_money(balance), "age": aging["oldest_due_days"], "aging_days": aging["oldest_due_days"], "last_payment": last_payment.isoformat() if last_payment else None, **aging})
            for key, value in aging["buckets"].items(): customer_aging[key] += value
    for distributor in distributors:
        txns = distributor_grouped[distributor.get("id")]
        aging = _outstanding_aging(txns, {"purchase", "sale", "opening_balance"}, {"payment", "purchase_return", "credit", "credit_adjustment"})
        balance = max(0.0, _current_distributor_balance(distributor, txns))
        if balance > 0:
            # Legacy opening balances may not have a transaction; retain them in the current bucket.
            gap = max(0.0, balance - sum(aging["buckets"].values())); aging["buckets"]["0-30"] = _round_ledger_money(aging["buckets"]["0-30"] + gap)
            last_purchase = max((_analytics_date(t.get("transaction_date") or t.get("date") or t.get("created_at")) for t in txns if str(t.get("type")).lower() in {"purchase", "opening_balance"}), default=None)
            dist_out.append({"id": distributor["id"], "name": distributor["name"], "distributor": distributor["name"], "balance": _round_ledger_money(balance), "outstanding": _round_ledger_money(balance), "age": aging["oldest_due_days"], "aging_days": aging["oldest_due_days"], "last_purchase": last_purchase.isoformat() if last_purchase else None, **aging})
            for key, value in aging["buckets"].items(): distributor_aging[key] += value
    customer_total = sum(row["balance"] for row in cust_out); distributor_total = sum(row["balance"] for row in dist_out)
    distributor_outstanding_movement = _distributor_monthly_outstanding_movement(distributors, distributor_txns)
    trends = [
        {
            "month": row["month"],
            "customer_receivables": 0.0,
            "distributor_payables": _round_ledger_money(row["closing_distributor_payable"]),
            "net_exposure": _round_ledger_money(row["closing_distributor_payable"]),
        }
        for row in distributor_outstanding_movement
    ]
    return {"customers": cust_out, "distributors": dist_out, "customer_total": _round_ledger_money(customer_total), "distributor_total": _round_ledger_money(distributor_total),
            "customer_receivables": _round_ledger_money(customer_total), "distributor_payables": _round_ledger_money(distributor_total),
            "net_exposure": _round_ledger_money(distributor_total - customer_total),
            "customer_recovery_ranking": sorted(cust_out, key=lambda x: x["outstanding"], reverse=True),
            "distributor_payable_ranking": sorted(dist_out, key=lambda x: x["outstanding"], reverse=True),
            "monthly_outstanding_trend": trends,
            "monthly_outstanding_trends": trends,
            "customer_aging": {key: _round_ledger_money(value) for key, value in customer_aging.items()}, "distributor_aging": {key: _round_ledger_money(value) for key, value in distributor_aging.items()},
            "aging_buckets": {"customers": {key: _round_ledger_money(value) for key, value in customer_aging.items()}, "distributors": {key: _round_ledger_money(value) for key, value in distributor_aging.items()}},
            "schema_notes": {"aging_days": "Numeric age in days for the oldest unpaid charge; legacy age is the same days value."},
            "distributor_outstanding_movement": distributor_outstanding_movement}


@api_router.post("/daily-summary")
async def add_daily_summary(payload: dict, user: dict = Depends(get_current_user)):
    doc = {
        "id": str(uuid.uuid4()),
        "date": payload.get("date"),
        "total_sales": payload.get("total_sales", 0),
        "cash": payload.get("cash", 0),
        "upi": payload.get("upi", 0),
        "pending": payload.get("pending", 0),
        "notes": payload.get("notes", ""),
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    await db.daily_summary.insert_one(doc)
    return doc
    

@api_router.get("/reports/expiry")
async def expiry_report(user: dict = Depends(get_current_user)):
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(5000)
    today = datetime.now(timezone.utc).date()
    expired, expiring_30, expiring_90, safe, risk_medicines = [], [], [], [], []
    total_inventory_cost_value = total_inventory_mrp_value = 0.0

    for m in medicines:
        available = _available_stock(m)
        if available <= 0:
            continue
        purchase_price = _safe_float(m.get("purchase_price"))
        total_inventory_cost_value += available * purchase_price
        total_inventory_mrp_value += available * _safe_float(m.get("mrp"))
        details = expiry_details(m.get("expiry_date"), today)

        item = {
            **m,
            "available_units": round_qty(available),
            "cost_value_at_risk": _round_ledger_money(available * _safe_float(m.get("purchase_price"))),
            "mrp_value_at_risk": _round_ledger_money(available * _safe_float(m.get("mrp"))),
            "expiry_status": details["expiry_status"],
            "days_to_expiry": details["days_to_expiry"],
            "days_expired": details["days_expired"],
            "expired_days_ago": details["expired_days_ago"],
        }

        if details["expiry_status"] == "expired":
            expired.append(item)
            risk_medicines.append(item)
        elif details["days_to_expiry"] is not None and details["days_to_expiry"] <= 30:
            expiring_30.append(item)
            risk_medicines.append(item)
        elif details["days_to_expiry"] is not None and details["days_to_expiry"] <= EXPIRY_WARNING_DAYS:
            # This is the non-overlapping 31-90 day risk bucket.
            expiring_90.append(item)
            risk_medicines.append(item)
        else:
            safe.append(item)

    near = expiring_30 + expiring_90
    expired_value = sum(item["cost_value_at_risk"] for item in expired)
    expiring_30_value = sum(item["cost_value_at_risk"] for item in expiring_30)
    expiring_90_value = sum(item["cost_value_at_risk"] for item in expiring_90)
    expiry_values = _expiry_risk_values(expired_value, expiring_30_value, expiring_90_value)

    return {
        "expired": sorted(
            expired,
            key=lambda x: x.get("days_expired") or 0,
            reverse=True
        ),
        "near_expiry": sorted(
            near,
            key=lambda x: x.get("days_to_expiry") or 0
        ),
        "expired_count": len(expired),
        "near_expiry_count": len(near),
        "expiry_risk_count": len(expired) + len(near),
        "expiring_30_count": len(expiring_30),
        "expiring_90_count": len(expiring_90),
        "safe_count": len(safe),
        "total_inventory_cost_value": _round_ledger_money(total_inventory_cost_value),
        "total_inventory_mrp_value": _round_ledger_money(total_inventory_mrp_value),
        "top_expiry_risk_medicines": [
            {
                "medicine": item.get("name") or item.get("medicine_name") or "Unknown",
                "batch": item.get("batch_no") or item.get("batch_number") or item.get("batch") or "",
                "expiry": item.get("expiry_date"),
                "stock": item["available_units"],
                "purchase_price": _round_ledger_money(_safe_float(item.get("purchase_price"))),
                "risk_value": item["cost_value_at_risk"],
            }
            for item in sorted(risk_medicines, key=lambda x: x["cost_value_at_risk"], reverse=True)[:20]
        ],
        **expiry_values,
        "near_expiry_value_at_risk": _round_ledger_money(
            expiry_values["expiring_30_value_at_risk"] + expiry_values["expiring_90_value_at_risk"]
        ),
        "total_value_at_risk": expiry_values["expiry_value_at_risk"],
    }


@api_router.get("/reports/top-medicines")
async def top_medicines(start: str, end: str, user: dict = Depends(get_current_user)):
    sales = await db.daily_sales.find({
        "sale_date": {"$gte": start, "$lte": end}
    }, {"_id": 0}).to_list(5000)

    map_data = {}

    for s in sales:
        mid = s.get("medicine_id")
        if not mid:
            continue

        qty = float(s.get("quantity", 0))
        amt = float(s.get("total_amount", 0))

        if mid not in map_data:
            map_data[mid] = {
                "medicine_id": mid,
                "medicine_name": s.get("medicine_name"),
                "total_qty": 0,
                "revenue": 0
            }

        map_data[mid]["total_qty"] += qty
        map_data[mid]["revenue"] += amt

    result = sorted(map_data.values(), key=lambda x: x["revenue"], reverse=True)

    return result[:20]


def _invoice_item_quantity(item: dict) -> float:
    return _safe_float(item.get("units_dispensed", item.get("quantity", 0)))


async def _invoice_item_profit_rows() -> Tuple[List[dict], dict]:
    invoices = await db.invoices.find({}, {"_id": 0}).to_list(20000)
    medicine_ids = {item.get("medicine_id") for invoice in invoices for item in invoice.get("items", []) if item.get("medicine_id")}
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(20000)
    by_id = {m.get("id"): m for m in medicines}
    rows = []
    for invoice in invoices:
        sold_on = _analytics_date(invoice.get("created_at") or invoice.get("invoice_date") or invoice.get("date"))
        for item in invoice.get("items", []):
            qty = _invoice_item_quantity(item)
            revenue = _safe_float(item.get("line_total", item.get("net_amount", item.get("mrp", 0) * qty)))
            cost = _safe_float(item.get("purchase_cost"), -1)
            med = by_id.get(item.get("medicine_id"), {})
            if cost < 0:
                cost = _safe_float(item.get("purchase_rate", item.get("purchase_price", med.get("purchase_price", 0)))) * qty
            rows.append({"medicine_id": item.get("medicine_id"), "medicine": item.get("name") or item.get("medicine_name") or med.get("name") or "Unknown",
                         "quantity": qty, "revenue": revenue, "cost": cost, "sold_on": sold_on, "category": item.get("category") or med.get("category")})
    return rows, by_id


def _finalize_profit_rows(grouped: dict) -> List[dict]:
    output = []
    for values in grouped.values():
        profit = values["revenue"] - values["cost"]
        output.append({"medicine": values.get("medicine", "Unknown"), "revenue": _round_ledger_money(values["revenue"]),
                       "cost": _round_ledger_money(values["cost"]), "profit": _round_ledger_money(profit),
                       "margin_percentage": _round_ledger_money((profit / values["revenue"] * 100) if values["revenue"] else 0),
                       "units_sold": round_qty(values["units_sold"])})
    return output


@api_router.get("/reports/medicine-profitability")
async def medicine_profitability(sort_by: Literal["revenue", "cost", "profit", "margin_percentage", "units_sold"] = "profit", user: dict = Depends(get_current_user)):
    rows, _ = await _invoice_item_profit_rows()
    grouped = defaultdict(lambda: {"medicine": "Unknown", "revenue": 0.0, "cost": 0.0, "units_sold": 0.0})
    for row in rows:
        key = row.get("medicine_id") or row["medicine"]
        grouped[key]["medicine"] = row["medicine"]; grouped[key]["revenue"] += row["revenue"]; grouped[key]["cost"] += row["cost"]; grouped[key]["units_sold"] += row["quantity"]
    return {"items": sorted(_finalize_profit_rows(grouped), key=lambda x: x[sort_by], reverse=True)}


@api_router.get("/reports/category-profitability")
async def category_profitability(user: dict = Depends(get_current_user)):
    rows, _ = await _invoice_item_profit_rows()
    grouped = defaultdict(lambda: {"category": "Other", "revenue": 0.0, "cost": 0.0})
    for row in rows:
        category = str(row.get("category") or "Other").upper()
        bucket = category if category in STANDARD_MEDICINE_CATEGORIES else "Other"
        grouped[bucket]["category"] = bucket
        grouped[bucket]["revenue"] += row["revenue"]; grouped[bucket]["cost"] += row["cost"]
    items = []
    for values in grouped.values():
        profit = values["revenue"] - values["cost"]
        items.append({"category": values["category"], "revenue": _round_ledger_money(values["revenue"]), "profit": _round_ledger_money(profit),
                      "margin": _round_ledger_money((profit / values["revenue"] * 100) if values["revenue"] else 0)})
    return {"items": sorted(items, key=lambda x: x["profit"], reverse=True)}


async def _medicine_movement_items() -> List[dict]:
    rows, by_id = await _invoice_item_profit_rows()
    grouped = defaultdict(lambda: {"medicine": "Unknown", "units_sold": 0.0, "revenue": 0.0, "last_sale_date": None})
    for row in rows:
        key = row.get("medicine_id") or row["medicine"]
        grouped[key]["medicine"] = row["medicine"]
        grouped[key]["units_sold"] += row["quantity"]
        grouped[key]["revenue"] += row["revenue"]
        if row["sold_on"]:
            grouped[key]["last_sale_date"] = max(grouped[key]["last_sale_date"] or date.min, row["sold_on"])
    items = []
    for key, values in grouped.items():
        medicine = by_id.get(key, {})
        stock = _available_stock(medicine) if medicine else 0.0
        items.append({
            "medicine": values["medicine"],
            "units_sold": round_qty(values["units_sold"]),
            "revenue": _round_ledger_money(values["revenue"]),
            "current_stock": round_qty(stock),
            "last_sale_date": values["last_sale_date"].isoformat() if values["last_sale_date"] else None,
        })
    return items


@api_router.get("/reports/fast-moving-medicines")
async def fast_moving_medicines(limit: int = 20, user: dict = Depends(get_current_user)):
    limit = max(1, min(limit, 100))
    items = await _medicine_movement_items()
    return {"items": sorted(items, key=lambda x: (x["units_sold"], x["revenue"]), reverse=True)[:limit]}


@api_router.get("/reports/slow-moving-medicines")
async def slow_moving_medicines(limit: int = 20, user: dict = Depends(get_current_user)):
    limit = max(1, min(limit, 100))
    items = [item for item in await _medicine_movement_items() if item["current_stock"] > 0]
    return {"items": sorted(items, key=lambda x: (x["units_sold"], -x["current_stock"], x["medicine"]))[:limit]}


@api_router.get("/reports/dead-stock")
async def dead_stock_report(days: Literal[90, 180, 365] = 90, user: dict = Depends(get_current_user)):
    rows, _ = await _invoice_item_profit_rows()
    last_sale = {}
    for row in rows:
        if row["sold_on"]:
            last_sale[row.get("medicine_id") or row["medicine"]] = max(last_sale.get(row.get("medicine_id") or row["medicine"], date.min), row["sold_on"])
    today = datetime.now(timezone.utc).date()
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(20000)
    items = []
    for m in medicines:
        stock = _available_stock(m)
        if stock <= 0: continue
        sold_on = last_sale.get(m.get("id") or m.get("name"))
        age = (today - sold_on).days if sold_on else 999999
        if age >= days:
            items.append({"medicine": m.get("name") or "Unknown", "stock": round_qty(stock), "inventory_value": _round_ledger_money(stock * _safe_float(m.get("purchase_price"))),
                          "last_sale_date": sold_on.isoformat() if sold_on else None, "days_since_last_sale": age})
    return {"days": days, "items": sorted(items, key=lambda x: x["inventory_value"], reverse=True)}


@api_router.get("/reports/reorder")
async def reorder_report(user: dict = Depends(get_current_user)):
    rows, _ = await _invoice_item_profit_rows()
    first_sale = min((r["sold_on"] for r in rows if r["sold_on"]), default=datetime.now(timezone.utc).date())
    months = max(1, ((datetime.now(timezone.utc).date() - first_sale).days + 29) // 30)
    sold = defaultdict(float)
    for row in rows: sold[row.get("medicine_id") or row["medicine"]] += row["quantity"]
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(20000)
    items = []
    for m in medicines:
        stock = _available_stock(m); avg = sold[m.get("id") or m.get("name")] / months
        if avg <= 0: continue
        days_remaining = stock / (avg / 30)
        items.append({"medicine": m.get("name") or "Unknown", "current_stock": round_qty(stock), "average_monthly_sale": round_qty(avg),
                      "estimated_days_remaining": round(days_remaining, 1), "suggested_reorder_quantity": round_qty(max(0, avg * 2 - stock))})
    return {"items": sorted(items, key=lambda x: x["estimated_days_remaining"])}


# ---------------- Analytics (compact, tenant-scoped payloads) ----------------
def _analytics_date(value, fallback: Optional[date] = None) -> Optional[date]:
    parsed = parse_iso_date(value)
    return parsed or fallback


def _month_key(value) -> Optional[str]:
    parsed = _analytics_date(value)
    return parsed.strftime("%Y-%m") if parsed else None


async def _analytics_snapshot(months: int = 12, limit: int = 10) -> dict:
    months = max(1, min(months, 36))
    limit = max(1, min(limit, 50))
    cutoff = datetime.now(timezone.utc).date().replace(day=1)
    for _ in range(months - 1):
        cutoff = (cutoff - timedelta(days=1)).replace(day=1)
    cutoff_iso = cutoff.isoformat()

    invoices = await db.invoices.find({"created_at": {"$gte": cutoff_iso}}, {"_id": 0}).to_list(10000)
    purchase_orders = await db.purchase_orders.find({}, {"_id": 0}).to_list(10000)
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(10000)
    customer_txns = await db.customer_transactions.find({}, {"_id": 0}).to_list(10000)
    distributor_txns = await db.distributor_transactions.find({}, {"_id": 0}).to_list(10000)

    sales_by_month = defaultdict(float)
    purchases_by_month = defaultdict(float)
    payment_modes = defaultdict(float)
    medicine_sales = defaultdict(lambda: {"quantity": 0.0, "revenue": 0.0})
    recovery_by_month = defaultdict(lambda: {"customer_charged": 0.0, "customer_recovered": 0.0, "distributor_charged": 0.0, "distributor_recovered": 0.0})

    for invoice in invoices:
        month = _month_key(invoice.get("created_at"))
        total = float(invoice.get("total", invoice.get("grand_total", 0)) or 0)
        if month:
            sales_by_month[month] += total
            payment_modes[str(invoice.get("payment_mode") or "unknown").lower()] += total
        for item in invoice.get("items", []):
            name = str(item.get("name") or "Unknown")
            quantity = float(item.get("units_dispensed", item.get("quantity", 0)) or 0)
            medicine_sales[name]["quantity"] += quantity
            medicine_sales[name]["revenue"] += float(item.get("line_total", item.get("price", item.get("mrp", 0)) * quantity) or 0)

    for po in purchase_orders:
        month = _month_key(po.get("created_at") or po.get("po_date"))
        if month and month >= cutoff.strftime("%Y-%m"):
            purchases_by_month[month] += float(po.get("final_payable_total", po.get("grand_total", 0)) or 0)

    for txn in customer_txns:
        month = _month_key(txn.get("transaction_date") or txn.get("date") or txn.get("created_at"))
        if not month or month < cutoff.strftime("%Y-%m"):
            continue
        if txn.get("type") == "sale":
            recovery_by_month[month]["customer_charged"] += float(txn.get("amount", 0) or 0)
        elif txn.get("type") == "payment":
            recovery_by_month[month]["customer_recovered"] += float(txn.get("amount", 0) or 0)

    for txn in distributor_txns:
        month = _month_key(txn.get("transaction_date") or txn.get("date") or txn.get("created_at"))
        if not month or month < cutoff.strftime("%Y-%m"):
            continue
        txn_type = str(txn.get("type") or "").lower()
        if txn_type in {"purchase", "opening_balance"}:
            recovery_by_month[month]["distributor_charged"] += float(txn.get("amount", 0) or 0)
        elif txn_type in {"payment", "purchase_return", "credit", "credit_adjustment"}:
            recovery_by_month[month]["distributor_recovered"] += float(txn.get("amount", 0) or 0)

    expiry_buckets = {"expired": {"count": 0, "units": 0.0, "cost_value": 0.0}, "within_30_days": {"count": 0, "units": 0.0, "cost_value": 0.0}, "within_90_days": {"count": 0, "units": 0.0, "cost_value": 0.0}, "safe": {"count": 0, "units": 0.0, "cost_value": 0.0}}
    today = datetime.now(timezone.utc).date()
    for medicine in medicines:
        available = _available_stock(medicine)
        if available <= 0:
            continue
        details = expiry_details(medicine.get("expiry_date"), today)
        days = details["days_to_expiry"]
        bucket = "expired" if details["expiry_status"] == "expired" else "within_30_days" if days is not None and days <= 30 else "within_90_days" if details["expiry_status"] == "warning" else "safe"
        expiry_buckets[bucket]["count"] += 1
        expiry_buckets[bucket]["units"] += available
        expiry_buckets[bucket]["cost_value"] += available * float(medicine.get("purchase_price", 0) or 0)

    purchase_sales_months = sorted(set(sales_by_month) | set(purchases_by_month))
    recovery_months = sorted(recovery_by_month)
    recovery_trends = []
    for month in recovery_months:
        values = recovery_by_month[month]
        charged = values["customer_charged"] + values["distributor_charged"]
        recovered = values["customer_recovered"] + values["distributor_recovered"]
        recovery_trends.append({"month": month, **{key: round(value, 2) for key, value in values.items()}, "charged": round(charged, 2), "recovered": round(recovered, 2), "net_outstanding_change": round(charged - recovered, 2)})

    return {
        "monthly_sales_trend": [{"month": month, "sales": round(sales_by_month[month], 2)} for month in sorted(sales_by_month)],
        "top_selling_medicines": [{"medicine_name": name, "quantity": round(values["quantity"], 2), "revenue": round(values["revenue"], 2)} for name, values in sorted(medicine_sales.items(), key=lambda item: item[1]["quantity"], reverse=True)[:limit]],
        "expiry_risk": [{"bucket": bucket, "count": values["count"], "units": round(values["units"], 2), "cost_value": round(values["cost_value"], 2)} for bucket, values in expiry_buckets.items()],
        "payment_mode_distribution": [{"mode": mode, "amount": round(amount, 2)} for mode, amount in sorted(payment_modes.items())],
        "purchase_vs_sales": [{"month": month, "purchases": round(purchases_by_month[month], 2), "sales": round(sales_by_month[month], 2)} for month in purchase_sales_months],
        "outstanding_recovery_trends": recovery_trends,
    }


@api_router.get("/reports/analytics")
async def report_analytics(months: int = 12, limit: int = 10, user: dict = Depends(get_current_user)):
    return await _analytics_snapshot(months, limit)


@api_router.get("/reports/analytics/{report_name}")
async def report_analytics_section(report_name: Literal["monthly-sales-trend", "top-selling-medicines", "expiry-risk", "payment-mode-distribution", "purchase-vs-sales", "outstanding-recovery-trends"], months: int = 12, limit: int = 10, user: dict = Depends(get_current_user)):
    key = report_name.replace("-", "_")
    return {"data": (await _analytics_snapshot(months, limit))[key]}



# ---------------- Local-first health, backup, and migration helpers ----------------
async def _database_connected() -> bool:
    if LOCAL_MODE:
        try:
            return LOCAL_DB_PATH.exists() and raw_db.conn.execute("SELECT 1").fetchone()[0] == 1
        except Exception:
            logger.exception("Local database health check failed")
            return False
    try:
        await raw_db.command("ping")
        return True
    except Exception:
        logger.exception("Cloud database health check failed")
        return False


async def _server_health_payload() -> dict:
    return _backend_readiness_payload()


@app.get("/health")
async def health():
    return await _server_health_payload()


@api_router.get("/health")
async def api_health():
    return await _server_health_payload()


async def _internet_available() -> bool:
    if LOCAL_MODE and not mongo_url:
        return False
    try:
        loop = asyncio.get_running_loop()

        await loop.run_in_executor(
            None,
            urllib.request.urlopen, "https://www.google.com/generate_204", timeout=3
        )
        return True
    except Exception:
        return False


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _uploads_backup_manifest() -> List[dict]:
    files = []
    if not UPLOAD_DIR.exists():
        return files
    for path in sorted(p for p in UPLOAD_DIR.rglob("*") if p.is_file()):
        rel_path = path.relative_to(UPLOAD_DIR).as_posix()
        files.append({
            "relative_path": rel_path,
            "size": path.stat().st_size,
            "sha256": _file_sha256(path),
            "content_b64": base64.b64encode(path.read_bytes()).decode("ascii"),
        })
    return files


async def _backup_payload() -> dict:
    return {
        "mode": RUNTIME_MODE,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "collections": {name: await db[name].find({}, {"_id": 0}).to_list(100000) for name in BACKUP_COLLECTIONS},
        "uploads": _uploads_backup_manifest(),
    }


async def _queue_backup_destination(destination: str, reason: str, backup_file: Optional[str] = None, checksum: Optional[str] = None) -> None:
    await raw_db[SYNC_QUEUE_COLLECTION].insert_one({
        "id": str(uuid.uuid4()),
        "type": f"{destination}_backup",
        "destination": destination,
        "reason": reason,
        "backup_file": backup_file,
        "backup_sha256": checksum,
        "status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "attempts": 0,
    })


async def _enqueue_cloud_sync(reason: str, backup_file: Optional[str] = None, checksum: Optional[str] = None) -> None:
    await _queue_backup_destination("atlas", reason, backup_file, checksum)
    await _queue_backup_destination("google_drive", reason, backup_file, checksum)


def _backup_record_id(checksum: str, reason: str) -> str:
    return hashlib.sha256(f"{checksum}:{reason}".encode("utf-8")).hexdigest()


def _derive_backup_key() -> bytes:
    seed = (BACKUP_ENCRYPTION_KEY or "pharmacyos-local-backup").encode("utf-8")
    return hashlib.sha256(seed).digest()


def _xor_crypt(data: bytes) -> bytes:
    key = _derive_backup_key()
    return bytes(byte ^ key[index % len(key)] for index, byte in enumerate(data))


def _write_encrypted_backup_package(json_backup: Path, checksum: str, reason: str, timestamp: str) -> Path:
    package = json_backup.with_suffix(".zip")
    metadata = {
        "reason": reason,
        "created_at": timestamp,
        "source_backup_file": str(json_backup),
        "source_sha256": checksum,
        "encryption": "sha256-derived-xor" if BACKUP_ENCRYPTION_KEY else "not_encrypted_set_BACKUP_ENCRYPTION_KEY",
    }
    encrypted_bytes = _xor_crypt(json_backup.read_bytes()) if BACKUP_ENCRYPTION_KEY else json_backup.read_bytes()
    with zipfile.ZipFile(package, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(json_backup.name + (".enc" if BACKUP_ENCRYPTION_KEY else ""), encrypted_bytes)
        zf.writestr("backup_metadata.json", json.dumps(metadata, indent=2))
    return package


async def _create_local_backup(reason: str = "manual") -> dict:
    payload = await _backup_payload()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_file = BACKUP_DIR / f"pharmacyos-{reason}-{stamp}.json"
    backup_file.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    if not backup_file.exists() or backup_file.stat().st_size <= 0:
        raise RuntimeError(f"Backup integrity check failed for {backup_file}")
    file_size = backup_file.stat().st_size
    checksum = _file_sha256(backup_file)
    package_file = _write_encrypted_backup_package(backup_file, checksum, reason, payload["exported_at"])
    await raw_db.backup_metadata.insert_one({
        "id": str(uuid.uuid4()), "reason": reason, "file": str(backup_file),
        "package_file": str(package_file),
        "created_at": payload["exported_at"], "timestamp": payload["exported_at"],
        "sha256": checksum, "package_sha256": _file_sha256(package_file), "size": file_size,
        "collection_counts": {k: len(v) for k, v in payload["collections"].items()},
        "upload_file_count": len(payload.get("uploads", [])),
    })
    await _enqueue_cloud_sync(reason, str(backup_file), checksum)
    return {"ok": True, "backup_file": str(backup_file), "package_file": str(package_file), "sha256": checksum, "size": file_size, "timestamp": payload["exported_at"], "queued_for_cloud_sync": True, "collection_counts": {k: len(v) for k, v in payload["collections"].items()}, "upload_file_count": len(payload.get("uploads", []))}


def _mark_queue_failed_fields(exc: Exception) -> dict:
    return {"status": "pending", "last_error": str(exc), "last_attempt_at": datetime.now(timezone.utc).isoformat()}


async def _upload_backup_to_atlas(backup_file: str, reason: str = "manual", queue_id: Optional[str] = None) -> dict:
    if not ATLAS_BACKUP_MONGO_URL:
        raise RuntimeError("ATLAS_BACKUP_MONGO_URL is not configured")
    path, payload, checksum, file_size = _load_backup_file_for_restore(backup_file)
    record_id = _backup_record_id(checksum, reason)
    atlas_client = AsyncIOMotorClient(ATLAS_BACKUP_MONGO_URL, serverSelectionTimeoutMS=5000)
    try:
        await atlas_client.admin.command("ping")
        atlas_db = atlas_client[ATLAS_BACKUP_DB_NAME]
        existing = await atlas_db.local_backup_snapshots.find_one({"id": record_id}, {"_id": 0})
        if existing:
            status = "already_uploaded"
        else:
            await atlas_db.local_backup_snapshots.insert_one({
                "id": record_id, "reason": reason, "created_at": datetime.now(timezone.utc).isoformat(),
                "source_exported_at": payload.get("exported_at"), "sha256": checksum, "size": file_size,
                "mode": payload.get("mode"), "collection_counts": {k: len(v) for k, v in payload.get("collections", {}).items()},
                "upload_file_count": len(payload.get("uploads", [])), "payload": payload,
            })
            status = "uploaded"
        await raw_db.backup_status.update_one({"id": "atlas"}, {"$set": {"id": "atlas", "status": status, "last_successful_atlas_backup": datetime.now(timezone.utc).isoformat(), "last_backup_file": str(path), "last_sha256": checksum}}, upsert=True)
        return {"ok": True, "status": status, "record_id": record_id, "sha256": checksum}
    finally:
        atlas_client.close()


def _google_api_request(url: str, token: Optional[str] = None, data: Optional[bytes] = None, headers: Optional[dict] = None, method: Optional[str] = None) -> dict:
    req_headers = dict(headers or {})
    if token:
        req_headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def _load_google_service_account() -> dict:
    if not GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_PATH.exists():
        raise RuntimeError("Google Drive service account key is not configured or file does not exist")
    try:
        data = json.loads(GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Google Drive service account key could not be read: {exc}")
    required = {"client_email", "private_key"}
    missing = sorted(field for field in required if not data.get(field))
    if missing:
        raise RuntimeError(f"Google Drive service account key is missing: {', '.join(missing)}")
    return data


async def _google_service_account_access_token() -> str:
    service_account = _load_google_service_account()
    now = int(datetime.now(timezone.utc).timestamp())
    claims = {
        "iss": service_account["client_email"],
        "scope": "https://www.googleapis.com/auth/drive",
        "aud": service_account.get("token_uri") or GOOGLE_DRIVE_TOKEN_URI,
        "iat": now,
        "exp": now + 3600,
    }
    assertion = jwt.encode(claims, service_account["private_key"], algorithm="RS256")
    if isinstance(assertion, bytes):
        assertion = assertion.decode("utf-8")
    form = urllib.parse.urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": assertion,
    }).encode()
    loop = asyncio.get_running_loop()

    token = await loop.run_in_executor(
        None,
        _google_api_request,
        service_account.get("token_uri") or GOOGLE_DRIVE_TOKEN_URI,
        None,
        form,
        {"Content-Type": "application/x-www-form-urlencoded"},
        "POST",
    )
    if not token.get("access_token"):
        raise RuntimeError("Google Drive service account token request did not return an access token")
    return token["access_token"]


async def _backup_package_for_file(path: Path, reason: str) -> Path:
    metadata = await raw_db.backup_metadata.find_one({"file": str(path)}, {"_id": 0})
    package = Path((metadata or {}).get("package_file") or path.with_suffix(".zip"))
    if not package.exists():
        package = _write_encrypted_backup_package(path, _file_sha256(path), reason, datetime.now(timezone.utc).isoformat())
    return package

async def _upload_backup_to_google_drive(backup_file: str, reason: str = "manual", queue_id: Optional[str] = None) -> dict:
    path = Path(backup_file).expanduser().resolve()
    if not path.exists():
        raise RuntimeError("Backup file not found for Google Drive upload")
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is not configured")
    package = await _backup_package_for_file(path, reason)
    access_token = await _google_service_account_access_token()
    folder_id = GOOGLE_DRIVE_FOLDER_ID
    boundary = "pharmacyosbackup"
    meta = {"name": package.name, "parents": [folder_id], "description": f"PharmacyOS {reason} backup sha256={_file_sha256(package)}"}
    body = (f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{json.dumps(meta)}\r\n--{boundary}\r\nContent-Type: application/zip\r\n\r\n").encode() + package.read_bytes() + f"\r\n--{boundary}--".encode()
    loop = asyncio.get_running_loop()

    uploaded = await loop.run_in_executor(
        None,
        _google_api_request,
        "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,webViewLink",
        access_token,
        body,
        {"Content-Type": f"multipart/related; boundary={boundary}"},
        "POST"
    )
    uploaded_at = datetime.now(timezone.utc).isoformat()
    await raw_db.backup_status.update_one({"id": "google_drive"}, {"$set": {"id": "google_drive", "status": "Google Drive backup successful", "connection_status": "connected", "last_successful_google_drive_backup": uploaded_at, "last_upload_time": uploaded_at, "last_backup_file": str(path), "last_drive_folder_id": folder_id, "last_drive_file_id": uploaded.get("id")}}, upsert=True)
    return {"ok": True, "status": "Google Drive backup successful", "message": "Google Drive backup successful", "drive_file_id": uploaded.get("id"), "drive_file_name": uploaded.get("name")}


async def _process_pending_backup_queue(destination: Optional[str] = None) -> dict:
    query: Dict[str, Any] = {"status": "pending"}
    if destination:
        query["destination"] = destination
    rows = await raw_db[SYNC_QUEUE_COLLECTION].find(query, {"_id": 0}).sort("created_at", 1).to_list(100)
    result = {"attempted": 0, "succeeded": 0, "failed": 0, "errors": []}
    for row in rows:
        result["attempted"] += 1
        dest = row.get("destination") or ("atlas" if row.get("type") == "cloud_backup" else row.get("type", "").replace("_backup", ""))
        try:
            if dest == "atlas":
                upload = await _upload_backup_to_atlas(row["backup_file"], row.get("reason", "queued"), row.get("id"))
            elif dest == "google_drive":
                upload = await _upload_backup_to_google_drive(row["backup_file"], row.get("reason", "queued"), row.get("id"))
            else:
                continue
            await raw_db[SYNC_QUEUE_COLLECTION].update_one({"id": row["id"]}, {"$set": {"status": "completed", "completed_at": datetime.now(timezone.utc).isoformat(), "result": upload}, "$inc": {"attempts": 1}})
            result["succeeded"] += 1
        except Exception as exc:
            await raw_db[SYNC_QUEUE_COLLECTION].update_one({"id": row["id"]}, {"$set": _mark_queue_failed_fields(exc), "$inc": {"attempts": 1}})
            if dest == "google_drive":
                await raw_db.backup_status.update_one({"id": "google_drive"}, {"$set": {"id": "google_drive", "status": "upload_failed_queued", "connection_status": "error", "last_error": str(exc), "last_failed_at": datetime.now(timezone.utc).isoformat()}}, upsert=True)
            result["failed"] += 1
            result["errors"].append({"id": row.get("id"), "destination": dest, "error": str(exc)})
    return result


async def _create_and_sync_backup(reason: str) -> dict:
    backup = await _create_local_backup(reason)
    if LOCAL_MODE and _local_request_busy():
        logger.info("LOCAL_MODE backup %s queued; cloud sync deferred while system is busy", reason)
        return {**backup, "atlas_backup_status": "queued_busy", "google_drive_backup_status": "queued_busy", "sync": {"deferred": True, "reason": "busy"}}
    async with _local_backup_sync_lock:
        if LOCAL_MODE and _local_request_busy():
            logger.info("LOCAL_MODE backup %s queued; cloud sync deferred while system became busy", reason)
            return {**backup, "atlas_backup_status": "queued_busy", "google_drive_backup_status": "queued_busy", "sync": {"deferred": True, "reason": "busy"}}
        sync = await _process_pending_backup_queue()
    return {**backup, "atlas_backup_status": _destination_status(sync, "atlas"), "google_drive_backup_status": _destination_status(sync, "google_drive"), "sync": sync}


def _destination_status(sync: dict, destination: str) -> str:
    if sync.get("succeeded") and not sync.get("failed"):
        return "uploaded"
    if sync.get("failed"):
        return "pending"
    return "pending"


async def _scheduled_local_backup_loop() -> None:
    while True:
        await asyncio.sleep(30 * 60)
        try:
            if LOCAL_MODE and _local_request_busy():
                logger.info("LOCAL_MODE scheduled backup skipped while busy; pending cloud sync remains queued")
                continue
            await _create_and_sync_backup("scheduled_30m")
        except Exception:
            logger.exception("Scheduled local backup failed")


async def _local_idle_backup_sync_loop() -> None:
    while True:
        await asyncio.sleep(15)
        if not LOCAL_MODE or _local_request_busy() or _local_backup_sync_lock.locked():
            continue
        try:
            if await _pending_backup_count():
                async with _local_backup_sync_lock:
                    if not _local_request_busy():
                        logger.info("LOCAL_MODE idle system detected; processing pending cloud backup queue")
                        await _process_pending_backup_queue()
        except Exception:
            logger.exception("LOCAL_MODE idle backup sync failed")


async def _last_backup_metadata() -> Optional[dict]:
    items = await raw_db.backup_metadata.find({}, {"_id": 0}).sort("created_at", -1).to_list(1)
    return items[0] if items else None


async def _pending_backup_count(destination: Optional[str] = None) -> int:
    query = {"status": "pending"}
    if destination:
        query["destination"] = destination
    return await raw_db[SYNC_QUEUE_COLLECTION].count_documents(query)


async def _last_destination_status(destination: str) -> dict:
    row = await raw_db.backup_status.find_one({"id": destination}, {"_id": 0})
    return row or {"id": destination, "status": "not_configured" if destination == "atlas" and not ATLAS_BACKUP_MONGO_URL else "pending"}


async def _verify_import_counts(payload: dict) -> dict:
    source = payload.get("collections", payload)
    names = ["medicines", "invoices", "purchase_orders", "distributors", "customers", "customer_transactions", "distributor_transactions", "purchase_returns", "settings"]
    return {name: {"source": len(source.get(name, [])), "target": await db[name].count_documents({})} for name in names}



def _json_safe_mongo_document(document: dict) -> dict:
    return json.loads(json.dumps(document, default=str))


async def _local_collection_counts(collections: Optional[List[str]] = None) -> dict:
    names = collections or LOCAL_FIRST_IMPORT_COLLECTIONS
    counts = {}
    for name in names:
        if LOCAL_MODE and hasattr(raw_db, "collection_table_count"):
            counts[name] = raw_db.collection_table_count(name)
        else:
            counts[name] = await raw_db[name].count_documents({})
    return counts


def _local_import_sqlite_path() -> Path:
    if not LOCAL_MODE or not isinstance(raw_db, LocalSQLiteDatabase):
        raise HTTPException(status_code=400, detail="Cloud-to-local import requires the LOCAL_MODE SQLite backend")
    configured = Path(LOCAL_DB_PATH).expanduser().resolve()
    if raw_db.path != configured:
        raise HTTPException(
            status_code=500,
            detail=f"Local import SQLite path mismatch: health path {configured} but active database is {raw_db.path}",
        )
    return raw_db.path


async def _cloud_to_local_source_counts(cloud_database, collections: Optional[List[str]] = None) -> dict:
    names = collections or LOCAL_FIRST_IMPORT_COLLECTIONS
    return {name: await cloud_database[name].count_documents({}) for name in names}


def _require_local_cloud_import_config() -> Tuple[str, str]:
    if not LOCAL_MODE:
        raise HTTPException(status_code=400, detail="Cloud-to-local import is only available in LOCAL_MODE")
    if not mongo_url:
        raise HTTPException(status_code=400, detail="MONGO_URL must point to the source Atlas database before importing Local Mode data")
    db_name = os.environ.get("DB_NAME")
    if not db_name:
        raise HTTPException(status_code=400, detail="DB_NAME must name the source Atlas database before importing Local Mode data")
    return mongo_url, db_name


async def _cloud_to_local_import(dry_run: bool, confirm: bool, overwrite_local: bool) -> dict:
    source_url, source_db_name = _require_local_cloud_import_config()
    sqlite_path = _local_import_sqlite_path()
    started_at = datetime.now(timezone.utc).isoformat()
    source_client = AsyncIOMotorClient(source_url, serverSelectionTimeoutMS=8000)
    try:
        await source_client.admin.command("ping")
        source_db = source_client[source_db_name]
        cloud_counts = await _cloud_to_local_source_counts(source_db)
        local_before = await _local_collection_counts()
        local_has_data = any(count > 0 for count in local_before.values())
        logger.info("Local first cloud import dry-run: cloud records found=%s", cloud_counts)
        logger.info("Local first cloud import dry-run: local records before import=%s", local_before)
        result = {
            "ok": True,
            "dry_run": dry_run or not confirm,
            "requires_confirm": True,
            "overwrite_local": overwrite_local,
            "started_at": started_at,
            "source_database": source_db_name,
            "local_database_path": str(sqlite_path),
            "collections": LOCAL_FIRST_IMPORT_COLLECTIONS,
            "cloud_records_found": cloud_counts,
            "local_records_before_import": local_before,
            "local_records_after_import": None,
            "message": "Dry run only. Re-run with dry_run=false&confirm=true after reviewing counts.",
        }
        if dry_run or not confirm:
            return result
        if local_has_data and not overwrite_local:
            raise HTTPException(
                status_code=409,
                detail="Local SQLite already contains data. Re-run with overwrite_local=true only after user confirmation.",
            )
        imported = {}
        for collection_name in LOCAL_FIRST_IMPORT_COLLECTIONS:
            docs = await source_db[collection_name].find({}, {"_id": 0}).to_list(500000)
            safe_docs = [_json_safe_mongo_document(doc) for doc in docs]
            if overwrite_local:
                await raw_db[collection_name].delete_many({})
            if safe_docs:
                await raw_db[collection_name].insert_many(safe_docs)
            imported[collection_name] = len(safe_docs)
        local_after = await _local_collection_counts()
        mismatches = {
            name: {"imported": imported.get(name, 0), "sqlite_count": local_after.get(name, 0)}
            for name in LOCAL_FIRST_IMPORT_COLLECTIONS
            if local_after.get(name, 0) != imported.get(name, 0)
        }
        if mismatches:
            logger.error("Local first cloud import verification failed: %s", mismatches)
            raise HTTPException(status_code=500, detail={"message": "Local import verification failed after SQLite commit", "mismatches": mismatches})
        logger.info("Local first cloud import complete: cloud records found=%s", cloud_counts)
        logger.info("Local first cloud import complete: local records before import=%s", local_before)
        logger.info("Local first cloud import complete: local records after import=%s", local_after)
        return {
            **result,
            "dry_run": False,
            "imported": imported,
            "local_records_after_import": local_after,
            "message": "Cloud data imported into local SQLite. Source Atlas data was read only and not modified.",
        }
    finally:
        source_client.close()

def _load_backup_file_for_restore(backup_file: str, expected_sha256: Optional[str] = None) -> Tuple[Path, dict, str, int]:
    path = Path(backup_file).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Backup file not found")
    if path.stat().st_size <= 0:
        raise HTTPException(status_code=400, detail="Backup file is empty")
    checksum = _file_sha256(path)
    if expected_sha256 and checksum != expected_sha256:
        raise HTTPException(status_code=400, detail="Backup checksum does not match expected_sha256")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Backup JSON is invalid: {exc}")
    if not isinstance(payload.get("collections"), dict):
        raise HTTPException(status_code=400, detail="Backup file is missing collections")
    return path, payload, checksum, path.stat().st_size


def _local_to_cloud_mongo_config() -> Tuple[str, str]:
    if not LOCAL_MODE or not isinstance(raw_db, LocalSQLiteDatabase):
        raise HTTPException(status_code=400, detail="Local-to-cloud sync is only available in LOCAL_MODE")
    cloud_url = os.environ.get("MONGO_URL") or os.environ.get("MONGO_URI") or ""
    if not cloud_url:
        raise HTTPException(status_code=400, detail="Cloud database not configured")
    cloud_db_name = os.environ.get("DB_NAME") or ATLAS_BACKUP_DB_NAME
    return cloud_url, cloud_db_name


def _local_sync_table_exists(table_name: str) -> bool:
    return bool(
        LOCAL_MODE
        and isinstance(raw_db, LocalSQLiteDatabase)
        and raw_db.collection_table_exists(table_name)
    )


def _local_sync_read_table(table_name: str) -> List[dict]:
    if not LOCAL_MODE or not isinstance(raw_db, LocalSQLiteDatabase):
        return []
    return raw_db.read_existing_collection(table_name)


def _local_sync_document_key(document: dict) -> Tuple[dict, dict]:
    safe_doc = _json_safe_mongo_document(document)
    if safe_doc.get("_id") is not None:
        return {"_id": safe_doc["_id"]}, safe_doc
    if safe_doc.get("id") is not None:
        safe_doc.setdefault("_id", str(safe_doc["id"]))
        return {"_id": safe_doc["_id"]}, safe_doc
    generated_id = hashlib.sha256(json.dumps(safe_doc, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    safe_doc["_id"] = generated_id
    safe_doc.setdefault("id", generated_id)
    return {"_id": generated_id}, safe_doc


async def _local_sync_pending_changes(last_sync_time: Optional[str]) -> int:
    if not LOCAL_MODE or not isinstance(raw_db, LocalSQLiteDatabase):
        return 0
    total = 0
    for table_name in LOCAL_TO_CLOUD_SYNC_TABLES:
        if not raw_db.collection_table_exists(table_name):
            continue
        if last_sync_time:
            total += raw_db.collection_table_count_updated_after(table_name, last_sync_time)
        else:
            total += raw_db.collection_table_count_existing(table_name)
    return total


async def _local_sync_status_payload() -> dict:
    status = await raw_db.local_sync_status.find_one({"id": LOCAL_TO_CLOUD_SYNC_STATUS_ID}, {"_id": 0}) if LOCAL_MODE else None
    last_sync_time = (status or {}).get("last_sync_time")
    return {
        "last_sync_time": last_sync_time,
        "last_sync_status": (status or {}).get("last_sync_status", "never_run"),
        "records_synced": (status or {}).get("records_synced", 0),
        "failed_tables": (status or {}).get("failed_tables", []),
        "pending_changes": await _local_sync_pending_changes(last_sync_time),
    }


async def _push_local_sqlite_to_cloud() -> dict:
    cloud_url, cloud_db_name = _local_to_cloud_mongo_config()
    started_at = datetime.now(timezone.utc).isoformat()
    cloud_client = AsyncIOMotorClient(cloud_url, serverSelectionTimeoutMS=8000)
    table_results: Dict[str, Any] = {}
    failed_tables: List[str] = []
    total_uploaded = 0
    try:
        await cloud_client.admin.command("ping")
        cloud_db = cloud_client[cloud_db_name]
        for table_name in LOCAL_TO_CLOUD_SYNC_TABLES:
            if not _local_sync_table_exists(table_name):
                logger.info("LOCAL_TO_CLOUD_SYNC table=%s skipped reason=local_table_missing", table_name)
                table_results[table_name] = {"skipped": True, "reason": "local_table_missing", "records_scanned": 0, "records_uploaded": 0, "errors": []}
                continue
            docs = _local_sync_read_table(table_name)
            scanned = len(docs)
            uploaded = 0
            errors: List[str] = []
            logger.info("LOCAL_TO_CLOUD_SYNC table=%s records_scanned=%s", table_name, scanned)
            for doc in docs:
                try:
                    filter_doc, replacement = _local_sync_document_key(doc)
                    await cloud_db[table_name].replace_one(filter_doc, replacement, upsert=True)
                    uploaded += 1
                except Exception as exc:
                    errors.append(str(exc))
                    logger.exception("LOCAL_TO_CLOUD_SYNC table=%s record upload failed", table_name)
            if errors:
                failed_tables.append(table_name)
            total_uploaded += uploaded
            table_results[table_name] = {"skipped": False, "records_scanned": scanned, "records_uploaded": uploaded, "errors": errors}
            logger.info("LOCAL_TO_CLOUD_SYNC table=%s records_scanned=%s records_uploaded=%s errors=%s", table_name, scanned, uploaded, len(errors))
        finished_at = datetime.now(timezone.utc).isoformat()
        status_value = "success" if not failed_tables else "partial_failure"
        await raw_db.local_sync_status.update_one(
            {"id": LOCAL_TO_CLOUD_SYNC_STATUS_ID},
            {"$set": {"id": LOCAL_TO_CLOUD_SYNC_STATUS_ID, "last_sync_time": finished_at, "last_sync_status": status_value, "records_synced": total_uploaded, "failed_tables": failed_tables, "started_at": started_at, "database": cloud_db_name, "tables": table_results}},
            upsert=True,
        )
        return {**await _local_sync_status_payload(), "ok": not failed_tables, "database": cloud_db_name, "tables": table_results}
    except HTTPException:
        raise
    except Exception as exc:
        failed_tables = LOCAL_TO_CLOUD_SYNC_TABLES
        logger.exception("LOCAL_TO_CLOUD_SYNC failed before table sync")
        await raw_db.local_sync_status.update_one(
            {"id": LOCAL_TO_CLOUD_SYNC_STATUS_ID},
            {"$set": {"id": LOCAL_TO_CLOUD_SYNC_STATUS_ID, "last_sync_time": started_at, "last_sync_status": "failed", "records_synced": 0, "failed_tables": failed_tables, "last_error": str(exc)}},
            upsert=True,
        )
        raise HTTPException(status_code=502, detail=f"Local-to-cloud sync failed: {exc}") from exc
    finally:
        cloud_client.close()


def _restore_upload_files(payload: dict) -> int:
    restored = 0
    for item in payload.get("uploads", []):
        rel_path = Path(str(item.get("relative_path") or ""))
        if rel_path.is_absolute() or ".." in rel_path.parts:
            raise HTTPException(status_code=400, detail="Backup contains unsafe upload path")
        target = UPLOAD_DIR / rel_path
        content = base64.b64decode(item.get("content_b64") or "")
        if hashlib.sha256(content).hexdigest() != item.get("sha256"):
            raise HTTPException(status_code=400, detail=f"Upload checksum mismatch: {rel_path.as_posix()}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        restored += 1
    return restored

# ---------------- Backup ----------------
@api_router.get("/local/performance")
async def local_performance(user: dict = Depends(require_role("admin"))):
    recent = list(_local_recent_requests)
    slowest = sorted(recent, key=lambda item: item.get("duration_ms", 0), reverse=True)[:20]
    grouped = defaultdict(lambda: {"count": 0, "total_ms": 0.0, "max_ms": 0.0})
    for row in recent:
        key = f"{row.get('method')} {row.get('path')}"
        grouped[key]["count"] += 1
        grouped[key]["total_ms"] += float(row.get("duration_ms") or 0)
        grouped[key]["max_ms"] = max(grouped[key]["max_ms"], float(row.get("duration_ms") or 0))
    endpoints = [
        {"endpoint": key, "count": data["count"], "avg_ms": round(data["total_ms"] / data["count"], 2), "max_ms": round(data["max_ms"], 2)}
        for key, data in grouped.items() if data["count"]
    ]
    endpoints.sort(key=lambda item: item["max_ms"], reverse=True)
    return {"local_mode": LOCAL_MODE, "slow_threshold_ms": LOCAL_SLOW_REQUEST_MS, "busy": _local_request_busy(), "recent_count": len(recent), "slowest_recent_requests": slowest, "slowest_endpoints": endpoints[:20]}


@api_router.get("/backup/export")
async def backup_export(user: dict = Depends(require_role("admin"))):
    payload = await _backup_payload()
    return {"exported_at": payload["exported_at"], **payload["collections"]}


@api_router.post("/backup/manual")
async def backup_manual(user: dict = Depends(require_role("admin"))):
    return await _create_and_sync_backup("manual")


@api_router.get("/backup/health")
@api_router.get("/backup/status")
async def backup_health(user: dict = Depends(get_current_user)):
    last_local = await _last_backup_metadata()
    atlas_status = await _last_destination_status("atlas")
    google_status = await _last_destination_status("google_drive")
    return {
        "runtime_mode": RUNTIME_MODE,
        "local_backend_running": True,
        "local_database_connected": await _database_connected(),
        "local_database_path": str(LOCAL_DB_PATH) if LOCAL_MODE else None,
        "local_backup_status": "ok" if last_local else "never_run",
        "atlas_backup_status": atlas_status.get("status", "pending"),
        "google_drive_backup_status": google_status.get("status", "pending"),
        "google_drive_connection_status": google_status.get("connection_status", "configured" if GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_PATH.exists() and GOOGLE_DRIVE_FOLDER_ID else "not_configured"),
        "google_drive_service_account_key_path": str(GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_PATH),
        "google_drive_folder_id_configured": bool(GOOGLE_DRIVE_FOLDER_ID),
        "last_local_backup_at": (last_local or {}).get("created_at"),
        "last_atlas_backup_at": atlas_status.get("last_successful_atlas_backup"),
        "last_google_drive_backup_at": google_status.get("last_successful_google_drive_backup"),
        "last_google_drive_upload_time": google_status.get("last_upload_time") or google_status.get("last_successful_google_drive_backup"),
        "google_drive_last_error": google_status.get("last_error"),
        "google_drive_last_failed_at": google_status.get("last_failed_at"),
        "pending_atlas_sync_count": await _pending_backup_count("atlas"),
        "pending_google_drive_upload_count": await _pending_backup_count("google_drive"),
        "last_backup": last_local,
        "pending_backup_count": await _pending_backup_count(),
        "cloud_sync_status": "online" if await _internet_available() else "queued_offline",
    }


@api_router.post("/backup/exit")
async def backup_exit(user: dict = Depends(require_role("admin"))):
    return await _create_and_sync_backup("app_exit")


@api_router.get("/local-sync/status")
async def local_sync_status(user: dict = Depends(require_role("admin"))):
    if not LOCAL_MODE or not isinstance(raw_db, LocalSQLiteDatabase):
        raise HTTPException(status_code=400, detail="Local-to-cloud sync is only available in LOCAL_MODE")
    return await _local_sync_status_payload()


@api_router.post("/local-sync/push-to-cloud")
async def local_sync_push_to_cloud(user: dict = Depends(require_role("admin"))):
    return await _push_local_sqlite_to_cloud()


@api_router.post("/local/app-exit")
async def local_app_exit_backup(request: Request):
    if not LOCAL_MODE:
        raise HTTPException(status_code=404, detail="Local app-exit backup is only available in LOCAL_MODE")
    client_host = request.client.host if request.client else ""
    if client_host not in {"127.0.0.1", "::1", "localhost"}:
        raise HTTPException(status_code=403, detail="Local app-exit backup is restricted to this computer")
    return await _create_and_sync_backup("app_exit")

@api_router.options("/local-mode/import/dry-run")
@api_router.options("/local/import/dry-run")
@api_router.options("/local-mode/import/confirm")
@api_router.options("/local/import/confirm")
async def local_mode_import_options(request: Request):
    logger.info(
        "Local import OPTIONS fallback: path=%s origin=%s auth=%s",
        request.url.path,
        request.headers.get("origin", "<missing>"),
        "present" if request.headers.get("Authorization") or request.cookies.get("access_token") else "missing",
    )
    return {"ok": True}


@api_router.post("/local-mode/import/dry-run")
@api_router.post("/local/import/dry-run")
async def local_mode_import_dry_run(request: Request = None):
    logger.info(
        "Local import dry-run POST handler: origin=%s auth=%s",
        request.headers.get("origin", "<missing>") if request else "<direct-call>",
        "present" if request and (request.headers.get("Authorization") or request.cookies.get("access_token")) else "missing",
    )
    try:
        return await _cloud_to_local_import(dry_run=True, confirm=False, overwrite_local=False)
    except HTTPException:
        raise
    except Exception:
        logger.exception("Local import dry-run failed with unexpected exception")
        raise HTTPException(status_code=500, detail="Local import dry-run failed; check local backend logs for details")


@api_router.post("/local-mode/import/confirm")
@api_router.post("/local/import/confirm")
async def local_mode_import_confirm(
    request: Request = None,
    payload: Optional[LocalImportConfirmRequest] = Body(None),
    overwrite_local: Optional[bool] = Query(None),
):
    raw_body = "<direct-call>"
    if request:
        try:
            raw_body_bytes = await request.body()
            raw_body = raw_body_bytes.decode("utf-8", errors="replace") if raw_body_bytes else "<empty>"
        except Exception as exc:
            raw_body = f"<unavailable: {exc}>"

    parsed_body = payload.model_dump() if isinstance(payload, LocalImportConfirmRequest) else None
    final_overwrite_local = (
        payload.overwrite_local
        if isinstance(payload, LocalImportConfirmRequest)
        else (overwrite_local if overwrite_local is not None else False)
    )
    logger.info(
        "Local import confirm POST handler: origin=%s auth=%s raw_body=%s parsed_body=%s overwrite_local_query=%s final_overwrite_local=%s",
        request.headers.get("origin", "<missing>") if request else "<direct-call>",
        "present" if request and (request.headers.get("Authorization") or request.cookies.get("access_token")) else "missing",
        raw_body,
        parsed_body,
        overwrite_local,
        final_overwrite_local,
    )
    try:
        logger.info("Local import confirm import logic overwrite_local=%s", final_overwrite_local)
        return await _cloud_to_local_import(dry_run=False, confirm=True, overwrite_local=final_overwrite_local)
    except HTTPException:
        raise
    except Exception:
        logger.exception("Local import confirm failed with unexpected exception")
        raise HTTPException(status_code=500, detail="Local import confirm failed; check local backend logs for details")


@api_router.post("/local-mode/import")
@api_router.post("/local/import")
async def local_mode_import(dry_run: bool = Query(True), confirm: bool = Query(False), overwrite_local: bool = Query(False)):
    return await _cloud_to_local_import(dry_run=dry_run, confirm=confirm, overwrite_local=overwrite_local)


@api_router.post("/backup/sync/retry")
async def backup_sync_retry(dry_run: bool = Query(False), confirm: bool = Query(True), user: dict = Depends(require_role("admin"))):
    online = await _internet_available()
    pending = await _pending_backup_count()
    if dry_run:
        return {"cloud_available": online, "pending_backup_count": pending, "dry_run": True, "requires_confirm": False, "message": "Pending backups are timestamped inserts and never overwrite existing cloud data."}
    sync = await _process_pending_backup_queue()
    return {"cloud_available": online, "pending_backup_count": pending, "dry_run": False, **sync}

@api_router.post("/backup/restore")
async def backup_restore(payload: dict, dry_run: bool = Query(True), confirm: bool = Query(False), user: dict = Depends(require_role("admin"))):
    path, backup_payload, checksum, file_size = _load_backup_file_for_restore(
        payload.get("backup_file", ""),
        payload.get("expected_sha256"),
    )
    verification = await _verify_import_counts(backup_payload)
    result = {
        "backup_file": str(path),
        "sha256": checksum,
        "size": file_size,
        "dry_run": dry_run or not confirm,
        "requires_confirm": True,
        "verification": verification,
        "upload_file_count": len(backup_payload.get("uploads", [])),
    }
    if dry_run or not confirm:
        return result

    pre_restore = await _create_local_backup("pre_restore")
    source = backup_payload.get("collections", {})
    for collection_name, items in source.items():
        if collection_name in BACKUP_COLLECTIONS:
            await db[collection_name].delete_many({})
            if items:
                await db[collection_name].insert_many(items)
    restored_uploads = _restore_upload_files(backup_payload)
    return {**result, "dry_run": False, "pre_restore_backup": pre_restore, "restored_upload_file_count": restored_uploads, "verification": await _verify_import_counts(backup_payload)}


@api_router.post("/backup/import")
async def backup_import(payload: dict, dry_run: bool = Query(True), user: dict = Depends(require_role("admin"))):
    source = payload.get("collections", payload)
    collections = ["medicines", "distributors", "customers", "invoices", "customer_transactions", "distributor_transactions", "purchase_returns", "purchase_orders", "stock_adjustments", "daily_closings", "settings"]
    counts = {}
    if dry_run:
        return {"dry_run": True, "verification": {c: len(source.get(c, [])) for c in collections}}
    for c in collections:
        items = source.get(c, [])
        if c == "medicines":
            items = [_normalize_inventory_quantities(item) for item in items]
        if items:
            await db[c].delete_many({})
            await db[c].insert_many(items)
            counts[c] = len(items)
    return {"dry_run": False, "imported": counts, "verification": await _verify_import_counts(source)}


# ---------------- Purchase Orders / GRN ----------------
class POItem(BaseModel):
    name: str
    batch_no: str
    quantity: float
    free_quantity: float = 0
    pack_size: Optional[str] = None

    purchase_price: float
    mrp: float

    manufacturer: Optional[str] = None
    category: Optional[str] = None

    expiry_date: Optional[str] = None
    gst_rate: float = 5

class POReturnCreditRow(BaseModel):
    id: Optional[str] = None
    medicine_name: str = ""
    medicine_key: str = ""
    medicine_id: str = ""
    batch_number: str = ""
    expiry_date: str = ""
    return_quantity: float = 0
    purchase_rate: float = 0
    gst_rate: Optional[float] = None
    reason: Optional[str] = None
    notes: str = ""

    @model_validator(mode="before")
    @classmethod
    def normalize_aliases(cls, data):
        if not isinstance(data, dict):
            return data
        result = dict(data)
        for field, aliases in {
            "medicine_name": ("name",), "batch_number": ("batch", "batch_no"),
            "expiry_date": ("expiry",), "return_quantity": ("quantity",),
            "purchase_rate": ("rate", "purchase_price"), "gst_rate": ("gst",),
        }.items():
            if result.get(field) in (None, ""):
                for alias in aliases:
                    if result.get(alias) not in (None, ""):
                        result[field] = result[alias]
                        break
        return result


class POCreate(BaseModel):
    distributor_id: str
    distributor_name: str
    invoice_ref: str

    po_date: Optional[str] = None   # 👈 ADD THIS

    items: List[POItem]

    notes: Optional[str] = None
    sub_total: float = 0
    scheme_discount: float = 0
    cash_discount: float = 0
    total_cgst: float = 0
    total_sgst: float = 0
    round_off: float = 0
    grand_total: float = 0
    purchase_return_ids: List[str] = Field(default_factory=list)
    purchase_returns: List[POReturnCreditRow] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def normalize_purchase_return_rows(cls, data):
        if not isinstance(data, dict):
            return data
        result = dict(data)
        if not result.get("purchase_returns"):
            for alias in ("purchase_return_rows", "return_rows", "return_items"):
                if result.get(alias):
                    result["purchase_returns"] = result[alias]
                    break
        return result


MONEY = Decimal("0.01")
WHOLE_RUPEE = Decimal("1")


def _to_decimal(value) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        decimal_value = Decimal(str(value))
    except Exception:
        return Decimal("0")
    return decimal_value if decimal_value.is_finite() else Decimal("0")


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY, rounding=ROUND_HALF_UP)


def _money_float(value: Decimal) -> float:
    return float(_round_money(value))


def _calculate_purchase_order_totals(payload: POCreate) -> dict:
    """Calculate PO totals with order discount applied slab-wise before GST."""
    slab_subtotals: Dict[Decimal, Decimal] = defaultdict(Decimal)

    for item in payload.items:
        # Free quantity is intentionally excluded from the PO taxable subtotal.
        line_subtotal = _round_money(_to_decimal(item.purchase_price) * _to_decimal(item.quantity))
        gst_rate = _to_decimal(item.gst_rate)
        slab_subtotals[gst_rate] += line_subtotal

    sub_total_raw = sum(slab_subtotals.values(), Decimal("0"))
    sub_total = _round_money(sub_total_raw)
    scheme_discount = _round_money(_to_decimal(payload.scheme_discount))
    cash_discount = _round_money(_to_decimal(payload.cash_discount))
    total_discount = scheme_discount + cash_discount
    taxable_total = sub_total - total_discount

    gst_breakup = []
    total_cgst = Decimal("0")
    total_sgst = Decimal("0")

    for gst_rate in sorted(slab_subtotals):
        slab_subtotal = slab_subtotals[gst_rate]
        slab_discount = (
            total_discount * slab_subtotal / sub_total_raw
            if sub_total_raw
            else Decimal("0")
        )
        slab_taxable = slab_subtotal - slab_discount
        slab_gst = slab_taxable * gst_rate / Decimal("100")
        slab_cgst = _round_money(slab_gst / Decimal("2"))
        slab_sgst = _round_money(slab_gst / Decimal("2"))

        total_cgst += slab_cgst
        total_sgst += slab_sgst

        gst_breakup.append({
            "gst_rate": _money_float(gst_rate),
            "sub_total": _money_float(slab_subtotal),
            "discount": _money_float(slab_discount),
            "taxable_total": _money_float(slab_taxable),
            "cgst": _money_float(slab_cgst),
            "sgst": _money_float(slab_sgst),
            "gst": _money_float(slab_cgst + slab_sgst),
        })

    total = _round_money(taxable_total + total_cgst + total_sgst)
    grand_total_decimal = total.quantize(WHOLE_RUPEE, rounding=ROUND_HALF_UP)
    round_off = grand_total_decimal - total

    return {
        "sub_total": _money_float(sub_total),
        "scheme_discount": _money_float(scheme_discount),
        "cash_discount": _money_float(cash_discount),
        "discount": _money_float(total_discount),
        "taxable_total": _money_float(taxable_total),
        "total_cgst": _money_float(total_cgst),
        "total_sgst": _money_float(total_sgst),
        "total": _money_float(total),
        "round_off": _money_float(round_off),
        "grand_total": _money_float(grand_total_decimal),
        "gst_breakup": gst_breakup,
    }


def _purchase_return_credit(item: dict) -> float:
    return _money_float(
        _to_decimal(item.get("return_quantity")) * _to_decimal(item.get("purchase_rate"))
    )


async def _resolve_po_purchase_returns(payload: POCreate, allow_po_id: Optional[str] = None) -> Tuple[List[dict], float]:
    """Resolve selected IDs and inline PO credit rows to one physical return each."""
    return_ids = list(dict.fromkeys(payload.purchase_return_ids))
    returns = []
    if return_ids:
        returns = await db.purchase_returns.find({"id": {"$in": return_ids}}, {"_id": 0}).to_list(len(return_ids))
        if len(returns) != len(return_ids):
            raise HTTPException(status_code=400, detail="One or more selected purchase returns were not found")

    def normalized(value):
        return str(value or "").strip().casefold()

    def same_number(left, right):
        return _round_money(_to_decimal(left)) == _round_money(_to_decimal(right))

    def matches(row: POReturnCreditRow, item: dict) -> bool:
        if item.get("distributor_id") and item.get("distributor_id") != payload.distributor_id:
            return False
        if item.get("medicine_id") and row.medicine_id:
            medicine_matches = item.get("medicine_id") == row.medicine_id
        elif normalized(item.get("medicine_key")) and normalized(row.medicine_key):
            medicine_matches = normalized(item.get("medicine_key")) == normalized(row.medicine_key)
        else:
            medicine_matches = normalized(item.get("medicine_name")) == normalized(row.medicine_name)
        required = (
            medicine_matches
            and normalized(item.get("batch_number")) == normalized(row.batch_number)
            and same_number(item.get("return_quantity"), row.return_quantity)
            and same_number(item.get("purchase_rate"), row.purchase_rate)
        )
        if not required:
            return False
        if item.get("expiry_date") and row.expiry_date and normalized(item.get("expiry_date")) != normalized(row.expiry_date):
            return False
        if row.gst_rate is not None and item.get("gst_rate") is not None and not same_number(item.get("gst_rate"), row.gst_rate):
            return False
        if row.reason and item.get("reason") and normalized(item.get("reason")) != normalized(row.reason):
            return False
        return True

    active_return_query = {
        "distributor_id": payload.distributor_id,
        "voided_at": {"$exists": False},
        "deleted_at": {"$exists": False},
        "settlement_status": {"$ne": "deleted"},
    }
    candidates = await db.purchase_returns.find(
        active_return_query, {"_id": 0}
    ).to_list(10000) if payload.purchase_returns else []

    known_ids = {item["id"] for item in returns}
    for row in payload.purchase_returns:
        if row.id:
            if row.id not in known_ids:
                selected = next((item for item in candidates if item.get("id") == row.id), None)
                if not selected:
                    raise HTTPException(status_code=400, detail="Selected purchase return was not found")
                returns.append(selected)
                known_ids.add(row.id)
            continue
        if row.return_quantity <= 0 or row.purchase_rate <= 0 or not row.medicine_name or not row.batch_number:
            raise HTTPException(status_code=400, detail="PO return row requires medicine, batch, quantity, and purchase rate")

        match = next((
            item for item in candidates
            if item.get("id") not in known_ids
            and not item.get("ledger_adjusted")
            and not item.get("adjust_distributor_ledger")
            and matches(row, item)
        ), None)
        if match:
            returns.append(match)
            known_ids.add(match["id"])
            continue

        medicine_filters = [{"name": row.medicine_name, "batch_no": row.batch_number}]
        if row.medicine_key:
            medicine_filters.append({"medicine_key": row.medicine_key, "batch_no": row.batch_number})
        if row.medicine_id:
            medicine_filters.append({"id": row.medicine_id})
        medicine = await db.medicines.find_one({"$or": medicine_filters})
        if not medicine:
            raise HTTPException(status_code=400, detail="Medicine batch for PO return row was not found")
        await _set_purchase_return_stock_delta(medicine["id"], row.return_quantity)
        now = datetime.now(timezone.utc).isoformat()
        created = {
            "id": str(uuid.uuid4()), "return_date": payload.po_date or now[:10],
            "distributor": payload.distributor_name, "distributor_id": payload.distributor_id,
            "medicine_id": medicine.get("id"), "medicine_key": medicine.get("medicine_key") or row.medicine_key,
            "medicine_name": row.medicine_name, "batch_number": row.batch_number, "expiry_date": row.expiry_date,
            "return_quantity": row.return_quantity, "purchase_rate": _money_float(_to_decimal(row.purchase_rate)),
            "gst_rate": _money_float(_to_decimal(row.gst_rate if row.gst_rate is not None else medicine.get("gst_rate"))),
            "return_amount": _purchase_return_credit(row.model_dump()), "reason": row.reason or "Other", "notes": row.notes,
            "adjust_distributor_ledger": False, "ledger_adjusted": False, "ledger_transaction_id": None,
            "settlement_status": "unsettled", "settled_by_po": None, "settled_at": None,
            "settlement_reference": None, "settled_return_value": 0.0,
            "created_at": now, "created_by": "PO return credit", "auto_created_from_po_credit": True,
        }
        try:
            await db.purchase_returns.insert_one(created)
        except Exception:
            await _set_purchase_return_stock_delta(medicine["id"], -row.return_quantity)
            raise
        candidates.append(created)
        returns.append(created)
        known_ids.add(created["id"])

    for item in returns:
        if item.get("distributor_id") and item.get("distributor_id") != payload.distributor_id:
            raise HTTPException(status_code=400, detail="Purchase return distributor does not match purchase order distributor")
        if item.get("voided_at") or item.get("deleted_at") or item.get("settlement_status") == "deleted":
            raise HTTPException(status_code=409, detail="Deleted purchase return credit cannot be applied to a purchase order")
        assigned_po = item.get("po_adjustment_id")
        if assigned_po and assigned_po != allow_po_id:
            raise HTTPException(status_code=409, detail="Purchase return credit is already assigned to another purchase order")
        if item.get("ledger_adjusted") or item.get("adjust_distributor_ledger"):
            raise HTTPException(status_code=409, detail="Purchase return credit is already reflected in the distributor ledger")
        item["return_amount"] = _purchase_return_credit(item)
        item["purchase_rate"] = _money_float(_to_decimal(item.get("purchase_rate")))
        if item.get("gst_rate") is not None:
            item["gst_rate"] = _money_float(_to_decimal(item.get("gst_rate")))
    credit = _money_float(sum((_to_decimal(item["return_amount"]) for item in returns), Decimal("0")))
    return returns, credit


def _apply_po_return_credit(po_totals: dict, returns: List[dict], credit: float) -> dict:
    credit = _money_float(_to_decimal(credit))
    payable_base = _to_decimal(po_totals.get("sub_total", po_totals["grand_total"]))
    payable = _money_float(max(Decimal("0"), payable_base - _to_decimal(credit)))
    return {
        "purchase_return_ids": [item["id"] for item in returns],
        "purchase_return_adjustment": credit,
        "purchase_return_details": [{"id": item["id"], "medicine_name": item.get("medicine_name"), "batch_number": item.get("batch_number"), "return_amount": _money_float(_to_decimal(item.get("return_amount")))} for item in returns],
        "subtotal_after_purchase_return": payable,
        "final_payable_total": payable,
    }


def _po_return_settlement_fields(po_id: str, settled_at: str) -> dict:
    return {
        "po_adjustment_id": po_id,
        "po_adjusted_at": settled_at,
        "settlement_status": "settled_by_po",
        "settled_by_po": po_id,
        "settled_at": settled_at,
        "settlement_reference": po_id,
    }


def _released_po_return_settlement_fields() -> dict:
    return {
        "settlement_status": "unsettled",
        "settled_by_po": None,
        "settled_at": None,
        "settlement_reference": None,
        "settled_return_value": 0.0,
    }


@api_router.get("/purchase-orders/eligible-purchase-returns/{distributor_id}")
async def eligible_po_purchase_returns(distributor_id: str, user: dict = Depends(get_current_user)):
    returns = await db.purchase_returns.find({
        "distributor_id": distributor_id,
        "voided_at": {"$exists": False},
        "deleted_at": {"$exists": False},
        "settlement_status": {"$ne": "deleted"},
        "$or": [{"po_adjustment_id": {"$exists": False}}, {"po_adjustment_id": None}],
    }, {"_id": 0}).sort("return_date", -1).to_list(1000)
    result = []
    for item in returns:
        if item.get("voided_at") or item.get("deleted_at") or item.get("settlement_status") == "deleted" or item.get("ledger_adjusted") or item.get("adjust_distributor_ledger"):
            continue
        medicine = await db.medicines.find_one({"id": item.get("medicine_id")}, {"_id": 0}) if item.get("medicine_id") else None
        medicine = medicine or {}
        credit = _purchase_return_credit(item)
        result.append({
            "id": item["id"], "medicine_id": item.get("medicine_id"),
            "medicine_name": item.get("medicine_name") or medicine.get("name"),
            "batch_number": item.get("batch_number") or medicine.get("batch_no"),
            "expiry_date": item.get("expiry_date") or medicine.get("expiry_date"),
            "manufacturer": item.get("manufacturer") or medicine.get("manufacturer"),
            "category": item.get("category") or medicine.get("category"),
            "purchase_rate": float(item.get("purchase_rate", medicine.get("purchase_price", 0)) or 0),
            "mrp": float(item.get("mrp", medicine.get("mrp", 0)) or 0),
            "gst_rate": float(item.get("gst_rate", medicine.get("gst_rate", 0)) or 0),
            "available_return_quantity": float(item.get("return_quantity", 0) or 0),
            "return_quantity": float(item.get("return_quantity", 0) or 0),
            "distributor_id": item.get("distributor_id"),
            "distributor_name": item.get("distributor") or item.get("distributor_name"),
            "calculated_return_credit_amount": credit, "return_amount": credit,
            "return_date": item.get("return_date"),
        })
    return result



async def _purchase_returns_by_ids(return_ids: Iterable[str]) -> List[dict]:
    ids = [item for item in (return_ids or []) if item]
    if not ids:
        return []
    return await db.purchase_returns.find({"id": {"$in": ids}}, {"_id": 0}).to_list(None)


def _purchase_return_units_by_key(return_docs: Optional[List[dict]] = None) -> dict:
    units_by_key = defaultdict(float)
    for return_doc in return_docs or []:
        key = return_doc.get("medicine_key")
        if not key and (return_doc.get("medicine_name") or return_doc.get("name")) and (return_doc.get("batch_number") or return_doc.get("batch_no")):
            key = f"{str(return_doc.get('medicine_name') or return_doc.get('name')).strip().lower()}::{str(return_doc.get('batch_number') or return_doc.get('batch_no')).strip().upper()}"
        if key:
            units_by_key[key] += _purchase_return_quantity(return_doc)
    return units_by_key


async def _reverse_po_inventory_delta(po: dict, selected_returns: Optional[List[dict]] = None) -> dict:
    """Incrementally reverse one PO's inventory impact without full rebuild scans."""
    updated_keys = set()
    return_units_by_key = _purchase_return_units_by_key(selected_returns)

    for item in po.get("items", []):
        key = item.get("medicine_key")
        if not key:
            continue
        qty = round_qty(round_qty(item.get("quantity", 0)) + round_qty(item.get("free_quantity", 0)))
        existing = await db.medicines.find_one({"medicine_key": key}, {"_id": 0})
        if not existing:
            continue
        purchased_units = round_qty(max(0.0, _purchased_stock(existing) - qty))
        purchase_return_units = round_qty(_purchase_return_stock(existing) + return_units_by_key.get(key, 0.0))
        refreshed = {**existing, "purchased_units": purchased_units, "purchase_return_units": purchase_return_units}
        await db.medicines.update_one(
            {"medicine_key": key},
            {"$set": {
                "purchased_units": purchased_units,
                **_inventory_derivatives(refreshed, purchase_return_units),
            }},
        )
        updated_keys.add(key)

    for key, return_qty in return_units_by_key.items():
        if key in updated_keys:
            continue
        existing = await db.medicines.find_one({"medicine_key": key}, {"_id": 0})
        if not existing:
            continue
        purchase_return_units = round_qty(_purchase_return_stock(existing) + return_qty)
        refreshed = {**existing, "purchase_return_units": purchase_return_units}
        await db.medicines.update_one({"medicine_key": key}, {"$set": _inventory_derivatives(refreshed, purchase_return_units)})
        updated_keys.add(key)

    return {"medicine_batches_updated": len(updated_keys), "purchase_returns_adjusted": len(return_units_by_key)}


async def _apply_po_inventory_delta(po: dict, selected_returns: Optional[List[dict]] = None) -> dict:
    """Incrementally apply a newly-created PO to inventory without full rebuild scans."""
    selected_returns = selected_returns or []
    updated_keys = set()
    return_units_by_key = defaultdict(float)
    for return_doc in selected_returns:
        key = return_doc.get("medicine_key")
        if not key and (return_doc.get("medicine_name") or return_doc.get("name")) and (return_doc.get("batch_number") or return_doc.get("batch_no")):
            key = f"{str(return_doc.get('medicine_name') or return_doc.get('name')).strip().lower()}::{str(return_doc.get('batch_number') or return_doc.get('batch_no')).strip().upper()}"
        if key:
            return_units_by_key[key] += _purchase_return_quantity(return_doc)

    for item in po.get("items", []):
        key = item.get("medicine_key")
        if not key:
            continue
        qty = round_qty(round_qty(item.get("quantity", 0)) + round_qty(item.get("free_quantity", 0)))
        existing = await db.medicines.find_one({"medicine_key": key}, {"_id": 0})
        if not existing:
            distributor_id = po.get("distributor_id") or item.get("distributor_id")
            distributor_name = (
                po.get("distributor_name")
                or po.get("distributor")
                or item.get("distributor_name")
                or item.get("distributor")
            )
            legacy_filters = [
                {"name": item.get("name"), "batch_no": item.get("batch_no"), "distributor_id": distributor_id},
            ]
            if distributor_name:
                legacy_filters.extend([
                    {"name": item.get("name"), "batch_no": item.get("batch_no"), "distributor_name": distributor_name},
                    {"name": item.get("name"), "batch_no": item.get("batch_no"), "distributor": distributor_name},
                ])
            legacy_candidates = []
            for legacy_filter in legacy_filters:
                legacy_match = await db.medicines.find_one(legacy_filter, {"_id": 0})
                if legacy_match and not any(
                    (candidate.get("id"), candidate.get("medicine_key"))
                    == (legacy_match.get("id"), legacy_match.get("medicine_key"))
                    for candidate in legacy_candidates
                ):
                    legacy_candidates.append(legacy_match)
                if len(legacy_candidates) > 1:
                    break
            if not legacy_candidates:
                unscoped_legacy = await db.medicines.find_one(
                    {"name": item.get("name"), "batch_no": item.get("batch_no")},
                    {"_id": 0},
                )
                if (
                    unscoped_legacy
                    and not unscoped_legacy.get("distributor_id")
                    and not unscoped_legacy.get("distributor_name")
                    and not unscoped_legacy.get("distributor")
                ):
                    legacy_candidates.append(unscoped_legacy)
            if len(legacy_candidates) == 1:
                existing = legacy_candidates[0]
                key = existing.get("medicine_key") or key
        purchased_units = round_qty(_purchased_stock(existing or {}) + qty)
        purchase_return_units = round_qty(max(0.0, _purchase_return_stock(existing or {}) - return_units_by_key.get(key, 0.0)))
        medicine = {
            **(existing or {}),
            "medicine_key": key,
            "name": item.get("name"),
            "batch_no": item.get("batch_no"),
            "expiry_date": item.get("expiry_date"),
            "manufacturer": item.get("manufacturer"),
            "category": item.get("category"),
            "mrp": item.get("mrp"),
            "purchase_price": item.get("purchase_price"),
            "pack_size": item.get("pack_size"),
            "gst_rate": item.get("gst_rate"),
            "distributor_id": po.get("distributor_id") or item.get("distributor_id") or (existing or {}).get("distributor_id"),
            "distributor_name": po.get("distributor_name") or item.get("distributor_name") or item.get("distributor") or (existing or {}).get("distributor_name") or (existing or {}).get("distributor"),
            "distributor": po.get("distributor") or po.get("distributor_name") or item.get("distributor") or item.get("distributor_name") or (existing or {}).get("distributor") or (existing or {}).get("distributor_name"),
            "purchased_units": purchased_units,
            "sold_units": _stock_quantity(existing or {}, "sold_units", "sold_quantity"),
            "purchase_return_units": purchase_return_units,
            "stock_adjustment_units": _stock_adjustment_stock(existing or {}),
            "low_stock_threshold": (existing or {}).get("low_stock_threshold"),
            "low_stock_status": _low_stock_status(existing or {}),
            "id": (existing or {}).get("id") or str(uuid.uuid4()),
        }
        derivatives = {
            "available_stock": _available_stock(medicine),
            "quantity_units": _available_stock(medicine),
            "return_status": _return_status(medicine),
            "status": _return_status(medicine),
        }
        await db.medicines.update_one({"medicine_key": key}, {"$set": {**medicine, **derivatives}}, upsert=True)
        updated_keys.add(key)

    # Selected purchase-return credits are settled by this PO. If any selected
    # return belongs to a batch that is not also in the PO items, refresh just
    # that existing batch's derived return stock instead of scanning everything.
    for key, return_qty in return_units_by_key.items():
        if key in updated_keys:
            continue
        existing = await db.medicines.find_one({"medicine_key": key}, {"_id": 0})
        if not existing:
            continue
        refreshed = {**existing, "purchase_return_units": round_qty(max(0.0, _purchase_return_stock(existing) - return_qty))}
        await db.medicines.update_one({"medicine_key": key}, {"$set": _inventory_derivatives(refreshed, refreshed["purchase_return_units"])})
        updated_keys.add(key)

    return {"medicine_batches_updated": len(updated_keys), "purchase_returns_adjusted": len(return_units_by_key)}


async def _collection_rows(cursor, length=None) -> List[dict]:
    """Return rows from Motor-style or test cursors without assuming async iteration."""
    if hasattr(cursor, "to_list"):
        return await cursor.to_list(length)
    rows = []
    async for row in cursor:
        rows.append(row)
    return rows


def _po_item_medicine_names(po: dict) -> set:
    return {str(item.get("name") or "").strip().casefold() for item in po.get("items", []) if str(item.get("name") or "").strip()}


async def _delete_inventory_rows_for_medicine_names(medicine_names: Iterable[str]) -> int:
    """Delete every inventory row for the affected PO medicine names."""
    target_names = {str(name or "").strip().casefold() for name in medicine_names if str(name or "").strip()}
    if not target_names:
        return 0

    existing_rows = await _collection_rows(db.medicines.find({}, {"_id": 0}), None)
    rows_to_delete = [
        row for row in existing_rows
        if str(row.get("name") or "").strip().casefold() in target_names
    ]
    if not rows_to_delete:
        return 0

    delete_filter = {"$or": []}
    for row in rows_to_delete:
        if row.get("id"):
            delete_filter["$or"].append({"id": row.get("id")})
        if row.get("medicine_key"):
            delete_filter["$or"].append({"medicine_key": row.get("medicine_key")})

    if delete_filter["$or"] and hasattr(db.medicines, "delete_many"):
        result = await db.medicines.delete_many(delete_filter)
        return getattr(result, "deleted_count", len(rows_to_delete))

    # Unit-test collections in this repository are intentionally tiny and some
    # do not implement delete_many. Mutate them directly only in that in-memory
    # test shape; production Mongo/Motor collections use the branch above.
    if hasattr(db.medicines, "rows"):
        before = len(db.medicines.rows)
        db.medicines.rows[:] = [
            row for row in db.medicines.rows
            if str(row.get("name") or "").strip().casefold() not in target_names
        ]
        return before - len(db.medicines.rows)

    deleted = 0
    if hasattr(db.medicines, "delete_one"):
        for row in rows_to_delete:
            identity = row.get("id") or row.get("medicine_key")
            if not identity:
                continue
            result = await db.medicines.delete_one(_medicine_identity_filter(identity))
            deleted += getattr(result, "deleted_count", 0)
    return deleted


def _po_item_group_identity(po: dict, item: dict) -> tuple:
    medicine_identity = str(item.get("medicine_id") or item.get("name") or "").strip().casefold()
    distributor_identity = str(po.get("distributor_id") or item.get("distributor_id") or po.get("distributor_name") or po.get("distributor") or item.get("distributor_name") or item.get("distributor") or "").strip().casefold()
    batch_identity = str(item.get("batch_no") or item.get("batch_number") or "").strip().upper()
    return medicine_identity, distributor_identity, batch_identity


async def _rebuild_inventory_for_po_medicines(medicine_names: Iterable[str], updated_pos: Optional[Iterable[dict]] = None) -> dict:
    """Deterministically rebuild PO-backed inventory for affected medicines.

    Purchase orders are the source of truth. For every affected medicine, all
    existing inventory rows are deleted first; fresh rows are then aggregated
    from active purchase orders only, grouped by medicine identity,
    distributor identity, and batch number. No previous inventory quantity is reused.
    """
    target_names = {str(name or "").strip().casefold() for name in medicine_names if str(name or "").strip()}
    if not target_names:
        return {"medicine_batches_updated": 0, "medicine_names_rebuilt": 0, "inventory_rows_deleted": 0}

    deleted_count = await _delete_inventory_rows_for_medicine_names(target_names)
    rebuilt = {}

    purchase_orders = await _collection_rows(db.purchase_orders.find({}, {"_id": 0}), None)
    updated_po_by_id = {po.get("id"): po for po in (updated_pos or []) if po and po.get("id")}
    if updated_po_by_id:
        seen_updated_po_ids = set()
        merged_purchase_orders = []
        for po in purchase_orders:
            replacement = updated_po_by_id.get(po.get("id"))
            if replacement:
                merged_purchase_orders.append(replacement)
                seen_updated_po_ids.add(po.get("id"))
            else:
                merged_purchase_orders.append(po)
        merged_purchase_orders.extend(po for po_id, po in updated_po_by_id.items() if po_id not in seen_updated_po_ids)
        purchase_orders = merged_purchase_orders

    for po in purchase_orders:
        if po.get("deleted_at") or po.get("voided_at") or po.get("status") == "deleted":
            continue
        po_distributor_id = po.get("distributor_id")
        po_distributor_name = po.get("distributor_name") or po.get("distributor")
        po_distributor = po.get("distributor") or po.get("distributor_name")
        for item in po.get("items", []):
            item_name = str(item.get("name") or "").strip()
            if item_name.casefold() not in target_names:
                continue
            group_key = _po_item_group_identity(po, item)
            qty = round_qty(round_qty(item.get("quantity", 0)) + round_qty(item.get("free_quantity", 0)))
            if group_key not in rebuilt:
                distributor_id = po_distributor_id or item.get("distributor_id")
                distributor_name = po_distributor_name or item.get("distributor_name") or item.get("distributor")
                batch_no = item.get("batch_no") or item.get("batch_number")
                medicine_key = _stock_lot_key(
                    item_name,
                    batch_no,
                    distributor_id,
                    distributor_name,
                    item.get("expiry_date"),
                    item.get("pack_size"),
                    item.get("purchase_price"),
                    item.get("mrp"),
                )
                legacy_batch_identity = f"{item_name.strip().casefold()}::{str(batch_no or '').strip().upper()}::{str(distributor_id or distributor_name or '').strip().casefold()}"
                rebuilt[group_key] = {
                    "id": item.get("medicine_id") or legacy_batch_identity or medicine_key,
                    "medicine_key": medicine_key,
                    "medicine_id": item.get("medicine_id") or legacy_batch_identity or medicine_key,
                    "name": item_name,
                    "batch_no": batch_no,
                    "expiry_date": item.get("expiry_date"),
                    "manufacturer": item.get("manufacturer"),
                    "category": item.get("category"),
                    "mrp": item.get("mrp"),
                    "purchase_price": item.get("purchase_price"),
                    "pack_size": item.get("pack_size"),
                    "gst_rate": item.get("gst_rate"),
                    "distributor_id": distributor_id,
                    "distributor_name": distributor_name,
                    "distributor": po_distributor or item.get("distributor") or item.get("distributor_name") or distributor_name,
                    "purchased_units": 0,
                    "sold_units": 0,
                    "purchase_return_units": 0,
                    "stock_adjustment_units": 0,
                }
            rebuilt[group_key]["purchased_units"] = round_qty(rebuilt[group_key]["purchased_units"] + qty)

    for medicine in rebuilt.values():
        derivatives = {
            "available_stock": _available_stock(medicine),
            "quantity_units": _available_stock(medicine),
            "return_status": _return_status(medicine),
            "status": _return_status(medicine),
        }
        await db.medicines.update_one({"medicine_key": medicine["medicine_key"]}, {"$set": {**medicine, **derivatives}}, upsert=True)

    return {"medicine_batches_updated": len(rebuilt), "medicine_names_rebuilt": len(target_names), "inventory_rows_deleted": deleted_count}


@api_router.post("/purchase-orders")
async def create_po(
    payload: POCreate,
    user: dict = Depends(require_role("admin", "pharmacist"))
):
    timer = _StepTimer("PO CREATE TIMING")

    def normalize_expiry(expiry):
        if not expiry:
            return ""

        # force MM/YY only
        if "/" in expiry:
            parts = expiry.split("/")
            if len(parts) == 2:
                mm = parts[0].zfill(2)
                yy = parts[1][-2:]
                return f"{mm}/{yy}"

        return expiry[:5]

    po_totals = _calculate_purchase_order_totals(payload)
    timer.mark("po_validation_and_total_calculation")
    selected_returns, return_credit = await _resolve_po_purchase_returns(payload)
    return_adjustment = _apply_po_return_credit(po_totals, selected_returns, return_credit)
    timer.mark("purchase_return_credit_resolution")
    po_no = await _next_po_no()
    timer.mark("counter_generation")

    po = {
    "id": str(uuid.uuid4()),
    "po_no": po_no,
    "po_date": payload.po_date,
    "distributor_id": payload.distributor_id,
    "distributor_name": payload.distributor_name,
    "invoice_ref": payload.invoice_ref,

    "items": [
        {
            **i.model_dump(),
            "quantity": round_qty(i.quantity),
            "free_quantity": round_qty(i.free_quantity),
            "item_total": _money_float(_to_decimal(i.purchase_price) * _to_decimal(i.quantity)),
            "medicine_key": _stock_lot_key(
                i.name, i.batch_no, payload.distributor_id, payload.distributor_name,
                normalize_expiry(i.expiry_date), i.pack_size, i.purchase_price, i.mrp,
            ),
            "expiry_date": normalize_expiry(i.expiry_date),
        }
        for i in payload.items
    ],
    "total": po_totals["total"],

    "sub_total": po_totals["sub_total"],
    "scheme_discount": po_totals["scheme_discount"],
    "cash_discount": po_totals["cash_discount"],
    "discount": po_totals["discount"],
    "taxable_total": po_totals["taxable_total"],
    "total_cgst": po_totals["total_cgst"],
    "total_sgst": po_totals["total_sgst"],
    "round_off": po_totals["round_off"],
    "grand_total": po_totals["grand_total"],
    "gst_breakup": po_totals["gst_breakup"],
    **return_adjustment,

    "notes": payload.notes,
    "created_at": datetime.now(timezone.utc).isoformat(),
    "received_at": None,
}
    timer.mark("po_document_build")

    await db.purchase_orders.insert_one(po)
    timer.mark("sqlite_purchase_order_write")
    if return_adjustment["purchase_return_ids"]:
        settled_at = datetime.now(timezone.utc).isoformat()
        await db.purchase_returns.update_many(
            {"id": {"$in": return_adjustment["purchase_return_ids"]}},
            {"$set": _po_return_settlement_fields(po["id"], settled_at)},
        )
    timer.mark("purchase_return_settlement_updates")

    rebuilt_inventory = await _rebuild_inventory_for_po_medicines(_po_item_medicine_names(po), updated_pos=[po])
    timer.mark("medicine_scoped_inventory_rebuild")

    timer.mark("purchase_return_stock_recalculation")
    timer.mark("dashboard_rebuild")
    _invalidate_inventory_dashboard_cache(_current_tenant.get())
    timer.mark("cache_invalidation")
    timer.mark("backup_queue_processing")

    po.pop("_id", None)
    timer.mark("response_serialization")
    largest_step, largest_ms = timer.largest_step()
    timer.log(
        largest_bottleneck=f"{largest_step} ({largest_ms} ms)",
        medicine_batches_updated=rebuilt_inventory["medicine_batches_updated"],
    )
    return po
    
@api_router.delete("/purchase-orders/{po_id}")
async def delete_po(
    po_id: str,
    user: dict = Depends(require_role("admin"))
):
    timer = _StepTimer("PO DELETE TIMING")

    po = await db.purchase_orders.find_one({"id": po_id})
    timer.mark("load_existing_po")

    if not po:
        raise HTTPException(404, "PO not found")

    affected_medicine_names = _po_item_medicine_names(po)

    if po.get("purchase_return_ids"):
        await db.purchase_returns.update_many(
            {"id": {"$in": po["purchase_return_ids"]}, "po_adjustment_id": po_id},
            {"$unset": {"po_adjustment_id": "", "po_adjusted_at": ""}, "$set": _released_po_return_settlement_fields()},
        )
    timer.mark("ledger_reversal")

    await db.purchase_orders.delete_one({"id": po_id})
    timer.mark("delete_po_document")

    rebuilt_inventory = await _rebuild_inventory_for_po_medicines(affected_medicine_names)
    timer.mark("medicine_scoped_inventory_rebuild")

    timer.mark("purchase_return_recalculation")
    timer.mark("dashboard_rebuild")
    _invalidate_inventory_dashboard_cache(_current_tenant.get())
    timer.mark("cache_invalidation")

    response = {"message": "PO deleted"}
    timer.mark("response_serialization")
    largest_step, largest_ms = timer.largest_step()
    timer.log(
        largest_bottleneck=f"{largest_step} ({largest_ms} ms)",
        medicine_batches_updated=rebuilt_inventory["medicine_batches_updated"],
    )
    return response

@api_router.put("/purchase-orders/{po_id}")
async def update_po(
    po_id: str,
    payload: POCreate,
    user: dict = Depends(require_role("admin"))
):
    timer = _StepTimer("PO UPDATE TIMING")

    def normalize_expiry(expiry):
        if not expiry:
            return ""

        if "/" in expiry:
            parts = expiry.split("/")
            if len(parts) == 2:
                mm = parts[0].zfill(2)
                yy = parts[1][-2:]
                return f"{mm}/{yy}"

        return expiry[:5]

    old_po = await db.purchase_orders.find_one({"id": po_id})
    timer.mark("load_existing_po")

    if not old_po:
        raise HTTPException(404, "PO not found")

    po_totals = _calculate_purchase_order_totals(payload)
    selected_returns, return_credit = await _resolve_po_purchase_returns(payload, allow_po_id=po_id)
    return_adjustment = _apply_po_return_credit(po_totals, selected_returns, return_credit)
    timer.mark("validation")

    new_items = [
        {
            **i.model_dump(),
            "quantity": round_qty(i.quantity),
            "free_quantity": round_qty(i.free_quantity),
            "item_total": _money_float(_to_decimal(i.purchase_price) * _to_decimal(i.quantity)),
            "medicine_key": _stock_lot_key(
                i.name, i.batch_no, payload.distributor_id, payload.distributor_name,
                normalize_expiry(i.expiry_date), i.pack_size, i.purchase_price, i.mrp,
            ),
            "expiry_date": normalize_expiry(i.expiry_date),
        }
        for i in payload.items
    ]
    updated_po = {
        **old_po,
        "po_date": payload.po_date,
        "distributor_id": payload.distributor_id,
        "distributor_name": payload.distributor_name,
        "invoice_ref": payload.invoice_ref,
        "notes": payload.notes,
        "items": new_items,
        "total": po_totals["total"],
        "sub_total": po_totals["sub_total"],
        "scheme_discount": po_totals["scheme_discount"],
        "cash_discount": po_totals["cash_discount"],
        "discount": po_totals["discount"],
        "taxable_total": po_totals["taxable_total"],
        "total_cgst": po_totals["total_cgst"],
        "total_sgst": po_totals["total_sgst"],
        "round_off": po_totals["round_off"],
        "grand_total": po_totals["grand_total"],
        "gst_breakup": po_totals["gst_breakup"],
        **return_adjustment,
    }
    await db.purchase_orders.update_one({"id": po_id}, {"$set": {k: v for k, v in updated_po.items() if k != "_id"}})
    timer.mark("purchase_order_update_write")

    # The medicine-scoped rebuild is authoritative for PO updates. Persist the
    # edited PO first, delete all inventory rows for affected medicines, then
    # replay active PO items from purchase_orders only.
    rebuilt_inventory = await _rebuild_inventory_for_po_medicines(_po_item_medicine_names(old_po) | _po_item_medicine_names(updated_po), updated_pos=[updated_po])
    timer.mark("medicine_scoped_inventory_rebuild")
    old_return_ids = set(old_po.get("purchase_return_ids", []))
    new_return_ids = set(return_adjustment["purchase_return_ids"])
    if old_return_ids - new_return_ids:
        await db.purchase_returns.update_many(
            {"id": {"$in": list(old_return_ids - new_return_ids)}, "po_adjustment_id": po_id},
            {"$unset": {"po_adjustment_id": "", "po_adjusted_at": ""}, "$set": _released_po_return_settlement_fields()},
        )
    if new_return_ids:
        settled_at = datetime.now(timezone.utc).isoformat()
        await db.purchase_returns.update_many(
            {"id": {"$in": list(new_return_ids)}},
            {"$set": _po_return_settlement_fields(po_id, settled_at)},
        )
    timer.mark("ledger_adjustment")

    timer.mark("purchase_return_recalculation")
    timer.mark("dashboard_rebuild")
    _invalidate_inventory_dashboard_cache(_current_tenant.get())
    timer.mark("cache_invalidation")

    response = {"message": "PO updated", **return_adjustment, "rebuilt_medicine_batches_updated": rebuilt_inventory["medicine_batches_updated"]}
    timer.mark("response_serialization")
    largest_step, largest_ms = timer.largest_step()
    timer.log(
        largest_bottleneck=f"{largest_step} ({largest_ms} ms)",
        rebuilt_medicine_batches_updated=rebuilt_inventory["medicine_batches_updated"],
    )
    return response

@api_router.get("/purchase-orders")
async def list_pos(user: dict = Depends(get_current_user)):
    purchase_orders = await db.purchase_orders.find({}, {"_id": 0}).sort("created_at", -1).to_list(2000)
    return [_ensure_action_aliases(po, alias_id_fields=("purchase_order_id",)) for po in purchase_orders]


@api_router.get("/purchase-orders/{pid}")
async def get_po(pid: str, user: dict = Depends(get_current_user)):
    po = await db.purchase_orders.find_one({"id": pid}, {"_id": 0})
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")
    return _ensure_action_aliases(po, alias_id_fields=("purchase_order_id",))


# ---------------- Doctors (referring history) ----------------
@api_router.get("/doctors")
async def list_doctors(user: dict = Depends(get_current_user)):
    docs = await db.doctor_history.find({}, {"_id": 0}).sort("count", -1).to_list(200)
    return docs


# ---------------- Settings (signature, branding, business info) ----------------
SETTINGS_DEFAULTS = {
    "key": "main",
    "business_name": "MedStock Pharmacy",
    "business_address": "",
    "business_phone": "",
    "business_gstin": "",
    "signature_b64": "",
    "dl_number_1": "",
    "dl_number_2": "",
    # May contain a storage path/URL plus optional name, content_type, and size.
    "pharmacy_logo": None,
    "selected_theme": "default",
    "selected_font": "system",
    # Reserved, backward-compatible structures for incremental settings features.
    "activity_logs": [],
    "role_permissions": {},
    "theme_settings": {},
    "backup_metadata": {},
}

WELCOME_SCREEN_FIELDS = {
    "welcome_screen",
    "welcome_screen_enabled",
    "welcome_title",
    "welcome_subtitle",
    "welcome_background",
    "welcome_logo",
    "welcome_message",
    "welcome_settings",
}
ALLOWED_LOGO_CONTENT_TYPES = {"image/png", "image/jpeg", "image/jpg", "image/webp", "image/svg+xml"}


def normalize_settings(settings: Optional[dict] = None) -> dict:
    """Add new settings defaults without changing or removing legacy fields."""
    normalized = {**SETTINGS_DEFAULTS, **(settings or {})}
    for field in ("selected_theme", "selected_font"):
        if not normalized.get(field):
            normalized[field] = SETTINGS_DEFAULTS[field]
    for field in ("activity_logs",):
        if normalized.get(field) is None:
            normalized[field] = list(SETTINGS_DEFAULTS[field])
    for field in ("role_permissions", "theme_settings", "backup_metadata"):
        if normalized.get(field) is None:
            normalized[field] = dict(SETTINGS_DEFAULTS[field])
    for field in ("business_name", "business_address", "business_phone", "business_gstin", "signature_b64", "dl_number_1", "dl_number_2"):
        if normalized.get(field) is None:
            normalized[field] = SETTINGS_DEFAULTS[field]
    return normalized


def _validate_settings_key(key: str) -> None:
    if key.startswith("$") or "." in key:
        raise HTTPException(status_code=422, detail=f"Invalid settings field: {key}")


def _validate_settings_keys(value, prefix: str = "settings") -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            key = str(key)
            _validate_settings_key(key)
            _validate_settings_keys(nested, f"{prefix}.{key}")
    elif isinstance(value, list):
        for item in value:
            _validate_settings_keys(item, prefix)


def _sanitize_settings_payload(payload: dict) -> dict:
    if not isinstance(payload, dict) or isinstance(payload, list):
        raise HTTPException(status_code=422, detail="Settings payload must be a JSON object")
    sanitized = dict(payload)
    _validate_settings_keys(sanitized)
    for field in WELCOME_SCREEN_FIELDS:
        sanitized.pop(field, None)
    logo = sanitized.get("pharmacy_logo")
    if "pharmacy_logo" in sanitized and logo is not None and not isinstance(logo, (str, dict)):
        raise HTTPException(status_code=422, detail="pharmacy_logo must be a string, object, or null")
    for field in ("selected_theme", "selected_font"):
        if sanitized.get(field) is None:
            sanitized.pop(field, None)
    for field in ("theme_settings", "role_permissions", "backup_metadata"):
        if field in sanitized and sanitized[field] is None:
            sanitized[field] = dict(SETTINGS_DEFAULTS[field])
    if "activity_logs" in sanitized and sanitized["activity_logs"] is None:
        sanitized["activity_logs"] = list(SETTINGS_DEFAULTS["activity_logs"])
    for field in ("business_name", "business_address", "business_phone", "business_gstin", "signature_b64", "dl_number_1", "dl_number_2"):
        if sanitized.get(field) is None:
            sanitized[field] = SETTINGS_DEFAULTS[field]
    return sanitized


def _settings_update_set(payload: dict) -> dict:
    """Build the mutable settings fields for $set without tenant ownership fields.

    TenantAwareCollection adds tenant_id/shop_id to the scoped filter and to
    $setOnInsert for upserts. If a client saves a full settings document that
    already contains tenant_id/shop_id, also placing those paths in $set would
    conflict with the wrapper-generated $setOnInsert ownership values.
    """
    return {key: value for key, value in payload.items() if key not in {"tenant_id", "shop_id"}}


def _settings_payload_field_snapshot(payload: dict) -> dict:
    return {
        "selected_theme": payload.get("selected_theme"),
        "selected_font": payload.get("selected_font"),
        "theme_settings": payload.get("theme_settings"),
        "pharmacy_logo": payload.get("pharmacy_logo"),
        "logo_fields": {key: payload.get(key) for key in ("pharmacy_logo", "pharmacy_logo_url", "logo_url") if key in payload},
        "removed_welcome_screen_fields": sorted(field for field in WELCOME_SCREEN_FIELDS if field in payload),
        "new_settings_fields": sorted(key for key in payload if key not in SETTINGS_DEFAULTS and key not in WELCOME_SCREEN_FIELDS and key not in {"updated_at"}),
    }


def _find_first_bson_encoding_failure(value, path: str = "payload") -> Optional[dict]:
    try:
        BSON.encode({"value": value})
        return None
    except Exception as exc:
        if isinstance(value, dict):
            for key, nested in value.items():
                child = _find_first_bson_encoding_failure(nested, f"{path}.{key}")
                if child:
                    return child
        elif isinstance(value, list):
            for index, nested in enumerate(value):
                child = _find_first_bson_encoding_failure(nested, f"{path}[{index}]")
                if child:
                    return child
        return {"field": path, "value_type": type(value).__name__, "exception": repr(exc)}


def _settings_save_log_context(payload: dict, user: dict, update_filter: dict, update_operation: dict) -> dict:
    return {
        "incoming_payload": payload,
        "settings_fields": _settings_payload_field_snapshot(payload),
        "selected_theme": payload.get("selected_theme"),
        "selected_font": payload.get("selected_font"),
        "pharmacy_logo": payload.get("pharmacy_logo"),
        "user_id": user.get("id") or user.get("user_id"),
        "tenant_id": user.get("tenant_id"),
        "shop_id": user.get("shop_id"),
        "mongo_filter": update_filter,
        "mongo_update": update_operation,
        "bson_failure_field": _find_first_bson_encoding_failure(update_operation),
    }

def settings_response(settings: Optional[dict] = None) -> dict:
    """Return active settings without legacy Welcome Screen configuration."""
    normalized = normalize_settings(settings)
    logo = normalized.get("pharmacy_logo")
    if isinstance(logo, dict):
        logo_path = logo.get("url") or logo.get("path")
        normalized["pharmacy_logo_url"] = logo_path
        normalized["logo_url"] = logo_path
    elif isinstance(logo, str):
        normalized["pharmacy_logo_url"] = logo
        normalized["logo_url"] = logo
    else:
        normalized["pharmacy_logo_url"] = None
        normalized["logo_url"] = None
    for field in WELCOME_SCREEN_FIELDS:
        normalized.pop(field, None)
    return normalized


def _tenant_upload_segment(user: dict) -> str:
    value = user.get("tenant_id") or user.get("shop_id") or "default"
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(value))[:80] or "default"


def _logo_extension(upload: UploadFile) -> str:
    suffix = Path(upload.filename or "").suffix.lower()
    if suffix in {".png", ".jpg", ".jpeg", ".webp", ".svg"}:
        return suffix
    return {
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/webp": ".webp",
        "image/svg+xml": ".svg",
    }.get(upload.content_type or "", ".bin")


@api_router.get("/settings")
async def get_settings(user: dict = Depends(get_current_user)):
    s = await db.settings.find_one({"key": "main"}, {"_id": 0})
    response = settings_response(s)
    returned_welcome_fields = sorted(field for field in WELCOME_SCREEN_FIELDS if field in response)
    stored_welcome_fields = sorted(field for field in WELCOME_SCREEN_FIELDS if field in (s or {}))
    logger.info(
        "GET /api/settings Welcome Screen configuration return audit: welcome_screen_config_returned=%s",
        bool(returned_welcome_fields),
        extra={
            "tenant_id": user.get("tenant_id"),
            "stored_welcome_screen_fields": stored_welcome_fields,
            "returned_welcome_screen_fields": returned_welcome_fields,
            "welcome_screen_config_returned": bool(returned_welcome_fields),
        },
    )
    return response


@api_router.put("/settings")
async def update_settings(payload: dict, user: dict = Depends(require_role("admin"))):
    payload = _sanitize_settings_payload(payload)
    payload["key"] = "main"
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()

    business_fields = {"business_name", "business_address", "business_phone", "business_gstin", "dl_number_1", "dl_number_2"}
    if "selected_theme" in payload or "theme_settings" in payload:
        logger.info("Saving settings theme fields", extra={"tenant_id": user.get("tenant_id"), "fields": [k for k in ("selected_theme", "theme_settings") if k in payload]})
    if "selected_font" in payload:
        logger.info("Saving settings font field", extra={"tenant_id": user.get("tenant_id")})
    if "pharmacy_logo" in payload:
        logger.info("Saving settings logo field", extra={"tenant_id": user.get("tenant_id"), "logo_present": payload.get("pharmacy_logo") is not None})
    if business_fields.intersection(payload):
        logger.info("Saving settings business profile fields", extra={"tenant_id": user.get("tenant_id"), "fields": sorted(business_fields.intersection(payload))})

    update_filter = {"key": "main"}
    update_operation = {"$set": _settings_update_set(payload)}
    log_context = _settings_save_log_context(payload, user, update_filter, update_operation)
    logger.info("Saving settings payload: %s", log_context, extra=log_context)

    try:
        await db.settings.update_one(update_filter, update_operation, upsert=True)
        s = await db.settings.find_one(update_filter, {"_id": 0})
    except PyMongoError as exc:
        logger.exception(
            "Database error while saving settings: %s | context=%s",
            exc,
            log_context,
            extra={**log_context, "exception_type": type(exc).__name__, "exception_message": str(exc)},
        )
        raise HTTPException(status_code=503, detail="Unable to save settings right now") from exc
    except TypeError as exc:
        logger.exception(
            "Invalid settings payload while saving settings: %s | context=%s",
            exc,
            log_context,
            extra={**log_context, "exception_type": type(exc).__name__, "exception_message": str(exc)},
        )
        raise HTTPException(status_code=422, detail="Invalid settings payload") from exc
    return settings_response(s)


@api_router.post("/settings/logo")
@api_router.post("/settings/pharmacy-logo")
async def upload_pharmacy_logo(
    request: Request,
    user: dict = Depends(require_role("admin")),
):
    try:
        form = await request.form()
    except Exception as exc:
        logger.exception("Failed to parse pharmacy logo upload form: %s", exc)
        raise HTTPException(status_code=400, detail="Invalid multipart logo upload") from exc
    logo = form.get("logo") or form.get("file") or form.get("pharmacy_logo")
    if not isinstance(logo, UploadFile):
        raise HTTPException(status_code=400, detail="Upload field must be named logo, file, or pharmacy_logo")
    if logo.content_type not in ALLOWED_LOGO_CONTENT_TYPES:
        raise HTTPException(status_code=400, detail="Logo must be a PNG, JPG, WEBP, or SVG image")

    tenant_dir = BRANDING_UPLOAD_DIR / _tenant_upload_segment(user)
    tenant_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{uuid.uuid4()}{_logo_extension(logo)}"
    destination = tenant_dir / filename

    size = 0
    with destination.open("wb") as output:
        while True:
            chunk = await logo.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > 5 * 1024 * 1024:
                destination.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail="Logo file must be 5 MB or smaller")
            output.write(chunk)

    public_path = f"/uploads/branding/{_tenant_upload_segment(user)}/{filename}"
    logo_doc = {
        "path": public_path,
        "url": public_path,
        "filename": logo.filename or filename,
        "content_type": logo.content_type,
        "size": size,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "uploaded_by": user.get("id") or user.get("email"),
    }
    logger.info("Saving settings logo upload", extra={"tenant_id": user.get("tenant_id"), "content_type": logo.content_type, "size": size})
    try:
        await db.settings.update_one(
            {"key": "main"},
            {"$set": {"key": "main", "pharmacy_logo": logo_doc, "updated_at": logo_doc["uploaded_at"]}},
            upsert=True,
        )
        s = await db.settings.find_one({"key": "main"}, {"_id": 0})
    except PyMongoError as exc:
        logger.exception("Database error while saving settings logo")
        raise HTTPException(status_code=503, detail="Unable to save pharmacy logo right now") from exc
    return settings_response(s)


# ---------------- Daily Sales Book (business-summary register) ----------------
class DailySaleCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cash_sales: float = 0
    upi_sales: float = 0
    outstanding_sales: float = 0
    card_sales: float = 0
    notes: str = ""
    sale_date: Optional[str] = None  # YYYY-MM-DD; defaults to today

    # Legacy quick-sale fields remain accepted so existing clients and records
    # continue to work, but they are descriptive only and never affect stock.
    medicine_id: Optional[str] = None
    quantity: Optional[int] = None
    unit_type: Literal["unit", "box"] = "unit"
    total_amount: Optional[float] = None
    customer_name: str = ""
    payment_status: Literal["paid", "pending"] = "paid"

    @field_validator("cash_sales", "upi_sales", "outstanding_sales", "card_sales")
    @classmethod
    def require_valid_daily_sale_amounts(cls, value, info):
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{info.field_name} must be a finite non-negative amount")
        return value


class DailySaleUpdate(BaseModel):
    cash_sales: Optional[float] = None
    upi_sales: Optional[float] = None
    outstanding_sales: Optional[float] = None
    card_sales: Optional[float] = None
    notes: Optional[str] = None
    sale_date: Optional[str] = None

    @field_validator("cash_sales", "upi_sales", "outstanding_sales", "card_sales")
    @classmethod
    def require_valid_daily_sale_update_amounts(cls, value, info):
        if value is not None and (not math.isfinite(value) or value < 0):
            raise ValueError(f"{info.field_name} must be a finite non-negative amount")
        return value


def _money(value) -> float:
    return _daily_closing_money(float(value or 0))


def _normalize_daily_sale(entry: dict) -> dict:
    """Expose both standardized summary fields and legacy aliases."""
    item = dict(entry)
    legacy_total = _money(item.get("total_amount"))
    cash = _money(item.get("cash_sales", legacy_total if item.get("payment_status") == "paid" else 0))
    upi = _money(item.get("upi_sales"))
    card = _money(item.get("card_sales"))
    outstanding = _money(item.get(
        "outstanding_sales",
        legacy_total if item.get("payment_status") == "pending" else 0,
    ))
    gross = _money(cash + upi + card + outstanding)
    item = _ensure_action_aliases(item)
    item.update({
        "cash_sales": cash,
        "upi_sales": upi,
        "card_sales": card,
        "outstanding_sales": outstanding,
        "gross_sales": gross,
        "total_paid": _money(cash + upi + card),
        "total_outstanding": outstanding,
        "total_amount": gross,
    })
    return item

@api_router.post("/daily-sales")
async def create_daily_sale(
    payload: DailySaleCreate,
    user: dict = Depends(get_current_user)
):

    sale_date = payload.sale_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data = payload.model_dump()
    # A legacy total is mapped to paid cash or outstanding unless the caller
    # supplied standardized payment splits.
    if payload.total_amount is not None and not any(
        data[field] for field in ("cash_sales", "upi_sales", "card_sales", "outstanding_sales")
    ):
        target = "outstanding_sales" if payload.payment_status == "pending" else "cash_sales"
        data[target] = payload.total_amount

    entry = {
        "id": str(uuid.uuid4()),
        **data,
        "sale_date": sale_date,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": user.get("name", ""),
    }
    entry = _normalize_daily_sale(entry)

    await db.daily_sales.insert_one(entry)

    return {
        key: value
        for key, value in entry.items()
        if key not in {"_id", "stock_deductions"}
    }

@api_router.get("/expenses")
async def get_expenses(
    date: Optional[str] = None,
    user: dict = Depends(get_current_user)
):
    query = {}
    if date:
        query["date"] = date

    expenses = await db.expenses.find(
        query,
        {"_id": 0}
    ).sort("created_at", -1).to_list(2000)

    return expenses

def _daily_closing_money(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _empty_daily_closing_splits() -> dict:
    return {
        "cash_sales": 0,
        "upi_sales": 0,
        "card_sales": 0,
        "credit_sales": 0,
    }


def _daily_sale_split_signature(row: dict) -> Tuple[float, float, float, float]:
    return tuple(_money(row.get(field)) for field in (
        "cash_sales", "upi_sales", "card_sales", "outstanding_sales"
    ))


def _invoice_daily_closing_splits(invoice: dict) -> dict:
    splits = _empty_daily_closing_splits()
    mode = str(invoice.get("payment_mode") or "cash").lower()
    paid = _money(invoice.get("paid_amount"))
    due = _money(invoice.get("due_amount"))
    if mode in {"cash", "upi", "card"}:
        splits[f"{mode}_sales"] += paid
    else:
        # Credit and legacy/mixed invoices do not retain a detailed payment split.
        splits["cash_sales"] += paid
    splits["credit_sales"] += due
    return {key: _money(value) for key, value in splits.items()}


def _invoice_split_signature(splits: dict) -> Tuple[float, float, float, float]:
    return tuple(_money(splits.get(field)) for field in (
        "cash_sales", "upi_sales", "card_sales", "credit_sales"
    ))


def _daily_sale_invoice_reference_values(row: dict) -> Set[str]:
    return {
        str(row.get(field)).strip()
        for field in (
            "invoice_id", "invoice_no", "invoice_number", "source_ref",
            "source_id", "reference", "reference_number",
        )
        if row.get(field) is not None and str(row.get(field)).strip()
    }


def _invoice_reference_values(invoice: dict) -> Set[str]:
    return {
        str(invoice.get(field)).strip()
        for field in ("id", "invoice_no", "invoice_number", "reference", "reference_number")
        if invoice.get(field) is not None and str(invoice.get(field)).strip()
    }


def _daily_sale_is_invoice_backed(row: dict, invoice_refs: Set[str]) -> bool:
    source = str(row.get("source") or row.get("source_type") or "").strip().lower()
    if source in {"invoice", "billing", "bill", "pos"}:
        return True
    if row.get("invoice_id") or row.get("invoice_no") or row.get("invoice_number"):
        return True
    return bool(_daily_sale_invoice_reference_values(row) & invoice_refs)


def _prepare_daily_closing(closing: dict) -> dict:
    prepared = dict(closing)
    # Old records used expected_total as the expected drawer amount.
    prepared.setdefault("expected_cash", prepared.get("expected_total", 0))
    prepared.setdefault("total_expenses", prepared.get("expenses", 0))
    prepared.setdefault("closing_notes", prepared.get("notes", ""))
    for field in DAILY_CLOSING_AMOUNT_FIELDS:
        prepared[field] = _daily_closing_money(prepared.get(field, 0))
    for field in ("expected_cash", "total_expenses"):
        prepared[field] = _daily_closing_money(prepared.get(field, 0))
    prepared["cash_mismatch"] = _daily_closing_money(prepared["counted_cash"] - prepared["expected_cash"])
    prepared["mismatch_amount"] = prepared["cash_mismatch"]
    tolerance = 0.01
    prepared["closing_status"] = (
        "balanced" if abs(prepared["cash_mismatch"]) <= tolerance
        else "shortage" if prepared["cash_mismatch"] < 0
        else "excess"
    )
    return prepared


async def _daily_closing_expected(closing_date: str, opening_cash: float = 0) -> dict:
    """Build an accounting snapshot without mutating sales or inventory."""
    daily = await db.daily_sales.find({"sale_date": closing_date}, {"_id": 0}).to_list(2000)
    invoice_collection = getattr(db, "invoices", None)
    expense_collection = getattr(db, "expenses", None)
    invoices = (
        await invoice_collection.find({}, {"_id": 0}).to_list(5000)
        if invoice_collection else []
    )
    invoices = [row for row in invoices if str(row.get("created_at", "")).startswith(closing_date)]
    expenses = (
        await expense_collection.find({"date": closing_date}, {"_id": 0}).to_list(2000)
        if expense_collection else []
    )
    splits = _empty_daily_closing_splits()
    invoice_refs = set()
    invoice_signatures = Counter()
    for invoice in invoices:
        invoice_splits = _invoice_daily_closing_splits(invoice)
        for key, value in invoice_splits.items():
            splits[key] += value
        invoice_signatures[_invoice_split_signature(invoice_splits)] += 1
        invoice_refs.update(_invoice_reference_values(invoice))

    normalized = [_normalize_daily_sale(row) for row in daily]
    # Very old daily-sale rows had only total_amount; they represented paid cash
    # unless explicitly marked pending.
    for source, row in zip(daily, normalized):
        if not any(source.get(field) is not None for field in (
            "cash_sales", "upi_sales", "card_sales", "outstanding_sales"
        )):
            amount = _money(source.get("total_amount"))
            row["outstanding_sales" if source.get("payment_status") == "pending" else "cash_sales"] = amount

    for source, row in zip(daily, normalized):
        source_marker = str(source.get("source") or source.get("source_type") or "").strip().lower()
        signature = _daily_sale_split_signature(row)
        if _daily_sale_is_invoice_backed(source, invoice_refs):
            continue
        # Legacy daily_sales rows often have no source marker. If one exactly
        # matches a same-day invoice payment split, treat the invoice as the
        # source of truth and consume one matching invoice signature. Explicit
        # manual rows remain counted even if their amount matches an invoice.
        if not source_marker and invoice_signatures[signature] > 0:
            invoice_signatures[signature] -= 1
            continue
        splits["cash_sales"] += row["cash_sales"]
        splits["upi_sales"] += row["upi_sales"]
        splits["card_sales"] += row["card_sales"]
        splits["credit_sales"] += row["outstanding_sales"]
    total_expenses = _money(sum(_money(row.get("amount")) for row in expenses))
    expected_total = _money(sum(splits.values()))
    return {
        **{key: _money(value) for key, value in splits.items()},
        "expenses": total_expenses,
        "total_expenses": total_expenses,
        "expected_total": expected_total,
        "expected_cash": _money(opening_cash + splits["cash_sales"] - total_expenses),
    }


def _daily_closing_public(closing: dict) -> dict:
    closing = _prepare_daily_closing(closing)
    closing = _ensure_action_aliases(closing)
    return {
        key: value
        for key, value in closing.items()
        if key not in {"_id", "tenant_id", "shop_id"}
    }


@api_router.post("/daily-closings", status_code=201)
async def create_daily_closing(
    payload: DailyClosingCreate,
    user: dict = Depends(get_current_user),
):
    if await db.daily_closings.find_one({"closing_date": payload.closing_date}):
        raise HTTPException(status_code=409, detail="A closing already exists for this date")

    data = payload.model_dump()
    expected = await _daily_closing_expected(payload.closing_date, data.get("opening_cash", 0))
    closing = _prepare_daily_closing({
        **data, **expected,
        "id": str(uuid.uuid4()),
        "created_by": user.get("name") or user.get("id", ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    await db.daily_closings.insert_one(closing)
    if LOCAL_MODE:
        asyncio.create_task(_create_and_sync_backup("daily_closing"))
    return _daily_closing_public(closing)


@api_router.get("/daily-closings")
async def list_daily_closings(user: dict = Depends(get_current_user)):
    closings = await db.daily_closings.find({}, {"_id": 0}).sort(
        "closing_date", -1
    ).to_list(2000)
    return [_daily_closing_public(closing) for closing in closings]


@api_router.get("/daily-closings/{closing_date}")
async def get_daily_closing(
    closing_date: str,
    user: dict = Depends(get_current_user),
):
    if not parse_iso_date(closing_date):
        raise HTTPException(status_code=422, detail="closing_date must be a valid ISO date")
    closing = await db.daily_closings.find_one({"closing_date": closing_date}, {"_id": 0})
    if not closing:
        raise HTTPException(status_code=404, detail="Daily closing not found")
    return _daily_closing_public(closing)


@api_router.put("/daily-closings/{closing_id}")
async def update_daily_closing(
    closing_id: str,
    payload: DailyClosingUpdate,
    user: dict = Depends(get_current_user),
):
    existing = await db.daily_closings.find_one({"id": closing_id}, {"_id": 0})
    if not existing:
        raise HTTPException(status_code=404, detail="Daily closing not found")
    if existing.get("locked") and user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only an admin can edit a locked closing")

    changes = payload.model_dump(exclude_unset=True)
    target_date = changes.get("closing_date")
    if target_date and target_date != existing.get("closing_date"):
        duplicate = await db.daily_closings.find_one({"closing_date": target_date}, {"_id": 0})
        if duplicate:
            raise HTTPException(status_code=409, detail="A closing already exists for this date")

    target_date = changes.get("closing_date", existing.get("closing_date"))
    opening_cash = changes.get("opening_cash", existing.get("opening_cash", 0))
    expected = await _daily_closing_expected(target_date, opening_cash)
    updated = _prepare_daily_closing({**existing, **changes, **expected})
    mutable_fields = {*DAILY_CLOSING_AMOUNT_FIELDS, "closing_date", "notes", "closing_notes", "locked",
                      "mismatch_amount", "expected_cash", "cash_mismatch", "total_expenses", "closing_status"}
    update_values = {key: updated[key] for key in mutable_fields if key in updated}
    await db.daily_closings.update_one({"id": closing_id}, {"$set": update_values})
    return _daily_closing_public({**existing, **update_values})


@api_router.get("/daily-sales")
async def list_daily_sales(
    date: Optional[str] = None,
    user: dict = Depends(get_current_user),
):
    q = {}
    if date:
        q["sale_date"] = date
    items = await db.daily_sales.find(
        q,
        {
            "_id": 0,
            "stock_deductions": 0
        }
    ).sort("created_at", -1).to_list(2000)
    return [_normalize_daily_sale(item) for item in items]


@api_router.put("/daily-sales/{sale_id}")
async def update_daily_sale(
    sale_id: str,
    payload: DailySaleUpdate,
    user: dict = Depends(require_role("admin", "pharmacist")),
):
    existing = await db.daily_sales.find_one({"id": sale_id}, {"_id": 0})
    if not existing:
        raise HTTPException(status_code=404, detail="Entry not found")
    changes = payload.model_dump(exclude_unset=True)
    updated = _normalize_daily_sale({**existing, **changes})
    update_values = {
        field: updated[field]
        for field in (
            "cash_sales", "upi_sales", "card_sales", "outstanding_sales",
            "gross_sales", "total_paid", "total_outstanding", "total_amount",
            "notes", "sale_date",
        )
        if field in updated
    }
    await db.daily_sales.update_one({"id": sale_id}, {"$set": update_values})
    return {key: value for key, value in updated.items() if key not in {"_id", "stock_deductions"}}


@api_router.get("/daily-sales/summary")
async def daily_sales_summary(
    date: Optional[str] = None,
    user: dict = Depends(get_current_user)
):
    target = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    items = await db.daily_sales.find(
        {"sale_date": target},
        {"_id": 0}
    ).to_list(2000)

    historical = await db.historical_sales.find(
        {"date": target},
        {"_id": 0}
    ).to_list(2000)

    expenses = await db.expenses.find({"date": target}, {"_id": 0}).to_list(2000)
    normalized = [_normalize_daily_sale(item) for item in items]
    cash = sum(i["cash_sales"] for i in normalized) + sum(_money(h.get("cash_amount")) for h in historical)
    upi = sum(i["upi_sales"] for i in normalized) + sum(_money(h.get("upi_amount")) for h in historical)
    card = sum(i["card_sales"] for i in normalized) + sum(_money(h.get("card_amount")) for h in historical)
    pending = sum(i["outstanding_sales"] for i in normalized) + sum(_money(h.get("pending_amount")) for h in historical)
    expense_total = sum(_money(expense.get("amount")) for expense in expenses)
    paid = cash + upi + card
    total = paid + pending

    return {
        "date": target,
        "count": len(items) + len(historical),
        "cash_sales": _money(cash),
        "upi_sales": _money(upi),
        "card_sales": _money(card),
        "outstanding_sales": _money(pending),
        "gross_sales": _money(total),
        "total_paid": _money(paid),
        "total_outstanding": _money(pending),
        "total_expenses": _money(expense_total),
        "estimated_net_profit": _money(total - expense_total),
        # Backward-compatible summary aliases.
        "total": _money(total),
        "paid": _money(paid),
        "pending": _money(pending),
    }

@api_router.delete("/daily-sales/{sale_id}")
async def delete_daily_sale(
    sale_id: str,
    user: dict = Depends(require_role("admin", "pharmacist"))
):
    sale = await db.daily_sales.find_one({
        "id": sale_id
    })

    if not sale:
        raise HTTPException(
            status_code=404,
            detail="Entry not found"
        )

    result = await db.daily_sales.delete_one({"id": sale_id})
    if result.deleted_count != 1:
        raise HTTPException(status_code=404, detail="Entry not found")
    return {"ok": True}


def _canonical_key_from_complete_fields(row: dict, *, distributor_id: Optional[str] = None) -> Optional[str]:
    """Normalize a legacy source row only when every canonical identity field exists."""
    name = row.get("name") or row.get("medicine_name")
    batch_no = row.get("batch_no") or row.get("batch_number")
    source_distributor_id = distributor_id or row.get("distributor_id")
    expiry_date = row.get("expiry_date")
    pack_size = row.get("pack_size")
    required = (name, source_distributor_id, batch_no, expiry_date, pack_size)
    if not all(str(value or "").strip() for value in required):
        return None
    return canonical_medicine_key(name, batch_no, source_distributor_id, expiry_date, pack_size)


def _source_row_medicine_key(row: dict, *, distributor_id: Optional[str] = None, allow_legacy_normalize: bool = False) -> Optional[str]:
    """Return the row's single canonical key; never use ids, names, or partial batches."""
    key = str(row.get("medicine_key") or "").strip()

    # Old keys without distributor are invalid
    if key and "::" in key and key.count("::") >= 4:
        return key
    if allow_legacy_normalize:
        return _canonical_key_from_complete_fields(row, distributor_id=distributor_id)
    return None


def _unmatched_source_record(source: str, document: dict, row: dict, reason: str, index: int) -> dict:
    """Small deterministic unmatched entry for rebuild safety reporting."""
    return {
        "source": source,
        "document_id": document.get("id"),
        "document_no": document.get("invoice_no") or document.get("po_no") or document.get("return_no"),
        "row_index": index,
        "reason": reason,
        "medicine_key": row.get("medicine_key"),
        "medicine_id": row.get("medicine_id"),
        "name": row.get("name") or row.get("medicine_name"),
        "batch_no": row.get("batch_no") or row.get("batch_number"),
    }


async def _aggregate_invoice_units_by_medicine_key() -> Tuple[dict, list]:
    """Aggregate invoice deductions strictly by medicine_key.

    Legacy rows are normalized exactly once only when all canonical identity
    fields are present. Rows that cannot produce medicine_key are logged as
    unmatched and excluded from stock math.
    """
    sold_by_key = defaultdict(float)
    unmatched_invoices_log = []
    invoices_collection = getattr(db, "invoices", None)
    if invoices_collection is None:
        return sold_by_key, unmatched_invoices_log

    async for invoice in invoices_collection.find({}):
        changed = False
        deductions = invoice.get("stock_deductions") or []
        if deductions:
            normalized_deductions = []
            for index, deduction in enumerate(deductions):
                key = _source_row_medicine_key(deduction, allow_legacy_normalize=True)
                quantity = round_qty(deduction.get("deduct", deduction.get("quantity", 0)))
                if not key:
                    unmatched = _unmatched_source_record("invoice_stock_deduction", invoice, deduction, "missing_medicine_key", index)
                    unmatched_invoices_log.append(unmatched)
                    logger.warning("Inventory rebuild unmatched invoice deduction: %s", unmatched)
                elif quantity > 0:
                    sold_by_key[key] = round_qty(sold_by_key[key] + quantity)
                    if deduction.get("medicine_key") != key:
                        deduction = {**deduction, "medicine_key": key}
                        changed = True
                normalized_deductions.append(deduction)
            if changed:
                invoice["stock_deductions"] = normalized_deductions
        else:
            normalized_items = []
            for index, item in enumerate(invoice.get("items", [])):
                key = _source_row_medicine_key(item, allow_legacy_normalize=True)
                quantity = round_qty(item.get("units_dispensed", item.get("quantity_units", item.get("quantity", 0))))
                if not key:
                    unmatched = _unmatched_source_record("invoice_item", invoice, item, "missing_medicine_key", index)
                    unmatched_invoices_log.append(unmatched)
                    logger.warning("Inventory rebuild unmatched invoice item: %s", unmatched)
                elif quantity > 0:
                    sold_by_key[key] = round_qty(sold_by_key[key] + quantity)
                    if item.get("medicine_key") != key:
                        item = {**item, "medicine_key": key}
                        changed = True
                normalized_items.append(item)
            if changed:
                invoice["items"] = normalized_items
        if changed and invoice.get("id"):
            await invoices_collection.update_one(
                {"id": invoice["id"]},
                {"$set": {"items": invoice.get("items", []), "stock_deductions": invoice.get("stock_deductions", [])}},
            )
    return sold_by_key, unmatched_invoices_log


async def _aggregate_purchase_return_units_by_medicine_key() -> Tuple[dict, list]:
    """Aggregate active purchase returns strictly by medicine_key."""
    returned_by_key = defaultdict(float)
    unmatched_returns_log = []
    returns_collection = getattr(db, "purchase_returns", None)
    if returns_collection is None:
        return returned_by_key, unmatched_returns_log
    async for index, return_doc in _async_enumerate(returns_collection.find({})):
        if return_doc.get("voided_at") or return_doc.get("deleted_at") or return_doc.get("settlement_status") == "deleted":
            continue
        key = _source_row_medicine_key(return_doc, allow_legacy_normalize=False)
        if not key:
            unmatched = _unmatched_source_record("purchase_return", return_doc, return_doc, "missing_medicine_key", index)
            unmatched_returns_log.append(unmatched)
            logger.warning("Inventory rebuild unmatched purchase return: %s", unmatched)
            continue
        returned_by_key[key] = round_qty(returned_by_key[key] + _purchase_return_quantity(return_doc))
    return returned_by_key, unmatched_returns_log


async def _async_enumerate(aiterable):
    index = 0
    async for item in aiterable:
        yield index, item
        index += 1


async def _aggregate_po_inventory_by_medicine_key() -> Tuple[dict, list]:
    """Aggregate purchased stock strictly from active purchase orders by medicine_key."""
    medicines = {}
    unmatched_purchase_orders_log = []
    async for po in db.purchase_orders.find({}):
        if po.get("deleted_at") or po.get("voided_at") or po.get("status") == "deleted":
            continue
        distributor_id = po.get("distributor_id")
        distributor_name = po.get("distributor_name") or po.get("distributor")
        for index, item in enumerate(po.get("items", [])):
            key = _source_row_medicine_key(item, distributor_id=distributor_id, allow_legacy_normalize=True)
            if not key:
                unmatched = _unmatched_source_record("purchase_order_item", po, item, "missing_medicine_key", index)
                unmatched_purchase_orders_log.append(unmatched)
                logger.warning("Inventory rebuild unmatched purchase order item: %s", unmatched)
                continue
            qty = round_qty(round_qty(item.get("quantity", 0)) + round_qty(item.get("free_quantity", 0)))
            if qty <= 0:
                continue
            if key not in medicines:
                medicines[key] = {
                    "id": key,
                    "medicine_id": key,
                    "medicine_key": key,
                    "name": item.get("name") or item.get("medicine_name"),
                    "batch_no": item.get("batch_no") or item.get("batch_number"),
                    "expiry_date": item.get("expiry_date"),
                    "manufacturer": item.get("manufacturer"),
                    "category": item.get("category"),
                    "mrp": item.get("mrp"),
                    "purchase_price": item.get("purchase_price"),
                    "pack_size": item.get("pack_size"),
                    "gst_rate": item.get("gst_rate"),
                    "distributor_id": distributor_id or item.get("distributor_id"),
                    "distributor_name": distributor_name or item.get("distributor_name") or item.get("distributor"),
                    "distributor": distributor_name or item.get("distributor") or item.get("distributor_name"),
                    "purchased_units": 0,
                    "sold_units": 0,
                    "purchase_return_units": 0,
                    "stock_adjustment_units": 0,
                }
            medicines[key]["purchased_units"] = round_qty(medicines[key]["purchased_units"] + qty)
    return medicines, unmatched_purchase_orders_log


async def rebuild_inventory():
    """Rebuild the materialized medicine stock view from ledger source tables only."""
    medicines, unmatched_purchase_orders_log = await _aggregate_po_inventory_by_medicine_key()
    sold_by_key, unmatched_invoices_log = await _aggregate_invoice_units_by_medicine_key()
    returned_by_key, unmatched_returns_log = await _aggregate_purchase_return_units_by_medicine_key()

    for key in sorted(medicines):
        medicine = medicines[key]
        medicine["sold_units"] = round_qty(sold_by_key.get(key, 0))
        medicine["purchase_return_units"] = round_qty(returned_by_key.get(key, 0))
        derivatives = {
            "available_stock": _available_stock(medicine),
            "quantity_units": _available_stock(medicine),
            "return_status": _return_status(medicine),
            "status": _return_status(medicine),
        }
        await db.medicines.update_one({"medicine_key": key}, {"$set": {**medicine, **derivatives}}, upsert=True)

    if hasattr(db.medicines, "delete_many"):
        await db.medicines.delete_many({"medicine_key": {"$nin": sorted(medicines.keys())}})
    elif hasattr(db.medicines, "rows"):
        db.medicines.rows[:] = [row for row in db.medicines.rows if row.get("medicine_key") in medicines]

    return {
        "ok": True,
        "medicine_batches_rebuilt": len(medicines),
        "unmatched_purchase_orders_log": unmatched_purchase_orders_log,
        "unmatched_invoices_log": unmatched_invoices_log,
        "unmatched_returns_log": unmatched_returns_log,
    }

# ---------------- Mount ----------------


app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_cors_origins(),
    allow_origin_regex=r"https://.*\.onrender\.com|http://localhost(:\d+)?|http://127\.0\.0\.1(:\d+)?",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(LocalImportRequestLoggingMiddleware)

app.include_router(api_router)
_record_startup_timing("Register routes", time.perf_counter() - _routes_started)


if LOCAL_MODE and FRONTEND_BUILD_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_BUILD_DIR / "assets")), name="frontend-assets") if (FRONTEND_BUILD_DIR / "assets").exists() else None
    app.mount("/static", StaticFiles(directory=str(FRONTEND_BUILD_DIR / "static")), name="frontend-static") if (FRONTEND_BUILD_DIR / "static").exists() else None
    app.mount("/", StaticFiles(directory=str(FRONTEND_BUILD_DIR), html=True), name="frontend")
    _record_startup_timing("Mount frontend static routes")
