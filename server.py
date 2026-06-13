from dotenv import load_dotenv
from pathlib import Path
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

import os
import uuid
import logging
import math
import re
import bcrypt
import jwt
import asyncio
import hashlib
import hmac
import secrets
import smtplib
import json
import csv
import io
import urllib.request
from contextvars import ContextVar
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta, date
from calendar import monthrange
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Literal
from collections import defaultdict

from fastapi import FastAPI, APIRouter, HTTPException, Depends, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder
from fastapi.security import HTTPBearer
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError
from pydantic import BaseModel, Field, EmailStr, ConfigDict, field_validator, model_validator
from fastapi import UploadFile, File

from version_config import VERSION_METADATA, get_version_metadata


# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
raw_db = client[os.environ['DB_NAME']]

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

app = FastAPI(title="Pharmacy Management API")

@app.get("/")
def home():
    return {"message": "Pharmacy backend is running"}
api_router = APIRouter(prefix="/api")


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
_background_tasks = set()

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
APP_VERSION = VERSION_METADATA["version"]
APP_BUILD = VERSION_METADATA["build"]
APP_UPDATE_MESSAGE = VERSION_METADATA["message"]
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


# ---------------- Auth helpers ----------------
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


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
    date: str | None = None
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


class PurchaseReturnUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    return_date: Optional[str] = None
    reason: Optional[Literal["Expired", "Damaged", "Wrong Item", "Other"]] = None
    return_quantity: Optional[float] = None
    purchase_rate: Optional[float] = None
    notes: Optional[str] = None
    adjust_distributor_ledger: Optional[bool] = None

    @field_validator("return_date")
    @classmethod
    def validate_return_date(cls, value):
        if value is not None and not parse_iso_date(value):
            raise ValueError("return_date must be a valid ISO date")
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


def _system_account_marker_query() -> list[dict]:
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


@app.on_event("startup")
async def startup():
    now_iso = datetime.now(timezone.utc).isoformat()
    await _backfill_tenant_data(now_iso)
    await _cleanup_unsafe_real_users()
    # Repair and safely reseed demo identities before unique email indexes are enforced.
    await _seed_demo_data(now_iso)
    await raw_db.users.create_index("email", unique=True)
    await raw_db.users.create_index("mobile", unique=True, sparse=True)
    await raw_db.password_reset_requests.create_index("created_at", expireAfterSeconds=FORGOT_PASSWORD_WINDOW_MINUTES * 60)
    await raw_db.pending_signups.create_index("expires_at", expireAfterSeconds=0)
    for collection_name, indexes in {
        "medicines": ["name", "batch_no", "manufacturer", "barcode"], "invoices": ["created_at"],
        "purchase_returns": ["return_date", "distributor", "medicine_name", "reason", "ledger_adjusted", "po_adjustment_id"],
    }.items():
        for index in indexes:
            await raw_db[collection_name].create_index(index)
    for collection_name, date_field in {
        "invoices": "created_at", "purchase_orders": "created_at",
        "customer_transactions": "created_at", "purchase_returns": "return_date",
    }.items():
        await raw_db[collection_name].create_index([("tenant_id", 1), (date_field, -1)])
    await raw_db.purchase_returns.create_index([("tenant_id", 1), ("distributor_id", 1), ("po_adjustment_id", 1)])
    await raw_db.daily_closings.create_index([("tenant_id", 1), ("closing_date", 1)], unique=True)
    await _seed_admin_if_enabled(now_iso)
    task = asyncio.create_task(_run_startup_purchase_return_stock_recalculation())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


@app.on_event("shutdown")
async def shutdown():
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

    await asyncio.to_thread(send)
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

        await asyncio.to_thread(send_email)
        return True

    webhook = os.environ.get("SMS_OTP_WEBHOOK_URL")
    if not webhook:
        return False

    def send_sms():
        body = json.dumps({"mobile": identifier, "otp": otp, "purpose": "signup"}).encode()
        request = urllib.request.Request(webhook, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=10) as response:
            return 200 <= response.status < 300

    return bool(await asyncio.to_thread(send_sms))


@api_router.get("/version")
async def version(response: Response):
    response.headers["Cache-Control"] = "no-store"
    return get_version_metadata()


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
    user = await raw_db.users.find_one({
        "$and": [
            {"$or": [{"email": identifier}, {"mobile": identifier}]},
            {"id": {"$ne": DEMO_USER_ID}},
            {"tenant_id": {"$ne": DEMO_TENANT_ID}},
            {"is_demo": {"$ne": True}},
        ]
    })
    if not user or user.get("active", True) is False or not verify_password(payload.password, user["password_hash"]):
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


# ---------------- Medicines ----------------
def _medicine_identity_filter(medicine_id: str) -> dict:
    return {
        "$or": [
            {"id": medicine_id},
            {"medicine_key": medicine_id},
        ]
    }


def _fifo_expiry_key(batch: dict) -> tuple:
    """Sort valid expiry dates first and keep malformed dates deterministic."""
    expiry = parse_expiry_date(batch.get("expiry_date"))
    return (
        expiry is None,
        expiry or date.max,
        str(batch.get("id") or batch.get("medicine_key") or ""),
    )


def _compact_billing_stock_summary(batches: list[dict]) -> dict:
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

    def normalized_medicine_key(name, batch_no):

        if not name or not batch_no:

            return None

        return f"{str(name).strip().lower()}::{str(batch_no).strip().upper()}"

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
                    item.get("batch_no")
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
        item["cost_value"] = round(float(item.get("purchase_price") or 0) * item["total_stock"], 2)
        item["mrp_value"] = round(float(item.get("mrp") or 0) * item["total_stock"], 2)

    result.sort(
        key=lambda x: (
            x.get(sort_by)
            or ""
        )
    )

    return _normalize_inventory_quantities(result)
    
@api_router.put("/medicines/{medicine_id}/threshold")
async def update_threshold(
    medicine_id: str,
    payload: dict,
    user: dict = Depends(require_role("admin", "pharmacist"))
):

    medicine_filter = _medicine_identity_filter(medicine_id)

    result = await db.medicines.update_one(
        medicine_filter,
        {
            "$set": {
                "low_stock_threshold": int(payload["low_stock_threshold"])
            }
        }
    )

    # IMPORTANT: verify update happened
    if result.matched_count == 0:
        raise HTTPException(404, "Medicine not found")

    # return fresh value
    updated = await db.medicines.find_one(medicine_filter)

    return {
        "message": "threshold updated",
        "low_stock_threshold": updated.get("low_stock_threshold")
    }

def _manual_sold_capacity(batch: dict) -> float:
    purchased = max(0.0, _purchased_stock(batch) + _stock_adjustment_stock(batch))
    returned = max(0.0, _purchase_return_stock(batch))
    return round_qty(max(0.0, purchased - returned))


def _manual_sold_allocations(batches: list[dict], requested_sold: float) -> list[tuple[dict, float]]:
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


async def _set_manual_sold_allocations(allocations: list[tuple[dict, float]], session=None) -> list[dict]:
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
    await db.regular_patients.insert_one(data)
    return {"success": True}

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

        current_balance = _current_distributor_balance(
            distributor,
            transactions_by_distributor.get(distributor.get("id"), []),
        )
        distributor["current_balance"] = current_balance
        distributor["outstanding_balance"] = current_balance
        distributor["distributor_status"] = distributor.get("distributor_status") or "active"
        purchases = [
            txn for txn in transactions_by_distributor.get(distributor.get("id"), [])
            if txn.get("type") in {"purchase", "sale", "opening_balance"}
        ]
        opening_in_txns = any(_is_opening_balance_transaction(txn, distributor.get("id")) for txn in purchases)
        opening = 0 if opening_in_txns else _safe_float(distributor.get("opening_balance"))
        distributor["total_purchases"] = _round_ledger_money(
            opening + sum(_safe_float(txn.get("amount")) for txn in purchases)
        )
        distributor["total_paid"] = _round_ledger_money(sum(
            _safe_float(txn.get("amount"))
            for txn in transactions_by_distributor.get(distributor.get("id"), [])
            if txn.get("type") == "payment"
        ))
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
    await db.distributors.insert_one(d.model_dump())
    return d.model_dump()


@api_router.put("/distributors/{did}")
async def update_distributor(did: str, d: Distributor, user: dict = Depends(require_role("admin", "pharmacist"))):
    existing = await db.distributors.find_one({"id": did}, {"_id": 0}) or {}
    data = d.model_dump()
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
    await db.customers.insert_one(c.model_dump())
    return c.model_dump()


@api_router.put("/customers/{cid}")
async def update_customer(cid: str, c: Customer, user: dict = Depends(get_current_user)):
    data = c.model_dump()
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
    counter = await db.counters.find_one_and_update(
        {
            "_id": f"{_current_tenant.get() or REAL_TENANT_ID}:{prefix}-{today}"
        },
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
    plan: list[dict],
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
    stock_requests: dict[str, float],
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


async def _restore_fifo_stock(applied: list[dict]):
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


def _stock_deductions_from_steps(steps: list[dict]) -> list[dict]:
    return [
        {
            "medicine_id": step["medicine_id"],
            "medicine_name": step.get("medicine_name", ""),
            "deduct": round_qty(step.get("deduct", 0)),
        }
        for step in steps
        if round_qty(step.get("deduct", 0)) > 0
    ]


def _stock_deductions_from_daily_sale(sale: dict) -> list[dict]:
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
    deductions: list[dict],
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


async def _reapply_daily_sale_stock(restored: list[dict]):
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
        await _apply_fifo_stock_requests(
            stock_requests,
            session=session
        )
        await write_invoice_records(session=session)
        return invoice

    async def fallback_operation():
        applied = []

        try:
            await _apply_fifo_stock_requests(
                stock_requests,
                applied=applied
            )
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


def _distributor_transaction_update_date(changes: dict) -> str | None:
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


def _opening_balance_transaction_id(distributor_id: str | None) -> str:
    return f"opening-balance-{distributor_id}"


def _is_opening_balance_transaction(txn: dict, distributor_id: str | None = None) -> bool:
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
        or (txn_type == "purchase" and (notes == "opening balance" or reference == "opening balance"))
    )


DISTRIBUTOR_OPENING_BALANCE_DATE_FIELDS = (
    "opening_balance_date",
    "opening_date",
    "balance_date",
    "transaction_date",
    "date",
)


def _first_present_field(source: dict, field_names: tuple[str, ...]):
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
        "type": "purchase",
        "subtype": "Opening Balance",
        "display_type": "Purchase",
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
        "running_balance": round(opening_balance, 2),
    }


def _normalize_opening_balance_transaction(txn: dict, distributor: dict) -> dict:
    transaction_date = _opening_balance_transaction_date(txn, distributor)
    normalized = {
        **txn,
        "id": txn.get("id") or _opening_balance_transaction_id(distributor.get("id")),
        "distributor_id": txn.get("distributor_id") or distributor.get("id"),
        "type": "purchase",
        "subtype": "Opening Balance",
        "display_type": "Purchase",
        "amount": _safe_float(txn.get("amount", distributor.get("opening_balance", 0))),
        "reference_number": txn.get("reference_number") or "Opening Balance",
        "notes": txn.get("notes") or "Opening Balance",
        "created_at": transaction_date,
        "opening_balance_date": transaction_date,
        "transaction_date": transaction_date,
        "date": transaction_date,
        "is_opening_balance": True,
    }
    return normalized


def _parse_ledger_transaction_date(value: str | None):
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


def _available_financial_years(transactions: list[dict]) -> list[str]:
    years = {
        _financial_year_for_date(txn_date)
        for txn in transactions
        if (txn_date := _distributor_transaction_date(txn))
    }
    return sorted(years, key=lambda year: int(year.split("-", 1)[0]), reverse=True)


def _filter_transactions_by_financial_year(
    transactions: list[dict],
    financial_year: str,
) -> list[dict]:
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
    transactions: list[dict],
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
    transactions: list[dict],
    financial_year: str | None,
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
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _round_ledger_money(value: float) -> float:
    """Round ledger currency with the same decimal-safe rule used by POs."""
    return _money_float(_to_decimal(value))


def _serializable_transaction_id(value) -> str | None:
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


def _json_safe_ledger_transaction(txn: dict) -> dict:
    safe_txn = _json_safe_ledger_value(txn if isinstance(txn, dict) else {})
    safe_txn.pop("items", None)
    for money_field in ("amount", "running_balance", "bill_amount", "paid_amount", "due_amount"):
        if money_field in safe_txn:
            safe_txn[money_field] = _round_ledger_money(safe_txn[money_field])
    for id_field in ("id", "distributor_id", "purchase_order_id", "sale_id"):
        if safe_txn.get(id_field) not in (None, ""):
            safe_txn[id_field] = str(safe_txn[id_field])
    return safe_txn


def _fifo_debug_enabled(distributor_id: str) -> bool:
    debug_distributor_id = os.environ.get("DISTRIBUTOR_FIFO_DEBUG_LEDGER_ID")
    return debug_distributor_id in {"*", str(distributor_id)}


def _build_distributor_fifo_metadata(
    transactions: list[dict],
    distributor_id: str,
) -> dict[str, dict]:
    metadata_by_id: dict[str, dict] = {}
    unpaid_bills: list[dict] = []
    pending_credits: list[dict] = []
    allocation_sequence: list[dict] = []
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
        txn: dict | None,
        txn_id: str | None,
        stage: str,
        sequence_no: int | None = None,
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
    transactions: list[dict],
    financial_year: str | None,
) -> list[dict]:
    if not financial_year:
        return list(transactions)

    _start_date, end_date = _financial_year_date_range(financial_year)
    return [
        txn
        for txn in transactions
        if (txn_date := _distributor_transaction_date(txn)) and txn_date <= end_date
    ]

def _apply_distributor_transaction(balance: float, txn: dict) -> tuple[float, str]:
    amount = _safe_float(txn.get("amount", 0) if isinstance(txn, dict) else 0)
    txn_type = txn.get("type") if isinstance(txn, dict) else None

    if txn_type in ["purchase", "sale", "opening_balance"]:
        return balance + amount, "purchase"

    if txn_type == "purchase_return":
        return balance - amount, "adjustment"

    return balance - amount, "payment"


def _current_distributor_balance(distributor: dict, transactions: list[dict]) -> float:
    opening_transactions = [
        txn for txn in transactions
        if _is_opening_balance_transaction(txn, distributor.get("id"))
    ]
    balance = 0.0 if opening_transactions else _safe_float(distributor.get("opening_balance", 0))
    used_opening_transaction = False

    for txn in transactions:
        if _is_opening_balance_transaction(txn, distributor.get("id")):
            if used_opening_transaction:
                continue
            normalized = _normalize_opening_balance_transaction(txn, distributor)
            balance, _bucket = _apply_distributor_transaction(balance, normalized)
            used_opening_transaction = True
            continue

        balance, _bucket = _apply_distributor_transaction(balance, txn)

    return round(balance, 2)


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
    dist = await db.distributors.find_one({"id": did}, {"_id": 0})
    if not dist:
        raise HTTPException(status_code=404, detail="Distributor not found")

    opening_balance_date = _distributor_opening_balance_date(dist)
    if opening_balance_date:
        dist["opening_balance_date"] = opening_balance_date

    txns = await db.distributor_transactions.find(
        {"distributor_id": did},
        {"_id": 0},
    ).sort("created_at", 1).to_list(1000)

    opening_txn = None
    non_opening_txns = []
    for txn in txns:
        if _is_opening_balance_transaction(txn, did):
            if opening_txn is None:
                opening_txn = _normalize_opening_balance_transaction(txn, dist)
            continue
        non_opening_txns.append(txn)

    ledger_txns = [
        opening_txn or _opening_balance_transaction(dist),
        *non_opening_txns,
    ]
    available_financial_years = _available_financial_years(ledger_txns)
    financial_year_metadata = _distributor_financial_year_metadata(
        ledger_txns,
        financial_year,
    )
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

    if financial_year:
        ledger_txns = _filter_transactions_by_financial_year(ledger_txns, financial_year)

    balance = 0.0
    running = []
    total_purchases = 0.0
    total_paid = 0.0
    total_adjustments = 0.0

    for txn in ledger_txns:
        balance, bucket = _apply_distributor_transaction(balance, txn)
        amount = _safe_float(txn.get("amount", 0) if isinstance(txn, dict) else 0)

        if bucket == "purchase":
            total_purchases += amount
        elif bucket == "adjustment":
            total_adjustments += amount
        else:
            total_paid += amount

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
        }))

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
    return {
        "distributor": dist,
        "transactions": filtered_running,
        "balance": round(balance, 2),
        "total_purchases": round(total_purchases, 2),
        "total_paid": round(total_paid, 2),
        "total_adjustments": round(total_adjustments, 2),
        "available_financial_years": available_financial_years,
        "current_financial_year": _current_financial_year(),
        **financial_year_metadata,
    }


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
    return normalized


def _purchase_return_settlement_status(return_doc: dict) -> str:
    if return_doc.get("settlement_status"):
        return return_doc["settlement_status"]
    if return_doc.get("po_adjustment_id") or return_doc.get("settled_by_po"):
        return "settled_by_po"
    if return_doc.get("ledger_adjusted") or return_doc.get("adjust_distributor_ledger"):
        return "ledger_adjusted"
    return "unsettled"


async def _find_purchase_return_medicine(payload: PurchaseReturnCreate, session=None) -> dict:
    batch_filter = {"batch_no": payload.batch_number}
    lookup_filters = [
        {**batch_filter, "name": payload.medicine_name},
    ]

    if payload.medicine_key:
        lookup_filters.append({**batch_filter, "medicine_key": payload.medicine_key})

    if payload.medicine_id:
        lookup_filters.append({"id": payload.medicine_id})

    medicine = await db.medicines.find_one(
        {"$or": lookup_filters},
        session=session,
    )

    if not medicine:
        raise HTTPException(
            status_code=400,
            detail="Medicine batch not found",
        )

    return medicine


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
        "created_at": return_doc["created_at"],
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
    return_doc: dict, medicines: list[dict], tenant_id: Optional[str] = None
) -> Optional[dict]:
    """Safely resolve a legacy purchase return to one unambiguous medicine batch."""
    return_id = return_doc.get("id")

    def unique_match(candidates: list[dict], method: str) -> Optional[dict]:
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
    # Dashboard summaries are calculated live from medicines and transactions.
    # Refreshing medicine derivatives rebuilds the inventory portion immediately.
    logger.info("Dashboard inventory summaries rebuilt tenant/shop=%s", tenant_id)


def _invalidate_inventory_dashboard_cache(tenant_id: Optional[str]) -> None:
    # There is currently no application cache to clear. Keep this hook for
    # future inventory/dashboard cache providers.
    logger.info("Inventory/dashboard cache invalidated tenant/shop=%s (no cache configured)", tenant_id)


async def recalculate_purchase_return_stock(tenant_id: Optional[str] = None) -> dict:
    """Backfill return quantities and refresh inventory/dashboard-derived fields."""
    tenant_id = tenant_id or _current_tenant.get()
    logger.info("Purchase return stock recalculation started tenant/shop=%s", tenant_id)
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(None)
    purchase_returns = await db.purchase_returns.find({}, {"_id": 0}).to_list(None)
    totals = defaultdict(float)
    unmatched_return_ids = []
    matched_returns = 0

    for return_doc in purchase_returns:
        if return_doc.get("voided_at"):
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
    try:
        tenant_ids = set(await raw_db.medicines.distinct("tenant_id"))
        tenant_ids.update(await raw_db.purchase_returns.distinct("tenant_id"))
        for tenant_id in sorted(item for item in tenant_ids if item):
            active_token = _request_active.set(True)
            tenant_token = _current_tenant.set(tenant_id)
            demo_token = _current_demo.set(False)
            try:
                await recalculate_purchase_return_stock(tenant_id)
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
        await db.distributor_transactions.update_one(
            {"id": existing_id, "return_id": old["id"]},
            {"$set": transaction},
            session=session,
        )
        return existing_id
    await db.distributor_transactions.insert_one(transaction, session=session)
    return transaction["id"]


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
    if status == "voided" or current.get("voided_at"):
        raise HTTPException(status_code=409, detail="Voided purchase returns cannot be edited")
    if status == "settled_by_po":
        raise HTTPException(status_code=409, detail="Purchase return is settled in a purchase order and cannot be edited")
    if status == "ledger_adjusted":
        raise HTTPException(status_code=409, detail="Ledger-adjusted purchase returns cannot be edited; void the ledger adjustment first")

    changes = payload.model_dump(exclude_unset=True)
    if not changes:
        return _return_public(current)
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
    if status == "voided" or current.get("voided_at"):
        raise HTTPException(status_code=409, detail="Purchase return is already voided")
    if status == "settled_by_po":
        raise HTTPException(status_code=409, detail="Purchase return is settled in a purchase order and cannot be voided")

    quantity = float(current.get("return_quantity", 0) or 0)
    stock_restored = False

    async def write(session=None):
        nonlocal stock_restored
        voided_at = datetime.now(timezone.utc).isoformat()
        if current.get("ledger_transaction_id"):
            await db.distributor_transactions.update_one(
                {"id": current["ledger_transaction_id"], "return_id": return_id},
                {"$set": {
                    "voided_at": voided_at,
                    "voided_by": user.get("name") or user.get("id", ""),
                    "void_reason": "Purchase return voided",
                }},
                session=session,
            )
        await _set_purchase_return_stock_delta(current["medicine_id"], -quantity, session=session)
        stock_restored = True
        result = await db.purchase_returns.update_one({
            "id": return_id, "$or": [{"po_adjustment_id": {"$exists": False}}, {"po_adjustment_id": None}]
        }, {"$set": {
            "voided_at": voided_at,
            "voided_by": user.get("name") or user.get("id", ""),
            "settlement_status": "voided",
            "ledger_adjusted": False,
            "adjust_distributor_ledger": False,
            "settled_return_value": 0.0,
        }}, session=session)
        if result.modified_count != 1:
            raise HTTPException(status_code=409, detail="Purchase return changed or was applied to a purchase order")
        return {"message": "Purchase return voided", "id": return_id}

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

    by_distributor = defaultdict(lambda: {"quantity": 0.0, "value": 0.0, "count": 0})
    by_medicine = defaultdict(lambda: {"quantity": 0.0, "value": 0.0, "count": 0})
    by_reason = defaultdict(lambda: {"quantity": 0.0, "value": 0.0, "count": 0})
    by_ledger_status = {
        "adjusted": {"quantity": 0.0, "value": 0.0, "count": 0},
        "pending": {"quantity": 0.0, "value": 0.0, "count": 0},
    }

    total_quantity = 0.0
    total_value = 0.0
    settled_value = 0.0

    def add_summary(bucket: dict, key: str, quantity: float, value: float):
        bucket[key]["quantity"] += quantity
        bucket[key]["value"] += value
        bucket[key]["count"] += 1

    for item in returns:
        quantity = float(item.get("return_quantity", 0) or 0)
        value = _purchase_return_credit(item)
        total_quantity += quantity
        total_value += value
        if _purchase_return_settlement_status(item) in {"ledger_adjusted", "settled_by_po"}:
            settled_value += value

        add_summary(by_distributor, item.get("distributor") or "Unknown", quantity, value)
        add_summary(by_medicine, item.get("medicine_name") or "Unknown", quantity, value)
        add_summary(by_reason, item.get("reason") or "Unknown", quantity, value)
        status_key = "adjusted" if item.get("ledger_adjusted") else "pending"
        add_summary(by_ledger_status, status_key, quantity, value)

    def finalize_summary(bucket: dict) -> list[dict]:
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
        "settled_return_value": _money_float(_to_decimal(settled_value)),
        "unsettled_return_value": _money_float(_to_decimal(total_value) - _to_decimal(settled_value)),
        "return_count": len(returns),
        "returns": [_normalized_purchase_return_money(item) for item in returns],
        "purchase_returns": [_normalized_purchase_return_money(item) for item in returns],
        "returns_by_distributor": finalize_summary(by_distributor),
        "returns_by_medicine": finalize_summary(by_medicine),
        "returns_by_reason": finalize_summary(by_reason),
        "ledger_adjusted_status": finalize_summary(by_ledger_status),
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
    distributor_lookup = {d.get("id"): d for d in distributors}

    txn_query = {"created_at": {"$regex": f"^{month}"}}
    if distributor_id:
        txn_query["distributor_id"] = distributor_id

    transactions = await db.distributor_transactions.find(txn_query, {"_id": 0}).to_list(5000)

    summaries = {}
    for distributor in distributors:
        did = distributor.get("id")
        summaries[did] = {
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

    for txn in transactions:
        did = txn.get("distributor_id")
        if did not in summaries:
            distributor = distributor_lookup.get(did, {})
            summaries[did] = {
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

        amount = float(txn.get("amount", 0) or 0)
        summary = summaries[did]

        if txn.get("type") in ["purchase", "sale"]:
            summary["purchase_total"] += amount
            summary["net_change"] += amount
        elif txn.get("type") == "payment":
            summary["payment_total"] += amount
            summary["net_change"] -= amount
        elif txn.get("type") == "purchase_return":
            summary["adjustment_total"] += amount
            summary["net_change"] -= amount

        summary["transaction_count"] += 1
        summary["transactions"].append(txn)

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
        running.append({**t, "amount": round(float(t.get("amount", 0) or 0), 2), "running_balance": balance})
    def matches(t):
        text = " ".join(str(t.get(k, "")) for k in ("invoice_number", "reference_number", "reference", "payment_mode", "mode", "type"))
        return (not search or search.lower() in text.lower()) and (not invoice_number or invoice_number.lower() in str(t.get("invoice_number") or t.get("reference") or "").lower()) and (not reference_number or reference_number.lower() in str(t.get("reference_number") or t.get("reference") or "").lower()) and (not payment_mode or payment_mode.lower() == str(t.get("payment_mode") or t.get("mode") or "").lower()) and (not transaction_type or transaction_type.lower() == str(t.get("type") or "").lower()) and (not start or str(t.get("created_at", "")) >= start) and (not end or str(t.get("created_at", "")) <= end) and (amount is None or round(float(t.get("amount", 0) or 0), 2) == round(amount, 2))
    running = [t for t in running if matches(t)]
    return {"customer": cust, "transactions": running, "balance": round(balance, 2)}


def _ledger_export_csv(owner_type: str, ledger: dict, start_date: date | None, end_date: date | None) -> str:
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
        item = low_stock_by_name.setdefault(name_key, {
            "id": m.get("id"), "name": m.get("name"), "qty": 0,
            "current_stock": 0, "available_qty": 0, "threshold": threshold,
            "status": _low_stock_status(m),
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
    daily, monthly = defaultdict(float), defaultdict(float)
    for invoice in invoices:
        invoice_total = _safe_float(invoice.get("total", invoice.get("grand_total", 0)))
        total_sales += invoice_total
        total_gst += _safe_float(invoice.get("gst_total", invoice.get("total_gst", 0)))
        total_discount += _safe_float(invoice.get("bill_discount", invoice.get("discount", 0)))
        day = str(invoice.get("created_at") or "")[:10]
        if day:
            daily[day] += invoice_total
            monthly[day[:7]] += invoice_total
        for item in invoice.get("items", []):
            quantity = _safe_float(item.get("units_dispensed", item.get("quantity", 0)))
            revenue = _safe_float(item.get("line_total", item.get("net_amount", item.get("mrp", 0) * quantity)))
            # Current invoices persist the batch-aware purchase cost; legacy rows fall back to batch medicine cost.
            cost = _safe_float(item.get("purchase_cost"), -1)
            if cost < 0:
                unit_cost = _safe_float(item.get("purchase_rate", item.get("purchase_price", costs.get(item.get("medicine_id"), 0))))
                cost = unit_cost * quantity
            profit += revenue - cost
    return {
        "total_sales": _round_ledger_money(total_sales), "total_gst": _round_ledger_money(total_gst),
        "total_discount": _round_ledger_money(total_discount), "estimated_profit": _round_ledger_money(profit),
        "daily": [{"date": key, "total": _round_ledger_money(value)} for key, value in sorted(daily.items())],
        "monthly_sales_trend": [{"month": key, "sales": _round_ledger_money(value)} for key, value in sorted(monthly.items())],
        "invoice_count": len(invoices),
    }


@api_router.get("/reports/stock-valuation")
async def stock_valuation(user: dict = Depends(get_current_user)):
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(5000)
    cost_value = mrp_value = total_units = 0.0
    risk = {"expired": {"count": 0, "value_at_risk": 0.0}, "near_expiry": {"count": 0, "value_at_risk": 0.0}}
    today = datetime.now(timezone.utc).date()
    for medicine in medicines:
        available = _available_stock(medicine)
        cost = available * _safe_float(medicine.get("purchase_price"))
        total_units += available; cost_value += cost; mrp_value += available * _safe_float(medicine.get("mrp"))
        details = expiry_details(medicine.get("expiry_date"), today)
        key = "expired" if details["expiry_status"] == "expired" else "near_expiry" if details["expiry_status"] == "warning" else None
        if key and available > 0:
            risk[key]["count"] += 1; risk[key]["value_at_risk"] += cost
    return {"total_items": len(medicines), "total_units": round_qty(total_units), "cost_value": _round_ledger_money(cost_value),
            "mrp_value": _round_ledger_money(mrp_value), "potential_profit": _round_ledger_money(mrp_value-cost_value),
            "expiry_risk_counts": {key: value["count"] for key, value in risk.items()},
            "expiry_value_at_risk": {key: _round_ledger_money(value["value_at_risk"]) for key, value in risk.items()},
            "total_expiry_value_at_risk": _round_ledger_money(sum(value["value_at_risk"] for value in risk.values()))}


def _outstanding_aging(transactions: list[dict], charge_types: set[str], credit_types: set[str]) -> dict:
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
            cust_out.append({"id": customer["id"], "name": customer["name"], "phone": customer.get("phone", ""), "balance": _round_ledger_money(balance), **aging})
            for key, value in aging["buckets"].items(): customer_aging[key] += value
    for distributor in distributors:
        txns = distributor_grouped[distributor.get("id")]
        aging = _outstanding_aging(txns, {"purchase", "sale", "opening_balance"}, {"payment", "purchase_return", "credit", "credit_adjustment"})
        balance = max(0.0, _current_distributor_balance(distributor, txns))
        if balance > 0:
            # Legacy opening balances may not have a transaction; retain them in the current bucket.
            gap = max(0.0, balance - sum(aging["buckets"].values())); aging["buckets"]["0-30"] = _round_ledger_money(aging["buckets"]["0-30"] + gap)
            dist_out.append({"id": distributor["id"], "name": distributor["name"], "balance": _round_ledger_money(balance), **aging})
            for key, value in aging["buckets"].items(): distributor_aging[key] += value
    customer_total = sum(row["balance"] for row in cust_out); distributor_total = sum(row["balance"] for row in dist_out)
    return {"customers": cust_out, "distributors": dist_out, "customer_total": _round_ledger_money(customer_total), "distributor_total": _round_ledger_money(distributor_total),
            "customer_receivables": _round_ledger_money(customer_total), "distributor_payables": _round_ledger_money(distributor_total),
            "customer_aging": {key: _round_ledger_money(value) for key, value in customer_aging.items()}, "distributor_aging": {key: _round_ledger_money(value) for key, value in distributor_aging.items()},
            "aging_buckets": {"customers": customer_aging, "distributors": distributor_aging},
            "outstanding_movement": {"customer_transactions": customer_txns, "distributor_transactions": distributor_txns}}


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
    expired, near = [], []

    for m in medicines:
        if _available_stock(m) <= 0:
            continue
        details = expiry_details(
            m.get("expiry_date"),
            today
        )

        available = _available_stock(m)
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
        elif details["expiry_status"] == "warning":
            near.append(item)

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
        "expired_value_at_risk": _round_ledger_money(sum(item["cost_value_at_risk"] for item in expired)),
        "near_expiry_value_at_risk": _round_ledger_money(sum(item["cost_value_at_risk"] for item in near)),
        "total_value_at_risk": _round_ledger_money(sum(item["cost_value_at_risk"] for item in expired + near)),
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


# ---------------- Backup ----------------
@api_router.get("/backup/export")
async def backup_export(user: dict = Depends(require_role("admin"))):
    data = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "medicines": await db.medicines.find({}, {"_id": 0}).to_list(10000),
        "distributors": await db.distributors.find({}, {"_id": 0}).to_list(10000),
        "customers": await db.customers.find({}, {"_id": 0}).to_list(10000),
        "invoices": await db.invoices.find({}, {"_id": 0}).to_list(10000),
        "customer_transactions": await db.customer_transactions.find({}, {"_id": 0}).to_list(10000),
        "distributor_transactions": await db.distributor_transactions.find({}, {"_id": 0}).to_list(10000),
        "purchase_returns": await db.purchase_returns.find({}, {"_id": 0}).to_list(10000),
    }
    return data


@api_router.post("/backup/import")
async def backup_import(payload: dict, user: dict = Depends(require_role("admin"))):
    collections = ["medicines", "distributors", "customers", "invoices", "customer_transactions", "distributor_transactions", "purchase_returns"]
    counts = {}
    for c in collections:
        items = payload.get(c, [])
        if c == "medicines":
            items = [_normalize_inventory_quantities(item) for item in items]
        if items:
            await db[c].delete_many({})
            await db[c].insert_many(items)
            counts[c] = len(items)
    return {"imported": counts}


# ---------------- Purchase Orders / GRN ----------------
class POItem(BaseModel):
    name: str
    batch_no: str
    quantity: float
    free_quantity: float = 0
    pack_size: str | None = None

    purchase_price: float
    mrp: float

    manufacturer: str | None = None
    category: str | None = None

    expiry_date: str | None = None
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

    po_date: str | None = None   # 👈 ADD THIS

    items: list[POItem]

    notes: str | None = None
    sub_total: float = 0
    scheme_discount: float = 0
    cash_discount: float = 0
    total_cgst: float = 0
    total_sgst: float = 0
    round_off: float = 0
    grand_total: float = 0
    purchase_return_ids: list[str] = Field(default_factory=list)
    purchase_returns: list[POReturnCreditRow] = Field(default_factory=list)

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
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY, rounding=ROUND_HALF_UP)


def _money_float(value: Decimal) -> float:
    return float(_round_money(value))


def _calculate_purchase_order_totals(payload: POCreate) -> dict:
    """Calculate PO totals with order discount applied slab-wise before GST."""
    slab_subtotals: dict[Decimal, Decimal] = defaultdict(Decimal)

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


async def _resolve_po_purchase_returns(payload: POCreate, allow_po_id: Optional[str] = None) -> tuple[list[dict], float]:
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

    candidates = await db.purchase_returns.find(
        {"distributor_id": payload.distributor_id}, {"_id": 0}
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
        if item.get("voided_at"):
            raise HTTPException(status_code=409, detail="Voided purchase return credit cannot be applied to a purchase order")
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


def _apply_po_return_credit(po_totals: dict, returns: list[dict], credit: float) -> dict:
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
    returns = await db.purchase_returns.find({"distributor_id": distributor_id, "$or": [{"po_adjustment_id": {"$exists": False}}, {"po_adjustment_id": None}]}, {"_id": 0}).sort("return_date", -1).to_list(1000)
    result = []
    for item in returns:
        if item.get("voided_at") or item.get("ledger_adjusted") or item.get("adjust_distributor_ledger"):
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


@api_router.post("/purchase-orders")
async def create_po(
    payload: POCreate,
    user: dict = Depends(require_role("admin", "pharmacist"))
):

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
    selected_returns, return_credit = await _resolve_po_purchase_returns(payload)
    return_adjustment = _apply_po_return_credit(po_totals, selected_returns, return_credit)

    po = {
    "id": str(uuid.uuid4()),
    "po_no": await _next_po_no(),
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
            "medicine_key": f"{str(i.name).strip().lower()}::{str(i.batch_no).strip().upper()}",
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

    await db.purchase_orders.insert_one(po)
    if return_adjustment["purchase_return_ids"]:
        settled_at = datetime.now(timezone.utc).isoformat()
        await db.purchase_returns.update_many(
            {"id": {"$in": return_adjustment["purchase_return_ids"]}},
            {"$set": _po_return_settlement_fields(po["id"], settled_at)},
        )

    po.pop("_id", None)
    await rebuild_inventory()
    return po
    
@api_router.delete("/purchase-orders/{po_id}")
async def delete_po(
    po_id: str,
    user: dict = Depends(require_role("admin"))
):

    po = await db.purchase_orders.find_one({
        "id": po_id
    })

    if not po:
        raise HTTPException(
            404,
            "PO not found"
        )

    for i in po.get("items", []):

        qty = round_qty(
            round_qty(i.get("quantity", 0)) + round_qty(i.get("free_quantity", 0))
        )

        medicine = await db.medicines.find_one({
            "name": i.get("name"),
            "batch_no": i.get("batch_no")
        })

        if medicine:

            await _set_rounded_stock_delta(
                medicine["id"], "purchased_units", -qty
            )

    if po.get("purchase_return_ids"):
        await db.purchase_returns.update_many(
            {"id": {"$in": po["purchase_return_ids"]}, "po_adjustment_id": po_id},
            {"$unset": {"po_adjustment_id": "", "po_adjusted_at": ""}, "$set": _released_po_return_settlement_fields()},
        )

    await db.purchase_orders.delete_one({
        "id": po_id
    })
    await rebuild_inventory()

    return {
        "message": "PO deleted"
    }

@api_router.put("/purchase-orders/{po_id}")
async def update_po(
    po_id: str,
    payload: POCreate,
    user: dict = Depends(require_role("admin"))
):

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

    if not old_po:
        raise HTTPException(404, "PO not found")

# ONLY UPDATE PO — DO NOT TOUCH INVENTORY

    po_totals = _calculate_purchase_order_totals(payload)
    selected_returns, return_credit = await _resolve_po_purchase_returns(payload, allow_po_id=po_id)
    return_adjustment = _apply_po_return_credit(po_totals, selected_returns, return_credit)

    await db.purchase_orders.update_one(
        {"id": po_id},
        {
            "$set": {
                "po_date": payload.po_date,
                "distributor_id": payload.distributor_id,
                "distributor_name": payload.distributor_name,
                "invoice_ref": payload.invoice_ref,
                "notes": payload.notes,
                "items": [
                   {
                     **i.model_dump(),
                     "quantity": round_qty(i.quantity),
                     "free_quantity": round_qty(i.free_quantity),
                     "item_total": _money_float(_to_decimal(i.purchase_price) * _to_decimal(i.quantity)),
                     "medicine_key": f"{str(i.name).strip().lower()}::{str(i.batch_no).strip().upper()}",
                     "expiry_date": normalize_expiry(i.expiry_date),
                   }
                   for i in payload.items
                ],
                "total": po_totals["total"],

                "sub_total":
                  po_totals["sub_total"],

                "scheme_discount":
                  po_totals["scheme_discount"],

                "cash_discount":
                  po_totals["cash_discount"],

                "discount":
                  po_totals["discount"],

                "taxable_total":
                  po_totals["taxable_total"],

                "total_cgst":
                  po_totals["total_cgst"],

                "total_sgst":
                  po_totals["total_sgst"],

                "round_off":
                  po_totals["round_off"],

                "grand_total":
                  po_totals["grand_total"],

                "gst_breakup":
                  po_totals["gst_breakup"],
                **return_adjustment,
            }
        }
    )
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
    await rebuild_inventory()

    return {"message": "PO updated", **return_adjustment}

@api_router.get("/purchase-orders")
async def list_pos(user: dict = Depends(get_current_user)):
    return await db.purchase_orders.find({}, {"_id": 0}).sort("created_at", -1).to_list(2000)


@api_router.get("/purchase-orders/{pid}")
async def get_po(pid: str, user: dict = Depends(get_current_user)):
    po = await db.purchase_orders.find_one({"id": pid}, {"_id": 0})
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")
    return po


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
    # Reserved, backward-compatible structures for incremental settings features.
    "activity_logs": [],
    "role_permissions": {},
    "theme_settings": {},
    "backup_metadata": {},
}


def normalize_settings(settings: Optional[dict] = None) -> dict:
    """Add new settings defaults without changing or removing legacy fields."""
    return {**SETTINGS_DEFAULTS, **(settings or {})}


@api_router.get("/settings")
async def get_settings(user: dict = Depends(get_current_user)):
    s = await db.settings.find_one({"key": "main"}, {"_id": 0})
    return normalize_settings(s)


@api_router.put("/settings")
async def update_settings(payload: dict, user: dict = Depends(require_role("admin"))):
    payload["key"] = "main"
    await db.settings.update_one({"key": "main"}, {"$set": payload}, upsert=True)
    s = await db.settings.find_one({"key": "main"}, {"_id": 0})
    return normalize_settings(s)


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

def _daily_closing_money(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


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
    normalized = [_normalize_daily_sale(row) for row in daily]
    # Very old daily-sale rows had only total_amount; they represented paid cash
    # unless explicitly marked pending.
    for source, row in zip(daily, normalized):
        if not any(source.get(field) is not None for field in (
            "cash_sales", "upi_sales", "card_sales", "outstanding_sales"
        )):
            amount = _money(source.get("total_amount"))
            row["outstanding_sales" if source.get("payment_status") == "pending" else "cash_sales"] = amount
    splits = {
        "cash_sales": sum(row["cash_sales"] for row in normalized),
        "upi_sales": sum(row["upi_sales"] for row in normalized),
        "card_sales": sum(row["card_sales"] for row in normalized),
        "credit_sales": sum(row["outstanding_sales"] for row in normalized),
    }
    for invoice in invoices:
        mode = invoice.get("payment_mode", "cash")
        paid = _money(invoice.get("paid_amount"))
        due = _money(invoice.get("due_amount"))
        if mode in {"cash", "upi", "card"}:
            splits[f"{mode}_sales"] += paid
        else:
            splits["cash_sales"] += paid  # legacy/mixed invoices do not retain a payment split
        splits["credit_sales"] += due
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


async def rebuild_inventory():

    medicines = {}

    # PRESERVE EXISTING MANUAL VALUES
    existing_medicines = {}

    existing_cursor = db.medicines.find({})

    async for old in existing_cursor:

        key = old.get("medicine_key")

        if key:
            existing_medicines[key] = old

    # Invoice lines are the source of truth for future sales rebuilds. Legacy
    # batches without matching invoice history retain their existing sold value.
    invoice_sold_by_id = defaultdict(float)
    invoice_sold_by_key = defaultdict(float)
    invoices_collection = getattr(db, "invoices", None)
    if invoices_collection is not None:
        async for invoice in invoices_collection.find({}):
            for item in invoice.get("items", []):
                quantity = round_qty(item.get("quantity_units", item.get("quantity", 0)))
                if item.get("medicine_id"):
                    invoice_sold_by_id[item["medicine_id"]] += quantity
                if item.get("medicine_key"):
                    invoice_sold_by_key[item["medicine_key"]] += quantity

    # REBUILD FROM PO
    cursor = db.purchase_orders.find({})

    async for po in cursor:

        po_distributor_id = po.get("distributor_id")

        po_distributor_name = (
            po.get("distributor_name")
            or po.get("distributor")
        )

        po_distributor = (
            po.get("distributor")
            or po.get("distributor_name")
        )

        for i in po.get("items", []):

            key = i.get("medicine_key")

            if not key:
                continue

            qty = (
                round_qty(i.get("quantity", 0))
                +
                round_qty(i.get("free_quantity", 0))
            )

            existing = existing_medicines.get(key)

            if key not in medicines:

                medicines[key] = {

                    "medicine_key": key,

                    "name": i.get("name"),

                    "batch_no": i.get("batch_no"),

                    "expiry_date": i.get("expiry_date"),

                    "manufacturer": i.get("manufacturer"),

                    "category": i.get("category"),

                    "mrp": i.get("mrp"),

                    "purchase_price": i.get("purchase_price"),

                    "pack_size": i.get("pack_size"),

                    "gst_rate": i.get("gst_rate"),

                    "distributor_id":
                        po_distributor_id
                        or i.get("distributor_id")
                        or (existing.get("distributor_id") if existing else None),

                    "distributor_name":
                        po_distributor_name
                        or i.get("distributor_name")
                        or i.get("distributor")
                        or (existing.get("distributor_name") if existing else None)
                        or (existing.get("distributor") if existing else None),

                    "distributor":
                        po_distributor
                        or i.get("distributor")
                        or i.get("distributor_name")
                        or (existing.get("distributor") if existing else None)
                        or (existing.get("distributor_name") if existing else None),

                    # PURCHASE STOCK FROM PO
                    "purchased_units": 0,

                    # Prefer invoice-derived sales; preserve legacy/manual data
                    # only when no matching invoice history is available.
                    "sold_units":
                        round_qty(
                            invoice_sold_by_id.get(existing.get("id"))
                            if existing and existing.get("id") in invoice_sold_by_id
                            else invoice_sold_by_key.get(key)
                            if key in invoice_sold_by_key
                            else _safe_legacy_sold_stock(existing)
                            if existing else 0
                        ),

                    # PURCHASE RETURNS PRESERVED
                    "purchase_return_units":
                        _purchase_return_stock(existing)
                        if existing else 0,

                    # AUDITED STOCK ADJUSTMENTS PRESERVED
                    "stock_adjustment_units":
                        _stock_adjustment_stock(existing)
                        if existing else 0,

                    # MANUAL THRESHOLD PRESERVED
                    "low_stock_threshold":
                        existing.get("low_stock_threshold")
                        if existing else None,

                    "id":
                       existing.get("id")
                       if existing
                       else str(uuid.uuid4()),
                }

            medicines[key]["purchased_units"] = round_qty(medicines[key]["purchased_units"] + qty)

    # Upsert calculated batches rather than deleting the collection. This keeps
    # historical/orphaned and long-expired inventory records intact.
    for m in medicines.values():
        derivatives = {
            "available_stock": _available_stock(m),
            "quantity_units": _available_stock(m),
            "return_status": _return_status(m),
            "status": _return_status(m),
        }
        await db.medicines.update_one(
            {"medicine_key": m["medicine_key"]},
            {"$set": {**m, **derivatives}},
            upsert=True,
        )

    # Rebuild the derived return quantity from the source-of-truth return
    # collection, including legacy records created before this field existed.
    await recalculate_purchase_return_stock()

# ---------------- Mount ----------------


app.add_middleware(
    CORSMiddleware,
     allow_origins=[
         "https://pharmacy-pro-01-frontend.onrender.com"
     ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
