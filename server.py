from dotenv import load_dotenv
from pathlib import Path
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

import os
import uuid
import logging
import bcrypt
import jwt
import asyncio
import hashlib
import hmac
import secrets
import smtplib
import json
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
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.security import HTTPBearer
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError
from pydantic import BaseModel, Field, EmailStr, ConfigDict, field_validator, model_validator
from fastapi import UploadFile, File


# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
raw_db = client[os.environ['DB_NAME']]

REAL_TENANT_ID = os.environ.get("REAL_TENANT_ID", "real_shop")
DEMO_TENANT_ID = os.environ.get("DEMO_TENANT_ID", "demo_shop")
BUSINESS_COLLECTIONS = {
    "counters", "customer_transactions", "customers", "daily_sales",
    "daily_summary", "distributor_transactions", "distributors",
    "doctor_history", "expenses", "historical_sales", "invoices",
    "medicines", "purchase_orders", "purchase_returns",
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

    async def insert_one(self, document, *args, **kwargs):
        self._write_guard()
        return await self._collection.insert_one(self._owned(document), *args, **kwargs)

    async def insert_many(self, documents, *args, **kwargs):
        self._write_guard()
        return await self._collection.insert_many([self._owned(d) for d in documents], *args, **kwargs)

    def _owned_update(self, update):
        if _request_active.get() and isinstance(update, list):
            return [*update, {"$set": {"tenant_id": _current_tenant.get()}}]
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

EXPIRY_WARNING_DAYS = 90
PASSWORD_MAX_AGE_DAYS = 183
PASSWORD_RESET_ATTEMPTS = 5
PASSWORD_RESET_TTL_MINUTES = 10
FORGOT_PASSWORD_RATE_LIMIT = 5
FORGOT_PASSWORD_WINDOW_MINUTES = 15
SIGNUP_OTP_TTL_MINUTES = 10
SIGNUP_OTP_ATTEMPTS = 5
APP_VERSION = os.environ.get("APP_VERSION", "2.0.0")
APP_UPDATE_MESSAGE = os.environ.get("APP_UPDATE_MESSAGE", "Backend security, onboarding, analytics, and purchase-order adjustment update")
APP_RELEASE_NOTES = [
    "Isolated, restart-safe demo pharmacy tenant",
    "Verified email or mobile self-signup with pharmacy onboarding",
    "Tenant-safe report analytics APIs",
    "Purchase-return credit adjustments for purchase orders",
]


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
                user = await raw_db.users.find_one({"id": payload.get("sub")})
                if user:
                    _current_tenant.set(user.get("tenant_id"))
                    _current_demo.set(bool(user.get("is_demo")))
            except jwt.InvalidTokenError:
                pass
        if _current_demo.get() and request.method.upper() not in {"GET", "HEAD", "OPTIONS"} and request.url.path not in {"/api/auth/login", "/api/auth/logout"}:
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


# ---------------- Auth helpers ----------------
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def create_access_token(user_id: str, email: str, role: str) -> str:
    payload = {
        "sub": user_id, "email": email, "role": role,
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
        user = await raw_db.users.find_one({"id": payload["sub"]}, {"_id": 0, "password_hash": 0, "reset_otp_hash": 0})
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
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
    
class RegularPatient(BaseModel):
    name: str
    age: int
    phone: str
    address: Optional[str] = None

    medicine_name: str
    duration_days: int
    last_refill_date: str

    condition: str = ""

    @field_validator(
        "name", "phone", "medicine_name", "last_refill_date", "condition", mode="before"
    )
    @classmethod
    def trim_string_fields(cls, value):
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        return value

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
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class Customer(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    phone: str = ""
    email: str = ""
    gstin: str = ""
    address: str = ""
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
    payment_mode: Literal["cash", "upi", "card", "credit"] = "cash"
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
    
# ---------------- Startup ----------------
async def _backfill_tenant_data(now_iso: str) -> None:
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


async def _seed_demo_data(now_iso: str) -> None:
    demo_email = os.environ.get("DEMO_EMAIL", "demo@pharmacy.com").lower()
    demo_password = os.environ.get("DEMO_PASSWORD", "DemoAccess123")
    await raw_db.users.update_one(
        {"email": demo_email},
        {"$setOnInsert": {
            "id": "demo-user", "email": demo_email, "name": "Demo Pharmacist",
            "role": "admin", "created_at": now_iso,
        }, "$set": {
            "password_hash": hash_password(demo_password), "password_changed_at": now_iso,
            "tenant_id": DEMO_TENANT_ID, "shop_id": DEMO_TENANT_ID, "is_demo": True, "active": True,
        }},
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
        "daily_summary": [{"id": "demo-summary-1", "date": datetime.now(timezone.utc).date().isoformat(), "sales": 10, "expenses": 25, "created_at": now_iso}],
    }
    for collection_name, documents in demo_documents.items():
        collection = raw_db[collection_name]
        for document in documents:
            owned = {**document, "tenant_id": DEMO_TENANT_ID, "shop_id": DEMO_TENANT_ID}
            await collection.replace_one({"id": document["id"], "tenant_id": DEMO_TENANT_ID}, owned, upsert=True)


@app.on_event("startup")
async def startup():
    now_iso = datetime.now(timezone.utc).isoformat()
    await raw_db.users.create_index("email", unique=True)
    await raw_db.users.create_index("mobile", unique=True, sparse=True)
    await raw_db.password_reset_requests.create_index("created_at", expireAfterSeconds=FORGOT_PASSWORD_WINDOW_MINUTES * 60)
    await raw_db.pending_signups.create_index("expires_at", expireAfterSeconds=0)
    await _backfill_tenant_data(now_iso)
    for collection_name, indexes in {
        "medicines": ["name", "barcode"], "invoices": ["created_at"],
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
    admin_email = os.environ.get("ADMIN_EMAIL", "admin@pharmacy.com").lower()
    admin_password = os.environ.get("ADMIN_PASSWORD", "admin123")
    existing = await raw_db.users.find_one({"email": admin_email})
    if not existing:
        await raw_db.users.insert_one({
            "id": str(uuid.uuid4()), "email": admin_email, "password_hash": hash_password(admin_password),
            "name": "Administrator", "role": "admin", "tenant_id": REAL_TENANT_ID, "shop_id": REAL_TENANT_ID, "is_demo": False,
            "created_at": now_iso, "password_changed_at": now_iso,
        })
        logger.info("Seeded admin user: %s", admin_email)
    await _seed_demo_data(now_iso)


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
async def version():
    return {"version": APP_VERSION, "message": APP_UPDATE_MESSAGE, "release_notes": APP_RELEASE_NOTES}


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


@api_router.post("/auth/login")
async def login(payload: UserLogin, response: Response):
    identifier = str(payload.email or payload.mobile or payload.identifier or "").strip().lower()
    user = await raw_db.users.find_one({"$or": [{"email": identifier}, {"mobile": identifier}]})
    if not user or user.get("active", True) is False or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    token = create_access_token(user["id"], user.get("email", user.get("mobile", "")), user["role"])
    response.set_cookie("access_token", token, httponly=True, samesite="lax", secure=ENVIRONMENT in {"production", "prod"}, max_age=43200, path="/")
    return {
        "id": user["id"], "email": user.get("email"), "mobile": user.get("mobile"), "name": user["name"], "role": user["role"],
        "tenant_id": user["tenant_id"], "shop_id": user.get("shop_id", user["tenant_id"]), "is_demo": bool(user.get("is_demo")),
        "password_expired": _password_expired(user), "token": token,
    }


@api_router.post("/auth/logout")
async def logout(response: Response):
    response.delete_cookie("access_token", path="/")
    return {"ok": True}


@api_router.get("/auth/me")
async def me(user: dict = Depends(get_current_user)):
    return user


@api_router.get("/auth/users")
async def list_users(user: dict = Depends(require_role("admin"))):
    return await raw_db.users.find({"tenant_id": user["tenant_id"]}, {"_id": 0, "password_hash": 0, "reset_otp_hash": 0}).to_list(1000)


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
        q["name"] = {
            "$regex": search,
            "$options": "i"
        }

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

        purchased = int(
            m.get("purchased_units", 0)
        )

        sold = int(
            m.get("sold_units", 0)
        )

        qty = max(
            purchased - sold,
            0
        )

        m["quantity_units"] = qty

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
                    sold,

                "name":
                    m.get("name"),

                "manufacturer":
                    m.get("manufacturer"),

                "category":
                    m.get("category"),

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

            "purchased_units":
                purchased,

            "sold_units":
                sold,

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

    result.sort(
        key=lambda x: (
            x.get(sort_by)
            or ""
        )
    )

    return result
    
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

@api_router.put("/medicines/{medicine_id}/sold")
async def update_sold_units(
    medicine_id: str,
    payload: dict,
    user: dict = Depends(require_role("admin", "pharmacist"))
):

    medicine_filter = _medicine_identity_filter(medicine_id)

    result = await db.medicines.update_one(
        medicine_filter,
        {
            "$set": {
                "sold_units":
                    float(payload["sold_units"])
            }
        }
    )

    if result.matched_count == 0:
        raise HTTPException(404, "Medicine not found")

    updated = await db.medicines.find_one(medicine_filter)

    return {
        "message": "sold qty updated",
        "sold_units": updated.get("sold_units")
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
        purchased_units=data.get("purchased_units", 0),
    )

    await db.medicines.insert_one(med.model_dump())
    return med.model_dump()

@api_router.get("/medicines/lookup/{barcode}")
async def lookup_barcode(barcode: str, user: dict = Depends(get_current_user)):
    med = await db.medicines.find_one({"barcode": barcode}, {"_id": 0})
    if not med:
        raise HTTPException(status_code=404, detail="Not found")
    return med


@api_router.post("/patients")
async def add_patient(
    payload: RegularPatient,
    user: dict = Depends(get_current_user)
):
    data = payload.model_dump()
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

    result = await db.regular_patients.update_one(
        {"phone": phone},
        {
            "$set": payload.model_dump()
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

    res = await db.medicines.update_one(
        {"id": med_id},
        {"$set": data}
    )

    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail="Medicine not found")

    return await db.medicines.find_one({"id": med_id}, {"_id": 0})


@api_router.delete("/medicines/{med_id}")
async def delete_medicine(med_id: str, user: dict = Depends(require_role("admin"))):
    result = await db.medicines.delete_one(_medicine_identity_filter(med_id))

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Medicine not found")

    return {"ok": True}


# ---------------- Distributors ----------------
@api_router.get("/distributors")
async def list_distributors(user: dict = Depends(get_current_user)):
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
async def list_customers(user: dict = Depends(get_current_user)):
    return await db.customers.find({}, {"_id": 0}).sort("name", 1).to_list(1000)


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
    qty: int,
    session=None
):

    batches = await db.medicines.find(
        {
            "name": medicine_name
        },
        {
            "_id": 0
        },
        session=session
    ).sort(
        "expiry_date",
        1
    ).to_list(100)

    remaining = qty
    plan = []

    for batch in batches:

        purchased = int(
            batch.get(
                "purchased_units",
                0
            )
        )

        sold = int(
            batch.get(
                "sold_units",
                0
            )
        )

        available = purchased - sold

        if available <= 0:
            continue

        deduct = min(
            available,
            remaining
        )

        plan.append({
            "medicine_id": batch["id"],
            "medicine_name": medicine_name,
            "deduct": deduct,
        })

        remaining -= deduct

        if remaining <= 0:
            break

    if remaining > 0:

        raise HTTPException(
            status_code=400,
            detail=f"Insufficient stock for {medicine_name}"
        )

    return plan


async def _apply_fifo_stock_plan(
    plan: list[dict],
    session=None,
    applied=None
):
    applied_steps = []

    for step in plan:

        result = await db.medicines.update_one(
            {
                "id": step["medicine_id"],
                "$expr": {
                    "$gte": [
                        {
                            "$subtract": [
                                {"$ifNull": ["$purchased_units", 0]},
                                {"$ifNull": ["$sold_units", 0]},
                            ]
                        },
                        step["deduct"],
                    ]
                }
            },
            {
                "$inc": {
                    "sold_units": step["deduct"]
                }
            },
            session=session
        )

        if result.modified_count != 1:
            raise HTTPException(
                status_code=409,
                detail=f"Stock changed while processing {step['medicine_name']}. Please retry."
            )

        applied_steps.append(step)
        if applied is not None:
            applied.append(step)

    return applied_steps


async def _apply_fifo_stock_requests(
    stock_requests: dict[str, int],
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
        result = await db.medicines.update_one(
            {
                "id": step["medicine_id"],
                "$expr": {
                    "$gte": [
                        {"$ifNull": ["$sold_units", 0]},
                        step["deduct"],
                    ]
                }
            },
            {
                "$inc": {
                    "sold_units": -step["deduct"]
                }
            }
        )

        if result.modified_count != 1:
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
            "deduct": int(step.get("deduct", 0)),
        }
        for step in steps
        if int(step.get("deduct", 0)) > 0
    ]


def _stock_deductions_from_daily_sale(sale: dict) -> list[dict]:
    deductions = sale.get("stock_deductions") or []
    if deductions:
        return _stock_deductions_from_steps(deductions)

    return [
        {
            "medicine_id": sale["medicine_id"],
            "medicine_name": sale.get("medicine_name", ""),
            "deduct": int(
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
        deduct = int(step.get("deduct", 0))
        if deduct <= 0:
            continue

        result = await db.medicines.update_one(
            {
                "id": step["medicine_id"],
                "$expr": {
                    "$gte": [
                        {"$ifNull": ["$sold_units", 0]},
                        deduct,
                    ]
                }
            },
            {
                "$inc": {
                    "sold_units": -deduct
                }
            },
            session=session,
        )

        if result.modified_count != 1:
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
        deduct = int(step.get("deduct", 0))
        if deduct <= 0:
            continue

        await db.medicines.update_one(
            {
                "id": step["medicine_id"]
            },
            {
                "$inc": {
                    "sold_units": deduct
                }
            }
        )


async def reduce_fifo_stock(
    medicine_name: str,
    qty: int,
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
    stock_requests = defaultdict(int)

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

        units_needed = item.quantity * (
            upb if item.unit_type == "box" else 1
        )

        stock_requests[item.name] += units_needed

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
            "line_total": round(taxable, 2),
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
            "net_amount": round(net, 2),
        })

    total = round(
        subtotal + gst_total,
        2
    )

    paid = float(
        payload.paid_amount
        if payload.payment_mode != "cash"
        else (
            payload.paid_amount or total
        )
    )

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

        "paid_amount": paid,

        "due_amount": round(
            total - paid,
            2
        ),

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

    return await _run_with_transaction(
        transaction_operation,
        fallback_operation
    )
    
@api_router.get("/invoices")
async def list_invoices(
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
    invoices = await db.invoices.find(q, {"_id": 0}).sort("created_at", -1).to_list(2000)
    return invoices


@api_router.get("/invoices/{inv_id}")
async def get_invoice(inv_id: str, user: dict = Depends(get_current_user)):
    inv = await db.invoices.find_one({"id": inv_id}, {"_id": 0})
    if not inv:
        raise HTTPException(status_code=404, detail="Invoice not found")
    return inv


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
    return round(_safe_float(value), 2)


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
        }))

    return {
        "distributor": dist,
        "transactions": running,
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
    return float(medicine.get("purchased_units", 0) or 0) - float(medicine.get("sold_units", 0) or 0)


def _return_public(return_doc: dict) -> dict:
    return {key: value for key, value in return_doc.items() if key != "_id"}


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
        "amount": return_doc["return_amount"],
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


async def _deduct_purchase_return_stock(medicine_id: str, quantity: float, session=None):
    result = await db.medicines.update_one(
        {
            "id": medicine_id,
            "$expr": {
                "$gte": [
                    {
                        "$subtract": [
                            {"$ifNull": ["$purchased_units", 0]},
                            {"$ifNull": ["$sold_units", 0]},
                        ]
                    },
                    quantity,
                ]
            },
        },
        {
            "$inc": {
                "sold_units": quantity,
            }
        },
        session=session,
    )

    if result.modified_count != 1:
        raise HTTPException(
            status_code=409,
            detail="Return quantity exceeds available stock for this batch",
        )


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

    return_amount = round(payload.return_quantity * payload.purchase_rate, 2)
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
        "purchase_rate": payload.purchase_rate,
        "return_amount": return_amount,
        "reason": payload.reason,
        "notes": payload.notes,
        "adjust_distributor_ledger": payload.adjust_distributor_ledger,
        "ledger_adjusted": payload.adjust_distributor_ledger,
        "ledger_transaction_id": None,
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
                await db.medicines.update_one(
                    {"id": medicine.get("id")},
                    {"$inc": {"sold_units": -payload.return_quantity}},
                )
            raise

        return _return_public(purchase_return)

    return await _run_with_transaction(
        transaction_operation,
        fallback_operation,
    )


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

    return {
        "items": items,
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

    def add_summary(bucket: dict, key: str, quantity: float, value: float):
        bucket[key]["quantity"] += quantity
        bucket[key]["value"] += value
        bucket[key]["count"] += 1

    for item in returns:
        quantity = float(item.get("return_quantity", 0) or 0)
        value = float(item.get("return_amount", 0) or 0)
        total_quantity += quantity
        total_value += value

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
        "total_returned_quantity": round(total_quantity, 2),
        "total_return_value": round(total_value, 2),
        "return_count": len(returns),
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
async def customer_ledger(cid: str, user: dict = Depends(get_current_user)):
    cust = await db.customers.find_one({"id": cid}, {"_id": 0})
    if not cust:
        raise HTTPException(status_code=404, detail="Customer not found")
    txns = await db.customer_transactions.find({"customer_id": cid}, {"_id": 0}).sort("created_at", 1).to_list(1000)
    balance = 0.0
    running = []
    for t in txns:
        if t["type"] == "sale":
            balance += t["amount"]
        else:
            balance -= t["amount"]
        running.append({**t, "running_balance": round(balance, 2)})
    return {"customer": cust, "transactions": running, "balance": round(balance, 2)}


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
        "amount": p.amount,
        "mode": p.mode,
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
        "amount": p.amount,
        "mode": p.mode,
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

    for m in medicines:

        purchased = int(m.get("purchased_units", 0))
        sold = int(m.get("sold_units", 0))

        available = max(0, purchased - sold)

        stock_value += available * float(m.get("purchase_price", 0))

        threshold = m.get("low_stock_threshold")

        if (
           threshold is not None
           and
           available <= threshold
        ):
            low_stock_items.append({
                "id": m.get("id"),
                "name": m.get("name"),
                "qty": available,
                "threshold": threshold,
            })

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

        if expiry_info["expiry_status"] == "expired":
            expired_items.append(expiry_item)
        elif expiry_info["expiry_status"] == "warning":
            expiring_soon_items.append(expiry_item)

    # CUSTOMER OUTSTANDING
    customer_txns = await db.customer_transactions.find({}, {"_id": 0}).to_list(5000)

    customer_outstanding = 0

    for t in customer_txns:

        if t.get("type") == "sale":
            amt = float(t.get("amount", 0))
            customer_outstanding += amt

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
            customer_outstanding -= amt
            received_total += amt

            try:
                dt = datetime.fromisoformat(t["created_at"]).date()

                if dt == today:
                    received_today += amt

                if dt.month == current_month and dt.year == current_year:
                    received_month += amt

            except Exception:
                pass

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
        "customer_outstanding_month": round(customer_outstanding_month, 2),
        "customer_outstanding_today": round(customer_outstanding_today, 2),

        "distributor_outstanding": round(distributor_outstanding, 2),

        "amount_received": round(received_total, 2),
        "amount_received_month": round(received_month, 2),
        "amount_received_today": round(received_today, 2),

        "low_stock_count": len(low_stock_items),
        "low_stock_items": low_stock_items,

        "expiring_soon_count": len(expiring_soon_items),
        "expiring_soon_items": expiring_soon_items,
        "expiring_soon": expiring_soon_items,

        "expired_count": len(expired_items),
        "expired_items": expired_items,

        "patient_alert_count": len(patient_alerts),
        "patient_alerts": patient_alerts,
        "patient_due_alerts_count": len(patient_alerts),
        "patient_due_alerts": patient_alerts,
    }
    
@api_router.get("/reports/sales")
async def sales_report(
    start: Optional[str] = None,
    end: Optional[str] = None,
    user: dict = Depends(get_current_user)
):
    q = {}

    if start or end:
        q["created_at"] = {}

        if start:
            q["created_at"]["$gte"] = start

        if end:
            q["created_at"]["$lte"] = end + "T23:59:59"

    invoices = await db.invoices.find(q, {"_id": 0}).to_list(5000)

    total_sales = 0.0
    total_gst = 0.0
    total_discount = 0.0

    daily = {}

    # SALES + DAILY
    for inv in invoices:
        total_sales += float(inv.get("total", 0))
        total_gst += float(inv.get("gst_total", 0))
        total_discount += float(inv.get("bill_discount", 0))

        day = inv.get("created_at", "")[:10]
        if day:
            daily[day] = daily.get(day, 0) + float(inv.get("total", 0))

    # PROFIT (safe + async optimized)
    profit = 0.0

    for inv in invoices:
        for it in inv.get("items", []):
            med = await db.medicines.find_one(
                {"id": it.get("medicine_id")},
                {"purchase_price": 1}
            )

            if med:
                profit += (
                    float(it.get("mrp", 0)) -
                    float(med.get("purchase_price", 0))
                ) * float(it.get("quantity", 0))

    # Convert daily → frontend-friendly array
    daily_list = [
        {"date": k, "total": v}
        for k, v in sorted(daily.items())
    ]

    return {
        "total_sales": round(total_sales, 2),
        "total_gst": round(total_gst, 2),
        "total_discount": round(total_discount, 2),
        "estimated_profit": round(profit, 2),

        # IMPORTANT: for your Recharts line chart
        "daily": daily_list,

        "invoice_count": len(invoices)
    }

@api_router.get("/reports/stock-valuation")
async def stock_valuation(
    user: dict = Depends(get_current_user)
):
    medicines = await db.medicines.find(
        {},
        {"_id": 0}
    ).to_list(5000)

    cost_value = 0
    mrp_value = 0
    total_units = 0

    for m in medicines:

        available = (
            int(m.get("purchased_units", 0))
            - int(m.get("sold_units", 0))
        )

        total_units += available

        cost_value += (
            available *
            float(m.get("purchase_price", 0))
        )

        mrp_value += (
            available *
            float(m.get("mrp", 0))
        )

    return {
        "total_items": len(medicines),

        "total_units": total_units,

        "cost_value": round(cost_value, 2),

        "mrp_value": round(mrp_value, 2),

        "potential_profit": round(
            mrp_value - cost_value,
            2
        ),
    }


@api_router.get("/reports/outstanding")
async def outstanding_report(
    user: dict = Depends(get_current_user)
):
    # ---------------- CUSTOMER OUTSTANDING ----------------

    customers = await db.customers.find(
        {},
        {"_id": 0}
    ).to_list(1000)

    cust_out = []
    customer_total = 0

    for c in customers:
        txns = await db.customer_transactions.find(
            {"customer_id": c["id"]}
        ).to_list(1000)

        bal = 0.0

        for t in txns:
            if t.get("type") == "sale":
                bal += float(t.get("amount", 0))

            elif t.get("type") == "payment":
                bal -= float(t.get("amount", 0))

        if bal > 0:
            bal = round(bal, 2)

            customer_total += bal

            cust_out.append({
                "id": c["id"],
                "name": c["name"],
                "phone": c.get("phone", ""),
                "balance": bal,
            })

    # ---------------- DISTRIBUTOR OUTSTANDING ----------------

    distributors = await db.distributors.find(
        {},
        {"_id": 0}
    ).to_list(1000)

    dist_out = []
    distributor_total = 0

    for d in distributors:
        txns = await db.distributor_transactions.find(
            {"distributor_id": d["id"]}
        ).to_list(1000)

        bal = _current_distributor_balance(d, txns)

        if bal > 0:
            bal = round(bal, 2)

            distributor_total += bal

            dist_out.append({
                "id": d["id"],
                "name": d["name"],
                "balance": bal,
            })

    return {
        "customers": cust_out,
        "distributors": dist_out,

        "customer_total": round(customer_total, 2),

        "distributor_total": round(distributor_total, 2),
    }


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
        details = expiry_details(
            m.get("expiry_date"),
            today
        )

        item = {
            **m,
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
    purchase_orders = await db.purchase_orders.find({"created_at": {"$gte": cutoff_iso}}, {"_id": 0}).to_list(10000)
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(10000)
    customer_txns = await db.customer_transactions.find({"created_at": {"$gte": cutoff_iso}}, {"_id": 0}).to_list(10000)

    sales_by_month = defaultdict(float)
    purchases_by_month = defaultdict(float)
    payment_modes = defaultdict(float)
    medicine_sales = defaultdict(lambda: {"quantity": 0.0, "revenue": 0.0})
    recovery_by_month = defaultdict(lambda: {"charged": 0.0, "recovered": 0.0})

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
        if month:
            purchases_by_month[month] += float(po.get("final_payable_total", po.get("grand_total", 0)) or 0)

    for txn in customer_txns:
        month = _month_key(txn.get("created_at"))
        if not month:
            continue
        if txn.get("type") == "sale":
            recovery_by_month[month]["charged"] += float(txn.get("amount", 0) or 0)
        elif txn.get("type") == "payment":
            recovery_by_month[month]["recovered"] += float(txn.get("amount", 0) or 0)

    expiry_buckets = {"expired": {"count": 0, "units": 0.0, "cost_value": 0.0}, "within_30_days": {"count": 0, "units": 0.0, "cost_value": 0.0}, "within_90_days": {"count": 0, "units": 0.0, "cost_value": 0.0}, "safe": {"count": 0, "units": 0.0, "cost_value": 0.0}}
    today = datetime.now(timezone.utc).date()
    for medicine in medicines:
        available = max(0.0, float(medicine.get("purchased_units", 0) or 0) - float(medicine.get("sold_units", 0) or 0))
        expiry = parse_expiry_date(medicine.get("expiry_date"))
        days = (expiry - today).days if expiry else 999999
        bucket = "expired" if days < 0 else "within_30_days" if days <= 30 else "within_90_days" if days <= 90 else "safe"
        expiry_buckets[bucket]["count"] += 1
        expiry_buckets[bucket]["units"] += available
        expiry_buckets[bucket]["cost_value"] += available * float(medicine.get("purchase_price", 0) or 0)

    month_keys = sorted(set(sales_by_month) | set(purchases_by_month) | set(recovery_by_month))
    return {
        "monthly_sales_trend": [{"month": month, "sales": round(sales_by_month[month], 2)} for month in month_keys],
        "top_selling_medicines": [{"medicine_name": name, "quantity": round(values["quantity"], 2), "revenue": round(values["revenue"], 2)} for name, values in sorted(medicine_sales.items(), key=lambda item: item[1]["quantity"], reverse=True)[:limit]],
        "expiry_risk": [{"bucket": bucket, "count": values["count"], "units": round(values["units"], 2), "cost_value": round(values["cost_value"], 2)} for bucket, values in expiry_buckets.items()],
        "payment_mode_distribution": [{"mode": mode, "amount": round(amount, 2)} for mode, amount in sorted(payment_modes.items())],
        "purchase_vs_sales": [{"month": month, "purchases": round(purchases_by_month[month], 2), "sales": round(sales_by_month[month], 2)} for month in month_keys],
        "outstanding_recovery_trends": [{"month": month, "charged": round(recovery_by_month[month]["charged"], 2), "recovered": round(recovery_by_month[month]["recovered"], 2), "net_outstanding_change": round(recovery_by_month[month]["charged"] - recovery_by_month[month]["recovered"], 2)} for month in month_keys],
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
        line_subtotal = _to_decimal(item.purchase_price) * _to_decimal(item.quantity)
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


async def _resolve_po_purchase_returns(payload: POCreate, allow_po_id: Optional[str] = None) -> tuple[list[dict], float]:
    return_ids = list(dict.fromkeys(payload.purchase_return_ids))
    if not return_ids:
        return [], 0.0
    returns = await db.purchase_returns.find({"id": {"$in": return_ids}}, {"_id": 0}).to_list(len(return_ids))
    if len(returns) != len(return_ids):
        raise HTTPException(status_code=400, detail="One or more selected purchase returns were not found")
    for item in returns:
        if item.get("distributor_id") and item.get("distributor_id") != payload.distributor_id:
            raise HTTPException(status_code=400, detail="Purchase return distributor does not match purchase order distributor")
        assigned_po = item.get("po_adjustment_id")
        if assigned_po and assigned_po != allow_po_id:
            raise HTTPException(status_code=409, detail="Purchase return credit is already assigned to another purchase order")
    credit = round(sum(float(item.get("return_amount", 0) or 0) for item in returns), 2)
    return returns, credit


def _apply_po_return_credit(po_totals: dict, returns: list[dict], credit: float) -> dict:
    return {
        "purchase_return_ids": [item["id"] for item in returns],
        "purchase_return_adjustment": credit,
        "purchase_return_details": [{"id": item["id"], "medicine_name": item.get("medicine_name"), "batch_number": item.get("batch_number"), "return_amount": round(float(item.get("return_amount", 0) or 0), 2)} for item in returns],
        "subtotal_after_purchase_return": round(max(0.0, float(po_totals.get("sub_total", po_totals["grand_total"])) - credit), 2),
        "final_payable_total": round(max(0.0, float(po_totals["grand_total"]) - credit), 2),
    }


@api_router.get("/purchase-orders/eligible-purchase-returns/{distributor_id}")
async def eligible_po_purchase_returns(distributor_id: str, user: dict = Depends(get_current_user)):
    returns = await db.purchase_returns.find({"distributor_id": distributor_id, "$or": [{"po_adjustment_id": {"$exists": False}}, {"po_adjustment_id": None}]}, {"_id": 0}).sort("return_date", -1).to_list(1000)
    return [{"id": item["id"], "medicine_id": item.get("medicine_id"), "medicine_name": item.get("medicine_name"), "batch_number": item.get("batch_number"), "return_quantity": item.get("return_quantity"), "return_amount": item.get("return_amount"), "return_date": item.get("return_date")} for item in returns]


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
        await db.purchase_returns.update_many({"id": {"$in": return_adjustment["purchase_return_ids"]}}, {"$set": {"po_adjustment_id": po["id"], "po_adjusted_at": datetime.now(timezone.utc).isoformat()}})

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

        qty = (
            float(i.get("quantity", 0)) +
            float(i.get("free_quantity", 0))
        )

        medicine = await db.medicines.find_one({
            "name": i.get("name"),
            "batch_no": i.get("batch_no")
        })

        if medicine:

            await db.medicines.update_one(
                {"_id": medicine["_id"]},
                {
                    "$inc": {
                        "purchased_units": -qty
                    }
                }
            )

    if po.get("purchase_return_ids"):
        await db.purchase_returns.update_many({"id": {"$in": po["purchase_return_ids"]}, "po_adjustment_id": po_id}, {"$unset": {"po_adjustment_id": "", "po_adjusted_at": ""}})

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
        await db.purchase_returns.update_many({"id": {"$in": list(old_return_ids - new_return_ids)}, "po_adjustment_id": po_id}, {"$unset": {"po_adjustment_id": "", "po_adjusted_at": ""}})
    if new_return_ids:
        await db.purchase_returns.update_many({"id": {"$in": list(new_return_ids)}}, {"$set": {"po_adjustment_id": po_id, "po_adjusted_at": datetime.now(timezone.utc).isoformat()}})
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


# ---------------- Settings (signature, business info) ----------------
@api_router.get("/settings")
async def get_settings(user: dict = Depends(get_current_user)):
    s = await db.settings.find_one({"key": "main"}, {"_id": 0})
    if not s:
        s = {
            "key": "main",
            "business_name": "MedStock Pharmacy",
            "business_address": "",
            "business_phone": "",
            "business_gstin": "",
            "signature_b64": "",
        }
    return s


@api_router.put("/settings")
async def update_settings(payload: dict, user: dict = Depends(require_role("admin"))):
    payload["key"] = "main"
    await db.settings.update_one({"key": "main"}, {"$set": payload}, upsert=True)
    s = await db.settings.find_one({"key": "main"}, {"_id": 0})
    return s


# ---------------- Daily Sales Book (quick non-invoice entries) ----------------
class DailySaleCreate(BaseModel):
    medicine_id: str
    quantity: int
    unit_type: Literal["unit", "box"] = "unit"
    total_amount: float
    customer_name: str = ""
    payment_status: Literal["paid", "pending"] = "paid"
    notes: str = ""
    sale_date: Optional[str] = None  # YYYY-MM-DD; defaults to today

@api_router.post("/daily-sales")
async def create_daily_sale(
    payload: DailySaleCreate,
    user: dict = Depends(get_current_user)
):

    med = await db.medicines.find_one({
        "id": payload.medicine_id
    })

    if not med:
        raise HTTPException(
            status_code=400,
            detail="Medicine not found"
        )

    upb = max(
        int(med.get("units_per_box") or 1),
        1
    )

    units_needed = payload.quantity * (
        upb if payload.unit_type == "box" else 1
    )

    stock_requests = {
        med["name"]: units_needed
    }

    sale_date = (
        payload.sale_date
        or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )

    entry = {
        "id": str(uuid.uuid4()),

        "medicine_id": payload.medicine_id,
        "medicine_name": med["name"],

        "batch_no": med.get("batch_no", ""),

        "quantity": payload.quantity,

        "unit_type": payload.unit_type,

        "units_dispensed": units_needed,

        "total_amount": float(payload.total_amount),

        "customer_name": payload.customer_name or "Walk-in",

        "payment_status": payload.payment_status,

        "notes": payload.notes,

        "sale_date": sale_date,

        "created_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "created_by": user.get("name", ""),
    }

    async def write_daily_sale(session=None):
        await db.daily_sales.insert_one(
            entry,
            session=session
        )

    async def transaction_operation(session):
        applied_steps = await _apply_fifo_stock_requests(
            stock_requests,
            session=session
        )
        entry["stock_deductions"] = _stock_deductions_from_steps(
            applied_steps
        )
        await write_daily_sale(session=session)
        return entry

    async def fallback_operation():
        applied = []

        try:
            await _apply_fifo_stock_requests(
                stock_requests,
                applied=applied
            )
            entry["stock_deductions"] = _stock_deductions_from_steps(
                applied
            )
            await write_daily_sale()
        except Exception:
            if applied:
                await _restore_fifo_stock(applied)
            raise

        return entry

    await _run_with_transaction(
        transaction_operation,
        fallback_operation
    )

    return {
        key: value
        for key, value in entry.items()
        if key not in {"_id", "stock_deductions"}
    }

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
    return items


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

    live_total = sum(
        i.get("total_amount", 0)
        for i in items
    )

    live_paid = sum(
        i.get("total_amount", 0)
        for i in items
        if i.get("payment_status") == "paid"
    )

    live_pending = sum(
        i.get("total_amount", 0)
        for i in items
        if i.get("payment_status") == "pending"
    )

    historical_total = sum(
        h.get("total_amount", 0)
        for h in historical
    )

    historical_paid = sum(
        h.get("cash_amount", 0)
        + h.get("upi_amount", 0)
        for h in historical
    )

    historical_pending = sum(
        h.get("pending_amount", 0)
        for h in historical
    )

    total = live_total + historical_total
    paid = live_paid + historical_paid
    pending = live_pending + historical_pending

    return {
        "date": target,
        "count": len(items) + len(historical),
        "total": round(total, 2),
        "paid": round(paid, 2),
        "pending": round(pending, 2),
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

    stock_deductions = _stock_deductions_from_daily_sale(sale)

    async def transaction_operation(session):
        await _restore_daily_sale_stock(
            stock_deductions,
            session=session
        )
        result = await db.daily_sales.delete_one(
            {
                "id": sale_id
            },
            session=session
        )

        if result.deleted_count != 1:
            raise HTTPException(
                status_code=404,
                detail="Entry not found"
            )

        return {"ok": True}

    async def fallback_operation():
        restored = []

        try:
            await _restore_daily_sale_stock(
                stock_deductions,
                restored=restored
            )
            result = await db.daily_sales.delete_one({
                "id": sale_id
            })

            if result.deleted_count != 1:
                raise HTTPException(
                    status_code=404,
                    detail="Entry not found"
                )
        except Exception:
            if restored:
                await _reapply_daily_sale_stock(restored)
            raise

        return {"ok": True}

    return await _run_with_transaction(
        transaction_operation,
        fallback_operation
    )


async def rebuild_inventory():

    medicines = {}

    # PRESERVE EXISTING MANUAL VALUES
    existing_medicines = {}

    existing_cursor = db.medicines.find({})

    async for old in existing_cursor:

        key = old.get("medicine_key")

        if key:
            existing_medicines[key] = old

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
                float(i.get("quantity", 0))
                +
                float(i.get("free_quantity", 0))
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

                    # MANUAL SOLD QTY PRESERVED
                    "sold_units":
                        existing.get("sold_units", 0)
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

            medicines[key]["purchased_units"] += qty

    # FULL REBUILD
    await db.medicines.delete_many({})

    for m in medicines.values():

        await db.medicines.insert_one(m)

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
