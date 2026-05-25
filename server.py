from dotenv import load_dotenv
from pathlib import Path
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

import os
import uuid
import logging
import bcrypt
import jwt
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Literal
from collections import defaultdict

from fastapi import FastAPI, APIRouter, HTTPException, Depends, Request, Response
from fastapi.security import HTTPBearer
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field, EmailStr
from fastapi import UploadFile, File
from PIL import Image
import pytesseract
import io
import pandas as pd


# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

app = FastAPI(title="Pharmacy Management API")

@app.get("/")
def home():
    return {"message": "Pharmacy backend is running"}
api_router = APIRouter(prefix="/api")

JWT_ALGORITHM = "HS256"
JWT_SECRET = os.environ.get("JWT_SECRET", "change-me")

logger = logging.getLogger("pharmacy")
logging.basicConfig(level=logging.INFO)


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
        user = await db.users.find_one({"id": payload["sub"]}, {"_id": 0, "password_hash": 0})
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
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
    role: Literal["admin", "cashier", "pharmacist"] = "cashier"


class UserLogin(BaseModel):
    email: EmailStr
    password: str


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

    condition: str = "general" 


class Distributor(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    phone: str = ""
    email: str = ""
    address: str = ""
    gstin: str = ""
    opening_balance: float = 0.0
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
    date: str| None = None


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
    
# ---------------- Startup ----------------
@app.on_event("startup")
async def startup():
    await db.users.create_index("email", unique=True)
    await db.medicines.create_index("name")
    await db.medicines.create_index("barcode")
    await db.invoices.create_index("created_at")
    # Seed admin
    admin_email = os.environ.get("ADMIN_EMAIL", "admin@pharmacy.com")
    admin_password = os.environ.get("ADMIN_PASSWORD", "admin123")
    existing = await db.users.find_one({"email": admin_email})
    if not existing:
        await db.users.insert_one({
            "id": str(uuid.uuid4()),
            "email": admin_email,
            "password_hash": hash_password(admin_password),
            "name": "Administrator",
            "role": "admin",
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.info(f"Seeded admin user: {admin_email}")


@app.on_event("shutdown")
async def shutdown():
    client.close()


# ---------------- Auth routes ----------------
@api_router.post("/auth/register")
async def register(payload: UserRegister):
    email = payload.email.lower()
    existing = await db.users.find_one({"email": email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    user = {
        "id": str(uuid.uuid4()),
        "email": email,
        "password_hash": hash_password(payload.password),
        "name": payload.name,
        "role": payload.role,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.users.insert_one(user)
    return {"id": user["id"], "email": user["email"], "name": user["name"], "role": user["role"]}


@api_router.post("/auth/login")
async def login(payload: UserLogin, response: Response):
    email = payload.email.lower()
    user = await db.users.find_one({"email": email})
    if not user or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    token = create_access_token(user["id"], user["email"], user["role"])
    response.set_cookie("access_token", token, httponly=True, samesite="lax", max_age=43200, path="/")
    return {"id": user["id"], "email": user["email"], "name": user["name"], "role": user["role"], "token": token}


@api_router.post("/auth/logout")
async def logout(response: Response):
    response.delete_cookie("access_token", path="/")
    return {"ok": True}


@api_router.get("/auth/me")
async def me(user: dict = Depends(get_current_user)):
    return user


@api_router.get("/auth/users")
async def list_users(user: dict = Depends(require_role("admin"))):
    users = await db.users.find({}, {"_id": 0, "password_hash": 0}).to_list(1000)
    return users


# ---------------- Medicines ----------------
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
    q = {}

    if search:
        q["name"] = {"$regex": search, "$options": "i"}
    if category:
        q["category"] = category
    if distributor:
        q["distributor"] = distributor
    if manufacturer:
        q["manufacturer"] = manufacturer
    if batch_no:
        q["batch_no"] = batch_no

    sort_field = sort_by if sort_by in ("name", "expiry_date", "mrp") else "name"

    items = await db.medicines.find(q, {"_id": 0}).sort(sort_field, 1).to_list(5000)

    # 🔥 IMPORTANT: compute stock properly here (temporary until invoice system)
    for m in items:
        purchased = int(m.get("purchased_units", 0))
        sold = int(m.get("sold_units", 0))

        m["quantity_units"] = max(purchased - sold, 0)

    return items


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


@api_router.get("/reports/monthly-summary")
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
async def add_patient(payload: RegularPatient):
    data = payload.model_dump()
    await db.regular_patients.insert_one(data)
    return {"success": True}


@api_router.get("/patients")
async def list_patients():
    items = await db.regular_patients.find({}, {"_id": 0}).to_list(2000)
    return items


@api_router.get("/patients/alerts")
async def patient_alerts():
    from datetime import datetime

    today = datetime.now().date()
    patients = await db.regular_patients.find({}, {"_id": 0}).to_list(2000)

    alerts = []

    for p in patients:
        try:
            last = datetime.fromisoformat(p["last_refill_date"]).date()
            days = int(p.get("duration_days") or 0)

            if (today - last).days >= days:
                alerts.append(p)
        except:
            continue

    return alerts


@api_router.delete("/patients/{phone}")
async def delete_patient(phone: str):
    await db.regular_patients.delete_one({"phone": phone})
    return {"success": True}


from datetime import datetime, timezone

@api_router.post("/patients/contacted/{phone}")
async def mark_contacted(phone: str):
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
    await db.medicines.delete_one({"id": med_id})
    return {"ok": True}


# ---------------- Distributors ----------------
@api_router.get("/distributors")
async def list_distributors(user: dict = Depends(get_current_user)):
    return await db.distributors.find({}, {"_id": 0}).sort("name", 1).to_list(1000)


@api_router.post("/distributors")
async def create_distributor(d: Distributor, user: dict = Depends(require_role("admin", "pharmacist"))):
    await db.distributors.insert_one(d.model_dump())
    return d.model_dump()


@api_router.put("/distributors/{did}")
async def update_distributor(did: str, d: Distributor, user: dict = Depends(require_role("admin", "pharmacist"))):
    data = d.model_dump()
    data["id"] = did
    await db.distributors.update_one({"id": did}, {"$set": data})
    return data


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
async def _next_invoice_no() -> str:
    today = datetime.now(timezone.utc).strftime("%y%m%d")
    count = await db.invoices.count_documents({"invoice_no": {"$regex": f"^INV-{today}-"}})
    return f"INV-{today}-{count + 1:04d}"


@api_router.post("/invoices")
async def create_invoice(payload: InvoiceCreate, user: dict = Depends(get_current_user)):
    subtotal = 0.0
    gst_total = 0.0
    items_out = []
    line_total_raw = 0.0

    for item in payload.items:
        med = await db.medicines.find_one({"id": item.medicine_id})

        if not med:
            raise HTTPException(
                status_code=400,
                detail=f"Medicine not found: {item.name}"
            )

        upb = max(
            int(med.get("units_per_box") or item.units_per_box or 1),
            1
        )

        units_needed = item.quantity * (
            upb if item.unit_type == "box" else 1
        )

        # FIXED STOCK SYSTEM
        available = (
            int(med.get("purchased_units", 0))
            - int(med.get("sold_units", 0))
        )

        if available < units_needed:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient stock for {item.name}"
            )

        # INCREASE SOLD UNITS
        await db.medicines.update_one(
            {"id": item.medicine_id},
            {
                "$inc": {
                    "sold_units": units_needed
                }
            }
        )

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
            (float(payload.bill_discount_pct) / 100.0)
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
                (1 + it["gst_rate"] / 100.0)
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
        else (payload.paid_amount or total)
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
            if payload.referring_doctor else ""
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

        "created_by": user.get("name", ""),
    }

    await db.invoices.insert_one(invoice)

    if invoice["referring_doctor"]:
        await db.doctor_history.update_one(
            {"name": invoice["referring_doctor"]},
            {
                "$inc": {"count": 1},
                "$set": {
                    "last_used": invoice["created_at"]
                }
            },
            upsert=True,
        )

    if payload.customer_id and invoice["due_amount"] > 0:
        await db.customer_transactions.insert_one({
            "id": str(uuid.uuid4()),
            "customer_id": payload.customer_id,
            "type": "sale",
            "amount": invoice["due_amount"],
            "reference": invoice["invoice_no"],
            "notes": "Credit sale",
            "created_at": invoice["created_at"],
        })

    return invoice
    
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
@api_router.get("/ledger/distributor/{did}")
async def distributor_ledger(did: str, user: dict = Depends(get_current_user)):
    dist = await db.distributors.find_one({"id": did}, {"_id": 0})
    if not dist:
        raise HTTPException(status_code=404, detail="Distributor not found")
    txns = await db.distributor_transactions.find({"distributor_id": did}, {"_id": 0}).sort("created_at", 1).to_list(1000)
    balance = dist.get("opening_balance", 0.0)
    running = []
    for t in txns:
        if t["type"] == "purchase":
            balance += t["amount"]
        else:  # payment
            balance -= t["amount"]
        running.append({**t, "running_balance": round(balance, 2)})
    return {"distributor": dist, "transactions": running, "balance": round(balance, 2)}


@api_router.delete("/ledger/distributor/{did}/transaction/{txn_id}")
async def delete_distributor_txn(
    did: str,
    txn_id: str,
    user: dict = Depends(require_role("admin", "pharmacist"))
):
    result = await db.distributor_transactions.delete_one({
        "id": txn_id,
        "distributor_id": did
    })

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Transaction not found")

    return {"ok": True}

    
@api_router.post("/ledger/distributor/{did}/purchase")
async def add_purchase(did: str, p: PaymentCreate, user: dict = Depends(require_role("admin", "pharmacist"))):
    txn = {
        "id": str(uuid.uuid4()),
        "distributor_id": did,
        "type": "purchase",
        "amount": p.amount,
        "mode": p.mode,
        "notes": p.notes,
        "created_at": p.date or datetime.now(timezone.utc).isoformat(),
    }
    await db.distributor_transactions.insert_one(txn)
    txn.pop("_id", None)
    return txn


@api_router.post("/ledger/distributor/{did}/payment")
async def add_dist_payment(did: str, p: PaymentCreate, user: dict = Depends(require_role("admin", "pharmacist"))):
    txn = {
        "id": str(uuid.uuid4()),
        "distributor_id": did,
        "type": "payment",
        "amount": p.amount,
        "mode": p.mode,
        "notes": p.notes,
        "created_at": p.date or datetime.now(timezone.utc).isoformat(),
    }
    await db.distributor_transactions.insert_one(txn)
    txn.pop("_id", None)
    return txn


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

    today = datetime.now(
        timezone.utc
    ).date()

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

    invoices = await db.invoices.find(
        q,
        {"_id": 0}
    ).to_list(5000)

    total_sales = 0

    for i in invoices:

        amt = float(
            i.get("total", 0)
        )

        total_sales += amt

        try:

            dt = datetime.fromisoformat(
                i["created_at"]
            ).date()

            if dt == today:
                sales_today += amt

            if (
                dt.month == current_month and
                dt.year == current_year
            ):
                sales_month += amt

        except Exception:
            pass

    total_gst = sum(
        i.get("gst_total", 0)
        for i in invoices
    )

    total_discount = sum(
        i.get("bill_discount", 0)
        for i in invoices
    )

    # EXPENSES

    expenses = await db.expenses.find(
        q,
        {"_id": 0}
    ).to_list(5000)

    total_expenses = 0

    for e in expenses:

        amt = float(
            e.get("amount", 0)
        )

        total_expenses += amt

        try:

            dt = datetime.fromisoformat(
                e["created_at"]
            ).date()

            if dt == today:
                expenses_today += amt

            if (
                dt.month == current_month and
                dt.year == current_year
            ):
                expenses_month += amt

        except Exception:
            pass

    profit = total_sales - total_expenses
    profit_today = sales_today - expenses_today
    profit_month = sales_month - expenses_month

    # STOCK

    medicines = await db.medicines.find(
        {},
        {"_id": 0}
    ).to_list(5000)

    stock_value = 0
    low_stock_items = []
    expiring = []

    for m in medicines:

        purchased = int(
            m.get("purchased_units", 0)
        )

        sold = int(
            m.get("sold_units", 0)
        )

        available = purchased - sold

        stock_value += (
            available *
            float(m.get("purchase_price", 0))
        )

        if available <= int(
            m.get("low_stock_threshold", 10)
        ):

            low_stock_items.append({
                "id": m["id"],
                "name": m["name"],
                "qty": available,
                "threshold": m.get(
                    "low_stock_threshold",
                    10
                ),
            })

        try:

            exp = datetime.strptime(
                m["expiry_date"],
                "%Y-%m-%d"
            ).date()

            days_left = (
                exp - today
            ).days

            if days_left <= 60:

                expiring.append({
                    "name": m["name"],
                    "batch_no": m.get(
                        "batch_no",
                        ""
                    ),
                    "days_left": days_left,
                })

        except Exception:
            pass

    # CUSTOMER OUTSTANDING

    customer_txns = await db.customer_transactions.find(
        {},
        {"_id": 0}
    ).to_list(5000)

    customer_outstanding = 0

    for t in customer_txns:

        if t.get("type") == "sale":

            amt = float(
                t.get("amount", 0)
            )

            customer_outstanding += amt

            try:

                dt = datetime.fromisoformat(
                    t["created_at"]
                ).date()

                if dt == today:
                    customer_outstanding_today += amt

                if (
                    dt.month == current_month and
                    dt.year == current_year
                ):
                    customer_outstanding_month += amt

            except Exception:
                pass

        elif t.get("type") == "payment":

            amt = float(
                t.get("amount", 0)
            )

            customer_outstanding -= amt

            received_total += amt

            try:

                dt = datetime.fromisoformat(
                    t["created_at"]
                ).date()

                if dt == today:
                    received_today += amt

                if (
                    dt.month == current_month and
                    dt.year == current_year
                ):
                    received_month += amt

            except Exception:
                pass
                
    # DISTRIBUTOR OUTSTANDING

    distributors = await db.distributors.find(
        {},
        {"_id": 0}
    ).to_list(1000)

    distributor_outstanding = 0

    for d in distributors:

        bal = float(
            d.get("opening_balance", 0)
        )

        txns = await db.distributor_transactions.find(
            {"distributor_id": d["id"]},
            {"_id": 0}
        ).to_list(1000)

        for t in txns:

            if t.get("type") == "purchase":

                bal += float(
                    t.get("amount", 0)
                )

            elif t.get("type") == "payment":

                bal -= float(
                    t.get("amount", 0)
                )

        if bal > 0:
            distributor_outstanding += bal

    return {

        "sales": round(total_sales, 2),
        "sales_month": round(sales_month, 2),
        "sales_today": round(sales_today, 2),

        "gst_collected": round(
            total_gst,
            2
        ),

        "discount_given": round(
            total_discount,
            2
        ),

        "expenses": round(
            total_expenses,
            2
        ),
        "expenses_month": round(expenses_month, 2),
        "expenses_today": round(expenses_today, 2),

        "profit": round(
            profit,
            2
        ),
        "expenses_month": round(expenses_month, 2),
        "expenses_today": round(expenses_today, 2),

        "stock_value": round(
            stock_value,
            2
        ),

        "customer_outstanding": round(
            customer_outstanding,
            2
        ),
        "customer_outstanding_month": round(
    customer_outstanding_month,
    2
),

"customer_outstanding_today": round(
    customer_outstanding_today,
    2
),

        "distributor_outstanding": round(
            distributor_outstanding,
            2
        ),
        "amount_received": round(received_total, 2),

"amount_received_month": round(
    received_month,
    2
),

"amount_received_today": round(
    received_today,
    2
),

        "low_stock_count": len(
            low_stock_items
        ),

        "low_stock_items": low_stock_items,

        "expiring_soon_count": len(
            expiring
        ),

        "expiring_soon": expiring,
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
    total = sum(i.get("total", 0) for i in invoices)
    gst = sum(i.get("gst_total", 0) for i in invoices)
    # Daily breakdown
    daily = {}
    for i in invoices:
        day = i["created_at"][:10]
        daily[day] = daily.get(day, 0) + i.get("total", 0)
    # Profit approximation (MRP - purchase_price) * qty from items
    profit = 0.0
    for inv in invoices:
        for it in inv.get("items", []):
            med = await db.medicines.find_one({"id": it["medicine_id"]}, {"purchase_price": 1})
            if med:
                profit += (it["mrp"] - med.get("purchase_price", 0)) * it["quantity"]
    return {
    "sales": round(total_sales, 2),

    "gst_collected": round(total_gst, 2),

    "discount_given": round(total_discount, 2),

    "expenses": 0,

    "profit": round(total_sales, 2),

    "customer_outstanding": round(
        customer_outstanding,
        2
    ),

    "distributor_outstanding": round(
        distributor_outstanding,
        2
    ),

    "stock_value": round(stock_value, 2),

    "low_stock_count": len(low_stock_items),

    "low_stock_items": low_stock_items,

    "expiring_soon_count": 0,

    "expiring_soon": [],
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

        bal = float(d.get("opening_balance", 0.0))

        for t in txns:
            if t.get("type") == "purchase":
                bal += float(t.get("amount", 0))

            elif t.get("type") == "payment":
                bal -= float(t.get("amount", 0))

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
        try:
            exp = datetime.strptime(m["expiry_date"], "%Y-%m-%d").date()
            days = (exp - today).days
            if days < 0:
                expired.append({**m, "days_to_expiry": days})
            elif days <= 90:
                near.append({**m, "days_to_expiry": days})
        except Exception:
            pass
    return {"expired": expired, "near_expiry": sorted(near, key=lambda x: x["days_to_expiry"])}


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
    }
    return data


@api_router.post("/backup/import")
async def backup_import(payload: dict, user: dict = Depends(require_role("admin"))):
    collections = ["medicines", "distributors", "customers", "invoices", "customer_transactions", "distributor_transactions"]
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
    sold_units: float = 0
    low_stock_threshold: int = 10


class POCreate(BaseModel):
    distributor_id: str
    distributor_name: str
    invoice_ref: str

    po_date: str | None = None   # 👈 ADD THIS

    items: list[POItem]

    notes: str | None = None


@api_router.post("/purchase-orders")
async def create_po(
    payload: POCreate,
    user: dict = Depends(
        require_role(
            "admin",
            "pharmacist"
        )
    )
):

    total = sum(
        i.purchase_price * i.quantity
        for i in payload.items
    )

    po = {

        "id": str(uuid.uuid4()),

        "po_no": (
            f"PO-"
            f"{datetime.now(timezone.utc).strftime('%y%m%d')}-"
            f"{await db.purchase_orders.count_documents({}) + 1:04d}"
        ),

        "po_date": payload.po_date,

        "distributor_id":
            payload.distributor_id,

        "distributor_name":
            payload.distributor_name,

        "invoice_ref":
            payload.invoice_ref,

        "items": [
            i.model_dump()
            for i in payload.items
        ],

        "total":
            round(total, 2),

        "notes":
            payload.notes,

        "created_at":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "received_at":
            None,
    }

    await db.purchase_orders.insert_one(po)

    for i in payload.items:

        purchased_units = float(
            i.quantity +
            i.free_quantity
        )

        name = str(
            i.name or ""
        ).strip()

        batch_no = str(
            i.batch_no or ""
        ).strip()

        medicine = await db.medicines.find_one({

            "name": name,

            "batch_no": batch_no,
        })

        if medicine:

            await db.medicines.update_one(

                {
                    "_id":
                        medicine["_id"]
                },

                {
                    "$inc": {

                        "purchased_units":
                            purchased_units
                    },

                    "$set": {

                        "name":
                            name,

                        "batch_no":
                            batch_no,

                        "expiry_date":
                            i.expiry_date,

                        "mrp":
                            i.mrp,

                        "purchase_price":
                            i.purchase_price,

                        "manufacturer":
                            i.manufacturer,

                        "category":
                            i.category,

                        "pack_size":
                            i.pack_size,

                        "sold_units":
                            float(
                                i.sold_units or 0
                            ),

                        "gst_rate":
                            i.gst_rate,

                        "low_stock_threshold":
                            i.low_stock_threshold or 10,

                        "distributor_name":
                            payload.distributor_name,
                    }
                }
            )

        else:

            await db.medicines.insert_one({

                "id":
                    str(uuid.uuid4()),

                "name":
                    name,

                "batch_no":
                    batch_no,

                "expiry_date":
                    i.expiry_date,

                "manufacturer":
                    i.manufacturer,

                "category":
                    i.category,

                "purchase_price":
                    i.purchase_price,

                "mrp":
                    i.mrp,

                "pack_size":
                    i.pack_size,

                "purchased_units":
                    purchased_units,

                "sold_units":
                    float(
                        i.sold_units or 0
                    ),

                "gst_rate":
                    i.gst_rate,

                "low_stock_threshold":
                    i.low_stock_threshold or 10,

                "distributor_name":
                    payload.distributor_name,
            })

    po.pop("_id", None)

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

    await db.purchase_orders.delete_one({
        "id": po_id
    })

    return {
        "message": "PO deleted"
    }

@api_router.put("/purchase-orders/{po_id}")
async def update_po(
    po_id: str,
    payload: POCreate,
    user: dict = Depends(require_role("admin"))
):

    old_po = await db.purchase_orders.find_one({
        "id": po_id
    })

    if not old_po:

        raise HTTPException(
            404,
            "PO not found"
        )

    # REVERSE OLD STOCK

    for i in old_po.get("items", []):

        qty = (
            float(i.get("quantity", 0)) +
            float(i.get("free_quantity", 0))
        )

        medicine = await db.medicines.find_one({

            "name":
                str(
                    i.get("name", "")
                ).strip(),

            "batch_no":
                str(
                    i.get("batch_no", "")
                ).strip(),
        })

        if medicine:

            await db.medicines.update_one(

                {
                    "_id":
                        medicine["_id"]
                },

                {
                    "$inc": {
                        "purchased_units": -qty
                    }
                }
            )

    # APPLY NEW STOCK

    for i in payload.items:

        qty = float(
            i.quantity +
            i.free_quantity
        )

        name = str(
            i.name or ""
        ).strip()

        batch_no = str(
            i.batch_no or ""
        ).strip()

        medicine = await db.medicines.find_one({

            "name": name,

            "batch_no": batch_no,
        })

        if medicine:

            await db.medicines.update_one(

                {
                    "_id":
                        medicine["_id"]
                },

                {
                    "$inc": {
                        "purchased_units": qty
                    },

                    "$set": {

                        "expiry_date":
                            i.expiry_date,

                        "mrp":
                            i.mrp,

                        "purchase_price":
                            i.purchase_price,

                        "manufacturer":
                            i.manufacturer,

                        "category":
                            i.category,

                        "pack_size":
                            i.pack_size,

                        "sold_units":
                            float(
                                i.sold_units or 0
                            ),

                        "gst_rate":
                            i.gst_rate,

                        "low_stock_threshold":
                            i.low_stock_threshold or 10,

                        "distributor_name":
                            payload.distributor_name,
                    }
                }
            )

        else:

            await db.medicines.insert_one({

                "id":
                    str(uuid.uuid4()),

                "name":
                    name,

                "batch_no":
                    batch_no,

                "expiry_date":
                    i.expiry_date,

                "manufacturer":
                    i.manufacturer,

                "category":
                    i.category,

                "purchase_price":
                    i.purchase_price,

                "mrp":
                    i.mrp,

                "pack_size":
                    i.pack_size,

                "purchased_units":
                    qty,

                "sold_units":
                    float(
                        i.sold_units or 0
                    ),

                "gst_rate":
                    i.gst_rate,

                "low_stock_threshold":
                    i.low_stock_threshold or 10,

                "distributor_name":
                    payload.distributor_name,
            })

    total = sum(
        i.purchase_price * i.quantity
        for i in payload.items
    )

    await db.purchase_orders.update_one(

        {
            "id": po_id
        },

        {
            "$set": {

                "po_date":
                    payload.po_date,

                "distributor_id":
                    payload.distributor_id,

                "distributor_name":
                    payload.distributor_name,

                "invoice_ref":
                    payload.invoice_ref,

                "notes":
                    payload.notes,

                "items": [
                    i.model_dump()
                    for i in payload.items
                ],

                "total":
                    round(total, 2),
            }
        }
    )

    return {
        "message": "PO updated"
    }

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

    # FIXED STOCK SYSTEM
    available = (
        int(med.get("purchased_units", 0))
        - int(med.get("sold_units", 0))
    )

    if available < units_needed:
        raise HTTPException(
            status_code=400,
            detail=f"Insufficient stock for {med['name']}"
        )

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

    await db.daily_sales.insert_one(entry)

    # INCREASE SOLD UNITS
    await db.medicines.update_one(
        {"id": payload.medicine_id},
        {
            "$inc": {
                "sold_units": units_needed
            }
        }
    )

    entry.pop("_id", None)

    return entry

@api_router.get("/daily-sales")
async def list_daily_sales(
    date: Optional[str] = None,
    user: dict = Depends(get_current_user),
):
    q = {}
    if date:
        q["sale_date"] = date
    items = await db.daily_sales.find(q, {"_id": 0}).sort("created_at", -1).to_list(2000)
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

    # RESTORE STOCK
    await db.medicines.update_one(
        {"id": sale["medicine_id"]},
        {
            "$inc": {
                "sold_units": -int(
                    sale.get(
                        "units_dispensed",
                        sale.get("quantity", 0)
                    )
                )
            }
        },
    )

    await db.daily_sales.delete_one({
        "id": sale_id
    })

    return {"ok": True}

@app.post("/ocr")
async def ocr_invoice(file: UploadFile = File(...)):

    import re

    image_bytes = await file.read()

    image = Image.open(io.BytesIO(image_bytes))

    image = image.convert("L")

    image = image.resize(
        (image.width * 2, image.height * 2)
    )

    image = image.point(
        lambda x: 0 if x < 140 else 255,
        "1"
    )

    data = pytesseract.image_to_data(
        image,
        config="--oem 3 --psm 4",
        output_type=pytesseract.Output.DATAFRAME
    )

    data = data.dropna()

    lines = []

    current_line = []

    last_top = None

    for _, row in data.iterrows():

        text = str(row["text"]).strip()

        if not text:
            continue

        top = int(row["top"])

        if last_top is None:
            last_top = top

        if abs(top - last_top) > 10:

            if current_line:
                lines.append(current_line)

            current_line = []

            last_top = top

        current_line.append(text)

    if current_line:
        lines.append(current_line)

    items = []

    for line in lines:

        joined = " ".join(line)

        print(joined)

        try:

            if any(x in joined.lower() for x in [
                "invoice",
                "gst",
                "amount",
                "tax",
                "total",
                "cgst",
                "sgst"
            ]):
                continue

            if len(line) < 6:
                continue

            qty = 1
            mrp = 0

            numbers = []

            for word in line:

                try:
                    numbers.append(float(word))
                except:
                    pass

            if len(numbers) >= 2:
                qty = int(numbers[0])
                mrp = float(numbers[-1])

            item = {
                "name": joined,
                "batch_no": "",
                "expiry_date": "",
                "manufacturer": "",
                "category": "OTC",
                "quantity": qty,
                "free_quantity": 0,
                "purchase_price": mrp,
                "mrp": mrp,
                "gst_rate": 5,
                "pack_size": "",
                "sold_units": 0,
                "low_stock_threshold": 10,
            }

            items.append(item)

        except Exception as e:
            print(e)

    return {
        "invoice_ref": "",
        "po_date": "",
        "items": items
    }



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
