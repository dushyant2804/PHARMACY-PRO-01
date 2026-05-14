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

from fastapi import FastAPI, APIRouter, HTTPException, Depends, Request, Response
from fastapi.security import HTTPBearer
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field, EmailStr

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
    quantity: int  # total units (boxes * units_per_box + loose_units)
    boxes: int = 0
    units_per_box: int = 1
    loose_units: int = 0
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
    quantity: int = 0
    boxes: int = 0
    units_per_box: int = 1
    loose_units: int = 0
    current_boxes: int = 0
    current_strips: int = 0
    current_loose_units: int = 0
    category: str = "OTC"
    gst_rate: float = 12.0
    barcode: Optional[str] = None
    low_stock_threshold: int = 10
    auto_ledger: bool = True  # if true, create distributor payable transaction


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
    sort_field = sort_by if sort_by in ("name", "expiry_date", "quantity", "mrp") else "name"
    items = await db.medicines.find(q, {"_id": 0}).sort(sort_field, 1).to_list(2000)
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


@api_router.post("/medicines")
async def create_medicine(payload: MedicineCreate, user: dict = Depends(require_role("admin", "pharmacist"))):
    data = payload.model_dump()
    auto_ledger = data.pop("auto_ledger", True)
    upb = max(int(data.get("units_per_box") or 1), 1)
    boxes = int(data.get("boxes") or 0)
    loose = int(data.get("loose_units") or 0)
    if boxes or loose:
        data["quantity"] = boxes * upb + loose
    elif data.get("quantity"):
        data["loose_units"] = int(data["quantity"])
        current_boxes = int(data.get("current_boxes") or 0)
        current_strips = int(data.get("current_strips") or 0)
        current_loose = int(data.get("current_loose_units") or 0)

        data["current_quantity"] = (
        (current_boxes * upb)
        + current_strips
        + current_loose
        )
    data["units_per_box"] = upb
    med = Medicine(**data)
    await db.medicines.insert_one(med.model_dump())

    if auto_ledger and data.get("distributor_id") and data.get("quantity") and data.get("purchase_price"):
        amount = round(float(data["purchase_price"]) * int(data["quantity"]), 2)
        if amount > 0:
            await db.distributor_transactions.insert_one({
                "id": str(uuid.uuid4()),
                "distributor_id": data["distributor_id"],
                "type": "purchase",
                "amount": amount,
                "reference": f"STOCK:{med.batch_no}",
                "notes": f"Manual stock entry: {med.name}",
                "mode": "",
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
    return med.model_dump()


@api_router.get("/medicines/lookup/{barcode}")
async def lookup_barcode(barcode: str, user: dict = Depends(get_current_user)):
    med = await db.medicines.find_one({"barcode": barcode}, {"_id": 0})
    if not med:
        raise HTTPException(status_code=404, detail="Not found")
    return med


@api_router.put("/medicines/{med_id}")
async def update_medicine(med_id: str, payload: MedicineCreate, user: dict = Depends(require_role("admin", "pharmacist"))):
    data = payload.model_dump()
    data.pop("auto_ledger", None)
    upb = max(int(data.get("units_per_box") or 1), 1)
    boxes = int(data.get("boxes") or 0)
    loose = int(data.get("loose_units") or 0)
    if boxes or loose:
        data["quantity"] = boxes * upb + loose
    data["units_per_box"] = upb
    res = await db.medicines.update_one({"id": med_id}, {"$set": data})
    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail="Medicine not found")
    med = await db.medicines.find_one({"id": med_id}, {"_id": 0})
    return med


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
    line_total_raw = 0.0  # taxable before bill-level discount

    for item in payload.items:
        med = await db.medicines.find_one({"id": item.medicine_id})
        if not med:
            raise HTTPException(status_code=400, detail=f"Medicine not found: {item.name}")
        upb = max(int(med.get("units_per_box") or item.units_per_box or 1), 1)
        units_needed = item.quantity * (upb if item.unit_type == "box" else 1)
        if med["quantity"] < units_needed:
            raise HTTPException(status_code=400, detail=f"Insufficient stock for {item.name}")

        unit_price = item.mrp * (upb if item.unit_type == "box" else 1)
        line_base = unit_price * item.quantity
        line_discount = line_base * (item.discount_pct / 100.0)
        taxable = line_base - line_discount
        line_total_raw += taxable
        items_out.append({
            **item.model_dump(),
            "units_per_box": upb,
            "units_dispensed": units_needed,
            "line_total": round(taxable, 2),
        })
        await db.medicines.update_one({"id": item.medicine_id}, {"$inc": {"quantity": -units_needed}})

    # Bill-level discount: prefer fixed amount if provided, else %
    bill_disc = float(payload.bill_discount_amount or 0.0)
    if not bill_disc and payload.bill_discount_pct:
        bill_disc = line_total_raw * (float(payload.bill_discount_pct) / 100.0)
    bill_disc = min(bill_disc, line_total_raw)
    after_disc = max(line_total_raw - bill_disc, 0.0)

    # Distribute discount + GST split per item
    final_items = []
    for it, raw in zip(items_out, [i["line_total"] for i in items_out]):
        share = (raw / line_total_raw) if line_total_raw else 0
        item_after = raw - bill_disc * share
        gst_amount = item_after - (item_after / (1 + it["gst_rate"] / 100.0))
        net = item_after - gst_amount
        gst_total += gst_amount
        subtotal += net
        final_items.append({**it, "gst_amount": round(gst_amount, 2), "net_amount": round(net, 2)})

    total = round(subtotal + gst_total, 2)
    paid = float(payload.paid_amount if payload.payment_mode != "cash" else (payload.paid_amount or total))
    invoice = {
        "id": str(uuid.uuid4()),
        "invoice_no": await _next_invoice_no(),
        "customer_id": payload.customer_id,
        "customer_name": payload.customer_name,
        "customer_phone": payload.customer_phone,
        "customer_gstin": payload.customer_gstin,
        "referring_doctor": payload.referring_doctor.strip() if payload.referring_doctor else "",
        "items": final_items,
        "subtotal": round(subtotal, 2),
        "gst_total": round(gst_total, 2),
        "bill_discount": round(bill_disc, 2),
        "total": total,
        "payment_mode": payload.payment_mode,
        "paid_amount": paid,
        "due_amount": round(total - paid, 2),
        "notes": payload.notes,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": user.get("name", ""),
    }
    await db.invoices.insert_one(invoice)

    # Track doctor history
    if invoice["referring_doctor"]:
        await db.doctor_history.update_one(
            {"name": invoice["referring_doctor"]},
            {"$inc": {"count": 1}, "$set": {"last_used": invoice["created_at"]}},
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
    invoice.pop("_id", None)
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
@api_router.get("/dashboard")
async def dashboard(user: dict = Depends(get_current_user)):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_invoices = await db.invoices.find({"created_at": {"$gte": today}}, {"_id": 0}).to_list(1000)
    today_sales = sum(i.get("total", 0) for i in today_invoices)

    medicines = await db.medicines.find({}, {"_id": 0}).to_list(2000)
    low_stock = [m for m in medicines if m["quantity"] <= m.get("low_stock_threshold", 10)]

    today_dt = datetime.now(timezone.utc).date()
    near_expiry = []
    expired = []
    for m in medicines:
        try:
            exp = datetime.strptime(m["expiry_date"], "%Y-%m-%d").date()
            days = (exp - today_dt).days
            if days < 0:
                expired.append({**m, "days_to_expiry": days})
            elif days <= 60:
                near_expiry.append({**m, "days_to_expiry": days})
        except Exception:
            pass

    # Pending payments
    cust_txns = await db.customer_transactions.find({}, {"_id": 0}).to_list(5000)
    pending_by_cust = {}
    for t in cust_txns:
        cid = t["customer_id"]
        pending_by_cust[cid] = pending_by_cust.get(cid, 0) + (t["amount"] if t["type"] == "sale" else -t["amount"])
    pending_total = sum(v for v in pending_by_cust.values() if v > 0)

    return {
        "today_sales": round(today_sales, 2),
        "today_invoice_count": len(today_invoices),
        "low_stock_count": len(low_stock),
        "low_stock": low_stock[:10],
        "near_expiry_count": len(near_expiry),
        "near_expiry": sorted(near_expiry, key=lambda x: x["days_to_expiry"])[:10],
        "expired_count": len(expired),
        "pending_payments": round(pending_total, 2),
        "total_medicines": len(medicines),
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
        "total_sales": round(total, 2),
        "total_gst": round(gst, 2),
        "invoice_count": len(invoices),
        "estimated_profit": round(profit, 2),
        "daily": [{"date": k, "total": round(v, 2)} for k, v in sorted(daily.items())],
    }


@api_router.get("/reports/stock-valuation")
async def stock_valuation(user: dict = Depends(get_current_user)):
    medicines = await db.medicines.find({}, {"_id": 0}).to_list(5000)
    cost_value = sum(m["purchase_price"] * m["quantity"] for m in medicines)
    mrp_value = sum(m["mrp"] * m["quantity"] for m in medicines)
    return {
        "total_items": len(medicines),
        "total_units": sum(m["quantity"] for m in medicines),
        "cost_value": round(cost_value, 2),
        "mrp_value": round(mrp_value, 2),
        "potential_profit": round(mrp_value - cost_value, 2),
    }


@api_router.get("/reports/outstanding")
async def outstanding_report(user: dict = Depends(get_current_user)):
    customers = await db.customers.find({}, {"_id": 0}).to_list(1000)
    cust_out = []
    for c in customers:
        txns = await db.customer_transactions.find({"customer_id": c["id"]}).to_list(1000)
        bal = 0.0
        for t in txns:
            bal += t["amount"] if t["type"] == "sale" else -t["amount"]
        if bal > 0:
            cust_out.append({"id": c["id"], "name": c["name"], "phone": c.get("phone", ""), "balance": round(bal, 2)})

    distributors = await db.distributors.find({}, {"_id": 0}).to_list(1000)
    dist_out = []
    for d in distributors:
        txns = await db.distributor_transactions.find({"distributor_id": d["id"]}).to_list(1000)
        bal = d.get("opening_balance", 0.0)
        for t in txns:
            bal += t["amount"] if t["type"] == "purchase" else -t["amount"]
        if bal > 0:
            dist_out.append({"id": d["id"], "name": d["name"], "balance": round(bal, 2)})

    return {"customers": cust_out, "distributors": dist_out}


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
    medicine_id: Optional[str] = None  # may be None for new medicines
    name: str
    batch_no: str
    expiry_date: str
    manufacturer: str = ""
    category: str = "OTC"
    quantity: int
    purchase_price: float
    mrp: float
    gst_rate: float = 12.0


class POCreate(BaseModel):
    distributor_id: str
    distributor_name: str
    invoice_ref: str = ""  # supplier's invoice no
    items: List[POItem]
    notes: str = ""


@api_router.post("/purchase-orders")
async def create_po(payload: POCreate, user: dict = Depends(require_role("admin", "pharmacist"))):
    total = sum(i.purchase_price * i.quantity for i in payload.items)
    po = {
        "id": str(uuid.uuid4()),
        "po_no": f"PO-{datetime.now(timezone.utc).strftime('%y%m%d')}-{await db.purchase_orders.count_documents({}) + 1:04d}",
        "distributor_id": payload.distributor_id,
        "distributor_name": payload.distributor_name,
        "invoice_ref": payload.invoice_ref,
        "items": [i.model_dump() for i in payload.items],
        "total": round(total, 2),
        "status": "pending",  # pending | received
        "notes": payload.notes,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "received_at": None,
    }
    await db.purchase_orders.insert_one(po)
    po.pop("_id", None)
    return po


@api_router.get("/purchase-orders")
async def list_pos(user: dict = Depends(get_current_user)):
    return await db.purchase_orders.find({}, {"_id": 0}).sort("created_at", -1).to_list(2000)


@api_router.get("/purchase-orders/{pid}")
async def get_po(pid: str, user: dict = Depends(get_current_user)):
    po = await db.purchase_orders.find_one({"id": pid}, {"_id": 0})
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")
    return po


@api_router.post("/purchase-orders/{pid}/receive")
async def receive_po(pid: str, user: dict = Depends(require_role("admin", "pharmacist"))):
    po = await db.purchase_orders.find_one({"id": pid})
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")
    if po["status"] == "received":
        raise HTTPException(status_code=400, detail="Already received")

    for it in po["items"]:
        if it.get("medicine_id"):
            existing = await db.medicines.find_one({"id": it["medicine_id"]})
        else:
            existing = await db.medicines.find_one({"name": it["name"], "batch_no": it["batch_no"]})
        if existing:
            await db.medicines.update_one(
                {"id": existing["id"]},
                {"$inc": {"quantity": it["quantity"]},
                 "$set": {"purchase_price": it["purchase_price"], "mrp": it["mrp"], "expiry_date": it["expiry_date"]}},
            )
        else:
            med = Medicine(
                name=it["name"], batch_no=it["batch_no"], expiry_date=it["expiry_date"],
                manufacturer=it.get("manufacturer", ""), distributor=po["distributor_name"],
                purchase_price=it["purchase_price"], mrp=it["mrp"], quantity=it["quantity"],
                category=it.get("category", "OTC"), gst_rate=it.get("gst_rate", 12.0),
            )
            await db.medicines.insert_one(med.model_dump())

    # Distributor ledger: add purchase transaction
    await db.distributor_transactions.insert_one({
        "id": str(uuid.uuid4()),
        "distributor_id": po["distributor_id"],
        "type": "purchase",
        "amount": po["total"],
        "reference": po["po_no"],
        "notes": f"GRN for {po.get('invoice_ref') or po['po_no']}",
        "mode": "",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })

    received_at = datetime.now(timezone.utc).isoformat()
    await db.purchase_orders.update_one(
        {"id": pid},
        {"$set": {"status": "received", "received_at": received_at}},
    )
    po["status"] = "received"
    po["received_at"] = received_at
    po.pop("_id", None)
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
async def create_daily_sale(payload: DailySaleCreate, user: dict = Depends(get_current_user)):
    med = await db.medicines.find_one({"id": payload.medicine_id})
    if not med:
        raise HTTPException(status_code=400, detail="Medicine not found")
    upb = max(int(med.get("units_per_box") or 1), 1)
    units_needed = payload.quantity * (upb if payload.unit_type == "box" else 1)
    if med["quantity"] < units_needed:
        raise HTTPException(status_code=400, detail=f"Insufficient stock for {med['name']}")

    sale_date = payload.sale_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
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
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": user.get("name", ""),
    }
    await db.daily_sales.insert_one(entry)
    await db.medicines.update_one({"id": payload.medicine_id}, {"$inc": {"quantity": -units_needed}})
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
async def daily_sales_summary(date: Optional[str] = None, user: dict = Depends(get_current_user)):
    target = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    items = await db.daily_sales.find({"sale_date": target}, {"_id": 0}).to_list(2000)
   historical = await db.historical_sales.find(
    {"date": target},
    {"_id": 0}
).to_list(2000)
    live_total = sum(i.get("total_amount", 0) for i in items)
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

historical_total = sum(h.get("total_amount", 0) for h in historical)
historical_paid = sum(
    (h.get("cash_amount", 0) + h.get("upi_amount", 0))
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
async def delete_daily_sale(sale_id: str, user: dict = Depends(require_role("admin", "pharmacist"))):
    sale = await db.daily_sales.find_one({"id": sale_id})
    if not sale:
        raise HTTPException(status_code=404, detail="Entry not found")
    # Restore stock
    await db.medicines.update_one(
        {"id": sale["medicine_id"]},
        {"$inc": {"quantity": int(sale.get("units_dispensed", sale.get("quantity", 0)))}},
    )
    await db.daily_sales.delete_one({"id": sale_id})
    return {"ok": True}


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
