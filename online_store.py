"""Online store/order foundation for PharmacyOS.

This module is intentionally isolated from invoice, ledger, and inventory
write paths. A website order is only a customer request until a pharmacist
accepts it and creates/completes the corresponding pharmacy sale.

Integration into server.py is deliberately kept as a small adapter step so
this module can be tested independently before touching the large monolith.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Body, Depends


ORDER_STATUSES = {
    "NEW", "REVIEWING", "CONFIRMED", "PREPARING", "READY", "COMPLETED",
    "REJECTED", "CANCELLED", "PARTIALLY_AVAILABLE", "AWAITING_PAYMENT", "PAID",
}
TERMINAL_STATUSES = {"COMPLETED", "REJECTED", "CANCELLED"}
ALLOWED_PRESCRIPTION_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".pdf"}
MAX_PRESCRIPTION_BYTES = 10 * 1024 * 1024
_SEQUENCE_LOCK = asyncio.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_name_part(value: Any) -> str:
    return re.sub(r"[^A-Za-z]", "", str(value or "")).upper()


def build_order_id(first_name: str, last_name: str, order_date: datetime, sequence: int) -> str:
    """Build a human-searchable customer order id: XXYYDDMMYYNN."""
    first = _clean_name_part(first_name)[:2].ljust(2, "X")
    last = _clean_name_part(last_name)[:2].ljust(2, "X")
    sequence = int(sequence)
    if sequence < 1 or sequence > 99:
        raise ValueError("Customer order sequence must be between 1 and 99")
    return f"{first}{last}{order_date.strftime('%d%m%y')}{sequence:02d}"


def split_name(full_name: str) -> tuple[str, str]:
    parts = [part for part in str(full_name or "").strip().split() if part]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def normalize_customer_name(first_name: str, last_name: str, full_name: str = "") -> tuple[str, str, str]:
    first = str(first_name or "").strip()
    last = str(last_name or "").strip()
    if not first and not last:
        first, last = split_name(full_name)
    display = " ".join(part for part in (first, last) if part).strip()
    return first, last, display


def normalize_order_items(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = []
    for raw in items or []:
        if not isinstance(raw, dict):
            continue
        medicine_id = str(raw.get("medicine_id") or raw.get("id") or "").strip()
        name = str(raw.get("medicine_name") or raw.get("name") or "").strip()
        try:
            quantity = float(raw.get("quantity", 1))
        except (TypeError, ValueError):
            quantity = 0
        if quantity <= 0 or (not medicine_id and not name):
            continue
        normalized.append({
            "medicine_id": medicine_id or None,
            "medicine_name": name,
            "quantity": quantity,
            "unit_price": raw.get("unit_price"),
            "line_total": raw.get("line_total"),
            "availability_at_order": raw.get("availability_at_order"),
        })
    return normalized


def medicine_display_name(medicine: Dict[str, Any]) -> str:
    return str(
        medicine.get("name") or medicine.get("medicine_name") or
        medicine.get("brand_name") or medicine.get("generic_name") or ""
    ).strip()


def medicine_available_quantity(medicine: Dict[str, Any]) -> float:
    for key in ("available_stock", "current_stock", "stock", "quantity", "available_quantity"):
        value = medicine.get(key)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
    return 0.0


def medicine_price(medicine: Dict[str, Any]) -> Optional[float]:
    for key in ("selling_price", "sale_price", "mrp", "retail_price", "price"):
        value = medicine.get(key)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
    return None


def make_whatsapp_url(phone_number: str, order: Dict[str, Any]) -> str:
    phone = re.sub(r"\D", "", str(phone_number or ""))
    if not phone:
        raise ValueError("WhatsApp business number is not configured")
    lines = [
        "SHREE SHYAM PHARMACY", "New Medicine Order", "",
        f"Order ID: {order['order_id']}",
        f"Customer: {order['customer']['name']}",
        f"Mobile: {order['customer']['mobile']}",
    ]
    address = order["customer"].get("address") or ""
    if address:
        lines.append(f"Address: {address}")
    lines.extend(["", "Medicines:"])
    for index, item in enumerate(order.get("items", []), 1):
        lines.append(f"{index}. {item.get('medicine_name') or 'Medicine'} - Qty {item['quantity']}")
    if order.get("prescription"):
        lines.extend(["", "Prescription: Uploaded on website"])
    if order.get("medicine_request"):
        lines.extend(["", "Medicine availability request:", str(order["medicine_request"])])
    lines.extend(["", "Please check availability and confirm the order."])
    return f"https://wa.me/{phone}?text={quote(chr(10).join(lines))}"


def _customer_order_response(order: Dict[str, Any]) -> Dict[str, Any]:
    """Return only customer-safe order fields for the public tracking endpoint."""
    return {
        "order_id": order.get("order_id"),
        "status": order.get("status"),
        "customer": order.get("customer"),
        "items": order.get("items") or [],
        "prescription": order.get("prescription"),
        "medicine_request": order.get("medicine_request"),
        "customer_note": order.get("customer_note") or "",
        "created_at": order.get("created_at"),
        "updated_at": order.get("updated_at"),
    }


def build_online_store_router(raw_db, *, tenant_id: str, whatsapp_number: str,
                              private_upload_dir: str | Path, require_current_user=None):
    """Build public store and private pharmacist order routes."""
    router = APIRouter()
    upload_dir = Path(private_upload_dir).resolve()
    upload_dir.mkdir(parents=True, exist_ok=True)

    async def _find_medicine(medicine_id: str) -> Optional[dict]:
        if not medicine_id:
            return None
        return await raw_db.medicines.find_one({"id": medicine_id, "tenant_id": tenant_id}, {"_id": 0})

    async def _next_customer_sequence(first_name: str, last_name: str) -> int:
        """Allocate a per-customer sequence safely in the single-process local app.

        The SQLite adapter's find_one_and_update is not itself a transaction,
        so the local critical section is protected by a process lock. MongoDB
        deployments still perform the database-side increment atomically.
        """
        first = _clean_name_part(first_name)
        last = _clean_name_part(last_name)
        counter_id = f"{tenant_id}:{first}:{last}"
        async with _SEQUENCE_LOCK:
            counter = await raw_db.online_order_counters.find_one_and_update(
                {"id": counter_id, "tenant_id": tenant_id},
                {"$setOnInsert": {
                    "id": counter_id, "tenant_id": tenant_id,
                    "first_name_normalized": first, "last_name_normalized": last,
                    "sequence": 0,
                }, "$inc": {"sequence": 1}},
                upsert=True, return_document=True, projection={"_id": 0},
            )
        sequence = int((counter or {}).get("sequence") or 0)
        if sequence < 1 or sequence > 99:
            raise HTTPException(status_code=409, detail="Customer order sequence limit reached (99)")
        return sequence

    @router.get("/api/store/medicines/search")
    async def search_store_medicines(q: str = Query("", min_length=1, max_length=100),
                                     limit: int = Query(20, ge=1, le=50)):
        term = str(q).strip()
        pattern = re.escape(term)
        rows = await raw_db.medicines.find(
            {"tenant_id": tenant_id, "name": {"$regex": pattern, "$options": "i"}},
            {"_id": 0},
        ).to_list(limit * 3)
        result = []
        needle = term.lower()
        for medicine in rows:
            name = medicine_display_name(medicine)
            if needle not in name.lower():
                continue
            result.append({
                "id": medicine.get("id") or medicine.get("_id"), "name": name,
                "price": medicine_price(medicine),
                "available": medicine_available_quantity(medicine) > 0,
                "available_quantity": medicine_available_quantity(medicine),
            })
            if len(result) >= limit:
                break
        return result

    @router.post("/api/store/prescriptions")
    async def upload_store_prescription(file: UploadFile = File(...)):
        extension = Path(file.filename or "").suffix.lower()
        if extension not in ALLOWED_PRESCRIPTION_EXTENSIONS:
            raise HTTPException(status_code=400, detail="Prescription must be JPG, PNG, WEBP, or PDF")
        content = await file.read(MAX_PRESCRIPTION_BYTES + 1)
        if len(content) > MAX_PRESCRIPTION_BYTES:
            raise HTTPException(status_code=413, detail="Prescription file is too large")
        token = uuid.uuid4().hex
        (upload_dir / f"{token}{extension}").write_bytes(content)
        return {"prescription_id": token, "filename": file.filename, "content_type": file.content_type}

    @router.post("/api/store/orders")
    async def create_store_order(payload: Dict[str, Any] = Body(...)):
        first_name, last_name, display_name = normalize_customer_name(
            payload.get("first_name"), payload.get("last_name"), payload.get("name"))
        mobile = str(payload.get("mobile") or payload.get("phone") or "").strip()
        address = str(payload.get("address") or payload.get("house_number") or "").strip()
        if not display_name or not mobile or not address:
            raise HTTPException(status_code=422, detail="Name, mobile number and address/house number are required")
        items = normalize_order_items(payload.get("items") or [])
        prescription = payload.get("prescription")
        medicine_request = str(payload.get("medicine_request") or "").strip()
        if not items and not prescription and not medicine_request:
            raise HTTPException(status_code=422, detail="Add medicines, upload a prescription, or request a medicine")

        sequence = await _next_customer_sequence(first_name, last_name)
        created = datetime.now(timezone.utc)
        order_id = build_order_id(first_name, last_name, created, sequence)
        checked_items = []
        for item in items:
            medicine = await _find_medicine(item.get("medicine_id")) if item.get("medicine_id") else None
            if medicine:
                item["medicine_name"] = medicine_display_name(medicine)
                item["unit_price"] = medicine_price(medicine)
                item["availability_at_order"] = medicine_available_quantity(medicine) > 0
            else:
                item["availability_at_order"] = None
            unit_price = item.get("unit_price")
            try:
                item["line_total"] = round(float(unit_price) * float(item["quantity"]), 2) if unit_price is not None else None
            except (TypeError, ValueError):
                item["line_total"] = None
            print(
                "### ORDER ITEM DEBUG:",
                "quantity=", item.get("quantity"),
                "unit_price=", item.get("unit_price"),
                "line_total=", item.get("line_total"),
            )
            checked_items.append(item)

        order = {
            "id": str(uuid.uuid4()), "order_id": order_id, "tenant_id": tenant_id,
            "shop_id": tenant_id, "source": "website", "status": "NEW",
            "customer": {
                "first_name": first_name, "last_name": last_name,
                "first_name_normalized": _clean_name_part(first_name),
                "last_name_normalized": _clean_name_part(last_name),
                "name": display_name, "mobile": mobile, "address": address,
            },
            "items": checked_items, "prescription": prescription,
            "medicine_request": medicine_request or None,
            "customer_note": str(payload.get("customer_note") or "").strip(),
            "created_at": _now(), "updated_at": _now(),
        }
        order["whatsapp_url"] = make_whatsapp_url(whatsapp_number, order)
        await raw_db.online_orders.insert_one(order)
        return {"order_id": order["order_id"], "status": order["status"], "whatsapp_url": order["whatsapp_url"]}

    @router.get("/api/store/orders/{order_id}")
    async def get_store_order(order_id: str):
        order = await raw_db.online_orders.find_one({"tenant_id": tenant_id, "order_id": order_id}, {"_id": 0})
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        return _customer_order_response(order)

    if require_current_user:
        @router.get("/api/online-orders")
        async def list_online_orders(user=Depends(require_current_user)):
            return await raw_db.online_orders.find({"tenant_id": tenant_id}, {"_id": 0}).sort("created_at", -1).to_list(500)

        @router.get("/api/online-orders/{order_id}")
        async def get_online_order(order_id: str, user=Depends(require_current_user)):
            order = await raw_db.online_orders.find_one({"tenant_id": tenant_id, "order_id": order_id}, {"_id": 0})
            if not order:
                raise HTTPException(status_code=404, detail="Order not found")
            return order

        @router.post("/api/online-orders/{order_id}/status")
        async def update_online_order_status(order_id: str, payload: Dict[str, Any] = Body(...), user=Depends(require_current_user)):
            status = str(payload.get("status") or "").upper().strip()
            if status not in ORDER_STATUSES:
                raise HTTPException(status_code=422, detail="Invalid online order status")
            existing = await raw_db.online_orders.find_one({"tenant_id": tenant_id, "order_id": order_id})
            if not existing:
                raise HTTPException(status_code=404, detail="Order not found")
            if existing.get("status") in TERMINAL_STATUSES and status != existing.get("status"):
                raise HTTPException(status_code=409, detail="Completed, rejected or cancelled orders cannot be reopened")
            await raw_db.online_orders.update_one(
                {"tenant_id": tenant_id, "order_id": order_id},
                {"$set": {"status": status, "updated_at": _now(), "updated_by": user.get("name", "")}},
            )
            return {"order_id": order_id, "status": status}

    return router
