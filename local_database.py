"""SQLite-backed Mongo-like adapter for PharmacyOS local-first mode.

This adapter intentionally preserves the async collection surface used by
server.py while storing each document as JSON in SQLite. It is designed for a
single-shop, single-PC local backend and keeps cloud MongoDB support untouched.
"""
from __future__ import annotations

import copy
import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Union


logger = logging.getLogger("pharmacy")


class LocalInsertOneResult(SimpleNamespace):
    inserted_id: str


class LocalDeleteResult(SimpleNamespace):
    deleted_count: int = 0


class LocalUpdateResult(SimpleNamespace):
    matched_count: int = 0
    modified_count: int = 0
    upserted_id: Optional[str] = None


class LocalCursor:
    def __init__(self, docs: List[Dict[str, Any]]):
        self.docs = docs
        self._index = 0

    def sort(self, key, direction: Optional[int] = None):
        if isinstance(key, list):
            sort_keys = key
        else:
            sort_keys = [(key, 1 if direction is None else direction)]
        for field, order in reversed(sort_keys):
            self.docs.sort(key=lambda d: _get_path(d, field) is None)
            self.docs.sort(key=lambda d: _get_path(d, field), reverse=order == -1)
        return self

    def skip(self, n: int):
        if n is not None and n > 0:
            self.docs = self.docs[n:]
        return self

    def limit(self, n: int):
        if n is not None and n >= 0:
            self.docs = self.docs[:n]
        return self

    async def to_list(self, length: Optional[int]):
        return copy.deepcopy(self.docs if length is None else self.docs[:length])

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self.docs):
            raise StopAsyncIteration
        item = self.docs[self._index]
        self._index += 1
        return copy.deepcopy(item)


class LocalCollection:
    def __init__(self, database: "LocalSQLiteDatabase", name: str):
        self.database = database
        self.name = name
        self.database._ensure_collection(name)

    async def create_index(self, *args, **kwargs):
        return None

    def find(self, query=None, projection=None, *args, **kwargs):
        docs = [_project(d, projection) for d in self.database._read_all(self.name) if _matches(d, query or {})]
        return LocalCursor(docs)

    async def find_one(self, query=None, projection=None, *args, **kwargs):
        docs = self.find(query, projection)
        if kwargs.get("sort"):
            docs.sort(kwargs["sort"])
        items = await docs.to_list(1)
        return items[0] if items else None

    async def count_documents(self, query, *args, **kwargs):
        return sum(1 for doc in self.database._read_all(self.name) if _matches(doc, query or {}))

    async def distinct(self, key, query=None, *args, **kwargs):
        values = []
        for doc in self.database._read_all(self.name):
            if _matches(doc, query or {}):
                value = _get_path(doc, key)
                if value not in values:
                    values.append(value)
        return values

    def aggregate(self, pipeline, *args, **kwargs):
        # Supports the common local health/report primitives. Complex reports keep
        # their API shape and can be expanded without touching route business logic.
        docs = self.database._read_all(self.name)
        for stage in pipeline or []:
            if "$match" in stage:
                docs = [d for d in docs if _matches(d, stage["$match"])]
            elif "$sort" in stage:
                cursor = LocalCursor(docs).sort(list(stage["$sort"].items()))
                docs = cursor.docs
            elif "$limit" in stage:
                docs = docs[: int(stage["$limit"])]
            elif "$project" in stage:
                docs = [_project(d, stage["$project"]) for d in docs]
        return LocalCursor(docs)

    async def insert_one(self, document, *args, **kwargs):
        doc = copy.deepcopy(document)
        doc.setdefault("_id", doc.get("id") or str(uuid.uuid4()))
        self.database._upsert(self.name, str(doc["_id"]), doc)
        return LocalInsertOneResult(inserted_id=doc["_id"])

    async def insert_many(self, documents, *args, **kwargs):
        ids = []
        for doc in documents:
            result = await self.insert_one(doc)
            ids.append(result.inserted_id)
        return SimpleNamespace(inserted_ids=ids)

    async def delete_many(self, query, *args, **kwargs):
        return LocalDeleteResult(deleted_count=self.database._delete(self.name, query or {}, multi=True))

    async def delete_one(self, query, *args, **kwargs):
        return LocalDeleteResult(deleted_count=self.database._delete(self.name, query or {}, multi=False))

    async def update_one(self, query, update, *args, upsert=False, **kwargs):
        return await self._update(query, update, upsert=upsert, multi=False)

    async def update_many(self, query, update, *args, upsert=False, **kwargs):
        return await self._update(query, update, upsert=upsert, multi=True)

    async def find_one_and_update(self, query, update, *args, upsert=False, return_document=None, projection=None, **kwargs):
        await self._update(query, update, upsert=upsert, multi=False)
        return await self.find_one(query, projection)

    async def replace_one(self, query, replacement, *args, upsert=False, **kwargs):
        found = await self.find_one(query)
        if not found and not upsert:
            return LocalUpdateResult()
        doc = copy.deepcopy(replacement)
        doc.setdefault("_id", (found or {}).get("_id") or doc.get("id") or str(uuid.uuid4()))
        self.database._upsert(self.name, str(doc["_id"]), doc)
        return LocalUpdateResult(matched_count=1 if found else 0, modified_count=1, upserted_id=None if found else doc["_id"])

    async def _update(self, query, update, *, upsert=False, multi=False):
        docs = self.database._read_all(self.name)
        matched = [d for d in docs if _matches(d, query or {})]
        if not matched and upsert:
            base = {k: v for k, v in (query or {}).items() if not k.startswith("$") and not isinstance(v, dict)}
            matched = [base]
        for doc in matched[: None if multi else 1]:
            _apply_update(doc, update)
            doc.setdefault("_id", doc.get("id") or str(uuid.uuid4()))
            self.database._upsert(self.name, str(doc["_id"]), doc)
        return LocalUpdateResult(matched_count=len(matched), modified_count=len(matched), upserted_id=None)


class LocalSQLiteDatabase:
    def __init__(self, path: Union[str, Path]):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("CREATE TABLE IF NOT EXISTS documents (collection TEXT NOT NULL, doc_id TEXT NOT NULL, data TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(collection, doc_id))")
        self._ensure_local_indexes()
        self.conn.commit()

    def _ensure_collection(self, name):
        return None

    def _sqlite_json1_available(self) -> bool:
        try:
            self.conn.execute("SELECT json_extract('{\"ok\": 1}', '$.ok')").fetchone()
            return True
        except sqlite3.OperationalError:
            return False

    def _ensure_local_indexes(self):
        # Non-JSON indexes are safe on all supported SQLite builds.
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_collection_updated ON documents(collection, updated_at)")

        # JSON expression indexes keep common Local Mode lookups responsive on
        # low-spec Windows 7 PCs while preserving the document storage model.
        # Older Windows 7 SQLite builds may not include JSON1, so these indexes
        # must be opportunistic and must never block LOCAL_MODE startup.
        if not self._sqlite_json1_available():
            logger.warning("SQLite JSON1 unavailable, skipping JSON indexes")
            return

        indexed_fields = {
            "medicines": ["id", "medicine_key", "name", "batch_no", "barcode", "manufacturer", "category", "expiry_date", "distributor_id"],
            "invoices": ["id", "invoice_no", "customer_id", "created_at", "payment_mode"],
            "purchase_orders": ["id", "po_no", "distributor_id", "created_at", "po_date", "invoice_ref"],
            "customers": ["id", "name", "mobile", "phone", "created_at"],
            "distributors": ["id", "name", "mobile", "phone", "created_at"],
            "customer_transactions": ["id", "customer_id", "type", "created_at", "invoice_number", "reference_number"],
            "distributor_transactions": ["id", "distributor_id", "type", "created_at", "invoice_number", "reference_number", "purchase_order_id"],
            "stock_adjustments": ["id", "medicine_id", "medicine_name", "batch_no", "adjustment_date", "adjustment_type", "created_at"],
            "purchase_returns": ["id", "distributor_id", "distributor", "medicine_id", "medicine_name", "batch_number", "return_date", "reason", "ledger_adjusted", "po_adjustment_id"],
            "daily_closings": ["id", "closing_date", "created_at"],
        }
        for collection, fields in indexed_fields.items():
            for field in fields:
                safe = f"idx_{collection}_{field}".replace("-", "_")
                path = f"$.{field}"
                try:
                    self.conn.execute(
                        f"CREATE INDEX IF NOT EXISTS {safe} ON documents(json_extract(data, '{path}')) WHERE collection='{collection}'"
                    )
                except sqlite3.OperationalError as exc:
                    if "json_extract" in str(exc).lower() or "no such function" in str(exc).lower():
                        logger.warning("SQLite JSON1 unavailable, skipping JSON indexes")
                        return
                    raise

    def __getattr__(self, name):
        return LocalCollection(self, name)

    def __getitem__(self, name):
        return LocalCollection(self, name)

    def _read_all(self, collection):
        rows = self.conn.execute("SELECT data FROM documents WHERE collection=?", (collection,)).fetchall()
        return [json.loads(row[0]) for row in rows]

    def _upsert(self, collection, doc_id, doc):
        self.conn.execute("INSERT OR REPLACE INTO documents(collection, doc_id, data, updated_at) VALUES (?, ?, ?, ?)", (collection, doc_id, json.dumps(doc, default=str), datetime.now(timezone.utc).isoformat()))
        self.conn.commit()

    def _delete(self, collection, query, multi=True):
        deleted = 0
        for doc in self._read_all(collection):
            if _matches(doc, query):
                self.conn.execute("DELETE FROM documents WHERE collection=? AND doc_id=?", (collection, str(doc.get("_id", doc.get("id")))))
                deleted += 1
                if not multi:
                    break
        self.conn.commit()
        return deleted


def _get_path(doc, path):
    value = doc
    for part in str(path).split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _matches(doc, query):
    for key, expected in (query or {}).items():
        if key == "$and":
            if not all(_matches(doc, q) for q in expected): return False
            continue
        if key == "$or":
            if not any(_matches(doc, q) for q in expected): return False
            continue
        if key == "$nor":
            if any(_matches(doc, q) for q in expected): return False
            continue
        if key == "$expr":
            if not _eval_expr(expected, doc): return False
            continue
        actual = _get_path(doc, key)
        if isinstance(expected, dict):
            for op, val in expected.items():
                if op == "$options": continue
                if op == "$eq" and actual != val: return False
                if op == "$in" and actual not in val: return False
                if op == "$ne" and actual == val: return False
                if op == "$exists" and ((actual is not None) != bool(val)): return False
                if op == "$gt" and not (actual is not None and actual > val): return False
                if op == "$gte" and not (actual is not None and actual >= val): return False
                if op == "$lt" and not (actual is not None and actual < val): return False
                if op == "$lte" and not (actual is not None and actual <= val): return False
                if op == "$regex" and not _regex_match(actual, val, expected.get("$options")): return False
        elif actual != expected:
            return False
    return True


def _regex_match(actual, pattern, options=None):
    import re
    flags = re.I if options and "i" in str(options) else 0
    return re.search(str(pattern), str(actual or ""), flags) is not None


def _eval_expr(expr, doc):
    if isinstance(expr, dict):
        if "$and" in expr:
            return all(_eval_expr(item, doc) for item in expr["$and"])
        if "$eq" in expr:
            left, right = expr["$eq"]
            return _eval_value(left, doc) == _eval_value(right, doc)
        if "$gte" in expr:
            left, right = expr["$gte"]
            return _eval_value(left, doc) >= _eval_value(right, doc)
        return bool(_eval_value(expr, doc))
    return bool(_eval_value(expr, doc))


def _eval_value(value, doc):
    if isinstance(value, str) and value.startswith("$"):
        return _get_path(doc, value[1:])
    if isinstance(value, dict):
        if "$ifNull" in value:
            for item in value["$ifNull"]:
                result = _eval_value(item, doc)
                if result is not None:
                    return result
            return None
        if "$add" in value:
            return sum((_eval_value(item, doc) or 0) for item in value["$add"])
        if "$subtract" in value:
            first, second = value["$subtract"]
            return (_eval_value(first, doc) or 0) - (_eval_value(second, doc) or 0)
        if "$max" in value:
            return max((_eval_value(item, doc) or 0) for item in value["$max"])
    return value


def _project(doc, projection):
    result = copy.deepcopy(doc)
    if not projection:
        return result
    excludes = {k for k, v in projection.items() if v == 0}
    includes = {k for k, v in projection.items() if v == 1}
    if includes:
        result = {k: _get_path(doc, k) for k in includes if _get_path(doc, k) is not None}
    for key in excludes:
        result.pop(key, None)
    return result


def _apply_update(doc, update):
    if isinstance(update, list):
        for stage in update:
            if "$set" in stage:
                for k, v in stage["$set"].items():
                    doc[k] = _eval_value(v, doc)
        return
    if not any(str(k).startswith("$") for k in update):
        doc.clear(); doc.update(copy.deepcopy(update)); return
    for k, v in update.get("$set", {}).items(): doc[k] = v
    for k, v in update.get("$setOnInsert", {}).items(): doc.setdefault(k, v)
    for k, v in update.get("$inc", {}).items(): doc[k] = (doc.get(k) or 0) + v
    for k, v in update.get("$max", {}).items(): doc[k] = max(doc.get(k, v), v)
    for k, v in update.get("$push", {}).items(): doc.setdefault(k, []).append(v)
    for k in update.get("$unset", {}): doc.pop(k, None)
