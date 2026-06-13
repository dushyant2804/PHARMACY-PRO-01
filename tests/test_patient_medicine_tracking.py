import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from server import RegularPatient, _get_patient_stock_alerts, _link_patient_medicine, add_patient, list_patients


class Cursor:
    def __init__(self, rows): self.rows = rows
    async def to_list(self, length): return self.rows[:length]


class Collection:
    def __init__(self, rows): self.rows = rows; self.inserted = None
    def find(self, *args, **kwargs): return Cursor(self.rows)
    async def insert_one(self, row): self.inserted = dict(row); self.rows.append(row)


class PatientMedicineTrackingTest(unittest.IsolatedAsyncioTestCase):
    def patient(self, **updates):
        values = dict(name="Ada", age=45, phone="555", medicine_name="Metformin", duration_days=30,
                      last_refill_date="2026-06-01", condition="Diabetes")
        values.update(updates)
        return RegularPatient(**values)

    async def test_patient_medicine_is_linked_with_inventory_snapshot_without_stock_deduction(self):
        medicine = {"id": "m1", "medicine_key": "metformin::B1", "name": "Metformin", "batch_no": "B1",
                    "expiry_date": "2028-01-01", "mrp": 12.345, "purchased_units": 20, "sold_units": 2}
        fake_db = SimpleNamespace(medicines=Collection([medicine]), regular_patients=Collection([]))
        with patch("server.db", fake_db):
            await add_patient(self.patient(batch="B1"), {"id": "u"})
        self.assertEqual(fake_db.regular_patients.inserted["medicine_id"], "m1")
        self.assertEqual(fake_db.regular_patients.inserted["medicine_key"], "metformin::B1")
        self.assertEqual(fake_db.regular_patients.inserted["current_mrp"], 12.35)
        self.assertEqual(fake_db.regular_patients.inserted["expiry"], "2028-01-01")
        self.assertEqual(medicine["sold_units"], 2)
        self.assertEqual(medicine["purchased_units"], 20)

    async def test_low_stock_alert_lists_affected_patients(self):
        medicines = Collection([{"id": "m1", "name": "Metformin", "batch_no": "B1", "purchased_units": 5,
                                 "sold_units": 4, "low_stock_threshold": 5}])
        patients = Collection([{"name": "Ada", "phone": "555", "medicine_id": "m1", "medicine_name": "Metformin"}])
        with patch("server.db", SimpleNamespace(medicines=medicines, regular_patients=patients)):
            alerts = await _get_patient_stock_alerts()
        self.assertEqual(alerts[0]["stock_status"], "critical")
        self.assertEqual(alerts[0]["affected_patient_count"], 1)
        self.assertEqual(alerts[0]["patient_names"], ["Ada"])

    async def test_legacy_patient_records_remain_readable_and_link_by_name(self):
        legacy = {"name": "Ada", "phone": "555", "medicine_name": "Metformin", "duration_days": 30}
        fake_db = SimpleNamespace(regular_patients=Collection([legacy]))
        with patch("server.db", fake_db):
            result = await list_patients({"id": "u"})
        self.assertEqual(result, [legacy])

    async def test_unmatched_medicine_preserves_entered_snapshot(self):
        fake_db = SimpleNamespace(medicines=Collection([]))
        entered = self.patient(current_mrp=10.129, dosage="1 tablet", frequency="daily").model_dump()
        with patch("server.db", fake_db):
            linked = await _link_patient_medicine(entered)
        self.assertEqual(linked["current_mrp"], 10.13)
        self.assertEqual(linked["dosage"], "1 tablet")


if __name__ == "__main__": unittest.main()
