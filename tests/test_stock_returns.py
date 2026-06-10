import os
import unittest

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from server import _available_stock, _return_status


class PurchaseReturnStockRegressionTest(unittest.TestCase):
    def assert_stock_and_status(self, purchased, sold, returned, stock, status):
        medicine = {
            "purchased_units": purchased,
            "sold_units": sold,
            "purchase_return_units": returned,
        }
        self.assertEqual(_available_stock(medicine), stock)
        self.assertEqual(_return_status(medicine), status)

    def test_sold_and_returned_stock_is_sold_out(self):
        self.assert_stock_and_status(5, 3, 2, 0, "Sold Out")

    def test_fully_returned_unsold_stock_is_returned(self):
        self.assert_stock_and_status(5, 0, 5, 0, "Returned")

    def test_partially_returned_stock_remains_available(self):
        self.assert_stock_and_status(5, 1, 1, 3, "Partially Returned")

    def test_fully_sold_stock_is_sold_out(self):
        self.assert_stock_and_status(5, 5, 0, 0, "Sold Out")

    def test_free_and_legacy_return_quantity_fields_are_supported(self):
        medicine = {
            "purchased_quantity": 5,
            "free_quantity": 2,
            "sold_quantity": 3,
            "purchase_return_quantity": 2,
        }
        self.assertEqual(_available_stock(medicine), 2)
        self.assertEqual(_return_status(medicine), "Partially Returned")


if __name__ == "__main__":
    unittest.main()
