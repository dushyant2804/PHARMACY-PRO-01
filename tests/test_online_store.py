from datetime import datetime, timezone

from online_store import build_order_id, make_whatsapp_url, normalize_customer_name


def test_order_id_matches_customer_date_and_sequence():
    created = datetime(2026, 9, 9, 10, 30, tzinfo=timezone.utc)
    assert build_order_id("Dushyant", "Bishnoi", created, 1) == "DUBI09092601"
    assert build_order_id("Dushyant", "Bishnoi", created, 12) == "DUBI09092612"


def test_name_normalization_accepts_full_name():
    first, last, display = normalize_customer_name("", "", "Dushyant Bishnoi")
    assert (first, last, display) == ("Dushyant", "Bishnoi", "Dushyant Bishnoi")


def test_whatsapp_url_contains_order_id_and_customer():
    order = {
        "order_id": "DUBI09092601",
        "customer": {
            "name": "Dushyant Bishnoi",
            "mobile": "9876543210",
            "address": "House 24",
        },
        "items": [{"medicine_name": "Telma 40", "quantity": 2}],
    }
    url = make_whatsapp_url("919876543210", order)
    assert url.startswith("https://wa.me/919876543210?text=")
    assert "DUBI09092601" in url
