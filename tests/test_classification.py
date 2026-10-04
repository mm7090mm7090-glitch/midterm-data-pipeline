import json

from src.elt_pipeline import process_record


def make_base_record():
    return {
        "order_id": "ORDER-1",
        "order_date": "2025-01-19",
        "status": "مؤكد",
        "customer_id": "CUST-1",
        "customer_name": "Mohammed",
        "customer_phone": "771234567",
        "customer_email": "test@example.com",
        "city": "Sanaa",
        "district": "Center",
        "delivery_type": "Normal",
        "delivery_cost": "10",
        "payment_method": "Cash",
        "payment_status": "تم الدفع",
        "payment_amount": "110",
        "currency": "YER",
        "total_amount": "110",
        "items_json": json.dumps(
            [
                {
                    "sku": "SKU-1",
                    "qty": 1,
                    "unit_price": 100,
                    "total": 100,
                }
            ]
        ),
    }


def test_valid_record():
    record = make_base_record()

    result = process_record(record)

    assert result["quality_status"] == "valid"
    assert result["error_codes"] == []


def test_corrected_record():
    record = make_base_record()
    record["total_amount"] = "110,000"

    result = process_record(record)

    assert result["quality_status"] == "corrected"
    assert len(result["corrections"]) > 0


def test_missing_order_id_goes_to_quarantine():
    record = make_base_record()
    record["order_id"] = ""

    result = process_record(record)

    assert result["quality_status"] == "quarantined"
    assert "ID_ORDER_MISSING" in result["error_codes"]


def test_corrupted_json_goes_to_quarantine():
    record = make_base_record()
    record["items_json"] = "{bad json}"

    result = process_record(record)

    assert result["quality_status"] == "quarantined"
    assert "JSON_ITEMS_CORRUPTED" in result["error_codes"]


def test_negative_value_goes_to_quarantine():
    record = make_base_record()

    record["items_json"] = json.dumps(
        [
            {
                "sku": "SKU-1",
                "qty": -1,
                "unit_price": 100,
                "total": -100,
            }
        ]
    )

    result = process_record(record)

    assert result["quality_status"] == "quarantined"
    assert "VALUE_NEGATIVE_AMBIGUOUS" in result["error_codes"]