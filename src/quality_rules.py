import re
import json
from datetime import datetime


ARABIC_DIGITS_MAP = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹",
    "01234567890123456789",
)


def make_correction(
    field,
    original_value,
    corrected_value,
    rule_code,
):
    return {
        "field": field,
        "original_value": original_value,
        "corrected_value": corrected_value,
        "rule_code": rule_code,
    }


# =========================================================
# 1. Arabic / Persian Digits
# =========================================================

def normalize_arabic_digits(value, field):
    if value is None:
        return value, None

    original_value = value
    value_as_text = str(value)

    corrected_value = value_as_text.translate(
        ARABIC_DIGITS_MAP
    )

    if corrected_value != value_as_text:
        correction = make_correction(
            field=field,
            original_value=original_value,
            corrected_value=corrected_value,
            rule_code="ARABIC_DIGITS_TO_LATIN",
        )

        return corrected_value, correction

    return value, None


# =========================================================
# 2. Thousands Separators
# =========================================================

def remove_thousands_separators(value, field):
    if value is None:
        return value, None

    original_value = value
    value_as_text = str(value).strip()

    numeric_pattern = (
        r"^[+-]?\d{1,3}(,\d{3})+(\.\d+)?$"
    )

    if not re.fullmatch(
        numeric_pattern,
        value_as_text,
    ):
        return value, None

    corrected_value = value_as_text.replace(
        ",",
        "",
    )

    correction = make_correction(
        field=field,
        original_value=original_value,
        corrected_value=corrected_value,
        rule_code="REMOVE_THOUSANDS_SEPARATOR",
    )

    return corrected_value, correction


# =========================================================
# 3. Currency
# =========================================================

def normalize_currency(
    value,
    field="currency",
):
    if value is None:
        return value, None

    original_value = value
    value_as_text = str(value).strip()

    yer_values = {
        "YER",
        "yer",
        "ريال",
        "ريال يمني",
        "ريال يمنى",
    }

    if value_as_text in yer_values:
        corrected_value = "YER"

        if corrected_value != value_as_text:
            correction = make_correction(
                field=field,
                original_value=original_value,
                corrected_value=corrected_value,
                rule_code="NORMALIZE_CURRENCY_YER",
            )

            return corrected_value, correction

        return corrected_value, None

    return value, None


# =========================================================
# 4. Price Words
# =========================================================

def normalize_price_words(value, field):
    if value is None:
        return value, None

    original_value = value

    value_as_text = " ".join(
        str(value).strip().split()
    )

    price_words = {
        "\u0623\u0644\u0641": "1000",
        "\u0627\u0644\u0641": "1000",

        "\u0623\u0644\u0641\u0627\u0646": "2000",
        "\u0627\u0644\u0641\u0627\u0646": "2000",
        "\u0623\u0644\u0641\u064a\u0646": "2000",
        "\u0627\u0644\u0641\u064a\u0646": "2000",

        "\u062b\u0644\u0627\u062b\u0629 \u0622\u0644\u0627\u0641": "3000",
        "\u062b\u0644\u0627\u062b\u0647 \u0622\u0644\u0627\u0641": "3000",

        "\u0623\u0631\u0628\u0639\u0629 \u0622\u0644\u0627\u0641": "4000",
        "\u0627\u0631\u0628\u0639\u0629 \u0622\u0644\u0627\u0641": "4000",
        "\u0623\u0631\u0628\u0639\u0647 \u0622\u0644\u0627\u0641": "4000",

        "\u062e\u0645\u0633\u0629 \u0622\u0644\u0627\u0641": "5000",
        "\u062e\u0645\u0633\u0647 \u0622\u0644\u0627\u0641": "5000",

        "\u0633\u062a\u0629 \u0622\u0644\u0627\u0641": "6000",
        "\u0633\u062a\u0647 \u0622\u0644\u0627\u0641": "6000",

        "\u0633\u0628\u0639\u0629 \u0622\u0644\u0627\u0641": "7000",
        "\u0633\u0628\u0639\u0647 \u0622\u0644\u0627\u0641": "7000",

        "\u062b\u0645\u0627\u0646\u064a\u0629 \u0622\u0644\u0627\u0641": "8000",
        "\u062b\u0645\u0627\u0646\u064a\u0647 \u0622\u0644\u0627\u0641": "8000",

        "\u062a\u0633\u0639\u0629 \u0622\u0644\u0627\u0641": "9000",
        "\u062a\u0633\u0639\u0647 \u0622\u0644\u0627\u0641": "9000",

        "\u0639\u0634\u0631\u0629 \u0622\u0644\u0627\u0641": "10000",
        "\u0639\u0634\u0631\u0647 \u0622\u0644\u0627\u0641": "10000",
    }

    corrected_value = price_words.get(
        value_as_text
    )

    if corrected_value is None:
        return value, None

    correction = make_correction(
        field=field,
        original_value=original_value,
        corrected_value=corrected_value,
        rule_code="PRICE_WORD_TO_NUMBER",
    )

    return corrected_value, correction


# =========================================================
# 5. Phone
# =========================================================

def normalize_phone(
    value,
    field="customer_phone",
):
    if value is None:
        return value, None

    original_value = value
    value_as_text = str(value).strip()

    if not re.fullmatch(
        r"[+\d\s\-()]+",
        value_as_text,
    ):
        return value, None

    cleaned = re.sub(
        r"[\s\-()]",
        "",
        value_as_text,
    )

    corrected_value = None

    # +967 77 123 4567
    if cleaned.startswith("+967"):
        local_number = cleaned[4:]

        if (
            local_number.isdigit()
            and len(local_number) == 9
        ):
            corrected_value = (
                "+967" + local_number
            )

    # 00967 77 123 4567
    elif cleaned.startswith("00967"):
        local_number = cleaned[5:]

        if (
            local_number.isdigit()
            and len(local_number) == 9
        ):
            corrected_value = (
                "+967" + local_number
            )

    # 967771234567
    elif (
        cleaned.startswith("967")
        and len(cleaned) == 12
    ):
        corrected_value = (
            "+" + cleaned
        )

    # Local 9-digit number.
    elif (
        cleaned.isdigit()
        and len(cleaned) == 9
    ):
        corrected_value = cleaned

    if (
        corrected_value is not None
        and corrected_value != value_as_text
    ):
        correction = make_correction(
            field=field,
            original_value=original_value,
            corrected_value=corrected_value,
            rule_code="NORMALIZE_PHONE_FORMAT",
        )

        return corrected_value, correction

    return value, None


# =========================================================
# 6. Email
# =========================================================

def normalize_email(
    value,
    field="customer_email",
):
    if value is None:
        return value, None

    original_value = value
    value_as_text = str(value).strip()

    corrected_value = value_as_text

    corrected_value = re.sub(
        r"@{2,}",
        "@",
        corrected_value,
    )

    if corrected_value.count("@") != 1:
        return value, None

    local_part, domain_part = (
        corrected_value.split(
            "@",
            1,
        )
    )

    domain_part = re.sub(
        r"\.{2,}",
        ".",
        domain_part,
    )

    corrected_value = (
        f"{local_part}@{domain_part}"
    )

    email_pattern = (
        r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+"
        r"@[A-Za-z0-9-]+"
        r"(?:\.[A-Za-z0-9-]+)+$"
    )

    if not re.fullmatch(
        email_pattern,
        corrected_value,
    ):
        return value, None

    if corrected_value != value_as_text:
        correction = make_correction(
            field=field,
            original_value=original_value,
            corrected_value=corrected_value,
            rule_code="EMAIL_REPEATED_SYMBOLS",
        )

        return corrected_value, correction

    return value, None


# =========================================================
# 7. Date
# =========================================================

def normalize_date(
    value,
    field="order_date",
):
    if value is None:
        return value, None

    original_value = value
    value_as_text = str(value).strip()

    # Already-valid ISO datetime:
    # 2026-06-14T18:01:00
    try:
        datetime.strptime(
            value_as_text,
            "%Y-%m-%dT%H:%M:%S",
        )

        return value_as_text, None

    except ValueError:
        pass

    # Valid ISO datetime with a space:
    # 2026-06-14 18:01:00
    try:
        parsed_date = datetime.strptime(
            value_as_text,
            "%Y-%m-%d %H:%M:%S",
        )

        corrected_value = (
            parsed_date.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        )

        correction = make_correction(
            field=field,
            original_value=original_value,
            corrected_value=corrected_value,
            rule_code="NORMALIZE_DATE_ISO",
        )

        return corrected_value, correction

    except ValueError:
        pass

    # Already-valid ISO date:
    # 2026-06-14
    try:
        datetime.strptime(
            value_as_text,
            "%Y-%m-%d",
        )

        return value_as_text, None

    except ValueError:
        pass

    accepted_formats = [
        (
            "%d/%m/%Y",
            "%Y-%m-%d",
        ),
        (
            "%Y/%m/%d",
            "%Y-%m-%d",
        ),
        (
            "%d-%m-%Y",
            "%Y-%m-%d",
        ),

        # Doctor test:
        # 14-06-2026 18:01:00
        (
            "%d-%m-%Y %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ),

        (
            "%d/%m/%Y %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ),
    ]

    for (
        date_format,
        output_format,
    ) in accepted_formats:

        try:
            parsed_date = datetime.strptime(
                value_as_text,
                date_format,
            )

            corrected_value = (
                parsed_date.strftime(
                    output_format
                )
            )

            correction = make_correction(
                field=field,
                original_value=original_value,
                corrected_value=corrected_value,
                rule_code="NORMALIZE_DATE_ISO",
            )

            return corrected_value, correction

        except ValueError:
            continue

    # Impossible dates remain unchanged.
    # Validation will send them to Quarantine.
    return value, None


# =========================================================
# 8. Text / Whitespace / Synonyms
# =========================================================

def normalize_text_value(value, field):
    if value is None:
        return value, None

    original_value = value
    value_as_text = str(value)

    cleaned_value = " ".join(
        value_as_text.split()
    )

    standard_values = {
        "payment_status": {
            "مدفوع": "تم الدفع",
            "تم الدفع": "تم الدفع",
            "غير مدفوع": "غير مدفوع",
        },

        "status": {
            "مؤكد": "مؤكد",
            "مؤكدة": "مؤكد",
        },
    }

    if field in standard_values:
        cleaned_value = (
            standard_values[field].get(
                cleaned_value,
                cleaned_value,
            )
        )

    if cleaned_value != value_as_text:
        correction = make_correction(
            field=field,
            original_value=original_value,
            corrected_value=cleaned_value,
            rule_code="NORMALIZE_TEXT_VALUE",
        )

        return cleaned_value, correction

    return value, None


# =========================================================
# 9. Quantity String Inside items_json
# =========================================================

def normalize_items_quantity(
    items_json,
    field="items_json",
):
    if items_json is None:
        return items_json, None

    original_value = items_json

    try:
        items = json.loads(
            items_json
        )

    except (
        json.JSONDecodeError,
        TypeError,
    ):
        return items_json, None

    if not isinstance(items, list):
        return items_json, None

    changed = False

    for item in items:
        if not isinstance(item, dict):
            continue

        qty = item.get("qty")

        # Only deterministic integer strings:
        # "2" -> 2
        #
        # Negative strings are deliberately NOT
        # corrected here.
        if (
            isinstance(qty, str)
            and re.fullmatch(
                r"\d+",
                qty.strip(),
            )
        ):
            item["qty"] = int(
                qty.strip()
            )

            changed = True

    if not changed:
        return items_json, None

    corrected_value = json.dumps(
        items,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    correction = make_correction(
        field=field,
        original_value=original_value,
        corrected_value=corrected_value,
        rule_code="qty_as_string_in_items",
    )

    return corrected_value, correction


# =========================================================
# 10. Recalculate Total Amount
# =========================================================

def recalculate_total_amount(
    items_json,
    delivery_cost,
    total_amount,
    field="total_amount",
):
    if items_json is None:
        return total_amount, None

    original_value = total_amount

    try:
        items = json.loads(
            items_json
        )

    except (
        json.JSONDecodeError,
        TypeError,
    ):
        return total_amount, None

    if (
        not isinstance(items, list)
        or len(items) == 0
    ):
        return total_amount, None

    try:
        item_total_sum = 0.0

        for item in items:
            if not isinstance(
                item,
                dict,
            ):
                return total_amount, None

            item_total = item.get(
                "total"
            )

            if item_total is None:
                return total_amount, None

            item_total_sum += float(
                item_total
            )

        delivery_value = float(
            delivery_cost
        )

        current_total = float(
            total_amount
        )

    except (
        TypeError,
        ValueError,
    ):
        return total_amount, None

    calculated_total = (
        item_total_sum
        + delivery_value
    )

    if abs(
        calculated_total
        - current_total
    ) < 0.001:
        return total_amount, None

    corrected_value = str(
        calculated_total
    )

    correction = make_correction(
        field=field,
        original_value=original_value,
        corrected_value=corrected_value,
        rule_code="RECALCULATE_TOTAL_AMOUNT",
    )

    return corrected_value, correction