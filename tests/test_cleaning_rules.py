import json

from src.quality_rules import (
    normalize_arabic_digits,
    remove_thousands_separators,
    normalize_currency,
    normalize_phone,
    normalize_email,
    normalize_date,
    normalize_text_value,
    recalculate_total_amount,
)


def test_normalize_arabic_digits():
    value, correction = normalize_arabic_digits(
        "١٢٣٤٥",
        "total_amount",
    )

    assert value == "12345"
    assert correction["rule_code"] == "ARABIC_DIGITS_TO_LATIN"


def test_remove_thousands_separators():
    value, correction = remove_thousands_separators(
        "135,000.00",
        "total_amount",
    )

    assert value == "135000.00"
    assert correction["rule_code"] == "REMOVE_THOUSANDS_SEPARATOR"


def test_normalize_currency():
    value, correction = normalize_currency("ريال يمني")

    assert value == "YER"
    assert correction["rule_code"] == "NORMALIZE_CURRENCY_YER"


def test_normalize_phone():
    value, correction = normalize_phone(
        "00967 771234567"
    )

    assert value == "+967771234567"
    assert correction["rule_code"] == "NORMALIZE_PHONE_FORMAT"


def test_normalize_email():
    value, correction = normalize_email(
        "student@@example..com"
    )

    assert value == "student@example.com"
    assert correction["rule_code"] == "EMAIL_REPEATED_SYMBOLS"


def test_normalize_date():
    value, correction = normalize_date(
        "19/01/2025"
    )

    assert value == "2025-01-19"
    assert correction["rule_code"] == "NORMALIZE_DATE_ISO"


def test_normalize_text_value():
    value, correction = normalize_text_value(
        "  مؤكدة  ",
        "status",
    )

    assert value == "مؤكد"
    assert correction["rule_code"] == "NORMALIZE_TEXT_VALUE"


def test_recalculate_total_amount():
    items = json.dumps(
        [
            {"total": 100},
            {"total": 50},
        ]
    )

    value, correction = recalculate_total_amount(
        items_json=items,
        delivery_cost="10",
        total_amount="140",
    )

    assert value == "160.0"
    assert correction["rule_code"] == "RECALCULATE_TOTAL_AMOUNT"


def test_invalid_date_is_not_guessed():
    value, correction = normalize_date(
        "31/02/2025"
    )

    assert value == "31/02/2025"
    assert correction is None

def test_normalize_price_words():
    from src.quality_rules import normalize_price_words

    value, correction = normalize_price_words(
        "خمسة آلاف",
        "total_amount",
    )

    assert value == "5000"
    assert correction["rule_code"] == "PRICE_WORD_TO_NUMBER"


def test_unknown_price_words_are_not_guessed():
    from src.quality_rules import normalize_price_words

    value, correction = normalize_price_words(
        "مبلغ غير معروف",
        "total_amount",
    )

    assert value == "مبلغ غير معروف"
    assert correction is None
