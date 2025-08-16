import base64
import datetime
import decimal
from typing import Optional


def btoa(s: str) -> bytes:
    utf8_data = s.encode("utf-8")
    base64_data = base64.b64encode(utf8_data)
    return base64_data


def date_from_string(date_string: str) -> datetime.date:
    return datetime.datetime.strptime(date_string, "%Y%m%d").date()


def pic_to_decimal(number: str, integral_places: int) -> Optional[decimal.Decimal]:
    integral_part = number[:integral_places]
    decimal_part = number[integral_places:]
    try:
        return decimal.Decimal(integral_part + "." + decimal_part)
    except decimal.InvalidOperation as _:
        return None


def pic11v99(number: str) -> Optional[decimal.Decimal]:
    return pic_to_decimal(number, 11)


def pic16v99(number: str) -> Optional[decimal.Decimal]:
    return pic_to_decimal(number, 16)


def pic7v06(number: str) -> Optional[decimal.Decimal]:
    return pic_to_decimal(number, 7)
