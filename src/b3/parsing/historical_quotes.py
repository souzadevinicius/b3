from __future__ import annotations
import collections
import contextlib
from decimal import Decimal
import enum
import functools
import os
from datetime import date
from typing import Dict, Union, Any, Tuple, Generator, TextIO, cast
from b3.datatypes import (
    DailyBulletinType,
    DailyBulletin,
    MarketType,
    ContractCorrection,
    QuoteSize,
    Quotes,
)
from b3.datatypes.specification import Specification
from b3.utils import date_from_string, pic11v99, pic16v99, pic7v06

__all__ = ["historical_quotes_reader"]


class _RegistryType(enum.Enum):
    HEADER = "00"
    QUOTES = "01"
    TRAILER = "99"


_QuotesField = collections.namedtuple("QuotesField", ["name", "size", "factory"])


@functools.lru_cache
def _quotes_fields():
    return (
        _QuotesField("EXCDAT", 8, date_from_string),
        _QuotesField("CODBDI", 2, lambda value: DailyBulletinType(int(value))),
        _QuotesField("CODNEG", 12, str),
        _QuotesField("TPMERC", 3, MarketType),
        _QuotesField("NOMRES", 12, str),
        _QuotesField("ESPECI", 10, str),
        _QuotesField("PRAZOT", 3, lambda value: int(value) if value != "" else None),
        _QuotesField("MODREF", 4, str),
        _QuotesField("PREABE", 13, pic11v99),
        _QuotesField("PREMAX", 13, pic11v99),
        _QuotesField("PREMIN", 13, pic11v99),
        _QuotesField("PREMED", 13, pic11v99),
        _QuotesField("PREULT", 13, pic11v99),
        _QuotesField("PREOFC", 13, pic11v99),
        _QuotesField("PREOFV", 13, pic11v99),
        _QuotesField("TOTNEG", 5, int),
        _QuotesField("QUATOT", 18, int),
        _QuotesField("VOLTOT", 18, pic16v99),
        _QuotesField("PREEXE", 13, pic11v99),
        _QuotesField(
            "INDOPC",
            1,
            lambda value: ContractCorrection(int(value)) if int(value) != 0 else None,
        ),
        _QuotesField("DATVEN", 8, date_from_string),
        _QuotesField("FATCOT", 7, lambda value: QuoteSize(int(value))),
        _QuotesField("PTOEXE", 13, pic7v06),
        _QuotesField("CODISI", 12, str),
        _QuotesField("DISMES", 3, int),
    )


def _parse_header_line(_: str) -> Dict:
    return {}  # Should return anything?


def _parse_quotes_line(line: str) -> Dict[str, Any]:
    values = {}
    pos = 0

    for field_name, field_size, factory in _quotes_fields():
        stop = pos + field_size

        try:
            value = line[pos:stop]
            values[field_name] = factory(value.strip())
        except (IndexError, ValueError) as exc:
            print(f"failed to read field '{field_name}': {exc}")
            # raise
            # continue
        else:
            pos = stop

    return values


def _parse_trailer_line(_: str) -> Dict:
    return {}  # Should return anything?


def _parse_line(line: str) -> Tuple[_RegistryType, Dict[str, Any]]:
    registry_type = _RegistryType(line[0:2])
    leftover_line = line[2:]

    if registry_type == _RegistryType.HEADER:
        return registry_type, _parse_header_line(leftover_line)

    if registry_type == _RegistryType.QUOTES:
        return registry_type, _parse_quotes_line(leftover_line)

    if registry_type == _RegistryType.TRAILER:
        return registry_type, _parse_trailer_line(leftover_line)

    raise ValueError(f"unknown registry type {registry_type}")


def _make_quotes(values: Dict[str, Any]) -> Quotes:
    return Quotes(
        open=values["PREABE"],
        high=values["PREMAX"],
        low=values["PREMIN"],
        average=values["PREMED"],
        close=values["PREULT"],
        best_ask=values["PREOFC"],
        best_bid=values["PREOFV"],
    )


def _make_daily_bulleting(values: Dict[str, Any]) -> DailyBulletin:
    return DailyBulletin(
        exchange_date=cast(date, values.get("EXCDAT")),
        type=cast(DailyBulletinType, values.get("CODBDI")),
        isin=cast(str, values.get("CODISI")),
        ticker=cast(str, values.get("CODNEG")),
        market_type=cast(MarketType, values.get("TPMERC")),
        company_short_name=cast(str, values.get("NOMRES")),
        especification=cast(Specification, values.get("ESPECI")),
        forward_market_remaining_days=cast(int, values.get("PRAZOT")),
        reference_currency=cast(str, values.get("MODREF")),
        quotes=_make_quotes(values),
        total_trade_market=cast(int, values.get("TOTNEG")),
        total_trade_count=cast(int, values.get("QUATOT")),
        total_trade_volume=cast(Decimal, values.get("VOLTOT")),
        strike_price=cast(Decimal, values.get("PREEXE")),
        strike_price_correction_type=cast(ContractCorrection, values.get("INDOPC")),
        maturity_date=cast(date, values.get("DATVEN")),
        quote_size=cast(QuoteSize, values.get("FATCOT")),
        strike_price_points=cast(Decimal, values.get("PTOEXE")),
        distribution_number=cast(int, values.get("DISMES")),
    )


def _reader(stream: TextIO) -> Generator[DailyBulletin, None, None]:
    for line in stream:
        registry_type, values = _parse_line(line)

        if registry_type == _RegistryType.QUOTES:
            try:
                yield _make_daily_bulleting(values)
            except (ValueError, KeyError) as _:
                continue


@contextlib.contextmanager
def historical_quotes_reader(file: Union[str, os.PathLike, TextIO]):
    if isinstance(file, (str, os.PathLike)):
        f = open(file, mode="r", encoding="ascii")
        try:
            reader = _reader(f)
            yield reader
        finally:
            f.close()
    else:
        reader = _reader(file)
        yield reader
