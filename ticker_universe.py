"""Resolve the valuation universe from defaults, an R2-backed registry, or explicit CLI input."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


REGISTRY_SCHEMA_VERSION = "1.0"
SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9.-]{0,14}$")

# Yahoo uses the current listed symbol, while the valuation cache still uses
# the legacy `SQ` key. Block changed NYSE `SQ` to `XYZ` effective 2025-01-21;
# keeping this mapping at the market-data boundary avoids an expensive FMP
# backfill before the existing SQ cache can be migrated deliberately.
YAHOO_SYMBOL_ALIASES = {
    "SQ": "XYZ",
}

# These symbols no longer have a live Yahoo price series and must not enter a
# release. Keep the rejection explicit so a stale registry request is visible
# instead of silently producing incomplete coverage.
RETIRED_SYMBOLS = {
    "WBA",
}

# This is the maintained baseline; requested coverage is appended from R2 at runtime.
DEFAULT_TICKERS = (
    "AAPL", "TSLA", "AMZN", "MSFT", "NVDA", "GOOGL", "META", "NFLX", "JPM", "V",
    "BAC", "PYPL", "DIS", "T", "PFE", "COST", "INTC", "KO", "TGT", "NKE",
    "BA", "BABA", "XOM", "WMT", "GE", "CSCO", "VZ", "JNJ", "CVX", "PLTR",
    "SQ", "SHOP", "SBUX", "SOFI", "HOOD", "RBLX", "SNAP", "AMD", "UBER", "FDX",
    "ABBV", "ETSY", "MRNA", "LMT", "GM", "F", "LCID", "CCL", "DAL", "UAL",
    "AAL", "TSM", "SONY", "ET", "COIN", "RIVN", "RIOT", "CPRX", "NOK",
    "ROKU", "BIDU", "DOCU", "ZM", "PINS", "TLRY", "MGM",
    "NIO", "C", "GS", "WFC", "ADBE", "PEP", "UNH", "CARR", "SIRI", "FUBO", "RKT",
)


class UniverseValidationError(ValueError):
    pass


def normalize_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not SYMBOL_RE.fullmatch(symbol):
        raise UniverseValidationError(f"Invalid ticker symbol: {value!r}")
    if symbol in RETIRED_SYMBOLS:
        raise UniverseValidationError(f"Ticker {symbol} is retired/delisted; remove it from coverage registry")
    return symbol


def yahoo_symbol(value: Any) -> str:
    """Return the current Yahoo Finance symbol for a validated ticker."""
    symbol = normalize_symbol(value)
    return YAHOO_SYMBOL_ALIASES.get(symbol, symbol)


def deduplicate_symbols(values: list[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = normalize_symbol(value)
        if symbol not in seen:
            seen.add(symbol)
            result.append(symbol)
    return result


def load_registry_symbols(path: Path | None) -> list[str]:
    if path is None:
        return []
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise UniverseValidationError(f"Cannot read coverage registry {path}: {error}") from error
    if not isinstance(body, dict) or body.get("schemaVersion") != REGISTRY_SCHEMA_VERSION:
        raise UniverseValidationError(f"Coverage registry {path} has an invalid schema version")
    records = body.get("symbols")
    if not isinstance(records, list):
        raise UniverseValidationError(f"Coverage registry {path} must contain a symbols array")
    values: list[Any] = []
    for record in records:
        if isinstance(record, str):
            values.append(record)
        elif isinstance(record, dict) and record.get("state", "queued") in {"queued", "published"}:
            values.append(record.get("symbol"))
        else:
            raise UniverseValidationError(f"Coverage registry {path} contains an invalid symbol record")
    return deduplicate_symbols(values)


def resolve_tickers(explicit_symbols: str | None = None, registry_path: Path | None = None) -> list[str]:
    if explicit_symbols is not None:
        values = [value.strip() for value in explicit_symbols.split(",") if value.strip()]
        if not values:
            raise UniverseValidationError("--symbols must contain at least one ticker")
        return deduplicate_symbols(values)
    return deduplicate_symbols([*DEFAULT_TICKERS, *load_registry_symbols(registry_path)])
