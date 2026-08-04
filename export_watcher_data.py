"""Publish a small, versioned Stock Watcher dataset from local pipeline outputs.

This module deliberately reads completed valuation/earnings artifacts only.  It never
calls FMP or Yahoo, so a public reader cannot trigger a financial-data refresh.
"""

from __future__ import annotations

import json
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ticker_universe import UniverseValidationError, deduplicate_symbols


SCHEMA_VERSION = "1.0"
MAX_EXPORT_BYTES = 64 * 1024 * 1024
METRICS = ("pe", "fcf", "ps")
WINDOWS = ("1Y", "2Y", "3Y", "5Y")
FINANCIAL_FIELDS = (
    "revenue",
    "netIncome",
    "eps",
    "operatingCashFlow",
    "freeCashFlow",
    "revenue_qoq",
    "revenue_yoy",
    "netIncome_qoq",
    "netIncome_yoy",
    "eps_qoq",
    "eps_yoy",
    "operatingCashFlow_qoq",
    "operatingCashFlow_yoy",
)


class ExportValidationError(ValueError):
    pass


def read_json(path: Path) -> Any:
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise ExportValidationError(f"Cannot read {path}: {error}") from error


def write_json(path: Path, body: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def encoded_json_size(body: Any) -> int:
    """Measure exactly what write_json would publish, before creating any artifacts."""
    return len(json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def normalize_timestamp(value: Any, context: str) -> str:
    """Return an RFC 3339 UTC timestamp; Cloudflare must not parse local time."""
    text = str(value or "").strip()
    if not text:
        raise ExportValidationError(f"{context} has no generated timestamp")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise ExportValidationError(f"{context} has an invalid generated timestamp: {text}") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def require_symbol(value: Any, path: Path) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or not symbol.replace("-", "").replace(".", "").isalnum():
        raise ExportValidationError(f"{path} has an invalid ticker")
    return symbol


def latest_valuation_point(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise ExportValidationError("Valuation data has no history")
    ordered = sorted(rows, key=lambda row: str(row.get("date", "")))
    latest = ordered[-1]
    if not isinstance(latest.get("date"), str) or not isinstance(latest.get("valuation"), dict):
        raise ExportValidationError("Valuation history has an invalid final row")
    return latest


def export_valuation(summary: dict[str, Any], symbol: str, metric: str, window: str) -> dict[str, Any]:
    rows = summary.get("data")
    if not isinstance(rows, list):
        raise ExportValidationError(f"{symbol} valuation data is not a list")
    ordered = sorted((row for row in rows if isinstance(row, dict)), key=lambda row: str(row.get("date", "")))
    if not ordered:
        raise ExportValidationError(f"{symbol} valuation data is empty")
    points: list[dict[str, Any]] = []
    for row in ordered[-252:]:
        bands = row.get("valuation", {}).get(window, {}).get(metric)
        if not isinstance(bands, dict) or not isinstance(row.get("date"), str):
            continue
        points.append({"date": row["date"], "price": row.get("price"), "bands": {key: bands.get(key) for key in ("mean", "up1", "up2", "down1", "down2")}})
    if not points:
        raise ExportValidationError(f"{symbol} has no {metric}/{window} valuation points")
    latest = points[-1]
    return {
        "schemaVersion": SCHEMA_VERSION,
        "source": "ValuationCalculation hybrid valuation model",
        "symbol": symbol,
        "generatedAt": normalize_timestamp(summary.get("last_updated"), f"{symbol} valuation"),
        "dataAsOf": latest["date"],
        "metric": metric,
        "window": window,
        "latest": latest,
        "points": points,
    }


def export_financials(rows: Any, symbol: str, generated_at: str) -> dict[str, Any]:
    if not isinstance(rows, list):
        raise ExportValidationError(f"{symbol} earnings report is not a list")
    normalized: list[dict[str, Any]] = []
    source_metadata: dict[str, Any] | None = None
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("date"), str):
            continue
        source = str(row.get("source") or "").strip()
        source_type = str(row.get("sourceType") or "").strip()
        fetched_at = str(row.get("sourceFetchedAt") or "").strip()
        data_as_of = str(row.get("sourceDataAsOf") or "").strip()
        if not source or not source_type or not fetched_at or not data_as_of:
            raise ExportValidationError(
                f"{symbol} earnings row has incomplete financial source provenance"
            )
        row_metadata = {
            "source": source,
            "sourceType": source_type,
            "sourceUrl": row.get("sourceUrl"),
            "fetchedAt": fetched_at,
            "dataAsOf": data_as_of,
            "filingDate": row.get("sourceLatestFilingDate") or row.get("filingDate"),
        }
        if source_metadata is None:
            source_metadata = row_metadata
        elif row_metadata != source_metadata:
            raise ExportValidationError(
                f"{symbol} earnings rows contain inconsistent financial source provenance"
            )
        normalized.append({
            "date": row["date"],
            "filingDate": row.get("filingDate"),
            "fiscalYear": row.get("fiscalYear"),
            "period": row.get("period"),
            "currency": row.get("reportedCurrency"),
            **{field: row.get(field) for field in FINANCIAL_FIELDS},
        })
    normalized.sort(key=lambda row: row["date"], reverse=True)
    if not normalized:
        raise ExportValidationError(f"{symbol} earnings report has no dated rows")
    if source_metadata is None:
        raise ExportValidationError(f"{symbol} earnings report has no financial source provenance")
    return {
        "schemaVersion": SCHEMA_VERSION,
        "source": "ValuationCalculation financial statements export",
        "symbol": symbol,
        "generatedAt": generated_at,
        "dataAsOf": normalized[0]["date"],
        "financialSource": source_metadata,
        "quarters": normalized[:12],
    }


def export_all(
    results_dir: Path,
    processed_dir: Path,
    output_dir: Path,
    generated_at: str | None = None,
    max_export_bytes: int = MAX_EXPORT_BYTES,
    expected_symbols: list[str] | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    manifests: list[dict[str, Any]] = []
    failures: list[str] = []
    staged: list[tuple[Path, Any]] = []
    summary_paths = (
        [results_dir / symbol / "valuation_summary.json" for symbol in expected_symbols]
        if expected_symbols is not None
        else sorted(results_dir.glob("*/valuation_summary.json"))
    )
    for summary_path in summary_paths:
        try:
            summary = read_json(summary_path)
            if not isinstance(summary, dict):
                raise ExportValidationError("Valuation summary is not an object")
            symbol = require_symbol(summary.get("ticker"), summary_path)
            earnings_path = processed_dir / f"{symbol}_combined.json"
            earnings = read_json(earnings_path)
            financials = export_financials(earnings, symbol, generated_at)
            staged.append((Path("financials") / f"{symbol}.json", financials))
            for metric in METRICS:
                for window in WINDOWS:
                    staged.append((Path("valuation") / symbol / metric / f"{window}.json", export_valuation(summary, symbol, metric, window)))
            manifests.append({"symbol": symbol, "dataAsOf": min(str(summary.get("last_updated") or ""), financials["dataAsOf"]), "financials": f"financials/{symbol}.json", "valuationMetrics": list(METRICS), "windows": list(WINDOWS)})
        except ExportValidationError as error:
            failures.append(f"{summary_path.parent.name}: {error}")
    if failures:
        raise ExportValidationError("Export aborted; no manifest published:\n" + "\n".join(failures))
    if not manifests:
        raise ExportValidationError("Export aborted; no valuation summaries found")
    actual_symbols = {item["symbol"] for item in manifests}
    if expected_symbols is not None:
        missing = [symbol for symbol in expected_symbols if symbol not in actual_symbols]
        if missing:
            details = []
            if missing:
                details.append("missing=" + ",".join(missing))
            raise ExportValidationError("Export aborted; ticker universe does not match completed exports: " + "; ".join(details))
    manifest = {"schemaVersion": SCHEMA_VERSION, "generatedAt": generated_at, "source": "ValuationCalculation", "symbols": manifests}
    export_bytes = encoded_json_size(manifest) + sum(encoded_json_size(body) for _, body in staged)
    if export_bytes > max_export_bytes:
        raise ExportValidationError(
            f"Export aborted; slim payload is {export_bytes} bytes, exceeding the {max_export_bytes} byte limit"
        )
    for relative_path, body in staged:
        write_json(output_dir / relative_path, body)
    write_json(output_dir / "manifest.json", manifest)
    return manifest


def load_expected_symbols(path: Path) -> list[str]:
    try:
        body = read_json(path)
        symbols = body.get("symbols") if isinstance(body, dict) else None
        if not isinstance(symbols, list):
            raise ExportValidationError("resolved-symbol file must contain a symbols array")
        return deduplicate_symbols(symbols)
    except UniverseValidationError as error:
        raise ExportValidationError(f"resolved-symbol file is invalid: {error}") from error


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build validated Stock Watcher exports.")
    parser.add_argument("--expected-symbols-file", type=Path)
    args = parser.parse_args()
    expected = load_expected_symbols(args.expected_symbols_file) if args.expected_symbols_file else None
    export_all(Path("data/results"), Path("data/processed"), Path("data/watcher_exports"), expected_symbols=expected)
