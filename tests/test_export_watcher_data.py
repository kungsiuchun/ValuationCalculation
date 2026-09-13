import json
import tempfile
import unittest
from pathlib import Path

from export_watcher_data import ExportValidationError, export_all, export_financials
from ticker_universe import UniverseValidationError, resolve_tickers


def summary(symbol="TEST"):
    row = {"date": "2026-07-30", "price": 100, "valuation": {}}
    for window in ("1Y", "2Y", "3Y", "5Y"):
        row["valuation"][window] = {metric: {"mean": 100, "up1": 110, "up2": 120, "down1": 90, "down2": 80} for metric in ("pe", "fcf", "ps")}
    return {"ticker": symbol, "last_updated": "2026-07-30 22:00:00", "data": [row]}


def source_row_fields():
    return {
        "source": "SEC Company Facts",
        "sourceType": "sec_companyfacts",
        "sourceUrl": "https://data.sec.gov/api/xbrl/companyfacts",
        "sourceFetchedAt": "2026-07-30T22:00:00Z",
        "sourceDataAsOf": "2026-06-30",
        "sourceLatestFilingDate": "2026-07-30",
    }


class WatcherExportTests(unittest.TestCase):
    def quarterly_rows(self):
        return [
            {
                "date": f"{2022 + index // 4}-{(index % 4 + 1) * 3:02d}-28",
                "period": f"Q{index % 4 + 1}",
                "reportedCurrency": "USD",
                "revenue": 100 + index * 10,
                "netIncome": 10 + index,
                "eps": 1 + index / 10,
                "operatingCashFlow": 20 + index,
                **source_row_fields(),
            }
            for index in range(16)
        ]

    def write_fixture(self, root: Path, symbol="TEST", quarters=13):
        results = root / "results" / symbol
        processed = root / "processed"
        results.mkdir(parents=True)
        processed.mkdir()
        (results / "valuation_summary.json").write_text(json.dumps(summary(symbol)), encoding="utf-8")
        rows = [{"date": f"2026-{month:02d}-01", "reportedCurrency": "USD", "revenue": month, "netIncome": -1, "eps": None, "operatingCashFlow": 2, "freeCashFlow": -3, **source_row_fields()} for month in range(1, quarters + 1)]
        (processed / f"{symbol}_combined.json").write_text(json.dumps(rows), encoding="utf-8")
        return results.parent, processed

    def test_exports_slim_versioned_data_and_limits_quarters(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            results, processed = self.write_fixture(root)
            manifest = export_all(results, processed, root / "exports", generated_at="2026-07-31T00:00:00Z")
            self.assertEqual(manifest["symbols"][0]["symbol"], "TEST")
            financials = json.loads((root / "exports/financials/TEST.json").read_text())
            self.assertEqual(len(financials["quarters"]), 12)
            self.assertEqual(financials["quarters"][0]["netIncome"], -1)
            self.assertEqual(financials["financialSource"]["sourceType"], "sec_companyfacts")
            valuation = json.loads((root / "exports/valuation/TEST/pe/1Y.json").read_text())
            self.assertEqual(valuation["latest"]["bands"]["mean"], 100)
            self.assertEqual(valuation["generatedAt"], "2026-07-30T22:00:00Z")

    def test_computes_missing_growth_before_limiting_to_twelve_quarters(self):
        rows = self.quarterly_rows()
        rows[-1]["revenue_qoq"] = 7.5
        financials = export_financials(rows, "TEST", "2026-07-31T00:00:00Z")
        self.assertEqual(len(financials["quarters"]), 12)
        self.assertEqual(financials["quarters"][0]["revenue_qoq"], 7.5)
        self.assertTrue(all(row["revenue_yoy"] is not None for row in financials["quarters"]))
        self.assertTrue(all(row["eps_yoy"] is not None for row in financials["quarters"]))
        self.assertAlmostEqual(financials["quarters"][0]["eps_yoy"], (2.5 / 2.1 - 1) * 100)

    def test_growth_stays_missing_for_gaps_and_nonpositive_bases(self):
        rows = self.quarterly_rows()
        rows[14]["revenue"] = 0
        rows[11]["eps"] = -1
        rows.pop(13)
        quarters = export_financials(rows, "TEST", "2026-07-31T00:00:00Z")["quarters"]
        self.assertIsNone(quarters[0]["revenue_qoq"])
        self.assertIsNone(quarters[0]["eps_yoy"])
        q3 = next(row for row in quarters if row["period"] == "Q3" and row["date"].startswith("2025"))
        self.assertIsNone(q3["revenue_qoq"])
        self.assertIsNotNone(q3["revenue_yoy"])

    def test_does_not_publish_manifest_when_a_symbol_is_incomplete(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            results = root / "results" / "TEST"
            results.mkdir(parents=True)
            (results / "valuation_summary.json").write_text(json.dumps(summary()), encoding="utf-8")
            with self.assertRaises(ExportValidationError):
                export_all(root / "results", root / "processed", root / "exports")
            self.assertFalse((root / "exports/manifest.json").exists())

    def test_aborts_before_writing_when_slim_export_exceeds_size_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            results, processed = self.write_fixture(root)
            with self.assertRaisesRegex(ExportValidationError, "exceeding the 1 byte limit"):
                export_all(results, processed, root / "exports", max_export_bytes=1)
            self.assertFalse((root / "exports/manifest.json").exists())

    def test_aborts_when_expected_ticker_has_no_complete_export(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            results, processed = self.write_fixture(root)
            with self.assertRaisesRegex(ExportValidationError, "MISSING"):
                export_all(results, processed, root / "exports", expected_symbols=["TEST", "MISSING"])
            self.assertFalse((root / "exports/manifest.json").exists())

    def test_aborts_when_financial_rows_have_no_source_provenance(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            results, processed = self.write_fixture(root)
            payload = json.loads((processed / "TEST_combined.json").read_text(encoding="utf-8"))
            for row in payload:
                row.pop("source", None)
            (processed / "TEST_combined.json").write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ExportValidationError, "provenance"):
                export_all(results, processed, root / "exports")
            self.assertFalse((root / "exports/manifest.json").exists())


class TickerUniverseTests(unittest.TestCase):
    def test_default_universe_excludes_unbackfilled_legacy_sq_symbol(self):
        self.assertNotIn("SQ", resolve_tickers())

    def test_default_universe_excludes_foreign_symbols_without_fx_contract(self):
        tickers = resolve_tickers()
        for symbol in ("TSM", "BABA", "SONY", "NOK", "BIDU", "NIO", "CPRX"):
            self.assertNotIn(symbol, tickers)

    def test_retired_wba_symbol_is_rejected_and_not_in_default_universe(self):
        self.assertNotIn("WBA", resolve_tickers())
        with self.assertRaisesRegex(UniverseValidationError, "retired/delisted"):
            resolve_tickers("WBA")

    def test_registry_is_deduplicated_with_default_universe(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = Path(temp) / "universe.json"
            registry.write_text(json.dumps({"schemaVersion": "1.0", "symbols": [{"symbol": "TSM", "state": "published"}, {"symbol": "ibm", "state": "queued"}, {"symbol": "IBM", "state": "queued"}]}), encoding="utf-8")
            tickers = resolve_tickers(registry_path=registry)
            self.assertEqual(tickers.count("TSM"), 1)
            self.assertEqual(tickers.count("IBM"), 1)

    def test_explicit_symbols_are_deduplicated_and_invalid_symbol_fails(self):
        self.assertEqual(resolve_tickers("tsm,TSM,nvda"), ["TSM", "NVDA"])
        with self.assertRaises(UniverseValidationError):
            resolve_tickers("TSM,not valid")
