import json
import tempfile
import unittest
from pathlib import Path

from export_watcher_data import ExportValidationError, export_all
from ticker_universe import UniverseValidationError, resolve_tickers


def summary(symbol="TEST"):
    row = {"date": "2026-07-30", "price": 100, "valuation": {}}
    for window in ("1Y", "2Y", "3Y", "5Y"):
        row["valuation"][window] = {metric: {"mean": 100, "up1": 110, "up2": 120, "down1": 90, "down2": 80} for metric in ("pe", "fcf", "ps")}
    return {"ticker": symbol, "last_updated": "2026-07-30 22:00:00", "data": [row]}


class WatcherExportTests(unittest.TestCase):
    def write_fixture(self, root: Path, symbol="TEST", quarters=13):
        results = root / "results" / symbol
        processed = root / "processed"
        results.mkdir(parents=True)
        processed.mkdir()
        (results / "valuation_summary.json").write_text(json.dumps(summary(symbol)), encoding="utf-8")
        rows = [{"date": f"2026-{month:02d}-01", "reportedCurrency": "USD", "revenue": month, "netIncome": -1, "eps": None, "operatingCashFlow": 2, "freeCashFlow": -3} for month in range(1, quarters + 1)]
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
            valuation = json.loads((root / "exports/valuation/TEST/pe/1Y.json").read_text())
            self.assertEqual(valuation["latest"]["bands"]["mean"], 100)
            self.assertEqual(valuation["generatedAt"], "2026-07-30T22:00:00Z")

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


class TickerUniverseTests(unittest.TestCase):
    def test_retired_sq_symbol_resolves_to_current_xyz_symbol(self):
        self.assertEqual(resolve_tickers("SQ"), ["XYZ"])
        self.assertIn("XYZ", resolve_tickers())
        self.assertNotIn("SQ", resolve_tickers())

    def test_registry_is_deduplicated_with_default_universe(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = Path(temp) / "universe.json"
            registry.write_text(json.dumps({"schemaVersion": "1.0", "symbols": [{"symbol": "TSM", "state": "published"}, {"symbol": "SQ", "state": "queued"}, {"symbol": "XYZ", "state": "queued"}, {"symbol": "ibm", "state": "queued"}, {"symbol": "IBM", "state": "queued"}]}), encoding="utf-8")
            tickers = resolve_tickers(registry_path=registry)
            self.assertEqual(tickers.count("TSM"), 1)
            self.assertEqual(tickers.count("XYZ"), 1)
            self.assertNotIn("SQ", tickers)
            self.assertEqual(tickers.count("IBM"), 1)

    def test_explicit_symbols_are_deduplicated_and_invalid_symbol_fails(self):
        self.assertEqual(resolve_tickers("tsm,TSM,nvda"), ["TSM", "NVDA"])
        with self.assertRaises(UniverseValidationError):
            resolve_tickers("TSM,not valid")
