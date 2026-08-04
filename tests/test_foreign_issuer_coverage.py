import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import generate_valuation
from financial_source_router import FinancialSourceRouter
from foreign_issuer_coverage import (
    FOREIGN_ISSUER_CIKS,
    ForeignIssuerCoverageSource,
    ForeignIssuerUnavailable,
    SEC_FOREIGN_SOURCE_TYPE,
    is_foreign_issuer,
    latest_20f_filing_date,
    normalize_foreign_company_facts,
)


FIXTURES = Path(__file__).parent / "fixtures"
FACTS = json.loads((FIXTURES / "sec_foreign_tsm_companyfacts.json").read_text(encoding="utf-8"))
SUBMISSIONS = json.loads((FIXTURES / "sec_foreign_submissions_20f.json").read_text(encoding="utf-8"))


class ForeignIssuerNormalizationTests(unittest.TestCase):
    def test_ifrs_fixture_normalizes_quarters_and_currency(self):
        rows = normalize_foreign_company_facts(
            FACTS,
            "TSM",
            FOREIGN_ISSUER_CIKS["TSM"],
            latest_filing_date="2025-02-15",
        )

        self.assertEqual([row["date"] for row in rows], [
            "2024-12-31", "2024-09-30", "2024-06-30", "2024-03-31"
        ])
        self.assertEqual([row["period"] for row in rows], ["Q4", "Q3", "Q2", "Q1"])
        self.assertEqual(rows[0]["reportedCurrency"], "TWD")
        self.assertEqual(rows[0]["sourceType"], SEC_FOREIGN_SOURCE_TYPE)
        self.assertEqual(rows[0]["filingForm"], "20-F")
        self.assertEqual(rows[0]["cik"], "0001046179")
        self.assertEqual(rows[0]["revenue"], 130.0)

    def test_missing_ifrs_anchor_is_typed_unavailable(self):
        unsupported = json.loads(
            (FIXTURES / "sec_foreign_unsupported_companyfacts.json").read_text(encoding="utf-8")
        )
        with self.assertRaises(ForeignIssuerUnavailable) as context:
            normalize_foreign_company_facts(unsupported, "TSM", FOREIGN_ISSUER_CIKS["TSM"])
        self.assertEqual(context.exception.code, "UNAVAILABLE")
        self.assertIn("revenue", context.exception.reason)

    def test_20f_parser_rejects_submissions_without_20f(self):
        no_20f = json.loads(
            (FIXTURES / "sec_foreign_submissions_no_20f.json").read_text(encoding="utf-8")
        )
        self.assertEqual(latest_20f_filing_date(SUBMISSIONS), "2025-02-15")
        self.assertIsNone(latest_20f_filing_date(no_20f))


class ForeignIssuerSourceTests(unittest.TestCase):
    def test_source_uses_dedicated_cache_and_no_us_cache_pollution(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = ForeignIssuerCoverageSource(
                cache_dir=root,
                user_agent="FixtureTests/1.0 contact@test.invalid",
            )
            with patch.object(source, "_request_json", side_effect=[SUBMISSIONS, FACTS]):
                rows = source.fetch("TSM")
            self.assertEqual(len(rows), 4)
            self.assertTrue((root / "submissions_0001046179.json").exists())
            self.assertTrue((root / "companyfacts_0001046179.json").exists())
            self.assertFalse((root / "company_tickers.json").exists())

    def test_missing_20f_is_unavailable_before_facts_can_be_used(self):
        with tempfile.TemporaryDirectory() as temp:
            source = ForeignIssuerCoverageSource(cache_dir=Path(temp), user_agent="FixtureTests/1.0")
            with patch.object(source, "_request_json", return_value=json.loads(
                (FIXTURES / "sec_foreign_submissions_no_20f.json").read_text(encoding="utf-8")
            )):
                with self.assertRaises(ForeignIssuerUnavailable) as context:
                    source.fetch("TSM")
            self.assertEqual(context.exception.code, "UNAVAILABLE")
            self.assertIn("20-F", context.exception.reason)


class ForeignIssuerRouterTests(unittest.TestCase):
    def test_known_foreign_issuer_never_enters_fmp(self):
        foreign_rows = normalize_foreign_company_facts(FACTS, "TSM", FOREIGN_ISSUER_CIKS["TSM"])
        fmp = Mock(return_value=[{"date": "2024-12-31", "revenue": 999, "netIncome": 999}])
        domestic_sec = Mock()
        router = FinancialSourceRouter(
            sec_fetcher=domestic_sec,
            foreign_fetcher=lambda symbol: foreign_rows,
            fmp_fetcher=fmp,
            clock=lambda: 1_780_000_000,
        )

        result = router.route("TSM")

        self.assertEqual(result.source_type, SEC_FOREIGN_SOURCE_TYPE)
        self.assertEqual(result.rows[0]["reportedCurrency"], "TWD")
        domestic_sec.assert_not_called()
        fmp.assert_not_called()

    def test_foreign_symbol_gate_is_explicit(self):
        self.assertTrue(is_foreign_issuer("TSM"))
        self.assertTrue(is_foreign_issuer("SONY"))
        self.assertTrue(is_foreign_issuer("BABA"))
        self.assertFalse(is_foreign_issuer("AAPL"))

    def test_production_router_wires_dedicated_foreign_source(self):
        router = generate_valuation.create_financial_source_router()

        self.assertIsNotNone(router.foreign_fetcher)
        self.assertIsInstance(router.foreign_fetcher.__self__, ForeignIssuerCoverageSource)


if __name__ == "__main__":
    unittest.main()
