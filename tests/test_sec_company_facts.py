import copy
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from sec_company_facts import (
    SECCompanyFactsSource,
    SECInvalidPayloadError,
    SECRateLimitError,
    normalize_company_facts,
)


FIXTURE = Path(__file__).parent / "fixtures" / "sec_aapl_companyfacts.json"


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


class SECCompanyFactsNormalizationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.payload = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_fixture_normalizes_ytd_and_annual_values_into_four_quarters(self):
        rows = normalize_company_facts(self.payload, "AAPL", "0000320193", max_quarters=12)

        self.assertEqual([row["date"] for row in rows], [
            "2024-12-31", "2024-09-30", "2024-06-30", "2024-03-31"
        ])
        self.assertEqual([row["period"] for row in rows], ["Q4", "Q3", "Q2", "Q1"])
        self.assertEqual(rows[0]["revenue"], 170)  # annual 500 - Q1 - Q2 - Q3
        self.assertEqual(rows[1]["operatingCashFlow"], 12)  # YTD 40 - (Q1 30 + Q2 -2)
        self.assertEqual(rows[2]["operatingCashFlow"], -2)
        self.assertEqual(rows[0]["filingDate"], "2025-02-01")
        self.assertEqual(rows[0]["source"], "SEC Company Facts")
        self.assertEqual(rows[0]["cik"], "0000320193")

    def test_negative_values_are_preserved_and_capex_sign_is_explicit(self):
        rows = normalize_company_facts(self.payload, "AAPL", "0000320193", max_quarters=None)
        q2 = next(row for row in rows if row["date"] == "2024-06-30")

        self.assertEqual(q2["netIncome"], -10)
        self.assertEqual(q2["eps"], -0.5)
        self.assertEqual(q2["capex"], 2)
        self.assertEqual(q2["capexReported"], 2)
        self.assertEqual(q2["capexSigned"], -2)
        self.assertEqual(q2["freeCashFlow"], -4)

    def test_missing_fact_is_a_null_field_not_a_fabricated_value(self):
        payload = copy.deepcopy(self.payload)
        del payload["facts"]["us-gaap"]["EarningsPerShareDiluted"]

        rows = normalize_company_facts(payload, "AAPL", "0000320193", max_quarters=None)

        self.assertTrue(rows)
        self.assertTrue(all(row["eps"] is None for row in rows))
        self.assertTrue(all(row["revenue"] is not None for row in rows))

    def test_quarter_limit_is_applied_after_newest_first_sort(self):
        rows = normalize_company_facts(self.payload, "AAPL", "0000320193", max_quarters=2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["date"], "2024-12-31")


class SECCompanyFactsSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.payload = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_user_agent_timeout_cik_resolution_and_cache_are_observable(self):
        calls = []

        def fake_get(url, headers, timeout):
            calls.append((url, headers, timeout))
            if url.endswith("company_tickers.json"):
                return FakeResponse({"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Fixture Apple"}})
            return FakeResponse(self.payload)

        with tempfile.TemporaryDirectory() as temp:
            source = SECCompanyFactsSource(
                cache_dir=Path(temp),
                user_agent="FixtureTests/1.0 contact@test.invalid",
                timeout_seconds=7,
                cache_ttl_seconds=3600,
            )
            with patch("sec_company_facts.requests.get", side_effect=fake_get):
                first = source.fetch("aapl", max_quarters=12)
                second = source.fetch("AAPL", max_quarters=12)

        self.assertEqual(len(first), 4)
        self.assertEqual(second, first)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(call[1]["User-Agent"] == "FixtureTests/1.0 contact@test.invalid" for call in calls))
        self.assertTrue(all(call[2] == 7 for call in calls))
        self.assertIn("CIK0000320193", calls[1][0])

    def test_expired_cache_and_429_fail_closed_without_returning_old_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            ticker_cache = root / "company_tickers.json"
            facts_cache = root / "companyfacts_0000320193.json"
            ticker_cache.write_text(json.dumps({"0": {"cik_str": 320193, "ticker": "AAPL"}}), encoding="utf-8")
            facts_cache.write_text(json.dumps(self.payload), encoding="utf-8")
            stale = time.time() - 3600
            ticker_cache.touch()
            facts_cache.touch()
            import os
            os.utime(ticker_cache, (stale, stale))
            os.utime(facts_cache, (stale, stale))
            source = SECCompanyFactsSource(cache_dir=root, user_agent="FixtureTests/1.0", cache_ttl_seconds=60)

            with patch(
                "sec_company_facts.requests.get",
                return_value=FakeResponse({"error": "rate limit"}, status_code=429),
            ) as request:
                with self.assertRaises(SECRateLimitError):
                    source.fetch("AAPL")

        request.assert_called_once()
        self.assertIn("company_tickers.json", request.call_args.args[0])

    def test_invalid_fresh_payload_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            source = SECCompanyFactsSource(cache_dir=Path(temp), user_agent="FixtureTests/1.0")
            responses = [
                FakeResponse({"0": {"cik_str": 320193, "ticker": "AAPL"}}),
                FakeResponse({"cik": 320193, "facts": []}),
            ]
            with patch("sec_company_facts.requests.get", side_effect=responses):
                with self.assertRaises(SECInvalidPayloadError):
                    source.fetch("AAPL")

    def test_ticker_alias_matches_sec_dash_symbol(self):
        with tempfile.TemporaryDirectory() as temp:
            source = SECCompanyFactsSource(cache_dir=Path(temp), user_agent="FixtureTests/1.0")
            source._write_cache(
                Path(temp) / "company_tickers.json",
                {"0": {"cik_str": 320193, "ticker": "BRK-B"}},
            )
            self.assertEqual(source.resolve_cik("BRK.B"), "0000320193")


if __name__ == "__main__":
    unittest.main()
