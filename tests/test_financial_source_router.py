import unittest
from unittest.mock import Mock

from financial_source_router import (
    FMPStalePayloadError,
    FMPCircuitBreaker,
    FMPCircuitOpenError,
    FMPRateLimitError,
    FinancialSourceInvalid,
    FinancialSourceResult,
    FinancialSourceRouter,
    FinancialSourceError,
    FinancialSourceStale,
)
from sec_company_facts import SECInvalidPayloadError, SECTickerNotFoundError


def rows(*dates):
    return [
        {
            "date": value,
            "filingDate": value,
            "revenue": 100.0,
            "netIncome": 10.0,
            "numberOfShares": 10.0,
            "source": "fixture",
        }
        for value in dates
    ]


class FinancialSourceRouterTests(unittest.TestCase):
    def test_sec_success_never_calls_fmp_and_keeps_provenance(self):
        fmp = Mock(return_value=rows("2026-03-31"))
        router = FinancialSourceRouter(
            sec_fetcher=lambda symbol: rows("2026-06-30", "2026-03-31"),
            fmp_fetcher=fmp,
            clock=lambda: 1_785_800_000,
        )

        result = router.route("aapl")

        self.assertIsInstance(result, FinancialSourceResult)
        self.assertEqual(result.source_type, "SEC_COMPANY_FACTS")
        self.assertEqual(result.data_as_of, "2026-06-30")
        self.assertEqual(result.latest_filing_date, "2026-06-30")
        self.assertEqual(result.rows[0]["sourceType"], "SEC_COMPANY_FACTS")
        self.assertEqual(result.rows[0]["sourceDataAsOf"], "2026-06-30")
        self.assertEqual(result.metadata["freshness"]["status"], "fresh")
        fmp.assert_not_called()

    def test_only_explicit_sec_unsupported_enters_fmp_fallback(self):
        fmp = Mock(return_value=rows("2026-06-30"))
        router = FinancialSourceRouter(
            sec_fetcher=Mock(side_effect=SECTickerNotFoundError("not covered")),
            fmp_fetcher=fmp,
            clock=lambda: 1_785_800_000,
        )

        result = router.fetch_financials("TSM")

        self.assertEqual(result.source_type, "FMP")
        self.assertEqual(result.rows[0]["sourceType"], "FMP")
        fmp.assert_called_once()

    def test_sec_invalid_payload_does_not_fallback(self):
        fmp = Mock(return_value=rows("2026-06-30"))
        router = FinancialSourceRouter(
            sec_fetcher=Mock(side_effect=SECInvalidPayloadError("invalid SEC JSON")),
            fmp_fetcher=fmp,
        )

        with self.assertRaises((SECInvalidPayloadError, FinancialSourceError)):
            router.route("AAPL")
        fmp.assert_not_called()

    def test_sec_no_us_gaap_payload_does_not_fallback(self):
        fmp = Mock(return_value=rows("2026-06-30"))
        router = FinancialSourceRouter(
            sec_fetcher=Mock(side_effect=SECInvalidPayloadError("SEC Company Facts payload has no us-gaap facts")),
            fmp_fetcher=fmp,
        )

        with self.assertRaises((SECInvalidPayloadError, FinancialSourceError)):
            router.route("AAPL")
        fmp.assert_not_called()

    def test_source_data_age_is_fail_closed(self):
        router = FinancialSourceRouter(
            sec_fetcher=lambda symbol: rows("2020-01-01"),
            fmp_fetcher=Mock(),
            clock=lambda: 1_785_800_000,
        )

        with self.assertRaises(FinancialSourceStale):
            router.route("AAPL")

    def test_fmp_rate_limit_opens_circuit_and_second_call_is_blocked(self):
        fmp = Mock(side_effect=FMPRateLimitError("429"))
        sec = Mock(side_effect=SECTickerNotFoundError("not covered"))
        breaker = FMPCircuitBreaker(cooldown_seconds=300, clock=lambda: 10.0)
        router = FinancialSourceRouter(sec_fetcher=sec, fmp_fetcher=fmp, fmp_circuit_breaker=breaker)

        with self.assertRaises(FMPRateLimitError):
            router.route("TSM")
        self.assertTrue(breaker.is_open)
        with self.assertRaises(FMPCircuitOpenError):
            router.route("TSM")
        self.assertEqual(fmp.call_count, 1)

    def test_fmp_stale_payload_fails_closed(self):
        fmp = Mock(side_effect=FMPStalePayloadError("stale payload"))
        router = FinancialSourceRouter(
            sec_fetcher=Mock(side_effect=SECTickerNotFoundError("not covered")),
            fmp_fetcher=fmp,
        )

        with self.assertRaises(FMPStalePayloadError):
            router.route("TSM")

    def test_fmp_invalid_row_fails_closed(self):
        router = FinancialSourceRouter(
            sec_fetcher=Mock(side_effect=SECTickerNotFoundError("not covered")),
            fmp_fetcher=lambda symbol: [{"revenue": 1}],
        )

        with self.assertRaises((FinancialSourceInvalid, FinancialSourceError)):
            router.route("TSM")

    def test_explicit_stale_marker_is_not_relabelled_fresh(self):
        router = FinancialSourceRouter(
            sec_fetcher=Mock(side_effect=SECTickerNotFoundError("not covered")),
            fmp_fetcher=lambda symbol: {"status": "stale", "rows": rows("2026-06-30")},
        )

        with self.assertRaises(FMPStalePayloadError):
            router.route("TSM")


if __name__ == "__main__":
    unittest.main()
