import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import generate_valuation


class GenerateValuationBehaviorTests(unittest.TestCase):
    def test_build_quarterly_ttm_returns_three_empty_metrics_when_data_is_missing(self):
        with patch.object(generate_valuation, "get_fmp_fragmented", return_value=[]):
            result = generate_valuation.build_quarterly_ttm("MISSING")

        self.assertEqual(result, (None, None, None))

    def test_fragmented_fetch_skips_missing_api_keys(self):
        calls = []

        class Response:
            def raise_for_status(self):
                return None

            def json(self):
                return [{"date": "2026-03-31", "period": "Q1", "value": 1}]

        def fake_get(url):
            calls.append(url)
            return Response()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(generate_valuation, "CACHE_BASE_DIR", str(Path(tmpdir))):
                with patch.object(generate_valuation, "QUARTERS", ["q1"]):
                    with patch.object(generate_valuation, "FMP_API_KEY", None):
                        with patch.object(generate_valuation, "FMP_API_KEY_2", "KEY2"):
                            with patch.object(generate_valuation, "FMP_API_KEY_3", None):
                                with patch.object(generate_valuation.requests, "get", side_effect=fake_get):
                                    data = generate_valuation.get_fmp_fragmented("income-statement", "AAPL")

        self.assertEqual(len(data), 1)
        self.assertEqual(len(calls), 1)
        self.assertIn("apikey=KEY2", calls[0])
        self.assertNotIn("apikey=None", calls[0])

    def test_price_history_retries_then_returns_data(self):
        prices = pd.DataFrame({"Close": [10.0], "Adj Close": [9.5]})

        class FakeTicker:
            calls = 0

            def __init__(self, ticker):
                self.ticker = ticker

            def history(self, period, auto_adjust):
                FakeTicker.calls += 1
                if FakeTicker.calls == 1:
                    raise RuntimeError("Too Many Requests")
                return prices

        with patch.object(generate_valuation.yf, "Ticker", FakeTicker):
            with patch.object(generate_valuation.time, "sleep") as sleep:
                result = generate_valuation.fetch_price_history("SQ", attempts=2, delay_seconds=0)

        self.assertIs(result, prices)
        self.assertEqual(FakeTicker.calls, 2)
        sleep.assert_called_once_with(0)

    def test_price_history_returns_empty_after_retries_are_exhausted(self):
        class FakeTicker:
            def __init__(self, ticker):
                self.ticker = ticker

            def history(self, period, auto_adjust):
                raise RuntimeError("Too Many Requests")

        with patch.object(generate_valuation.yf, "Ticker", FakeTicker):
            with patch.object(generate_valuation.time, "sleep"):
                result = generate_valuation.fetch_price_history("SQ", attempts=2, delay_seconds=0)

        self.assertTrue(result.empty)


if __name__ == "__main__":
    unittest.main()
