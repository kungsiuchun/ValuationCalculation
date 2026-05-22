import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
