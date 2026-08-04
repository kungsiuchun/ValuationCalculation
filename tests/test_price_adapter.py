import unittest
from unittest.mock import patch

import pandas as pd

from price_adapter import PriceSourceUnavailable, YahooPriceAdapter, validate_price_history
from ticker_universe import UniverseValidationError, is_retired_symbol, yahoo_symbol


class PriceAdapterBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.prices = pd.DataFrame({"Close": [10.0], "Adj Close": [9.5]})

    def test_sq_lookup_uses_xyz_without_rewriting_requested_cache_symbol(self):
        symbols = []

        class FakeTicker:
            def __init__(self, symbol):
                symbols.append(symbol)

            def history(self, **kwargs):
                return self.prices

        adapter = YahooPriceAdapter(
            ticker_factory=FakeTicker,
            retry_delay_seconds=0,
            sleep=lambda _delay: None,
        )
        # The adapter returns source data while the caller's SQ identity stays
        # untouched; only the provider lookup symbol changes.
        with patch.object(FakeTicker, "history", return_value=self.prices):
            result = adapter.fetch_history("SQ")
        self.assertIs(result, self.prices)
        self.assertEqual(symbols, ["XYZ"])
        self.assertEqual(yahoo_symbol("SQ"), "XYZ")

    def test_timeout_is_bounded_and_propagated_to_yahoo(self):
        calls = []

        class FakeTicker:
            def __init__(self, symbol):
                self.symbol = symbol

            def history(self, **kwargs):
                calls.append((self.symbol, kwargs["timeout"]))
                raise TimeoutError("network deadline")

        with self.assertRaises(PriceSourceUnavailable) as context:
            YahooPriceAdapter(
                ticker_factory=FakeTicker,
                max_attempts=2,
                timeout_seconds=7,
                retry_delay_seconds=0,
                sleep=lambda _delay: None,
            ).fetch_history("AAPL")
        self.assertEqual(calls, [("AAPL", 7.0), ("AAPL", 7.0)])
        self.assertIn("unavailable", str(context.exception))
        self.assertEqual(len(context.exception.attempts), 2)

    def test_empty_primary_response_uses_one_configured_fallback(self):
        primary_calls = []
        fallback_calls = []

        class EmptyTicker:
            def __init__(self, symbol):
                primary_calls.append(symbol)

            def history(self, **_kwargs):
                return pd.DataFrame()

        def fallback(symbol, timeout):
            fallback_calls.append((symbol, timeout))
            return self.prices

        result = YahooPriceAdapter(
            ticker_factory=EmptyTicker,
            timeout_seconds=11,
            retry_delay_seconds=0,
            sleep=lambda _delay: None,
            fallback_sources=(("fixture", fallback),),
        ).fetch_history("SQ")
        self.assertIs(result, self.prices)
        self.assertEqual(primary_calls, ["XYZ"])
        self.assertEqual(fallback_calls, [("XYZ", 11.0)])

    def test_invalid_columns_are_unavailable_and_do_not_pass_through(self):
        class InvalidTicker:
            def __init__(self, _symbol):
                pass

            def history(self, **_kwargs):
                return pd.DataFrame({"Close": [10.0]})

        self.assertEqual(
            validate_price_history(pd.DataFrame({"Close": [10.0]})),
            "missing columns: Adj Close",
        )
        with self.assertRaises(PriceSourceUnavailable) as context:
            YahooPriceAdapter(
                ticker_factory=InvalidTicker,
                max_attempts=3,
                retry_delay_seconds=0,
                sleep=lambda _delay: None,
            ).fetch_history("AAPL")
        self.assertIn("missing columns: Adj Close", str(context.exception))
        self.assertEqual(len(context.exception.attempts), 1)

    def test_fallback_exhaustion_is_typed_and_observable(self):
        calls = []

        class EmptyTicker:
            def __init__(self, symbol):
                calls.append(("yahoo", symbol))

            def history(self, **_kwargs):
                return pd.DataFrame()

        def empty_fallback(symbol, _timeout):
            calls.append(("fallback", symbol))
            return pd.DataFrame()

        with self.assertRaises(PriceSourceUnavailable) as context:
            YahooPriceAdapter(
                ticker_factory=EmptyTicker,
                retry_delay_seconds=0,
                sleep=lambda _delay: None,
                fallback_sources=(("fixture", empty_fallback),),
            ).fetch_history("AAPL")
        self.assertEqual(calls, [("yahoo", "AAPL"), ("fallback", "AAPL")])
        self.assertEqual([item.source for item in context.exception.attempts], ["yahoo", "fixture"])
        self.assertIn("empty response", str(context.exception))

    def test_retired_wba_is_rejected_before_any_provider_call(self):
        called = False

        def never_called(_symbol):
            nonlocal called
            called = True
            raise AssertionError("retired symbol reached Yahoo")

        with self.assertRaisesRegex(UniverseValidationError, "retired/delisted"):
            YahooPriceAdapter(ticker_factory=never_called).fetch_history("WBA")
        self.assertFalse(called)
        self.assertTrue(is_retired_symbol("wba"))
        self.assertFalse(is_retired_symbol("AAPL"))


if __name__ == "__main__":
    unittest.main()
