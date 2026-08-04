"""Yahoo price-source adapter with bounded, testable fallback handling.

The valuation pipeline keeps its requested ticker as the cache key.  This
adapter only translates that ticker at the market-data boundary (for example,
the legacy ``SQ`` key is looked up as ``XYZ`` on Yahoo) and never changes the
financial cache path.

Yahoo is the only built-in source.  Optional fallback sources are injected by
callers as a finite sequence of callables; this module does not configure a
second API-key-backed provider or an unbounded retry loop.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import math
import time
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd
import yfinance as yf

from ticker_universe import normalize_symbol, yahoo_symbol


REQUIRED_PRICE_COLUMNS = frozenset({"Close", "Adj Close"})
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_RETRY_DELAY_SECONDS = 15


@dataclass(frozen=True)
class PriceFetchAttempt:
    """A single source attempt retained for an observable failure message."""

    source: str
    symbol: str
    attempt: int
    reason: str


class PriceSourceUnavailable(RuntimeError):
    """Raised when no configured price source returns a valid history frame."""

    code = "PRICE_SOURCE_UNAVAILABLE"

    def __init__(
        self,
        ticker: str,
        lookup_symbol: str,
        attempts: Sequence[PriceFetchAttempt],
    ) -> None:
        self.ticker = ticker
        self.lookup_symbol = lookup_symbol
        self.attempts = tuple(attempts)
        if self.attempts:
            details = "; ".join(
                f"{item.source}[{item.attempt}] {item.reason}"
                for item in self.attempts
            )
        else:
            details = "no source attempts"
        self.source = self.attempts[-1].source if self.attempts else "yahoo"
        self.reason = details
        super().__init__(
            f"{ticker}: price source unavailable (Yahoo lookup {lookup_symbol}; {details})"
        )


PriceFetcher = Callable[[str, float], Any]
FallbackSource = tuple[str, PriceFetcher]


def _finite_positive(value: float, name: str) -> float:
    """Validate a timeout/delay value without silently accepting infinity."""

    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a finite positive number") from error
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"{name} must be a finite positive number")
    return result


def _finite_nonnegative(value: float, name: str) -> float:
    """Validate a delay value; zero is useful for deterministic tests."""

    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a finite non-negative number") from error
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{name} must be a finite non-negative number")
    return result


def validate_price_history(prices: Any) -> str | None:
    """Return a reason when ``prices`` is unusable, otherwise ``None``.

    The adapter deliberately checks both required columns and the presence of
    at least one finite numeric value in each.  This prevents malformed Yahoo
    payloads from being mistaken for a successful source response.
    """

    if not isinstance(prices, pd.DataFrame):
        return "response is not a pandas DataFrame"
    if prices.empty:
        return "empty response"
    missing_columns = REQUIRED_PRICE_COLUMNS.difference(prices.columns)
    if missing_columns:
        return f"missing columns: {', '.join(sorted(missing_columns))}"
    for column in sorted(REQUIRED_PRICE_COLUMNS):
        numeric = pd.to_numeric(prices[column], errors="coerce")
        if not numeric.map(math.isfinite).any():
            return f"column {column} has no finite numeric values"
    return None


class YahooPriceAdapter:
    """Fetch Yahoo history through a bounded and injectable source seam.

    ``fallback_sources`` is intentionally an explicit finite iterable of
    callables or ``(label, fetcher)`` pairs.  A fetcher receives the translated
    Yahoo symbol and timeout seconds and returns a DataFrame-like value.  The
    adapter never invents fallback data: every response goes through the same
    validation gate before it can be returned.
    """

    source_name = "yahoo"

    def __init__(
        self,
        *,
        ticker_factory: Callable[[str], Any] | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
        sleep: Callable[[float], None] | None = None,
        fallback_sources: Iterable[FallbackSource] = (),
        logger: logging.Logger | None = None,
    ) -> None:
        if isinstance(max_attempts, bool):
            raise ValueError("max_attempts must be a positive integer")
        try:
            attempts = int(max_attempts)
        except (TypeError, ValueError) as error:
            raise ValueError("max_attempts must be a positive integer") from error
        if attempts < 1 or attempts != max_attempts:
            raise ValueError("max_attempts must be a positive integer")
        self.max_attempts = attempts
        self.timeout_seconds = _finite_positive(timeout_seconds, "timeout_seconds")
        self.retry_delay_seconds = _finite_nonnegative(
            retry_delay_seconds, "retry_delay_seconds"
        )
        if sleep is None:
            sleep = time.sleep
        if not callable(sleep):
            raise TypeError("sleep must be callable")
        self._sleep = sleep
        self._ticker_factory = ticker_factory or yf.Ticker
        if fallback_sources is None:
            fallback_sources = ()
        elif isinstance(fallback_sources, Mapping):
            fallback_sources = fallback_sources.items()
        normalized_fallbacks: list[FallbackSource] = []
        for index, source in enumerate(fallback_sources, start=1):
            if callable(source):
                label = getattr(source, "__name__", "").strip() or f"fallback-{index}"
                normalized_fallbacks.append((label, source))
                continue
            if (
                isinstance(source, tuple)
                and len(source) == 2
                and isinstance(source[0], str)
                and source[0].strip()
                and callable(source[1])
            ):
                normalized_fallbacks.append((source[0].strip(), source[1]))
                continue
            raise TypeError(
                "fallback_sources must contain callables or (non-empty label, callable) pairs"
            )
        self._fallback_sources = tuple(normalized_fallbacks)
        self._logger = logger or logging.getLogger(__name__)

    def _fetch_yahoo(self, symbol: str, timeout_seconds: float) -> Any:
        return self._ticker_factory(symbol).history(
            period="10y",
            auto_adjust=False,
            timeout=timeout_seconds,
        )

    def _sources(self) -> tuple[FallbackSource, ...]:
        return ((self.source_name, self._fetch_yahoo), *self._fallback_sources)

    def fetch_history(self, ticker: str) -> pd.DataFrame:
        """Return validated history or raise :class:`PriceSourceUnavailable`.

        The input ticker is normalized only for validation.  ``lookup_symbol``
        is used for Yahoo/fallback calls, while ``ticker`` remains the caller's
        cache identity.  Transport exceptions receive at most
        ``max_attempts`` attempts per configured source.  Empty or malformed
        payloads are terminal for that source and move to the next finite
        fallback immediately.
        """

        cache_symbol = normalize_symbol(ticker)
        lookup = yahoo_symbol(cache_symbol)
        attempts: list[PriceFetchAttempt] = []

        for source_label, fetcher in self._sources():
            for attempt_number in range(1, self.max_attempts + 1):
                try:
                    prices = fetcher(lookup, self.timeout_seconds)
                except Exception as error:  # provider transport/runtime failure
                    reason = f"{type(error).__name__}: {error}"
                    attempts.append(
                        PriceFetchAttempt(
                            source=source_label,
                            symbol=lookup,
                            attempt=attempt_number,
                            reason=reason,
                        )
                    )
                    self._logger.warning(
                        "<%s> %s price fetch failed on attempt %s/%s: %s",
                        cache_symbol,
                        source_label,
                        attempt_number,
                        self.max_attempts,
                        error,
                    )
                    if attempt_number < self.max_attempts:
                        self._sleep(self.retry_delay_seconds)
                    continue

                invalid_reason = validate_price_history(prices)
                if invalid_reason is None:
                    return prices
                attempts.append(
                    PriceFetchAttempt(
                        source=source_label,
                        symbol=lookup,
                        attempt=attempt_number,
                        reason=invalid_reason,
                    )
                )
                self._logger.warning(
                    "<%s> %s price response unavailable: %s",
                    cache_symbol,
                    source_label,
                    invalid_reason,
                )
                # Retrying an empty/invalid payload wastes the bounded source
                # budget; move to the next explicitly configured fallback.
                break

        raise PriceSourceUnavailable(cache_symbol, lookup, attempts)

    # ``fetch`` is a short alias for routers that use source-agnostic adapters.
    def fetch(self, ticker: str) -> pd.DataFrame:
        return self.fetch_history(ticker)


def fetch_price_history(
    ticker: str,
    *,
    adapter: YahooPriceAdapter | None = None,
    **adapter_options: Any,
) -> pd.DataFrame:
    """Source-aware convenience entry point for routers and small callers."""

    if adapter is not None and adapter_options:
        raise TypeError("adapter_options cannot be combined with an adapter instance")
    selected_adapter = adapter or YahooPriceAdapter(**adapter_options)
    return selected_adapter.fetch_history(ticker)


__all__ = [
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_RETRY_DELAY_SECONDS",
    "DEFAULT_TIMEOUT_SECONDS",
    "FallbackSource",
    "PriceFetchAttempt",
    "PriceSourceUnavailable",
    "REQUIRED_PRICE_COLUMNS",
    "YahooPriceAdapter",
    "fetch_price_history",
    "validate_price_history",
]
