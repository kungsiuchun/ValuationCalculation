"""SEC-first financial-source routing with a bounded FMP fallback.

The valuation pipeline has two materially different financial sources:

* SEC Company Facts is the canonical source for US issuers.
* FMP is a controlled fallback for symbols that SEC explicitly does not
  support.

This module owns the source decision and its provenance contract.  It does
not silently turn an HTTP 429, quota response, malformed payload, or stale
payload into a successful-looking release.  Callers receive a
``FinancialSourceResult`` containing both normalized rows and source/freshness
metadata, while source-specific failures remain observable through typed
exceptions.
"""

from __future__ import annotations

import inspect
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from sec_company_facts import (
    SECCompanyFactsError,
    SECInvalidPayloadError,
    SECRateLimitError,
    SECRequestError,
    SECTickerNotFoundError,
)
from foreign_issuer_coverage import (
    ForeignIssuerCoverageError,
    ForeignIssuerCoverageResult,
    ForeignIssuerUnavailable,
    SEC_FOREIGN_FACTS_URL,
    SEC_FOREIGN_SOURCE,
    SEC_FOREIGN_SOURCE_TYPE,
    is_foreign_issuer,
)


logger = logging.getLogger(__name__)

SEC_SOURCE = "SEC Company Facts"
SEC_SOURCE_TYPE = "SEC_COMPANY_FACTS"
FMP_SOURCE = "Financial Modeling Prep"
FMP_SOURCE_TYPE = "FMP"


class FinancialSourceError(RuntimeError):
    """Base class for source-router failures."""


class FinancialSourceUnsupported(FinancialSourceError):
    """The primary source explicitly does not cover the requested symbol."""


# A descriptive alias is useful to source adapters and fixture tests.
SECUnsupportedError = FinancialSourceUnsupported


class FinancialSourceInvalid(FinancialSourceError):
    """A source returned malformed or unusable normalized financial data."""


class FinancialSourceStale(FinancialSourceError):
    """A source returned data older than the existing source boundary."""


class FinancialSourceUnavailable(FinancialSourceError):
    """A source could not be reached or did not complete successfully."""


class FinancialSourceRateLimited(FinancialSourceError):
    """A source rejected a request with a rate-limit response."""


class FinancialSourceQuotaExhausted(FinancialSourceError):
    """A source reported quota exhaustion."""


class FMPSourceError(FinancialSourceError):
    """Base class for controlled FMP fallback errors."""


class FMPRateLimitError(FMPSourceError, FinancialSourceRateLimited):
    """FMP returned HTTP 429 or an equivalent rate-limit response."""


class FMPQuotaError(FMPSourceError, FinancialSourceQuotaExhausted):
    """FMP reported a quota/key exhaustion response."""


class FMPStalePayloadError(FMPSourceError, FinancialSourceStale):
    """FMP returned a payload no newer than the cached source boundary."""


class FMPInvalidPayloadError(FMPSourceError, FinancialSourceInvalid):
    """FMP returned malformed or unusable financial rows."""


class FMPCircuitOpenError(FMPSourceError):
    """The FMP circuit is open after a rate-limit/quota failure."""


@dataclass
class FMPCircuitBreaker:
    """Small fail-closed breaker for FMP fallback calls.

    A single 429/quota event opens the circuit immediately.  The router never
    rotates through a second or third API key after that event.  The cooldown
    is deliberately explicit and injectable for deterministic fixture tests.
    """

    cooldown_seconds: float = 300.0
    clock: Callable[[], float] = time.time
    _opened_at: Optional[float] = field(default=None, init=False, repr=False)
    _reason: Optional[str] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.cooldown_seconds < 0:
            raise ValueError("FMP circuit cooldown must be non-negative")

    @property
    def is_open(self) -> bool:
        if self._opened_at is None:
            return False
        if self.cooldown_seconds == 0:
            return False
        elapsed = self.clock() - self._opened_at
        if elapsed >= self.cooldown_seconds:
            # A half-open probe is allowed after cooldown.  The next failure
            # can open the circuit again, while success closes it explicitly.
            return False
        return True

    @property
    def reason(self) -> Optional[str]:
        return self._reason

    def check(self) -> None:
        if self.is_open:
            detail = self._reason or "FMP circuit is open"
            raise FMPCircuitOpenError(detail)

    # Familiar aliases make this object convenient for callback adapters.
    before_request = check

    def trip(self, reason: str) -> None:
        self._opened_at = self.clock()
        self._reason = str(reason or "FMP circuit opened")
        logger.warning("FMP circuit opened: %s", self._reason)

    record_failure = trip

    def record_success(self) -> None:
        self._opened_at = None
        self._reason = None

    def reset(self) -> None:
        self.record_success()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now(clock: Callable[[], Any]) -> str:
    value = clock()
    if isinstance(value, datetime):
        stamp = value
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
    else:
        stamp = datetime.fromtimestamp(float(value), tz=timezone.utc)
    return stamp.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_date(value: Any, *, field_name: str = "date") -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        raise ValueError(field_name + " is missing")
    try:
        return date.fromisoformat(text[:10])
    except (TypeError, ValueError) as error:
        raise ValueError(field_name + " is invalid: " + text) from error


def _normalise_symbol(symbol: Any) -> str:
    value = str(symbol or "").strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", value):
        raise ValueError("invalid financial symbol: " + value)
    return value


@dataclass(frozen=True)
class FinancialSourceResult:
    """Normalized rows plus immutable source/freshness metadata."""

    symbol: str
    rows: Tuple[Dict[str, Any], ...]
    source: str
    source_type: str
    source_url: Optional[str]
    fetched_at: str
    data_as_of: str
    latest_filing_date: Optional[str]

    @property
    def records(self) -> Tuple[Dict[str, Any], ...]:
        return self.rows

    @property
    def sourceType(self) -> str:  # compatibility with JSON contracts
        return self.source_type

    @property
    def sourceUrl(self) -> Optional[str]:
        return self.source_url

    @property
    def fetchedAt(self) -> str:
        return self.fetched_at

    @property
    def dataAsOf(self) -> str:
        return self.data_as_of

    @property
    def filingDate(self) -> Optional[str]:
        return self.latest_filing_date

    @property
    def freshness(self) -> Dict[str, Optional[str]]:
        return {
            "fetchedAt": self.fetched_at,
            "dataAsOf": self.data_as_of,
            "filingDate": self.latest_filing_date,
        }

    @property
    def metadata(self) -> Dict[str, Any]:
        """JSON-safe provenance/freshness projection for release artifacts."""

        return {
            "source": self.source,
            "sourceType": self.source_type,
            "sourceUrl": self.source_url,
            "fetchedAt": self.fetched_at,
            "dataAsOf": self.data_as_of,
            "filingDate": self.latest_filing_date,
            "freshness": self.freshness,
        }

    def __iter__(self):
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, index: int) -> Dict[str, Any]:
        return self.rows[index]


def _rows_from_fetcher_payload(payload: Any) -> List[Dict[str, Any]]:
    """Accept common callback shapes without guessing financial values."""

    if isinstance(payload, FinancialSourceResult):
        return [dict(row) for row in payload.rows]
    if isinstance(payload, Mapping):
        for key in ("rows", "records", "quarters", "data"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                payload = candidate
                break
        else:
            # FMP adapters commonly return endpoint -> list mappings.  Merge
            # each endpoint by exact statement date; values are never guessed.
            endpoint_rows = [value for value in payload.values() if isinstance(value, list)]
            if endpoint_rows and len(endpoint_rows) == len(payload):
                merged: Dict[str, Dict[str, Any]] = {}
                for records in endpoint_rows:
                    for row in records:
                        if not isinstance(row, Mapping):
                            raise FMPInvalidPayloadError("FMP endpoint row is not an object")
                        try:
                            key = _parse_date(row.get("date")).isoformat()
                        except ValueError as error:
                            raise FMPInvalidPayloadError(str(error)) from error
                        merged.setdefault(key, {}).update(dict(row))
                return list(merged.values())
    if not isinstance(payload, list):
        raise FMPInvalidPayloadError("financial source returned no rows list")
    rows: List[Dict[str, Any]] = []
    for raw in payload:
        if not isinstance(raw, Mapping):
            raise FinancialSourceInvalid("financial source row is not an object")
        rows.append(dict(raw))
    return rows


def _assert_payload_is_fresh(payload: Any) -> None:
    """Reject explicit stale/invalid markers instead of relabelling them."""

    if not isinstance(payload, Mapping):
        return
    status_values: List[Any] = [payload.get("status"), payload.get("freshnessStatus")]
    freshness = payload.get("freshness")
    if isinstance(freshness, Mapping):
        status_values.append(freshness.get("status"))
    if payload.get("stale") is True or any(str(value or "").lower() == "stale" for value in status_values):
        raise FinancialSourceStale("financial source payload is explicitly stale")
    if payload.get("invalid") is True or any(str(value or "").lower() == "invalid" for value in status_values):
        raise FinancialSourceInvalid("financial source payload is explicitly invalid")


def _classify_fmp_exception(error: BaseException) -> FMPSourceError:
    if isinstance(error, FMPSourceError):
        return error
    text = str(error or "").lower()
    response = getattr(error, "response", None)
    status = getattr(response, "status_code", None)
    if status == 429 or "429" in text or "rate limit" in text or "too many requests" in text:
        return FMPRateLimitError(str(error) or "FMP rate limit")
    if any(token in text for token in ("quota", "limit reached", "api key", "apikey")):
        return FMPQuotaError(str(error) or "FMP quota exhausted")
    if "stale" in text or "older than" in text:
        return FMPStalePayloadError(str(error) or "FMP payload is stale")
    if any(token in text for token in ("invalid payload", "malformed", "no usable", "missing date")):
        return FMPInvalidPayloadError(str(error) or "FMP payload is invalid")
    return FMPSourceError(str(error) or "FMP source failed")


def _is_sec_unsupported(error: BaseException) -> bool:
    if isinstance(error, FinancialSourceUnsupported):
        return True
    if isinstance(error, SECTickerNotFoundError):
        return True
    # The SEC adapter uses this exact message when a valid Company Facts
    # document has no valuation anchors.  Other invalid-payload messages
    # (malformed JSON, missing facts object, etc.) remain fail-closed.
    if isinstance(error, SECInvalidPayloadError):
        text = str(error).lower()
        # Foreign private issuers publish IFRS facts.  The domestic adapter
        # reports the taxonomy mismatch as ``no us-gaap facts``; that is an
        # explicit hand-off to the dedicated foreign lane, not permission to
        # substitute Yahoo/FMP data for a known foreign issuer.
        return (
            "lacks quarterly revenue/net income" in text
            or "no quarterly observations" in text
            or "no us-gaap facts" in text
        )
    return False


class FinancialSourceRouter:
    """Route one symbol through SEC first, then a controlled FMP fallback."""

    def __init__(
        self,
        *,
        sec_fetcher: Optional[Callable[..., Any]] = None,
        foreign_fetcher: Optional[Callable[..., Any]] = None,
        fmp_fetcher: Optional[Callable[..., Any]] = None,
        sec_source: Optional[Any] = None,
        foreign_source: Optional[Any] = None,
        fmp_circuit_breaker: Optional[FMPCircuitBreaker] = None,
        clock: Callable[[], Any] = time.time,
    ) -> None:
        if sec_fetcher is not None and sec_source is not None:
            raise ValueError("provide sec_fetcher or sec_source, not both")
        if foreign_fetcher is not None and foreign_source is not None:
            raise ValueError("provide foreign_fetcher or foreign_source, not both")
        if sec_fetcher is None and sec_source is not None:
            sec_fetcher = sec_source.fetch
        if foreign_fetcher is None and foreign_source is not None:
            foreign_fetcher = foreign_source.fetch
        self.sec_fetcher = sec_fetcher
        self.foreign_fetcher = foreign_fetcher
        self.fmp_fetcher = fmp_fetcher
        self.clock = clock
        self.fmp_circuit_breaker = fmp_circuit_breaker or FMPCircuitBreaker(clock=clock)

    @property
    def circuit_breaker(self) -> FMPCircuitBreaker:
        return self.fmp_circuit_breaker

    def _call_sec(self, symbol: str) -> Any:
        if self.sec_fetcher is None:
            raise SECUnsupportedError("SEC source is not configured")
        try:
            signature = inspect.signature(self.sec_fetcher)
        except (TypeError, ValueError):
            return self.sec_fetcher(symbol)
        if "max_quarters" in signature.parameters:
            return self.sec_fetcher(symbol, max_quarters=12)
        return self.sec_fetcher(symbol)

    def _call_fmp(self, symbol: str) -> Any:
        if self.fmp_fetcher is None:
            raise FinancialSourceUnavailable("FMP fallback is not configured")
        try:
            signature = inspect.signature(self.fmp_fetcher)
        except (TypeError, ValueError):
            return self.fmp_fetcher(symbol)
        parameters = signature.parameters
        if "circuit_breaker" in parameters or any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
        ):
            return self.fmp_fetcher(symbol, circuit_breaker=self.fmp_circuit_breaker)
        return self.fmp_fetcher(symbol)

    def _call_foreign(self, symbol: str) -> Any:
        if self.foreign_fetcher is None:
            raise ForeignIssuerUnavailable(
                "SEC foreign issuer coverage is not configured", symbol=symbol
            )
        try:
            signature = inspect.signature(self.foreign_fetcher)
        except (TypeError, ValueError):
            return self.foreign_fetcher(symbol)
        parameters = signature.parameters
        if "max_quarters" in parameters:
            return self.foreign_fetcher(symbol, max_quarters=12)
        return self.foreign_fetcher(symbol)

    @staticmethod
    def _foreign_rows(payload: Any, symbol: str) -> Any:
        """Unwrap a diagnostic coverage result while preserving typed gaps."""

        if isinstance(payload, ForeignIssuerCoverageResult):
            if not payload.available:
                raise ForeignIssuerUnavailable(
                    payload.reason or "SEC foreign issuer coverage is unavailable",
                    symbol=symbol,
                    cik=payload.cik,
                )
            return list(payload.rows)
        return payload

    def _route_foreign(self, symbol: str) -> FinancialSourceResult:
        """Fetch a known foreign issuer without touching the domestic SEC lane."""

        payload = self._foreign_rows(self._call_foreign(symbol), symbol)
        return self._result(
            symbol,
            payload,
            source=SEC_FOREIGN_SOURCE,
            source_type=SEC_FOREIGN_SOURCE_TYPE,
            source_url=SEC_FOREIGN_FACTS_URL,
        )

    def _result(
        self,
        symbol: str,
        payload: Any,
        *,
        source: str,
        source_type: str,
        source_url: Optional[str],
    ) -> FinancialSourceResult:
        _assert_payload_is_fresh(payload)
        try:
            rows = _rows_from_fetcher_payload(payload)
        except FMPSourceError:
            raise
        except FinancialSourceError:
            raise
        except Exception as error:
            raise FinancialSourceInvalid(str(error)) from error
        if not rows:
            raise FinancialSourceInvalid(source + " returned no financial rows")
        dated: List[Tuple[date, Dict[str, Any]]] = []
        for row in rows:
            row_freshness = row.get("sourceFreshness")
            row_status = row_freshness.get("status") if isinstance(row_freshness, Mapping) else row.get("freshnessStatus")
            if str(row_status or "").lower() == "stale":
                raise FinancialSourceStale("financial source row is explicitly stale")
            if str(row_status or "").lower() == "invalid":
                raise FinancialSourceInvalid("financial source row is explicitly invalid")
            try:
                row_date = _parse_date(row.get("date"))
            except ValueError as error:
                raise FinancialSourceInvalid(str(error)) from error
            row["date"] = row_date.isoformat()
            dated.append((row_date, row))
        dated.sort(key=lambda item: item[0], reverse=True)
        rows = [row for _, row in dated]
        if not any(row.get("revenue") is not None for row in rows) or not any(
            row.get("netIncome") is not None for row in rows
        ):
            raise FinancialSourceInvalid(source + " rows lack revenue/netIncome anchors")
        data_as_of = dated[0][0].isoformat()
        filing_dates: List[date] = []
        for row in rows:
            value = row.get("filingDate") or row.get("filed")
            if value:
                try:
                    filing_dates.append(_parse_date(value, field_name="filingDate"))
                except ValueError as error:
                    raise FinancialSourceInvalid(str(error)) from error
        latest_filing = max(filing_dates).isoformat() if filing_dates else None
        fetched_at = _iso_now(self.clock)
        for row in rows:
            row["source"] = source
            row["sourceType"] = source_type
            row["sourceUrl"] = source_url
            row["sourceFetchedAt"] = fetched_at
            row["sourceDataAsOf"] = data_as_of
            row["sourceLatestFilingDate"] = latest_filing
            row["sourceFreshness"] = {
                "fetchedAt": fetched_at,
                "dataAsOf": data_as_of,
                "filingDate": row.get("filingDate") or latest_filing,
                "status": "fresh",
            }
        return FinancialSourceResult(
            symbol=symbol,
            rows=tuple(rows),
            source=source,
            source_type=source_type,
            source_url=source_url,
            fetched_at=fetched_at,
            data_as_of=data_as_of,
            latest_filing_date=latest_filing,
        )

    def _route_fmp(self, symbol: str) -> FinancialSourceResult:
        self.fmp_circuit_breaker.check()
        try:
            payload = self._call_fmp(symbol)
            result = self._result(
                symbol,
                payload,
                source=FMP_SOURCE,
                source_type=FMP_SOURCE_TYPE,
                source_url="https://financialmodelingprep.com/stable/",
            )
        except FMPCircuitOpenError:
            raise
        except (FMPSourceError, FinancialSourceError) as error:
            classified = _classify_fmp_exception(error)
            if isinstance(classified, (FMPRateLimitError, FMPQuotaError)):
                self.fmp_circuit_breaker.trip(str(classified))
            raise classified
        except Exception as error:
            classified = _classify_fmp_exception(error)
            if isinstance(classified, (FMPRateLimitError, FMPQuotaError)):
                self.fmp_circuit_breaker.trip(str(classified))
            raise classified from error
        self.fmp_circuit_breaker.record_success()
        return result

    def route(self, symbol: str) -> FinancialSourceResult:
        symbol = _normalise_symbol(symbol)
        if self.foreign_fetcher is not None and is_foreign_issuer(symbol):
            # Foreign private issuers must not pass through the domestic
            # ``us-gaap`` resolver/cache first: their source contract is
            # explicit CIK + 20-F + IFRS, and an unavailable result is typed.
            return self._route_foreign(symbol)
        try:
            payload = self._call_sec(symbol)
        except Exception as error:
            if _is_sec_unsupported(error):
                if self.foreign_fetcher is not None and is_foreign_issuer(symbol):
                    # A known foreign issuer is never sent to FMP/Yahoo.  The
                    # foreign lane either returns SEC 20-F/IFRS rows or a
                    # typed UNAVAILABLE error with its concrete reason.
                    return self._route_foreign(symbol)
                logger.info("%s is unsupported by SEC; entering controlled FMP fallback", symbol)
                return self._route_fmp(symbol)
            if isinstance(error, (SECCompanyFactsError, FinancialSourceError)):
                # Preserve the source-specific exception so callers can
                # distinguish a SEC 429/request failure from unsupported
                # coverage without an opaque wrapper.
                raise
            raise FinancialSourceUnavailable("SEC source failed for " + symbol) from error
        try:
            result = self._result(
                symbol,
                payload,
                source=SEC_SOURCE,
                source_type=SEC_SOURCE_TYPE,
                source_url="https://data.sec.gov/api/xbrl/companyfacts",
            )
        except FinancialSourceError:
            # A successful SEC call with malformed/empty rows is not an
            # unsupported symbol.  Never hide that defect behind FMP.
            raise
        return result

    # Method aliases keep call sites explicit while retaining one code path.
    fetch = route
    fetch_financials = route
    get_financials = route


__all__ = [
    "FMPInvalidPayloadError",
    "FMPQuotaError",
    "FMPRateLimitError",
    "FMPSourceError",
    "FMPStalePayloadError",
    "FMPCircuitBreaker",
    "FMPCircuitOpenError",
    "FMP_SOURCE",
    "FMP_SOURCE_TYPE",
    "FinancialSourceError",
    "FinancialSourceInvalid",
    "FinancialSourceQuotaExhausted",
    "FinancialSourceRateLimited",
    "FinancialSourceResult",
    "FinancialSourceRouter",
    "FinancialSourceStale",
    "FinancialSourceUnavailable",
    "FinancialSourceUnsupported",
    "ForeignIssuerCoverageError",
    "ForeignIssuerCoverageResult",
    "ForeignIssuerUnavailable",
    "SECUnsupportedError",
    "SEC_SOURCE",
    "SEC_SOURCE_TYPE",
]
