"""SEC 20-F / IFRS coverage for foreign issuers.

Foreign ADRs are a different source boundary from domestic US issuers.  The
SEC Company Facts endpoint exposes their facts under ``ifrs-full`` (rather
than ``us-gaap``), and only a 20-F filing is an acceptable filing anchor for
this adapter.  This module therefore owns a small, fail-closed foreign lane:

* CIKs are explicit and do not share the domestic SEC cache namespace.
* facts must contain usable IFRS revenue and profit/loss observations;
* the issuer must have a recent SEC 20-F/20-F/A filing;
* missing tags, malformed payloads, stale/network failures, and missing
  filings become a typed ``UNAVAILABLE`` result/exception;
* Yahoo/FMP data is never consulted by this module.

The normalized row shape intentionally mirrors ``sec_company_facts`` so a
caller can pass the rows through ``FinancialSourceRouter`` without inventing
foreign financial values.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import requests


logger = logging.getLogger(__name__)

SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_FOREIGN_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
SEC_FOREIGN_SOURCE = "SEC 20-F IFRS Company Facts"
SEC_FOREIGN_SOURCE_TYPE = "SEC_FOREIGN_IFRS"
DEFAULT_FOREIGN_CACHE_DIR = Path(__file__).resolve().parent / "data" / "sec_foreign_cache"
DEFAULT_USER_AGENT = os.getenv(
    "SEC_USER_AGENT", "ValuationCalculation/1.0 (contact: github-actions[bot]@users.noreply.github.com)"
)
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_CACHE_TTL_SECONDS = 24 * 60 * 60


# Explicitly supported foreign issuers.  Keeping this allow-list separate from
# the domestic SEC ticker cache prevents a foreign coverage probe from writing
# or re-labelling US cache entries.  CIK values are the SEC's ten-digit IDs.
FOREIGN_ISSUER_CIKS: Mapping[str, str] = {
    "TSM": "0001046179",   # Taiwan Semiconductor Manufacturing Company Ltd.
    "SONY": "0000313838",  # Sony Group Corporation
    "BABA": "0001577552",  # Alibaba Group Holding Limited
    "BIDU": "0001723690",
    "NIO": "0001736541",
    "PDD": "0001737806",
    "JD": "0001549802",
    "NOK": "0000924613",
    "SAP": "0001000184",
    "ASML": "0000937966",
    "TM": "0001156784",
    "VALE": "0000917851",
}
# Compatibility names for callers that prefer an explicit ``MAP`` or
# ``SUPPORTED`` label.  They are read-only aliases, not a second registry.
FOREIGN_ISSUER_CIK_MAP = FOREIGN_ISSUER_CIKS
SUPPORTED_FOREIGN_ISSUERS = FOREIGN_ISSUER_CIKS


class ForeignIssuerCoverageError(RuntimeError):
    """Base error for the foreign SEC lane."""

    code = "ERROR"

    def __init__(self, reason: str, *, symbol: Optional[str] = None, cik: Optional[str] = None):
        self.reason = str(reason or "foreign issuer coverage failed")
        self.status = self.code
        self.unavailable_reason = self.reason
        self.symbol = str(symbol or "").strip().upper() or None
        self.cik = str(cik or "").strip() or None
        super().__init__(self.reason)


class ForeignIssuerUnavailable(ForeignIssuerCoverageError):
    """Coverage is explicitly unavailable; callers must not substitute Yahoo."""

    code = "UNAVAILABLE"


# Descriptive aliases make the typed boundary discoverable to callers/tests.
ForeignCoverageUnavailable = ForeignIssuerUnavailable
ForeignIssuerUnsupported = ForeignIssuerUnavailable


class ForeignIssuerInvalid(ForeignIssuerCoverageError):
    """SEC returned malformed or unusable foreign facts."""

    code = "INVALID"


@dataclass(frozen=True)
class ForeignIssuerDefinition:
    symbol: str
    cik: str


@dataclass(frozen=True)
class ForeignIssuerCoverageResult:
    """Explicit coverage outcome used by diagnostics and non-raising callers."""

    symbol: str
    status: str
    reason: Optional[str]
    rows: Tuple[Dict[str, Any], ...]
    cik: Optional[str]
    latest_filing_date: Optional[str]
    source: str = SEC_FOREIGN_SOURCE
    source_type: str = SEC_FOREIGN_SOURCE_TYPE

    @property
    def available(self) -> bool:
        return self.status == "AVAILABLE"

    @property
    def is_available(self) -> bool:
        return self.available

    @classmethod
    def unavailable(
        cls, symbol: str, reason: str, *, cik: Optional[str] = None
    ) -> "ForeignIssuerCoverageResult":
        return cls(
            symbol=str(symbol or "").strip().upper(),
            status="UNAVAILABLE",
            reason=str(reason),
            rows=(),
            cik=cik,
            latest_filing_date=None,
        )


@dataclass(frozen=True)
class _Observation:
    value: float
    end: date
    start: Optional[date]
    filed: str
    fy: Optional[int]
    fp: Optional[str]
    form: Optional[str]
    frame: Optional[str]
    accn: Optional[str]
    unit: str
    tag: str

    @property
    def duration_days(self) -> Optional[int]:
        if self.start is None:
            return None
        return (self.end - self.start).days + 1


@dataclass(frozen=True)
class _QuarterValue:
    value: float
    observation: _Observation
    period: Optional[str]


_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
_CIK_RE = re.compile(r"^\d{10}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_FRAME_RE = re.compile(r"^CY(?P<year>\d{4})Q(?P<quarter>[1-4])(?:I)?$")

# IFRS-full names used by SEC foreign private issuer filings.  A small set of
# well-known US-GAAP aliases is retained only for issuers whose SEC 20-F facts
# are tagged under a mixed taxonomy; the source still requires an ``ifrs-full``
# facts object and never falls back to Yahoo/FMP.
_FACT_TAGS: Mapping[str, Tuple[str, ...]] = {
    "revenue": (
        "Revenue",
        "RevenueFromContractsWithCustomers",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
    ),
    "netIncome": (
        "ProfitLoss",
        "ProfitLossAttributableToOwnersOfParent",
        "NetIncomeLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
    ),
    "eps": (
        "DilutedEarningsLossPerShare",
        "BasicEarningsLossPerShare",
        "EarningsPerShareDiluted",
        "EarningsPerShareBasic",
    ),
    "operatingCashFlow": (
        "CashFlowsFromUsedInOperatingActivities",
        "CashFlowsFromUsedInOperatingActivitiesContinuingOperations",
        "NetCashProvidedByUsedInOperatingActivities",
    ),
    "capex": (
        "PurchaseOfPropertyPlantAndEquipment",
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "AdditionsToPropertyPlantAndEquipment",
        "PropertyPlantAndEquipmentAdditions",
    ),
    "shares": (
        "WeightedAverageNumberOfOrdinarySharesOutstandingDiluted",
        "WeightedAverageNumberOfOrdinarySharesOutstandingBasic",
        "WeightedAverageNumberOfSharesOutstandingDiluted",
        "WeightedAverageNumberOfSharesOutstandingBasic",
        "NumberOfSharesOutstanding",
        "EntityCommonStockSharesOutstanding",
    ),
}


def _normalise_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not _TICKER_RE.fullmatch(symbol):
        raise ForeignIssuerCoverageError("invalid foreign issuer symbol: " + symbol)
    return symbol


def _normalise_cik(value: Any) -> str:
    text = str(value or "").strip()
    if text.isdigit():
        text = text.zfill(10)
    if not _CIK_RE.fullmatch(text):
        raise ForeignIssuerInvalid("foreign issuer CIK is invalid")
    return text


def _as_date(value: Any, *, required: bool = False) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        if required:
            raise ForeignIssuerInvalid("IFRS fact observation has no end date")
        return None
    if not _DATE_RE.fullmatch(text):
        if required:
            raise ForeignIssuerInvalid("IFRS fact observation has an invalid end date")
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as error:
        if required:
            raise ForeignIssuerInvalid("IFRS fact observation has an invalid end date") from error
        return None


def _as_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _sort_key(observation: _Observation) -> Tuple[str, str, str]:
    return (observation.filed or "", observation.end.isoformat(), observation.accn or "")


def _bucket(observation: _Observation) -> str:
    duration = observation.duration_days
    if duration is None:
        return "instant"
    if duration <= 130:
        return "quarter"
    if duration <= 330:
        return "ytd"
    return "annual"


def _period(observation: _Observation) -> Optional[str]:
    fp = str(observation.fp or "").upper()
    if fp in {"Q1", "Q2", "Q3"}:
        return fp
    if fp == "FY" and observation.duration_days is not None and observation.duration_days <= 130:
        return _period_from_frame(observation.frame) or "Q4"
    return _period_from_frame(observation.frame)


def _period_from_frame(frame: Optional[str]) -> Optional[str]:
    match = _FRAME_RE.match(str(frame or ""))
    return "Q" + match.group("quarter") if match else None


def _select_unit(units: Mapping[str, Any], field: str) -> Optional[str]:
    candidates = [unit for unit, values in units.items() if isinstance(values, list)]
    if not candidates:
        return None
    if field == "shares":
        preferred = [unit for unit in candidates if "share" in unit.lower() or unit.lower() == "shares"]
    elif field == "eps":
        preferred = [unit for unit in candidates if "/" in unit or "per" in unit.lower()]
    else:
        preferred = [
            unit
            for unit in candidates
            if "/" not in unit and "share" not in unit.lower() and unit.lower() not in {"pure"}
        ]
    return (preferred or candidates)[0] if candidates else None


def _fact_observations(facts: Mapping[str, Any], field: str) -> Tuple[List[_Observation], Optional[str]]:
    for tag in _FACT_TAGS[field]:
        raw_fact = facts.get(tag)
        if not isinstance(raw_fact, Mapping):
            continue
        units = raw_fact.get("units")
        if not isinstance(units, Mapping):
            continue
        unit = _select_unit(units, field)
        if not unit:
            continue
        values = units.get(unit)
        if not isinstance(values, list):
            continue
        observations: List[_Observation] = []
        for raw in values:
            if not isinstance(raw, Mapping):
                continue
            number = _as_number(raw.get("val"))
            end = _as_date(raw.get("end"))
            if number is None or end is None:
                continue
            start = _as_date(raw.get("start"))
            filed = str(raw.get("filed") or "").strip()
            if filed and not _DATE_RE.fullmatch(filed):
                continue
            raw_fy = raw.get("fy")
            try:
                fy = int(raw_fy) if raw_fy is not None else None
            except (TypeError, ValueError):
                continue
            observations.append(
                _Observation(
                    value=number,
                    end=end,
                    start=start,
                    filed=filed,
                    fy=fy,
                    fp=str(raw.get("fp") or "").upper() or None,
                    form=str(raw.get("form") or "").upper() or None,
                    frame=str(raw.get("frame") or "") or None,
                    accn=str(raw.get("accn") or "") or None,
                    unit=str(unit),
                    tag=tag,
                )
            )
        if observations:
            return observations, tag
    return [], None


def _group_fiscal_year(observations: Sequence[_Observation]) -> Dict[int, List[_Observation]]:
    grouped: Dict[int, List[_Observation]] = {}
    for observation in observations:
        grouped.setdefault(observation.fy or observation.end.year, []).append(observation)
    return grouped


def _normalise_duration(observations: Sequence[_Observation]) -> Dict[date, _QuarterValue]:
    output: Dict[date, _QuarterValue] = {}
    for fiscal_year, rows in _group_fiscal_year(observations).items():
        direct: Dict[str, _Observation] = {}
        ytd: Dict[str, _Observation] = {}
        annual: Optional[_Observation] = None
        for observation in rows:
            bucket = _bucket(observation)
            period = _period(observation)
            if bucket == "quarter":
                # Calendar-quarter fallback is used only if the filing omits
                # fp/frame; it is never applied to annual/YTD facts.
                period = period or "Q" + str((observation.end.month - 1) // 3 + 1)
                current = direct.get(period)
                if current is None or _sort_key(observation) > _sort_key(current):
                    direct[period] = observation
            elif bucket == "ytd":
                period = str(observation.fp or "").upper()
                period = period if period in {"Q1", "Q2", "Q3"} else (_period_from_frame(observation.frame) or "")
                if period:
                    current = ytd.get(period)
                    if current is None or _sort_key(observation) > _sort_key(current):
                        ytd[period] = observation
            elif bucket == "annual":
                if annual is None or _sort_key(observation) > _sort_key(annual):
                    annual = observation

        values: Dict[str, _QuarterValue] = {
            period: _QuarterValue(observation.value, observation, period)
            for period, observation in direct.items()
        }
        prior_total = 0.0
        for period in ("Q1", "Q2", "Q3"):
            if period in values:
                prior_total += values[period].value
                continue
            cumulative = ytd.get(period)
            if cumulative is None:
                continue
            value = cumulative.value - prior_total
            values[period] = _QuarterValue(value, cumulative, period)
            prior_total += value
        if "Q4" not in values and annual is not None and all(period in values for period in ("Q1", "Q2", "Q3")):
            values["Q4"] = _QuarterValue(
                annual.value - sum(values[period].value for period in ("Q1", "Q2", "Q3")),
                annual,
                "Q4",
            )
        for quarter_value in values.values():
            current = output.get(quarter_value.observation.end)
            if current is None or _sort_key(quarter_value.observation) > _sort_key(current.observation):
                output[quarter_value.observation.end] = quarter_value
    return output


def _normalise_instant(observations: Sequence[_Observation]) -> List[_Observation]:
    by_end: Dict[date, _Observation] = {}
    for observation in observations:
        current = by_end.get(observation.end)
        if current is None or _sort_key(observation) > _sort_key(current):
            by_end[observation.end] = observation
    return sorted(by_end.values(), key=lambda row: row.end)


def _nearest_instant(observations: Sequence[_Observation], period_end: date) -> Optional[_Observation]:
    if not observations:
        return None
    prior = [row for row in observations if row.end <= period_end]
    return (prior or list(observations))[-1]


def _latest_filing_date(submissions: Mapping[str, Any]) -> Optional[str]:
    filings = submissions.get("filings") if isinstance(submissions, Mapping) else None
    recent = filings.get("recent") if isinstance(filings, Mapping) else None
    if not isinstance(recent, Mapping):
        return None
    forms = recent.get("form")
    filing_dates = recent.get("filingDate")
    if not isinstance(forms, list) or not isinstance(filing_dates, list):
        return None
    values: List[str] = []
    for index, form in enumerate(forms):
        if str(form or "").upper() not in {"20-F", "20-F/A"}:
            continue
        if index >= len(filing_dates):
            continue
        value = str(filing_dates[index] or "")
        if _DATE_RE.fullmatch(value):
            values.append(value)
    return max(values) if values else None


def latest_20f_filing_date(submissions: Mapping[str, Any]) -> Optional[str]:
    """Public parser for fixture/tests and diagnostics."""

    return _latest_filing_date(submissions)


def normalize_foreign_company_facts(
    payload: Mapping[str, Any],
    symbol: str = "",
    cik: Optional[str] = None,
    *,
    latest_filing_date: Optional[str] = None,
    max_quarters: Optional[int] = 12,
    source_url: Optional[str] = None,
    require_quarters: int = 4,
) -> List[Dict[str, Any]]:
    """Normalize IFRS facts into newest-first quarter rows.

    ``ForeignIssuerUnavailable`` is used for coverage gaps (missing IFRS
    taxonomy, missing anchor tags, or too few quarters); malformed payloads
    use ``ForeignIssuerInvalid`` so callers can distinguish bad source data.
    """

    if not isinstance(payload, Mapping) or not isinstance(payload.get("facts"), Mapping):
        raise ForeignIssuerInvalid("SEC foreign Company Facts payload has no facts object")
    facts = payload["facts"].get("ifrs-full")
    if not isinstance(facts, Mapping):
        raise ForeignIssuerUnavailable("SEC foreign Company Facts has no ifrs-full facts")
    symbol = _normalise_symbol(symbol or payload.get("entityName") or "FOREIGN")
    normalized_cik = _normalise_cik(cik) if cik is not None else None

    field_maps: Dict[str, Dict[date, _QuarterValue]] = {}
    instant_maps: Dict[str, List[_Observation]] = {}
    selected_tags: Dict[str, str] = {}
    selected_units: Dict[str, str] = {}
    for field in _FACT_TAGS:
        observations, tag = _fact_observations(facts, field)
        if not observations:
            continue
        if tag:
            selected_tags[field] = tag
        selected_units[field] = observations[0].unit
        if all(row.start is None for row in observations):
            instant_maps[field] = _normalise_instant(observations)
        else:
            field_maps[field] = _normalise_duration(observations)

    if not field_maps.get("revenue"):
        raise ForeignIssuerUnavailable("SEC foreign IFRS facts lack a usable revenue tag", symbol=symbol, cik=normalized_cik)
    if not field_maps.get("netIncome"):
        raise ForeignIssuerUnavailable(
            "SEC foreign IFRS facts lack a usable profit/loss tag", symbol=symbol, cik=normalized_cik
        )

    # Revenue and profit/loss are valuation anchors.  Publishing a quarter
    # with one anchor missing would let downstream forward-fill or rolling
    # windows hide a real coverage gap, so only their date intersection is a
    # releasable row set.
    all_dates = set(field_maps["revenue"]) & set(field_maps["netIncome"])
    required_quarters = max(1, int(require_quarters or 0))
    if len(all_dates) < required_quarters:
        raise ForeignIssuerUnavailable(
            "SEC foreign IFRS facts contain fewer than " + str(required_quarters) + " quarterly revenue/profit anchors",
            symbol=symbol,
            cik=normalized_cik,
        )
    revenue_currency = selected_units.get("revenue")
    profit_currency = selected_units.get("netIncome")
    if revenue_currency and profit_currency and revenue_currency != profit_currency:
        raise ForeignIssuerUnavailable(
            "SEC foreign IFRS revenue/profit currencies differ ("
            + revenue_currency
            + " vs "
            + profit_currency
            + ")",
            symbol=symbol,
            cik=normalized_cik,
        )

    endpoint = source_url or SEC_FOREIGN_FACTS_URL.format(cik=normalized_cik or "")
    rows: List[Dict[str, Any]] = []
    for period_end in sorted(all_dates, reverse=True):
        values: Dict[str, Any] = {}
        provenance: List[_Observation] = []
        period: Optional[str] = None
        fiscal_year: Optional[int] = None
        for field in _FACT_TAGS:
            quarter_value = field_maps.get(field, {}).get(period_end)
            observation: Optional[_Observation] = None
            if quarter_value is not None:
                values[field] = quarter_value.value
                observation = quarter_value.observation
                period = period or quarter_value.period
            else:
                values[field] = None
                instant = _nearest_instant(instant_maps.get(field, []), period_end)
                if instant is not None:
                    values[field] = instant.value
                    observation = instant
            if observation is not None:
                provenance.append(observation)
                fiscal_year = fiscal_year or observation.fy
        capex_reported = values.get("capex")
        capex = abs(capex_reported) if capex_reported is not None else None
        operating_cash_flow = values.get("operatingCashFlow")
        rows.append(
            {
                "date": period_end.isoformat(),
                "filingDate": max((row.filed for row in provenance if row.filed), default=latest_filing_date),
                "fiscalYear": fiscal_year or period_end.year,
                "period": period or "FY",
                "reportedCurrency": selected_units.get("revenue"),
                "currency": selected_units.get("revenue"),
                "revenue": values.get("revenue"),
                "netIncome": values.get("netIncome"),
                "eps": values.get("eps"),
                "operatingCashFlow": operating_cash_flow,
                "capex": capex,
                "capexReported": capex_reported,
                "capexSigned": -capex if capex is not None else None,
                "freeCashFlow": (
                    operating_cash_flow - capex
                    if operating_cash_flow is not None and capex is not None
                    else None
                ),
                "shares": values.get("shares"),
                "numberOfShares": values.get("shares"),
                "source": SEC_FOREIGN_SOURCE,
                "sourceType": SEC_FOREIGN_SOURCE_TYPE,
                "sourceUrl": endpoint,
                "cik": normalized_cik,
                "filingForm": "20-F",
                "tags": dict(selected_tags),
            }
        )
    if max_quarters is not None:
        if max_quarters <= 0:
            raise ForeignIssuerCoverageError("max_quarters must be positive")
        rows = rows[:max_quarters]
    if not rows:
        raise ForeignIssuerUnavailable("SEC foreign IFRS facts produced no quarterly rows", symbol=symbol, cik=normalized_cik)
    return rows


# US spelling alias used by some callers.
normalize_foreign_issuer_facts = normalize_foreign_company_facts


@dataclass
class ForeignIssuerCoverageSource:
    """Bounded SEC 20-F/IFRS fetcher with a dedicated foreign cache."""

    cache_dir: Path = DEFAULT_FOREIGN_CACHE_DIR
    user_agent: str = DEFAULT_USER_AGENT
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS
    submissions_url: str = SEC_SUBMISSIONS_URL
    facts_url_template: str = SEC_FOREIGN_FACTS_URL
    clock: Callable[[], float] = time.time
    issuer_ciks: Mapping[str, str] = field(default_factory=lambda: dict(FOREIGN_ISSUER_CIKS))

    def __post_init__(self) -> None:
        self.cache_dir = Path(self.cache_dir)
        self.user_agent = str(self.user_agent or "").strip()
        if not self.user_agent:
            raise ForeignIssuerCoverageError("SEC User-Agent is required")
        if self.timeout_seconds <= 0:
            raise ForeignIssuerCoverageError("SEC timeout must be positive")
        if self.cache_ttl_seconds < 0:
            raise ForeignIssuerCoverageError("SEC foreign cache TTL cannot be negative")
        self.issuer_ciks = {
            str(symbol).strip().upper(): _normalise_cik(cik) for symbol, cik in dict(self.issuer_ciks).items()
        }

    def _cache_path(self, kind: str, cik: str) -> Path:
        return self.cache_dir / (kind + "_" + cik + ".json")

    def _load_cache(self, path: Path) -> Optional[Any]:
        if not path.exists():
            return None
        try:
            age = self.clock() - path.stat().st_mtime
        except OSError:
            return None
        if age < 0 or age > self.cache_ttl_seconds:
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ForeignIssuerInvalid("invalid foreign SEC cache: " + str(path)) from error

    def _write_cache(self, path: Path, payload: Any) -> None:
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        except OSError as error:
            # A cache write cannot turn an available SEC response into an
            # unavailable one; the source response remains the authority.
            logger.warning("unable to write foreign SEC cache %s: %s", path, error)

    def _request_json(self, url: str) -> Any:
        try:
            response = requests.get(
                url,
                headers={"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"},
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as error:
            raise ForeignIssuerUnavailable("SEC foreign request failed: " + url) from error
        status = getattr(response, "status_code", None)
        if status == 429:
            raise ForeignIssuerUnavailable("SEC foreign request rate limited (429): " + url)
        if status is not None and status >= 400:
            raise ForeignIssuerUnavailable("SEC foreign request returned HTTP " + str(status) + ": " + url)
        try:
            return response.json()
        except (ValueError, TypeError, AttributeError) as error:
            raise ForeignIssuerInvalid("SEC foreign response is not valid JSON: " + url) from error

    def _payload(self, kind: str, cik: str, url: str) -> Any:
        path = self._cache_path(kind, cik)
        payload = self._load_cache(path)
        if payload is not None:
            return payload
        payload = self._request_json(url)
        self._write_cache(path, payload)
        return payload

    def resolve_cik(self, symbol: str) -> str:
        symbol = _normalise_symbol(symbol)
        try:
            return _normalise_cik(self.issuer_ciks[symbol])
        except KeyError as error:
            raise ForeignIssuerUnavailable(
                "foreign issuer CIK is not configured for " + symbol, symbol=symbol
            ) from error

    def fetch(
        self,
        symbol: str,
        *,
        max_quarters: Optional[int] = 12,
        cik: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        symbol = _normalise_symbol(symbol)
        cik = _normalise_cik(cik) if cik is not None else self.resolve_cik(symbol)
        submissions_url = self.submissions_url.format(cik=cik)
        facts_url = self.facts_url_template.format(cik=cik)
        submissions = self._payload("submissions", cik, submissions_url)
        filing_date = _latest_filing_date(submissions)
        if filing_date is None:
            raise ForeignIssuerUnavailable(
                "SEC submissions contain no 20-F/20-F/A filing", symbol=symbol, cik=cik
            )
        facts = self._payload("companyfacts", cik, facts_url)
        return normalize_foreign_company_facts(
            facts,
            symbol,
            cik,
            latest_filing_date=filing_date,
            max_quarters=max_quarters,
            source_url=facts_url,
        )

    fetch_financials = fetch
    route = fetch

    def coverage(self, symbol: str, *, max_quarters: Optional[int] = 12) -> ForeignIssuerCoverageResult:
        symbol = str(symbol or "").strip().upper()
        try:
            cik = self.resolve_cik(symbol)
            rows = self.fetch(symbol, max_quarters=max_quarters, cik=cik)
            filing_date = max((str(row.get("filingDate")) for row in rows if row.get("filingDate")), default=None)
            return ForeignIssuerCoverageResult(
                symbol=symbol,
                status="AVAILABLE",
                reason=None,
                rows=tuple(rows),
                cik=cik,
                latest_filing_date=filing_date,
            )
        except ForeignIssuerCoverageError as error:
            return ForeignIssuerCoverageResult.unavailable(symbol, error.reason, cik=error.cik)


# Concise aliases for integration code.
ForeignIssuerCoverage = ForeignIssuerCoverageSource
ForeignCoverageSource = ForeignIssuerCoverageSource


def is_foreign_issuer(symbol: Any) -> bool:
    try:
        normalized = _normalise_symbol(symbol)
    except ForeignIssuerCoverageError:
        return False
    return normalized in FOREIGN_ISSUER_CIKS


def foreign_issuer_definition(symbol: Any) -> Optional[ForeignIssuerDefinition]:
    try:
        normalized = _normalise_symbol(symbol)
    except ForeignIssuerCoverageError:
        return None
    cik = FOREIGN_ISSUER_CIKS.get(normalized)
    return ForeignIssuerDefinition(normalized, cik) if cik else None


def fetch_foreign_issuer_financials(
    symbol: str,
    *,
    source: Optional[ForeignIssuerCoverageSource] = None,
    max_quarters: Optional[int] = 12,
    cik: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Convenience wrapper for the SEC-only foreign source."""

    adapter = source or ForeignIssuerCoverageSource()
    return adapter.fetch(symbol, max_quarters=max_quarters, cik=cik)


def get_foreign_issuer_coverage(
    symbol: str,
    *,
    source: Optional[ForeignIssuerCoverageSource] = None,
    max_quarters: Optional[int] = 12,
) -> ForeignIssuerCoverageResult:
    """Return ``AVAILABLE``/``UNAVAILABLE`` without hiding the reason."""

    adapter = source or ForeignIssuerCoverageSource()
    return adapter.coverage(symbol, max_quarters=max_quarters)


__all__ = [
    "DEFAULT_FOREIGN_CACHE_DIR",
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_USER_AGENT",
    "FOREIGN_ISSUER_CIKS",
    "FOREIGN_ISSUER_CIK_MAP",
    "SUPPORTED_FOREIGN_ISSUERS",
    "ForeignCoverageSource",
    "ForeignCoverageUnavailable",
    "ForeignIssuerCoverage",
    "ForeignIssuerCoverageError",
    "ForeignIssuerCoverageResult",
    "ForeignIssuerCoverageSource",
    "ForeignIssuerDefinition",
    "ForeignIssuerInvalid",
    "ForeignIssuerUnavailable",
    "ForeignIssuerUnsupported",
    "SEC_FOREIGN_FACTS_URL",
    "SEC_FOREIGN_SOURCE",
    "SEC_FOREIGN_SOURCE_TYPE",
    "SEC_SUBMISSIONS_URL",
    "foreign_issuer_definition",
    "fetch_foreign_issuer_financials",
    "get_foreign_issuer_coverage",
    "is_foreign_issuer",
    "latest_20f_filing_date",
    "normalize_foreign_company_facts",
    "normalize_foreign_issuer_facts",
]
