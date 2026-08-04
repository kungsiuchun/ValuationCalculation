"""SEC Company Facts financial-source adapter.

The SEC Company Facts endpoint exposes XBRL facts rather than one row per
quarter.  This module keeps the source boundary deliberately small: resolve a
ticker to its SEC CIK, fetch/cache the raw facts, and turn duration/YTD/annual
facts into normalized quarterly rows used by the valuation pipeline.

The adapter fails closed.  An expired cache is never used as a substitute for
a failed refresh, and malformed or rate-limited SEC responses are surfaced to
the caller with a source-specific exception.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import requests


logger = logging.getLogger(__name__)

SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_CACHE_TTL_SECONDS = 24 * 60 * 60
DEFAULT_CACHE_DIR = Path(__file__).resolve().parent / "data" / "sec_cache"
# The SEC requires a descriptive User-Agent with a contact address.  A caller
# should normally override this through ``SEC_USER_AGENT`` or the constructor.
DEFAULT_USER_AGENT = os.getenv(
    "SEC_USER_AGENT", "ValuationCalculation/1.0 contact@sius.ai"
)


class SECCompanyFactsError(RuntimeError):
    """Base exception raised by the SEC source adapter."""


class SECTickerNotFoundError(SECCompanyFactsError):
    """The SEC ticker map does not contain the requested symbol."""


class SECRequestError(SECCompanyFactsError):
    """The SEC request failed or was rate limited."""


class SECRateLimitError(SECRequestError):
    """The SEC rejected a request with HTTP 429."""


class SECInvalidPayloadError(SECCompanyFactsError):
    """The response/cache is not a valid Company Facts payload."""


_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
_FRAME_RE = re.compile(r"^CY(?P<year>\d{4})Q(?P<quarter>[1-4])(?:I)?$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# Standard US-GAAP tags.  Company-specific extensions are intentionally not
# guessed: unsupported/missing fields stay ``None`` and the source provenance
# remains visible to the caller.
_FACT_TAGS: Mapping[str, Tuple[str, ...]] = {
    "revenue": (
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "RevenuesNetOfInterestExpense",
        "SalesRevenueNet",
    ),
    "netIncome": (
        "ProfitLoss",
        "NetIncomeLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
    ),
    "eps": ("EarningsPerShareDiluted", "EarningsPerShareBasic"),
    "operatingCashFlow": (
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ),
    "capex": (
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
        "PaymentsToAcquireBusinessesNetOfCashAcquired",
    ),
    "shares": (
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingBasic",
        "EntityCommonStockSharesOutstanding",
    ),
}

_FACT_UNITS: Mapping[str, Tuple[str, ...]] = {
    "revenue": ("USD",),
    "netIncome": ("USD",),
    "eps": ("USD/shares", "USD/share"),
    "operatingCashFlow": ("USD",),
    "capex": ("USD",),
    "shares": ("shares",),
}


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


def _as_date(value: Any, *, required: bool = False) -> Optional[date]:
    if value is None or value == "":
        if required:
            raise SECInvalidPayloadError("SEC Company Facts record has no end date")
        return None
    text = str(value)
    if not _DATE_RE.match(text):
        if required:
            raise SECInvalidPayloadError("SEC Company Facts record has an invalid date")
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as error:
        if required:
            raise SECInvalidPayloadError("SEC Company Facts record has an invalid date") from error
        return None


def _as_number(value: Any) -> Optional[float]:
    # SEC JSON values are numeric.  Do not coerce arbitrary strings: silently
    # accepting a malformed value would make a fresh release look trustworthy.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def _normalise_ticker(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not _TICKER_RE.fullmatch(symbol):
        raise SECCompanyFactsError("invalid SEC ticker: " + symbol)
    return symbol


def _normalise_cik(value: Any) -> str:
    try:
        digits = str(int(value))
    except (TypeError, ValueError) as error:
        raise SECInvalidPayloadError("SEC ticker map has an invalid CIK") from error
    if not digits.isdigit() or len(digits) > 10:
        raise SECInvalidPayloadError("SEC ticker map has an invalid CIK")
    return digits.zfill(10)


def _ticker_equivalent(value: str) -> str:
    return value.replace("-", "").replace(".", "").upper()


def _parse_frame_period(frame: Optional[str]) -> Optional[str]:
    if not frame:
        return None
    match = _FRAME_RE.match(frame)
    if not match:
        return None
    return "Q" + match.group("quarter")


def _period_from_observation(observation: _Observation) -> Optional[str]:
    fp = str(observation.fp or "").upper()
    if fp in {"Q1", "Q2", "Q3"}:
        return fp
    # A 10-K commonly reports the fourth three-month period with fp=FY.  A
    # frame is useful for distinguishing that quarter from the annual value.
    if fp == "FY" and observation.duration_days is not None and observation.duration_days <= 130:
        # Annual filings may carry three-month comparative rows with a frame;
        # preserve that frame so Q1-Q4 do not collapse onto one value.
        return _parse_frame_period(observation.frame) or "Q4"
    return _parse_frame_period(observation.frame)


def _observation_sort_key(observation: _Observation) -> Tuple[str, str, str]:
    return (observation.filed or "", observation.end.isoformat(), observation.accn or "")


def _choose_latest(observations: Iterable[_Observation]) -> Optional[_Observation]:
    rows = list(observations)
    return max(rows, key=_observation_sort_key) if rows else None


def _bucket(observation: _Observation) -> str:
    duration = observation.duration_days
    if duration is None:
        return "instant"
    if duration <= 130:
        return "quarter"
    if duration <= 330:
        return "ytd"
    return "annual"


def _fact_observations(
    facts: Mapping[str, Any], field: str
) -> Tuple[List[_Observation], Optional[str]]:
    """Extract observations from the usable tag with the newest filing data.

    Issuers can leave an older preferred taxonomy tag populated while moving
    current filings to a sibling tag (NVDA is a concrete example). Selecting
    the first non-empty tag would therefore create a false "no anchors" gap.
    """

    best: Optional[Tuple[date, int, List[_Observation], str]] = None
    for tag in _FACT_TAGS[field]:
        raw_fact = facts.get(tag)
        if not isinstance(raw_fact, Mapping):
            continue
        units = raw_fact.get("units")
        if not isinstance(units, Mapping):
            continue
        preferred_units = _FACT_UNITS[field]
        selected_units = [unit for unit in preferred_units if unit in units]
        if not selected_units:
            # If SEC introduces an equivalent spelling (for example USD/share),
            # use a single unambiguous unit rather than mixing scales.
            candidates = [unit for unit, values in units.items() if isinstance(values, list)]
            if len(candidates) == 1:
                selected_units = candidates
        observations: List[_Observation] = []
        malformed = False
        for unit in selected_units:
            values = units.get(unit)
            if not isinstance(values, list):
                malformed = True
                continue
            for raw in values:
                if not isinstance(raw, Mapping):
                    malformed = True
                    continue
                number = _as_number(raw.get("val"))
                end = _as_date(raw.get("end"))
                if number is None or end is None:
                    malformed = True
                    continue
                start = _as_date(raw.get("start"))
                filed = str(raw.get("filed") or "").strip()
                if filed and not _DATE_RE.match(filed):
                    malformed = True
                    continue
                fy = raw.get("fy")
                if fy is not None:
                    try:
                        fy = int(fy)
                    except (TypeError, ValueError):
                        malformed = True
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
            # A malformed sibling record should not poison otherwise valid
            # observations. Prefer the tag whose observations reach furthest
            # forward in time, then the one with the broader usable history.
            score = (max(observation.end for observation in observations), len(observations))
            if best is None or score > best[:2]:
                best = (score[0], score[1], observations, tag)
        if malformed and raw_fact:
            continue
    if best is None:
        return [], None
    return best[2], best[3]


def _group_fiscal_year(observations: Sequence[_Observation]) -> Dict[int, List[_Observation]]:
    grouped: Dict[int, List[_Observation]] = {}
    for observation in observations:
        fiscal_year = observation.fy or observation.end.year
        grouped.setdefault(fiscal_year, []).append(observation)
    return grouped


def _normalise_duration_fact(
    observations: Sequence[_Observation], *, derive_annual: bool = True
) -> Dict[date, _QuarterValue]:
    """Convert duration observations to one value per quarter end date.

    SEC reports often expose Q2/Q3 cash-flow facts as year-to-date values only;
    those are differenced against the previous quarter.  A missing Q4 is
    derived from the annual value after Q1-Q3 are available.
    """

    output: Dict[date, _QuarterValue] = {}
    for fiscal_year, rows in _group_fiscal_year(observations).items():
        direct: Dict[str, _Observation] = {}
        ytd: Dict[str, _Observation] = {}
        annual: Optional[_Observation] = None
        for row in rows:
            bucket = _bucket(row)
            period = _period_from_observation(row)
            if bucket == "quarter" and period:
                current = direct.get(period)
                if current is None or _observation_sort_key(row) > _observation_sort_key(current):
                    direct[period] = row
            elif bucket == "ytd":
                # fp=Q2/Q3 marks cumulative values.  If fp is absent, the
                # frame gives a calendar-quarter hint; this is still useful
                # for fixtures and issuers that omit fp.
                period = str(row.fp or "").upper()
                if period not in {"Q1", "Q2", "Q3"}:
                    period = _parse_frame_period(row.frame) or ""
                if period:
                    current = ytd.get(period)
                    if current is None or _observation_sort_key(row) > _observation_sort_key(current):
                        ytd[period] = row
            elif bucket == "annual":
                if annual is None or _observation_sort_key(row) > _observation_sort_key(annual):
                    annual = row

        values: Dict[str, _QuarterValue] = {}
        # Prefer direct three-month observations.  For Q1, a short duration is
        # already a quarter even when fp is missing.
        for period, row in direct.items():
            values[period] = _QuarterValue(row.value, row, period)

        if not derive_annual:
            # Weighted-average share facts are not additive.  Never subtract
            # Q1-Q3 from an annual average (that produces impossible negative
            # share counts); direct quarter observations are the only safe
            # duration values.  Instant DEI shares are merged separately.
            for quarter_value in values.values():
                output[quarter_value.observation.end] = quarter_value
            continue

        # Difference YTD values into missing Q2/Q3 (and Q1 where an issuer
        # labels a short YTD record as Q1).
        prior_values: List[float] = []
        for period in ("Q1", "Q2", "Q3"):
            if period in values:
                prior_values.append(values[period].value)
                continue
            cumulative = ytd.get(period)
            if cumulative is None:
                continue
            value = cumulative.value - sum(prior_values)
            values[period] = _QuarterValue(value, cumulative, period)
            prior_values.append(value)

        # Annual facts are not a quarter.  Use them only to derive a missing
        # Q4 once Q1-Q3 are known; this avoids publishing a duplicate FY row.
        if "Q4" not in values and annual is not None and all(p in values for p in ("Q1", "Q2", "Q3")):
            value = annual.value - sum(values[p].value for p in ("Q1", "Q2", "Q3"))
            values["Q4"] = _QuarterValue(value, annual, "Q4")

        # A direct Q4 with no annual record is valid (some 10-Q records use
        # fiscal Q1 dates that happen to be calendar Q4); preserve it.
        for period, quarter_value in values.items():
            current = output.get(quarter_value.observation.end)
            if current is None or _observation_sort_key(quarter_value.observation) > _observation_sort_key(current.observation):
                output[quarter_value.observation.end] = quarter_value
    return output


def _normalise_instant_fact(observations: Sequence[_Observation]) -> List[_Observation]:
    by_end: Dict[date, _Observation] = {}
    for row in observations:
        current = by_end.get(row.end)
        if current is None or _observation_sort_key(row) > _observation_sort_key(current):
            by_end[row.end] = row
    return sorted(by_end.values(), key=lambda row: row.end)


def _normalise_fact(
    observations: Sequence[_Observation], *, derive_annual: bool = True
) -> Tuple[Dict[date, _QuarterValue], List[_Observation]]:
    if observations and all(row.start is None for row in observations):
        return {}, _normalise_instant_fact(observations)
    return _normalise_duration_fact(observations, derive_annual=derive_annual), []


def _nearest_instant(observations: Sequence[_Observation], period_end: date) -> Optional[_Observation]:
    if not observations:
        return None
    prior = [row for row in observations if row.end <= period_end]
    return (prior or list(observations))[-1] if prior else observations[0]


def normalize_company_facts(
    payload: Mapping[str, Any],
    symbol: str = "",
    cik: Optional[str] = None,
    *,
    max_quarters: Optional[int] = 12,
    source_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Normalize a raw Company Facts payload into newest-first quarterly rows."""

    if not isinstance(payload, Mapping) or not isinstance(payload.get("facts"), Mapping):
        raise SECInvalidPayloadError("SEC Company Facts payload has no facts object")
    us_gaap = payload["facts"].get("us-gaap")
    if not isinstance(us_gaap, Mapping):
        raise SECInvalidPayloadError("SEC Company Facts payload has no us-gaap facts")
    dei = payload["facts"].get("dei")
    if not isinstance(dei, Mapping):
        dei = {}

    field_maps: Dict[str, Dict[date, _QuarterValue]] = {}
    instant_maps: Dict[str, List[_Observation]] = {}
    selected_tags: Dict[str, str] = {}
    for field in _FACT_TAGS:
        observations, tag = _fact_observations(us_gaap, field)
        entity_share_observations: List[_Observation] = []
        entity_share_tag: Optional[str] = None
        if field == "shares":
            # EntityCommonStockSharesOutstanding lives in ``dei`` and is an
            # instant fact; keep it as a fallback even when weighted-average
            # US-GAAP shares are available for only some quarters.
            entity_share_observations, entity_share_tag = _fact_observations(dei, field)
            if not observations and entity_share_observations:
                observations, tag = entity_share_observations, entity_share_tag
        if tag:
            selected_tags[field] = tag
        if observations:
            duration_map, instant = _normalise_fact(observations, derive_annual=field != "shares")
            field_maps[field] = duration_map
            instant_maps[field] = instant
        if entity_share_observations and field == "shares":
            instant_maps[field] = instant_maps.get("shares", []) + _normalise_instant_fact(entity_share_observations)
            if entity_share_tag:
                selected_tags["sharesEntity"] = entity_share_tag

    # Revenue and net income anchor the release.  A payload with no usable
    # anchor is unsupported, even if it contains unrelated valid XBRL facts.
    if not field_maps.get("revenue") or not field_maps.get("netIncome"):
        raise SECInvalidPayloadError("SEC Company Facts payload lacks quarterly revenue/net income")

    all_dates: set[date] = set()
    for values in field_maps.values():
        all_dates.update(values)
    if not all_dates:
        raise SECInvalidPayloadError("SEC Company Facts payload has no quarterly observations")

    symbol = str(symbol or "").strip().upper() or str(payload.get("entityName") or "SEC")
    normalized: List[Dict[str, Any]] = []
    endpoint = source_url or "https://data.sec.gov/api/xbrl/companyfacts"
    for period_end in sorted(all_dates, reverse=True):
        row_values: Dict[str, Any] = {}
        provenance: List[_Observation] = []
        period: Optional[str] = None
        fiscal_year: Optional[int] = None
        for field in _FACT_TAGS:
            quarter_value = field_maps.get(field, {}).get(period_end)
            observation: Optional[_Observation] = None
            if quarter_value is not None:
                row_values[field] = quarter_value.value
                observation = quarter_value.observation
                period = period or quarter_value.period
            else:
                row_values[field] = None
                instant = _nearest_instant(instant_maps.get(field, []), period_end)
                if instant is not None:
                    row_values[field] = instant.value
                    observation = instant
            if observation is not None:
                provenance.append(observation)
                fiscal_year = fiscal_year or observation.fy

        # Shares are exposed under both names so existing FMP-shaped consumers
        # can migrate without a second source-specific branch.
        row_values["numberOfShares"] = row_values.get("shares")
        capex_reported = row_values.get("capex")
        capex = abs(capex_reported) if capex_reported is not None else None
        if capex is not None:
            # SEC PaymentsToAcquire* tags are positive cash outflows.  Keep
            # ``capex`` as that positive magnitude and expose the signed form
            # separately for consumers that add cash-flow components directly.
            row_values["capex"] = capex
            row_values["capexReported"] = capex_reported
            row_values["capexSigned"] = -capex
        else:
            row_values["capexReported"] = None
            row_values["capexSigned"] = None
        operating_cash_flow = row_values.get("operatingCashFlow")
        row_values["freeCashFlow"] = (
            operating_cash_flow - capex
            if operating_cash_flow is not None and capex is not None
            else None
        )
        # Do not publish rows consisting solely of an instant share fallback.
        if all(row_values.get(field) is None for field in ("revenue", "netIncome", "eps", "operatingCashFlow", "capex")):
            continue
        filing_dates = [row.filed for row in provenance if row.filed]
        filing_date = max(filing_dates) if filing_dates else None
        if fiscal_year is None:
            fiscal_year = period_end.year
        normalized.append(
            {
                "date": period_end.isoformat(),
                "filingDate": filing_date,
                "fiscalYear": fiscal_year,
                "period": period or "FY",
                "reportedCurrency": "USD",
                "currency": "USD",
                "revenue": row_values.get("revenue"),
                "netIncome": row_values.get("netIncome"),
                "eps": row_values.get("eps"),
                "operatingCashFlow": row_values.get("operatingCashFlow"),
                "capex": row_values.get("capex"),
                "capexReported": row_values.get("capexReported"),
                "capexSigned": row_values.get("capexSigned"),
                "freeCashFlow": row_values.get("freeCashFlow"),
                "shares": row_values.get("shares"),
                "numberOfShares": row_values.get("numberOfShares"),
                "source": "SEC Company Facts",
                "sourceType": "SEC_COMPANY_FACTS",
                "sourceUrl": endpoint,
                "cik": _normalise_cik(cik) if cik is not None else None,
                "tags": dict(selected_tags),
            }
        )
    if not normalized:
        raise SECInvalidPayloadError("SEC Company Facts payload produced no normalized quarters")
    if max_quarters is not None:
        if max_quarters <= 0:
            raise SECCompanyFactsError("max_quarters must be positive")
        normalized = normalized[:max_quarters]
    return normalized


# Alias with an explicit source name for callers that prefer it.
normalize_sec_company_facts = normalize_company_facts


@dataclass
class SECCompanyFactsSource:
    """Fetch and normalize SEC Company Facts with bounded, testable I/O."""

    cache_dir: Path = DEFAULT_CACHE_DIR
    user_agent: str = DEFAULT_USER_AGENT
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS
    ticker_url: str = SEC_TICKER_URL
    facts_url_template: str = SEC_FACTS_URL
    clock: Callable[[], float] = time.time

    def __post_init__(self) -> None:
        self.cache_dir = Path(self.cache_dir)
        self.user_agent = str(self.user_agent or "").strip()
        if not self.user_agent:
            raise SECCompanyFactsError("SEC User-Agent is required")
        if self.timeout_seconds <= 0:
            raise SECCompanyFactsError("SEC timeout must be positive")
        if self.cache_ttl_seconds < 0:
            raise SECCompanyFactsError("SEC cache TTL cannot be negative")

    def _cache_path(self, name: str) -> Path:
        return self.cache_dir / name

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
            raise SECInvalidPayloadError("invalid SEC cache: " + str(path)) from error

    def _write_cache(self, path: Path, payload: Any) -> None:
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        except OSError as error:
            # A cache write must not make a valid source unavailable.  Log the
            # condition, but never treat an old cache as a fresh response.
            logger.warning("unable to write SEC cache %s: %s", path, error)

    def _request_json(self, url: str) -> Any:
        headers = {"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"}
        try:
            response = requests.get(url, headers=headers, timeout=self.timeout_seconds)
        except requests.RequestException as error:
            raise SECRequestError("SEC request failed: " + url) from error
        status = getattr(response, "status_code", None)
        if status == 429:
            raise SECRateLimitError("SEC request rate limited (429): " + url)
        if status is not None and status >= 400:
            raise SECRequestError("SEC request returned HTTP " + str(status) + ": " + url)
        try:
            payload = response.json()
        except (ValueError, TypeError, AttributeError) as error:
            raise SECInvalidPayloadError("SEC response is not valid JSON: " + url) from error
        return payload

    def _load_ticker_map(self) -> Any:
        path = self._cache_path("company_tickers.json")
        cached = self._load_cache(path)
        if cached is not None:
            _validate_ticker_map(cached)
            return cached
        payload = self._request_json(self.ticker_url)
        _validate_ticker_map(payload)
        self._write_cache(path, payload)
        return payload

    def resolve_cik(self, symbol: str) -> str:
        symbol = _normalise_ticker(symbol)
        body = self._load_ticker_map()
        rows = body.values() if isinstance(body, Mapping) else body
        equivalent = _ticker_equivalent(symbol)
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            ticker = str(row.get("ticker") or "").strip().upper()
            if ticker == symbol or _ticker_equivalent(ticker) == equivalent:
                return _normalise_cik(row.get("cik_str", row.get("cik")))
        raise SECTickerNotFoundError("SEC ticker not found: " + symbol)

    def fetch(self, symbol: str, *, max_quarters: Optional[int] = 12, cik: Optional[str] = None) -> List[Dict[str, Any]]:
        symbol = _normalise_ticker(symbol)
        cik = _normalise_cik(cik) if cik is not None else self.resolve_cik(symbol)
        path = self._cache_path("companyfacts_" + cik + ".json")
        payload = self._load_cache(path)
        if payload is None:
            url = self.facts_url_template.format(cik=cik)
            payload = self._request_json(url)
            _validate_company_facts(payload)
            self._write_cache(path, payload)
        else:
            _validate_company_facts(payload)
            url = self.facts_url_template.format(cik=cik)
        return normalize_company_facts(payload, symbol, cik, max_quarters=max_quarters, source_url=url)


def _validate_ticker_map(payload: Any) -> None:
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, Mapping):
        rows = list(payload.values())
    else:
        raise SECInvalidPayloadError("SEC ticker map is not an object/list")
    if not rows or not any(isinstance(row, Mapping) and row.get("ticker") for row in rows):
        raise SECInvalidPayloadError("SEC ticker map has no ticker rows")


def _validate_company_facts(payload: Any) -> None:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("facts"), Mapping):
        raise SECInvalidPayloadError("SEC Company Facts payload has no facts object")
    if not isinstance(payload["facts"].get("us-gaap"), Mapping):
        raise SECInvalidPayloadError("SEC Company Facts payload has no us-gaap facts")


def fetch_sec_financials(
    symbol: str,
    *,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    user_agent: str = DEFAULT_USER_AGENT,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
    max_quarters: Optional[int] = 12,
    cik: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Convenience wrapper for one SEC Company Facts fetch."""

    source = SECCompanyFactsSource(
        cache_dir=cache_dir,
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
        cache_ttl_seconds=cache_ttl_seconds,
    )
    return source.fetch(symbol, max_quarters=max_quarters, cik=cik)


__all__ = [
    "DEFAULT_CACHE_DIR",
    "DEFAULT_CACHE_TTL_SECONDS",
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_USER_AGENT",
    "SECCompanyFactsError",
    "SECCompanyFactsSource",
    "SECInvalidPayloadError",
    "SECRateLimitError",
    "SECRequestError",
    "SECTickerNotFoundError",
    "fetch_sec_financials",
    "normalize_company_facts",
    "normalize_sec_company_facts",
]
