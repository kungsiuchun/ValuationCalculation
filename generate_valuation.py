import requests
import pandas as pd
import numpy as np
import yfinance as yf
import json
import os
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path

from price_adapter import PriceSourceUnavailable, YahooPriceAdapter
from financial_source_router import (
    FMPInvalidPayloadError,
    FMPQuotaError,
    FMPRateLimitError,
    FMPStalePayloadError,
    FinancialSourceResult,
    FinancialSourceRouter,
    FMPCircuitBreaker,
)
from sec_company_facts import SECCompanyFactsSource
from foreign_issuer_coverage import ForeignIssuerCoverageSource
from ticker_universe import DEFAULT_TICKERS, UniverseValidationError, resolve_tickers, yahoo_symbol

# from dotenv import load_dotenv

# load_dotenv()

# è¨­å®šæ—¥èªŒæ ¼å¼
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- 1. é…ç½® ---
FMP_API_KEY = os.getenv('FMP_API_KEY')
FMP_API_KEY_2 = os.getenv('FMP_API_KEY_2')
FMP_API_KEY_3 = os.getenv('FMP_API_KEY_3')
FMP_REQUEST_TIMEOUT_SECONDS = 30
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "data")
CACHE_BASE_DIR = os.path.join(OUTPUT_DIR, "fmp_cache") # ç·©å­˜ä¸»ç›®éŒ„
SOURCE_FINANCIAL_DIR = os.path.join(OUTPUT_DIR, "source_financials")
DOW_30 = list(DEFAULT_TICKERS)
# DOW_30 = [
#     "AAPL", "ABBV", "ADBE", "AMD", "AMZN", "BA", "BABA", "BAC",
#     "COST", "CSCO", "CVX", "DIS", "ETSY", "FDX", "GE", "GOOGL",
#     "HOOD", "INTC", "JNJ", "JPM", "KO", "META", "MSFT", "NFLX",
#     "NKE", "NVDA", "PFE", "PLTR", "PYPL", "RBLX", "SBUX", "SHOP",
#     "SNAP", "SOFI", "T", "TGT", "TSLA", "TSM", "UBER",
#     "UNH", "V", "VZ", "WMT", "XOM"
# ]


WINDOWS = {"1Y": 252, "2Y": 504, "3Y": 756, "5Y": 1260}
QUARTERS = ['q1', 'q2', 'q3', 'q4']
SUPPORTED_FX_RATES = {"USD": 1.0, "TWD": 32.5}
CACHE_EXPIRY_DAYS = 3
YFINANCE_MAX_ATTEMPTS = 3
YFINANCE_RETRY_DELAY_SECONDS = 15
YFINANCE_TIMEOUT_SECONDS = 30
FINANCIAL_SOURCE_ROUTER = None
LAST_FINANCIAL_SOURCE_RESULT = None

# --- Helper Functions --- Get latest processed quarter ---
def get_latest_processed_quarter(ticker):
    # Path to the ticker's cache directory
    ticker_cache_dir = os.path.join(CACHE_BASE_DIR, ticker.upper())
    if not os.path.exists(ticker_cache_dir):
        return None

    # Use pandas to read all JSON files in the directory
    try:
        dfs = []
        for f_name in os.listdir(ticker_cache_dir):
            if f_name.endswith('.json'):
                file_path = os.path.join(ticker_cache_dir, f_name)
                df = pd.read_json(file_path)
                # Extract the statement type from the filename (e.g., "income-statement_q1.json" -> "income-statement")
                df['statement_type'] = f_name.split('_')[0]
                dfs.append(df)
    except Exception as e:
        logger.warning(f"Error reading JSON files from {ticker_cache_dir}: {e}")
        return None

    if not dfs:
        return None

    combined_df = pd.concat(dfs).set_index('date')
    combined_df = combined_df.groupby('date').agg(statement_count=('statement_type', 'count'), period=('period', 'first')).reset_index().set_index('date').sort_index()

    # Filter for dates where all statement types are present (or at least 4, assuming 4 statement types)
    combined_df = combined_df[combined_df['statement_count'] > 3]

    # Find the maximum date from the combined DataFrame's index

    if combined_df.empty:
        return None
    else:
        latest_date_str = combined_df.index.max().strftime('%Y-%m-%d')
        latest_period = combined_df.loc[latest_date_str, 'period']
        quarter = latest_period.lower()  # e.g., 'Q1', 'Q2', etc.
        return quarter




def get_next_quarter(current_q_str):
    """
    Input: 'Q3' -> Output: 'Q4'
    Input: 'Q4' -> Output: 'Q1'
    """
    q_num = int(current_q_str[-1])

    if q_num < 4:
        return f"q{q_num + 1}"
    else:
        return "q1"

# --- 2. æŠ½å–å±¤ (Extract Layer) ---
def get_fmp_fragmented(endpoint, ticker, *, api_keys=None, circuit_breaker=None):
    """
    [Data Engineering Logic]:
    è‡ªå‹•å»ºç«‹å°æ‡‰ ticker çš„å­è³‡æ–™å¤¾ï¼Œä¸¦å¯¦æ–½ã€Žå¢žé‡åˆä½µç­–ç•¥ã€ã€‚
    é˜²æ­¢æ–° API æ•¸æ“šè¦†è“‹æŽ‰èˆŠçš„æ­·å²è²¡å ±æ•¸æ“š (å°¤å…¶æ˜¯è§£æ±º FMP 5å¹´é™åˆ¶)ã€‚
    """
    ticker = ticker.upper()
    combined_all_quarters = []
    ticker_cache_dir = os.path.join(CACHE_BASE_DIR, ticker)
    os.makedirs(ticker_cache_dir, exist_ok=True)

    # 1. ç²å–æœ€æ–°å·²è™•ç†çš„å­£åº¦ï¼Œæ±ºå®šå¢žé‡æŠ“å–çš„ç›®æ¨™
    latest_q = get_latest_processed_quarter(ticker)
    next_q = get_next_quarter(latest_q) if latest_q else None

    logger.info(f"<{ticker}> Start fragmented fetch. Latest processed: {latest_q} and next target is: {next_q}")

    # 2. éæ­·å››å­£é€²è¡Œè™•ç†
    for q in QUARTERS:
        print("--- Processing", q, "for", ticker," ", endpoint, "---")
        cache_path = os.path.join(ticker_cache_dir, f"{endpoint}_{q}.json")
        is_target_increment = (q == next_q)

        # æª¢æŸ¥ç·©å­˜ç‹€æ…‹
        cache_exists = os.path.exists(cache_path)
        is_expired = False
        if cache_exists:
            mtime = os.path.getmtime(cache_path)
            is_expired = (time.time() - mtime) > (CACHE_EXPIRY_DAYS * 86400)
            is_expired = True # Forcing expiry for running in Github Actions frequently

        # æ±ºå®šæ˜¯å¦éœ€è¦èª¿ç”¨ API
        # æ¢ä»¶ï¼šç·©å­˜ä¸å­˜åœ¨ OR è©²å­£åº¦æ˜¯æˆ‘å€‘è¿½è¹¤çš„ã€Œä¸‹ä¸€å€‹å¢žé‡é»žã€ä¸”å·²éŽæœŸ
        needs_api_call = not cache_exists or (is_target_increment and is_expired)
        print(f"needs_api_call for {ticker} {q} {endpoint}: {needs_api_call}")

        existing_data = []
        if cache_exists:
            try:
                with open(cache_path, 'r') as f:
                    logger.info(f"<{ticker}> Loading existing cache for {q} {endpoint}...")
                    existing_data = json.load(f)
            except Exception as e:
                logger.error(f"<{ticker}> Failed to load cache {q} {endpoint}: {e}")

        if needs_api_call:
            action = "Incremental Update" if cache_exists else "Initial Fetch"
            logger.info(f"<{ticker}> {action} for {q} {endpoint}...")
            refresh_succeeded = False

            configured_keys = [key for key in (FMP_API_KEY, FMP_API_KEY_2, FMP_API_KEY_3) if key] if api_keys is None else list(api_keys)
            # FMP fallback is intentionally single-key.  Additional keys are
            # not a retry lane: a rate-limit/quota response must stop the
            # ticker rather than hide provider exhaustion.
            if api_keys is None:
                configured_keys = configured_keys[:1]
            api_keys_to_try = [key for key in configured_keys if key]
            if not api_keys_to_try:
                logger.error(f"<{ticker}> No FMP API keys configured; cannot refresh {q} {endpoint}.")
            else:
              for api_key in api_keys_to_try:
                if circuit_breaker is not None:
                    circuit_breaker.check()
                url = f"https://financialmodelingprep.com/stable/{endpoint}/?symbol={ticker}&period={q}&apikey={api_key}"
                try:
                    response = requests.get(url, timeout=FMP_REQUEST_TIMEOUT_SECONDS)
                    response.raise_for_status() # æª¢æŸ¥ HTTP ç‹€æ…‹ç¢¼
                    res_json = response.json()

                    if isinstance(res_json, list):
                        if len(res_json) > 0:
                            incoming_dates = []
                            invalid_date = False
                            for item in res_json:
                                try:
                                    incoming_dates.append(pd.to_datetime(item["date"], errors="raise"))
                                except (KeyError, TypeError, ValueError):
                                    invalid_date = True
                                    break
                            if invalid_date or not incoming_dates:
                                logger.error(f"<{ticker}> API returned records with invalid dates for {q} {endpoint} using key: {api_key}.")
                                if circuit_breaker is not None:
                                    raise FMPInvalidPayloadError(f"{ticker}: invalid FMP dates for {q} {endpoint}")
                                continue
                            existing_dates = []
                            for item in existing_data:
                                try:
                                    existing_dates.append(pd.to_datetime(item["date"], errors="raise"))
                                except (KeyError, TypeError, ValueError):
                                    existing_dates = []
                                    break
                            if existing_dates and max(incoming_dates) <= max(existing_dates):
                                logger.error(f"<{ticker}> API returned stale dates for {q} {endpoint} using key: {api_key}; refusing stale refresh.")
                                if circuit_breaker is not None:
                                    raise FMPStalePayloadError(f"{ticker}: stale FMP payload for {q} {endpoint}")
                                continue
                            # åŸ·è¡Œå¢žé‡åˆä½µé‚è¼¯
                            data_map = {item['date']: item for item in existing_data}
                            for item in res_json:
                                data_map[item['date']] = item

                            merged_res = sorted(data_map.values(), key=lambda x: x['date'], reverse=True)

                            with open(cache_path, 'w') as f:
                                json.dump(merged_res, f, indent=4)

                            existing_data = merged_res
                            refresh_succeeded = True
                            logger.info(f"<{ticker}> {q} Cache updated. Records: {len(merged_res)} using key: {api_key}")
                            break # Successfully fetched, break from API key loop
                        else:
                            logger.warning(f"<{ticker}> API returned empty list for {q} using key: {api_key}.")
                            # If empty list, try next key if available, or continue if it's the last key
                    else:
                        # è™•ç† API å›žå‚³éŒ¯èª¤è¨Šæ¯çš„æƒ…æ³ (ä¾‹å¦‚ï¼šInvalid API Key)
                        error_msg = res_json.get("Error Message", "Unknown API error")
                        logger.error(f"<{ticker}> API Error for {q} using key: {api_key}: {error_msg}")
                        lowered_error = str(error_msg).lower()
                        if circuit_breaker is not None and any(
                            marker in lowered_error for marker in ("quota", "rate limit", "limit reached", "too many requests")
                        ):
                            reason = f"{ticker}: FMP quota/rate limit for {q} {endpoint}: {error_msg}"
                            circuit_breaker.trip(reason)
                            raise FMPQuotaError(reason)
                        # If error, try next key if available

                    time.sleep(0.1) # ç¨å¾®é™ä½Žé »çŽ‡ï¼Œé¿å… Rate Limit
                except requests.exceptions.HTTPError as http_err:
                    status_code = getattr(getattr(http_err, "response", None), "status_code", None)
                    if status_code == 429:
                        logger.warning(f"<{ticker}> Rate limit hit (429) for {q} {endpoint} using key: {api_key}. Trying next key if available.")
                        if circuit_breaker is not None:
                            reason = f"{ticker}: FMP HTTP 429 for {q} {endpoint}"
                            circuit_breaker.trip(reason)
                            raise FMPRateLimitError(reason)
                        time.sleep(1) # Wait a bit longer before trying the next key
                        continue # Try the next API key
                    else:
                        logger.error(f"<{ticker}> HTTP error fetching {q} {endpoint} using key: {api_key}: {http_err}")
                        break # Other HTTP errors are critical, stop trying
                except Exception as e:
                    logger.error(f"<{ticker}> Critical error fetching {q} {endpoint} using key: {api_key}: {e}")
                    break # Other errors are critical, stop trying

            if is_target_increment and is_expired and not refresh_succeeded:
                raise RuntimeError(f"{ticker}: {endpoint} {q} refresh failed; refusing stale financial release")

        # å°‡æ•¸æ“šåŒ¯ç¸½åˆ°æœ€çµ‚çµæžœ
        combined_all_quarters.extend(existing_data)

    logger.info(f"<{ticker}> Completed. Total records across all quarters: {len(combined_all_quarters)}")
    return combined_all_quarters


def fetch_fmp_financials(ticker, *, circuit_breaker=None):
    """Fetch all FMP statements with one controlled API key."""

    ticker = ticker.upper()
    configured_keys = [key for key in (FMP_API_KEY, FMP_API_KEY_2, FMP_API_KEY_3) if key]
    api_keys = configured_keys[:1]
    endpoints = (
        "income-statement",
        "cash-flow-statement",
        "enterprise-values",
        "balance-sheet-statement",
    )
    statement_rows = {}
    for endpoint in endpoints:
        rows = get_fmp_fragmented(
            endpoint,
            ticker,
            api_keys=api_keys,
            circuit_breaker=circuit_breaker,
        )
        if not rows:
            raise FMPInvalidPayloadError(f"{ticker}: FMP returned no {endpoint} rows")
        statement_rows[endpoint] = rows

    merged = {}
    for rows in statement_rows.values():
        for raw in rows:
            if not isinstance(raw, dict) or not raw.get("date"):
                raise FMPInvalidPayloadError(f"{ticker}: FMP row is missing date")
            try:
                key = pd.to_datetime(raw["date"], errors="raise").strftime("%Y-%m-%d")
            except (TypeError, ValueError) as error:
                raise FMPInvalidPayloadError(f"{ticker}: FMP row has invalid date") from error
            merged.setdefault(key, {}).update(raw)
            merged[key]["date"] = key
    return [merged[key] for key in sorted(merged, reverse=True)]


def create_financial_source_router():
    """Construct the SEC-first router used by the normal valuation CLI."""

    router = FinancialSourceRouter(
        sec_source=SECCompanyFactsSource(),
        foreign_source=ForeignIssuerCoverageSource(),
    )
    router.fmp_fetcher = fetch_fmp_financials
    return router


# --- 3. è½‰æå±¤ (Transform Layer) ---
def _build_quarterly_ttm_from_rows(rows):
    """Build valuation metric frames from normalized source rows."""

    if not rows:
        return None, None, None
    try:
        source_df = pd.DataFrame(rows)
        source_df["date"] = pd.to_datetime(source_df["date"], errors="raise")
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("financial source returned invalid dated rows") from error
    source_df = source_df.drop_duplicates("date").set_index("date").sort_index()
    for field in ("eps", "revenue", "netIncome", "freeCashFlow", "numberOfShares", "reportedCurrency"):
        if field not in source_df.columns:
            source_df[field] = np.nan
    if source_df["freeCashFlow"].isna().all() and {"operatingCashFlow", "capex"}.issubset(source_df.columns):
        source_df["freeCashFlow"] = source_df["operatingCashFlow"] - source_df["capex"]
    if source_df["numberOfShares"].isna().all() and "shares" in source_df.columns:
        source_df["numberOfShares"] = source_df["shares"]
    if source_df["revenue"].isna().all() or source_df["netIncome"].isna().all() or source_df["numberOfShares"].isna().all():
        return None, None, None

    currency_values = source_df["reportedCurrency"].dropna()
    currency = str(currency_values.iloc[-1]).upper() if not currency_values.empty else "USD"
    try:
        fx_rate = SUPPORTED_FX_RATES[currency]
    except KeyError as error:
        raise RuntimeError(f"unsupported financial currency {currency}; refusing unscaled valuation") from error
    df_main = source_df[["eps", "revenue", "netIncome", "freeCashFlow", "numberOfShares"]].copy().ffill()
    df_main["sales_ps_adj"] = (df_main["revenue"] / df_main["numberOfShares"]) / fx_rate
    df_main["eps_adj"] = (df_main["netIncome"] / df_main["numberOfShares"]) / fx_rate
    df_main["fcf_ps_adj"] = (df_main["freeCashFlow"] / df_main["numberOfShares"]) / fx_rate
    df_main["eps_ttm"] = df_main["eps_adj"].rolling(window=4).sum()
    df_main["fcf_ps_ttm"] = df_main["fcf_ps_adj"].rolling(window=4).sum()
    df_main["sales_ps_ttm"] = df_main["sales_ps_adj"].rolling(window=4).sum()
    return (
        df_main[["eps_ttm"]].dropna(),
        df_main[["fcf_ps_ttm"]].dropna(),
        df_main[["sales_ps_ttm"]].dropna(),
    )


def build_quarterly_ttm(ticker, *, source_router=None):
    global LAST_FINANCIAL_SOURCE_RESULT
    source_router = source_router or FINANCIAL_SOURCE_ROUTER
    if source_router is not None:
        result = source_router.route(ticker)
        if not isinstance(result, FinancialSourceResult):
            LAST_FINANCIAL_SOURCE_RESULT = None
            return _build_quarterly_ttm_from_rows(result)
        LAST_FINANCIAL_SOURCE_RESULT = result
        logger.info(
            "<%s> Financial source=%s dataAsOf=%s filingDate=%s fetchedAt=%s",
            ticker,
            result.source_type,
            result.data_as_of,
            result.latest_filing_date,
            result.fetched_at,
        )
        return _build_quarterly_ttm_from_rows(result.rows)

    LAST_FINANCIAL_SOURCE_RESULT = None
    inc_list = get_fmp_fragmented("income-statement", ticker)
    cf_list = get_fmp_fragmented("cash-flow-statement", ticker)
    ev_list = get_fmp_fragmented("enterprise-values", ticker)
    bs_list = get_fmp_fragmented("balance-sheet-statement", ticker)


    if not all([inc_list, cf_list, ev_list, bs_list]):
        return None, None, None

    df_inc = pd.DataFrame(inc_list).drop_duplicates('date').set_index('date').sort_index()
    df_cf = pd.DataFrame(cf_list).drop_duplicates('date').set_index('date').sort_index()
    df_ev = pd.DataFrame(ev_list).drop_duplicates('date').set_index('date').sort_index()

    for df in [df_inc, df_cf, df_ev]:
        df.index = pd.to_datetime(df.index).tz_localize(None)

    # --- é—œéµä¿®æ­£ï¼šè‡ªå‹•åµæ¸¬åŒ¯çŽ‡èˆ‡ ADR æ¯”ä¾‹ ---
    currency = df_inc['reportedCurrency'].iloc[-1] if 'reportedCurrency' in df_inc.columns else "USD"
    try:
        fx_rate = SUPPORTED_FX_RATES[str(currency).upper()]
    except KeyError as error:
        raise RuntimeError(f"unsupported financial currency {currency}; refusing unscaled valuation") from error

    # --- è¨ˆç®— P/S å¿…å‚™çš„ Revenue TTM ---
    # å…ˆè¨ˆç®—æ¯å­£åº¦çš„ Sales Per Share
    # æ³¨æ„ï¼šRevenue åœ¨ income-statementï¼ŒnumberOfShares åœ¨ enterprise-values
    df_main = pd.concat([
        df_inc[['eps', 'revenue','netIncome']],
        df_cf['freeCashFlow'],
        df_ev['numberOfShares']
    ], axis=1).ffill()

    # çµ±ä¸€ä½¿ç”¨ç¸½é¡é™¤ä»¥ (ç¸½è‚¡æ•¸/ADRæ¯”ä¾‹) å†é™¤ä»¥åŒ¯çŽ‡
    # é€™æ¨£ç®—å‡ºä¾†æ‰æ˜¯ã€Œæ¯ä¸€å–®ä½ç¾Žé‡‘ ADRã€å°æ‡‰çš„åƒ¹å€¼
    # è¨ˆç®—æ¯è‚¡ç‡Ÿæ”¶ (Sales Per Share)

    df_main['sales_ps_adj'] = (df_main['revenue'] / df_main['numberOfShares'] ) / fx_rate
    df_main['eps_adj'] = (df_main['netIncome'] / df_main['numberOfShares'] ) / fx_rate
    df_main['fcf_ps_adj'] = (df_main['freeCashFlow'] / df_main['numberOfShares'] ) / fx_rate

    # Set to None to display all columns
    # pd.set_option('display.max_columns', None)

    # # Prevents the dataframe from wrapping to a new line
    # pd.set_option('display.expand_frame_repr', False)

    # è¨ˆç®— TTM (æ»¾å‹•å››å€‹å­£åº¦ç¸½å’Œ)
    df_main['eps_ttm'] = df_main['eps_adj'].rolling(window=4).sum()
    df_main['fcf_ps_ttm'] = df_main['fcf_ps_adj'].rolling(window=4).sum()
    df_main['sales_ps_ttm'] = df_main['sales_ps_adj'].rolling(window=4).sum()


    return (
        df_main[['eps_ttm']].dropna(),
        df_main[['fcf_ps_ttm']].dropna(),
        df_main[['sales_ps_ttm']].dropna()
    )

# --- 3. æ ¸å¿ƒä¼°å€¼é‚è¼¯ (Senior Analyst Hybrid Version) ---
def calculate_bands(ticker, prices_df, metrics_df, col_name):
    # æ—¥æœŸæ¨™æº–åŒ–èˆ‡å…¨æ™‚é–“è»¸åˆä½µ
    prices_df.index = pd.to_datetime(prices_df.index).tz_localize(None).normalize()
    metrics_df.index = pd.to_datetime(metrics_df.index).tz_localize(None).normalize()

    all_dates = prices_df.index.union(metrics_df.index).sort_values()
    df = pd.DataFrame(index=all_dates).join(prices_df)
    df['metric_raw'] = metrics_df[col_name]

    # è™•ç†æ‹†åˆ†èª¿æ•´å› å­ (å³ä½¿ yfinance èª¿æ•´éŽï¼Œæ­¤è™•ä»ä¿ç•™é‚è¼¯ä»¥é˜²è¬ä¸€)
    df['adj_ratio'] = (df['Adj Close'] / df['Close'].replace(0, np.nan)).ffill().bfill()
    df['metric_adj'] = df['metric_raw'] * df['adj_ratio']
    df['metric_final'] = df['metric_adj'].interpolate(method='time').ffill().bfill()

    # è¨ˆç®—å€æ•¸ï¼šæŽ’é™¤è² å€¼
    df['multiple'] = df['Adj Close'] / df['metric_final']
    df.loc[df['metric_final'] <= 0, 'multiple'] = np.nan

    # --- ç­–ç•¥é¸æ“‡é‚è¼¯ ---
    # å¦‚æžœè² å€¼æˆ–æ¥µç«¯å€¼æ¯”ä¾‹éŽé«˜ (å¦‚ AMZN)ï¼Œè‡ªå‹•åˆ‡æ›è‡³ Median
    null_ratio = df['multiple'].isna().mean()
    use_median = True if (ticker == "AMZN" or null_ratio > 0.1) else False

    # 3. ã€æ ¸å¿ƒä¿®æ­£ã€‘ç™¾åˆ†ä½å‰ªæž (Percentile Approach)
    # æˆ‘å€‘è¨ˆç®—è©²è‚¡ç¥¨æ­·å²ä¸Š 90% åˆ†ä½æ•¸çš„å€¼ä½œç‚ºä¸Šé™
    # é€™æ¨£ AMZN çš„ 1000x æœƒè¢«å‰ªæŽ‰ï¼Œä½† AAPL çš„ 35x æœƒè¢«å®Œæ•´ä¿ç•™
    if df['multiple'].notna().any():
        upper_limit = df['multiple'].quantile(0.95)
        lower_limit = df['multiple'].quantile(0.05)
        df['multiple'] = df['multiple'].clip(lower=lower_limit, upper=upper_limit)

    results = {}
    avgs = {}

    for label, window in WINDOWS.items():
        # Hybrid æ»¾å‹•è¨ˆç®—
        if use_median:
            m_col = df['multiple'].rolling(window=window, min_periods=60).median()
        else:
            m_col = df['multiple'].rolling(window=window, min_periods=60).mean()

        s_col = df['multiple'].rolling(window=window, min_periods=60).std().fillna(0)

        # é˜²æ­¢æ¨™æº–å·®éŽå¤§å°Žè‡´ Band ç‚¸é–‹ (ä¸Šé™è¨­ç‚ºå‡å€¼çš„ 50%)
        s_col = s_col.clip(upper=m_col * 0.5)

        res = pd.DataFrame(index=df.index)
        res['mean'] = m_col * df['metric_final']
        res['up1'] = (m_col + s_col) * df['metric_final']
        res['up2'] = (m_col + 2 * s_col) * df['metric_final']
        res['down1'] = (m_col - s_col) * df['metric_final']
        res['down2'] = (m_col - 2 * s_col) * df['metric_final']

        # å¼·åˆ¶æ­¸é›¶é‚è¼¯ï¼šæŒ‡æ¨™ç‚ºè² å‰‡ä¼°å€¼ç‚º 0
        for c in res.columns:
            res.loc[df['metric_final'] <= 0, c] = 0

        results[label] = res.loc[prices_df.index].clip(lower=0).ffill().round(2)

        last_val = m_col.dropna().iloc[-1] if not m_col.dropna().empty else 0
        avgs[label] = round(float(last_val), 2)



    return results, avgs

def clean_nans(obj):
    if isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nans(v) for v in obj]
    elif isinstance(obj, float) and np.isnan(obj):
        return None # JSON æ”¯æ´ nullï¼Œä¸æ”¯æ´ NaN
    return obj

def fetch_price_history(
    ticker,
    attempts=YFINANCE_MAX_ATTEMPTS,
    delay_seconds=YFINANCE_RETRY_DELAY_SECONDS,
    timeout_seconds=YFINANCE_TIMEOUT_SECONDS,
):
    """Compatibility wrapper around the testable Yahoo price adapter.

    Existing callers use an empty DataFrame as the failure sentinel, so the
    wrapper keeps that contract while the adapter raises a typed,
    observable ``PriceSourceUnavailable`` for source-aware callers.
    """

    adapter = YahooPriceAdapter(
        ticker_factory=yf.Ticker,
        max_attempts=attempts,
        retry_delay_seconds=delay_seconds,
        timeout_seconds=timeout_seconds,
        sleep=time.sleep,
        logger=logger,
    )
    try:
        return adapter.fetch_history(ticker)
    except PriceSourceUnavailable as error:
        logger.error("<%s> Skipping after price source exhaustion: %s", ticker, error)
        return pd.DataFrame()

# --- 5. ä¸»ç¨‹åº ---
def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Generate valuation data for the configured ticker universe.")
    parser.add_argument("--symbols", help="Comma-separated symbols. When present, does not include the default universe.")
    parser.add_argument("--universe-file", type=Path, help="Local copy of the private coverage/universe.json R2 object.")
    parser.add_argument("--write-resolved-symbols", type=Path, help="Write the exact resolved universe for downstream export validation.")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        tickers = resolve_tickers(args.symbols, args.universe_file)
    except UniverseValidationError as error:
        raise RuntimeError(f"Ticker universe is invalid: {error}") from error
    if args.write_resolved_symbols:
        args.write_resolved_symbols.parent.mkdir(parents=True, exist_ok=True)
        args.write_resolved_symbols.write_text(json.dumps({"symbols": tickers}, separators=(",", ":")), encoding="utf-8")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    global FINANCIAL_SOURCE_ROUTER
    FINANCIAL_SOURCE_ROUTER = create_financial_source_router()

    # å‘¼å« Debug
    ## test_amzn_valuation_logic()

    for ticker in tickers:
        final_dir = os.path.join(OUTPUT_DIR, "results", ticker.upper())
        output_file = os.path.join(final_dir, "valuation_summary.json")

        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                try:
                    data = json.load(f)
                    last_updated_str = data.get("last_updated")
                    if last_updated_str:
                        last_updated = datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
                        if (datetime.now() - last_updated).days < 1:
                            print(f"Skipping {ticker}: Valuation data is less than 1 day old.")
                            continue
                except json.JSONDecodeError:
                    print(f"Error decoding JSON for {ticker}, reprocessing.")

        # 1. ç²å–è‚¡åƒ¹æ•¸æ“š
        # æˆ‘å€‘ä½¿ç”¨ auto_adjust=False ä»¥æ‰‹å‹•è™•ç† Close/Adj Close ä¾†å°é½ŠæŒ‡æ¨™é‡ç´š
        print(f"\nðŸ—ï¸  Pipeline Starting: {ticker}")
        prices = fetch_price_history(ticker)

        if prices.empty:
            raise RuntimeError(f"{ticker}: no Yahoo price data; refusing partial valuation release")

        prices.index = prices.index.tz_localize(None)

        prices_df = prices[['Close', 'Adj Close']].copy()

        # 2. ç²å–è²¡å‹™æŒ‡æ¨™æ•¸æ“š (TTM)
        # ç¾åœ¨ build_quarterly_ttm æœƒå›žå‚³ä¸‰å€‹æŒ‡æ¨™
        eps_ttm, fcf_ttm, sales_ttm = build_quarterly_ttm(ticker, source_router=FINANCIAL_SOURCE_ROUTER)
        if eps_ttm is None:
            raise RuntimeError(f"{ticker}: no usable routed quarterly data; refusing partial valuation release")

        if LAST_FINANCIAL_SOURCE_RESULT is not None:
            os.makedirs(SOURCE_FINANCIAL_DIR, exist_ok=True)
            with open(os.path.join(SOURCE_FINANCIAL_DIR, f"{ticker.upper()}_combined.json"), "w", encoding="utf-8") as source_file:
                json.dump(list(LAST_FINANCIAL_SOURCE_RESULT.rows), source_file, indent=2)

        # 3. è¨ˆç®—ä¼°å€¼å¸¶
        pe_res, pe_avgs = calculate_bands(ticker, prices_df, eps_ttm, 'eps_ttm')
        fcf_res, fcf_avgs = calculate_bands(ticker, prices_df, fcf_ttm, 'fcf_ps_ttm')
        ps_res, ps_avgs = calculate_bands(ticker, prices_df, sales_ttm, 'sales_ps_ttm')

        # 4. å°è£æ­·å²æ•¸æ“šç”¨æ–¼å‰ç«¯ç¹ªåœ–
        history = []
        # åªå– 2021 å¹´ä»¥å¾Œçš„æ•¸æ“šé»žä»¥å„ªåŒ–å‰ç«¯åŠ è¼‰é€Ÿåº¦
        plot_df = prices_df[prices_df.index >= '2021-01-01']
        plot_df.index = plot_df.index.tz_localize(None).normalize()

        for date, row in plot_df.iterrows():
            # ç¢ºä¿è©²æ—¥æœŸåœ¨æ‰€æœ‰æŒ‡æ¨™è¨ˆç®—çµæžœä¸­éƒ½å­˜åœ¨
            if date not in pe_res["1Y"].index: continue
            history.append({
                "date": date.strftime("%Y-%m-%d"),
                "price": round(float(row['Adj Close']), 2),
                "valuation": {
                    lb: {
                        "pe": pe_res[lb].loc[date].round(2).to_dict(),
                        "fcf": fcf_res[lb].loc[date].round(2).to_dict(),
                        "ps": ps_res[lb].loc[date].to_dict()   # åŠ å…¥ P/S
                    } for lb in WINDOWS
                }
            })
        # --- æ›´æ–° JSON çµæ§‹ï¼ŒåŠ å…¥ last_updated ---
        output_data = {
            "ticker": ticker.upper(),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # åŠ å…¥é€™è¡Œ
            "averages": {
                "pe": pe_avgs,
                "fcf": fcf_avgs,
                "ps": ps_avgs
            },
            "data": history,
        }
        if LAST_FINANCIAL_SOURCE_RESULT is not None:
            output_data["financialSource"] = LAST_FINANCIAL_SOURCE_RESULT.metadata

        # æœ€å¾Œçµæžœä¹Ÿå­˜å…¥ ticker è³‡æ–™å¤¾
        final_dir = os.path.join(OUTPUT_DIR, "results", ticker.upper())
        os.makedirs(final_dir, exist_ok=True)

        with open(os.path.join(final_dir, "valuation_summary.json"), "w") as f:
            json.dump(clean_nans(output_data), f, indent=4)
        print(f"âœ¨ [Success] {ticker} pipeline execution completed. Folder: {final_dir} {len(history)} points generated.")

if __name__ == "__main__":
    main()
