import requests
import pandas as pd
import numpy as np
import yfinance as yf
import json
import os
import time
from datetime import datetime, timedelta

# --- 1. 初始化與配置 ---
FMP_API_KEY = "F9dROu64FwpDqETGsu1relweBEoTcpID"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "data")
CACHE_DIR = os.path.join(OUTPUT_DIR, "fmp_cache") 
DOW_30 = ["AMZN"]

WINDOWS = {
    "1Y": 252, "2Y": 504, "3Y": 756, "5Y": 1260
}

# --- 2. 緩存邏輯 (Caching Logic) ---

def get_fmp_data_with_cache(url, cache_filename, expiry_days=7):
    """檢查本地是否有 7 天內的緩存，否則呼叫 API"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(CACHE_DIR, cache_filename)
    
    if os.path.exists(cache_path):
        file_age_days = (time.time() - os.path.getmtime(cache_path)) / (24 * 3600)
        if file_age_days < expiry_days:
            print(f"  📦 Loading from cache: {cache_filename}")
            with open(cache_path, 'r') as f:
                return json.load(f)
    
    print(f"  🚀 No fresh cache, calling FMP API...")
    try:
        response = requests.get(url)
        data = response.json()
        if not data or "Error" in str(data): return None
        with open(cache_path, 'w') as f:
            json.dump(data, f)
        return data
    except:
        return None

# --- 3. 數據獲取函數 ---

def get_income_statement(ticker):
    url = f"https://financialmodelingprep.com/stable/income-statement?symbol={ticker}&apikey={FMP_API_KEY}"
    res = get_fmp_data_with_cache(url, f"{ticker}_income.json")
    if not res: return None
    df = pd.DataFrame(res)[['date', 'eps']]
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
    return df.set_index('date').sort_index()

def get_cash_flow_statement(ticker):
    url_cf = f"https://financialmodelingprep.com/stable/cash-flow-statement?symbol={ticker}&apikey={FMP_API_KEY}"
    url_ev = f"https://financialmodelingprep.com/stable/enterprise-values/?symbol={ticker}&apikey={FMP_API_KEY}"
    res_cf = get_fmp_data_with_cache(url_cf, f"{ticker}_cf.json")
    res_ev = get_fmp_data_with_cache(url_ev, f"{ticker}_ev.json")
    if not res_cf or not res_ev: return None
    
    df_cf = pd.DataFrame(res_cf)[['date', 'freeCashFlow']]
    df_ev = pd.DataFrame(res_ev)[['date', 'numberOfShares']]
    for df in [df_cf, df_ev]:
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        df.set_index('date', inplace=True)
        
    df_merge = pd.merge_asof(df_cf.sort_index(), df_ev.sort_index(), left_index=True, right_index=True, direction='nearest')
    df_merge['fcf_ps'] = df_merge['freeCashFlow'] / df_merge['numberOfShares']
    return df_merge[['fcf_ps']]

# --- 4. 核心算法 (含異常值優化) ---

def calculate_multi_period_bands(ticker, price_series, metric_series, metric_name):
    """
    修正 AMZN 圖形異常的關鍵：
    1. 剔除極端倍數 (Outlier Removal)
    2. 使用對齊後的插值數據
    修正版核心算法：
    1. 獲取股票拆分歷史並調整財務數據 (解決 AMZN/GOOGL 拆分導致的估值斷層)
    2. 將股價與調整後的財務數據對齊並線性插值
    3. 計算歷史滾動 PE/FCF 倍數 (Multiple)
    4. 生成 1Y, 2Y, 3Y, 5Y 的 5 條估值通道線
    """
    # A. 拆分調整
    tk = yf.Ticker(ticker)
    splits = tk.splits
    adj_metric = metric_series.copy()
    if not splits.empty:
        for split_date, ratio in splits.items():
            split_dt = split_date.tz_localize(None)
            adj_metric.loc[adj_metric.index < split_dt] /= ratio

    # B. 數據平滑對齊
    combined = pd.concat([price_series, adj_metric], axis=1).sort_index()
    combined[f'{metric_name}_smooth'] = combined[metric_name].interpolate(method='time').ffill().bfill()
    df = combined.dropna(subset=['Close']).copy()
    
    # C. 計算 Multiple 並「剔除異常值」
    df['raw_multiple'] = df['Close'] / df[f'{metric_name}_smooth']
    
    # 這裡加入 Outlier Filter: 只有 0 到 250 之間的 PE/FCF 會被納入平均計算
    # 這樣 AMZN 那些 900+ 的數據就不會把整條 Mean 線拉高
    df['clean_multiple'] = df['raw_multiple'].copy()
    df.loc[(df['raw_multiple'] <= 0) | (df['raw_multiple'] > 250), 'clean_multiple'] = np.nan

    period_results = {}
    current_averages = {}

    for label, window_size in WINDOWS.items():
        # 基於 clean_multiple 計算滾動均值與標準差
        m_col = df['clean_multiple'].rolling(window=window_size, min_periods=int(window_size*0.1)).mean()
        s_col = df['clean_multiple'].rolling(window=window_size, min_periods=int(window_size*0.1)).std().fillna(0)
        
        # 即使當前是負數或異常，我們用最近一個有效的平均值來畫線 (ffill)
        valid_m = m_col.ffill()
        valid_s = s_col.ffill()
        val_col = df[f'{metric_name}_smooth']

        bands = pd.DataFrame(index=df.index)
        bands['mean'] = valid_m * val_col
        bands['up1'] = (valid_m + valid_s) * val_col
        bands['up2'] = (valid_m + 2 * valid_s) * val_col
        bands['down1'] = (valid_m - valid_s) * val_col
        bands['down2'] = (valid_m - 2 * valid_s) * val_col
        
        period_results[label] = bands
        current_averages[label] = round(valid_m.iloc[-1], 2) if not valid_m.empty else 0

    return period_results, current_averages

# --- 5. 主程序管線 ---

def process_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for ticker in DOW_30:
        print(f"\n🔍 [Step 1/5] Checking {ticker} status...")
        
        # 檢查結果是否最新，若是則不重複 API Call
        local_res = os.path.join(OUTPUT_DIR, f"{ticker}_valuation.json")
        if os.path.exists(local_res):
            with open(local_res, 'r') as f:
                last_upd = json.load(f).get('last_updated', '')
                if last_upd.startswith(datetime.now().strftime("%Y-%m-%d")):
                    print(f"  ✅ {ticker} is already updated today.")
                    continue

        # 抓取價格
        full_price = yf.Ticker(ticker).history(period="7y")[['Close']]
        full_price.index = full_price.index.tz_localize(None)

        # 抓取財務數據 (會自動判斷緩存)
        eps_df = get_income_statement(ticker)
        fcf_df = get_cash_flow_statement(ticker)

        # 計算
        pe_res, pe_avgs = calculate_multi_period_bands(ticker, full_price['Close'], eps_df['eps'], 'eps')
        fcf_res, fcf_avgs = calculate_multi_period_bands(ticker, full_price['Close'], fcf_df['fcf_ps'], 'fcf_ps')

        # 封裝 JSON
        history = []
        for date, row in full_price[full_price.index >= '2021-01-01'].iterrows():
            d_str = date.strftime("%Y-%m-%d")
            record = {"date": d_str, "price": round(row['Close'], 2), "valuation": {}}
            for label in WINDOWS:
                record["valuation"][label] = {
                    "pe": {k: round(v, 2) for k, v in pe_res[label].loc[date].to_dict().items()},
                    "fcf": {k: round(v, 2) for k, v in fcf_res[label].loc[date].to_dict().items()}
                }
            history.append(record)

        output = {
            "ticker": ticker,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "averages": {"pe": pe_avgs, "fcf": fcf_avgs},
            "data": history
        }

        with open(local_res, "w") as f:
            json.dump(output, f)
        print(f"✨ [Success] {ticker} pipeline completed.")

if __name__ == "__main__":
    process_pipeline()