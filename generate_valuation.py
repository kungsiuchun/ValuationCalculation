import requests
import pandas as pd
import numpy as np
import yfinance as yf
import json
import os
import time
from datetime import datetime

# --- 1. 配置 ---
FMP_API_KEY = "F9dROu64FwpDqETGsu1relweBEoTcpID"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "data")
CACHE_BASE_DIR = os.path.join(OUTPUT_DIR, "fmp_cache") # 緩存主目錄
DOW_30 = ["AMZN", "AAPL", "GOOGL", "MSFT", "WMT"] 

WINDOWS = {"1Y": 252, "2Y": 504, "3Y": 756, "5Y": 1260}
QUARTERS = ['q1', 'q2', 'q3', 'q4']

# --- 2. 抽取層 (Extract Layer) ---
def get_fmp_fragmented(endpoint, ticker):
    """
    [Data Engineering Logic]: 
    自動建立對應 ticker 的子資料夾 (例如 fmp_cache/AMZN/)。
    """
    combined = []
    
    # 建立 ticker 專屬路徑：data/fmp_cache/{ticker}
    ticker_cache_dir = os.path.join(CACHE_BASE_DIR, ticker.upper())
    os.makedirs(ticker_cache_dir, exist_ok=True) # 自動建立多層目錄

    for q in QUARTERS:
        # 文件命名保持 endpoint 區分
        cache_path = os.path.join(ticker_cache_dir, f"{endpoint}_{q}.json")
        
        # 緩存檢查 (7天有效期)
        if os.path.exists(cache_path) and (time.time() - os.path.getmtime(cache_path)) < (7 * 86400):
            with open(cache_path, 'r') as f:
                combined.extend(json.load(f))
            continue

        url = f"https://financialmodelingprep.com/stable/{endpoint}/?symbol={ticker}&period={q}&apikey={FMP_API_KEY}"
        
        try:
            print(f"  🚀 [API Call] Fetching {ticker} {endpoint} {q}...")
            res = requests.get(url).json()
            if isinstance(res, list):
                with open(cache_path, 'w') as f:
                    json.dump(res, f, indent=4) # 增加 indent 方便 DE 進行 Debug
                combined.extend(res)
            time.sleep(0.2)
        except Exception as e:
            print(f"  ❌ [Error] Failed to fetch {endpoint} {q}: {e}")
            
    return combined

# --- 3. 轉換層 (Transform Layer) ---
def build_quarterly_ttm(ticker):
    inc_list = get_fmp_fragmented("income-statement", ticker)
    cf_list = get_fmp_fragmented("cash-flow-statement", ticker)
    ev_list = get_fmp_fragmented("enterprise-values", ticker)
    
    if not all([inc_list, cf_list, ev_list]): return None, None

    df_inc = pd.DataFrame(inc_list).drop_duplicates('date').set_index('date').sort_index()
    df_cf = pd.DataFrame(cf_list).drop_duplicates('date').set_index('date').sort_index()
    df_ev = pd.DataFrame(ev_list).drop_duplicates('date').set_index('date').sort_index()

    for df in [df_inc, df_cf, df_ev]:
        df.index = pd.to_datetime(df.index).tz_localize(None)

    # 數據合併與計算 TTM
    df_inc['eps_ttm'] = df_inc['eps'].rolling(window=4).sum()
    df_main = pd.concat([df_inc[['eps_ttm']], df_cf['freeCashFlow'], df_ev['numberOfShares']], axis=1).ffill()
    df_main['fcf_ps_ttm'] = (df_main['freeCashFlow'] / df_main['numberOfShares']).rolling(window=4).sum()

    return df_main[['eps_ttm']].dropna(), df_main[['fcf_ps_ttm']].dropna()

def calculate_bands(ticker, prices_adj, metrics_df, col_name):
    """
    1. 統一時區 (Fix TypeError)
    2. 使用 Raw Price 與財報量級對齊
    3. 簡單 Rolling Mean 唔搞複雜邏輯
    """
    tk = yf.Ticker(ticker)
    # 重新抓取未經調整的原始價格
    hist = tk.history(period="5y", auto_adjust=False)
    raw_prices = hist['Close'].copy()
    
    # --- 時區處理：統一化為 tz-naive ---
    if raw_prices.index.tz is not None:
        raw_prices.index = raw_prices.index.tz_localize(None)
    
    # 確保財報數據亦係 tz-naive (通常已經係，但做多步保險)
    if metrics_df.index.tz is not None:
        metrics_df.index = metrics_df.index.tz_localize(None)

    # --- 數據合併 ---
    df = pd.concat([raw_prices, metrics_df], axis=1).sort_index()
    
    # --- 基本計算 ---
    # 簡單插值填充季度間空白
    df['metric_val'] = df[col_name].interpolate(method='time').ffill().bfill()
    
    # 計算倍數 (原始價格 / 原始指標)
    df['multiple'] = df['Close'] / df['metric_val'].replace(0, np.nan)
    
    # 處理倍數中的空值
    df['multiple'] = df['multiple'].ffill().bfill()

    results = {}
    avgs = {}
    windows = {'1Y': 252, '2Y': 504, '3Y': 756, '5Y': 1260}

    for label, window in windows.items():
        # 簡單 Rolling Mean，唔做 Winsorization 或其他濾波
        m_col = df['multiple'].rolling(window=window, min_periods=1).mean()
        s_col = df['multiple'].rolling(window=window, min_periods=1).std().fillna(0)

        # 將「原始倍數」套用到「當前價格體系 (Adjusted)」
        # adj_ratio = 目前股價(已調整) / 原始股價(未調整)
        # 用個比例將計出嚟嘅軌道縮放返去現價量級
        adj_ratio = prices_adj / raw_prices
        adj_metric = df['metric_val'] * adj_ratio

        res = pd.DataFrame(index=df.index)
        res['mean'] = m_col * adj_metric
        res['up1'] = (m_col + s_col) * adj_metric
        res['up2'] = (m_col + 2 * s_col) * adj_metric
        res['down1'] = (m_col - s_col) * adj_metric
        res['down2'] = (m_col - 2 * s_col) * adj_metric

        results[label] = res.ffill().bfill().round(2)
        avgs[label] = round(float(m_col.dropna().iloc[-1]), 2) if not m_col.dropna().empty else 0

    return results, avgs

# --- 5. 主程序 ---
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for ticker in DOW_30:
        print(f"\n🏗️  Pipeline Starting: {ticker}")
        prices = yf.Ticker(ticker).history(period="7y")[['Close']]
        prices.index = prices.index.tz_localize(None)

        eps_ttm, fcf_ttm = build_quarterly_ttm(ticker)
        if eps_ttm is None: continue

        pe_res, pe_avgs = calculate_bands(ticker, prices['Close'], eps_ttm, 'eps_ttm')
        fcf_res, fcf_avgs = calculate_bands(ticker, prices['Close'], fcf_ttm, 'fcf_ps_ttm')

        history = []
        for date, row in prices[prices.index >= '2021-01-01'].iterrows():
            if date not in pe_res["1Y"].index: continue
            history.append({
                "date": date.strftime("%Y-%m-%d"),
                "price": round(row['Close'], 2),
                "valuation": {
                    lb: {
                        "pe": pe_res[lb].loc[date].round(2).to_dict(),
                        "fcf": fcf_res[lb].loc[date].round(2).to_dict()
                    } for lb in WINDOWS
                }
            })

        # 最後結果也存入 ticker 資料夾
        final_dir = os.path.join(OUTPUT_DIR, "results", ticker.upper())
        os.makedirs(final_dir, exist_ok=True)
        
        with open(os.path.join(final_dir, "valuation_summary.json"), "w") as f:
            json.dump({"ticker": ticker, "averages": {"pe": pe_avgs, "fcf": fcf_avgs}, "data": history}, f, indent=4)
        print(f"✨ [Success] {ticker} pipeline execution completed. Folder: {final_dir}")

if __name__ == "__main__":
    main()