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

def calculate_bands(ticker, prices, metrics_df, col_name):
    """
    [Data Engineering Logic]:
    1. 實施動態拆分調整 (Fixed NameError)
    2. 使用 Rolling Mean 取代 Median 以獲得更好的平滑度
    3. 實施動態縮尾處理 (Winsorization) 消除離群值
    """
    tk = yf.Ticker(ticker)
    adj_metrics = metrics_df.copy()
    
    # --- 1. 拆分調整 (修復 splits 未定義問題) ---
    try:
        # 正確定義 splits
        splits = tk.splits
        if not splits.empty:
            for split_date, ratio in splits.items():
                split_date_naive = split_date.tz_localize(None)
                # 歷史財報數據需與調整後股價對齊
                adj_metrics.loc[adj_metrics.index < split_date_naive, col_name] /= ratio
    except Exception as e:
        print(f"  ⚠️ [Warning] Could not process splits for {ticker}: {e}")

    # --- 2. 數據對齊與填充 (解決 0.0 與 NaN) ---
    df = pd.concat([prices, adj_metrics], axis=1).sort_index()
    # 使用 bfill() 確保時間序列開頭不為空，再進行線性插值
    df['val_smooth'] = df[col_name].ffill().bfill().interpolate(method='time').ffill().bfill()

    # --- 3. 計算原始倍數 (處理 AMZN 負 FCF 問題) ---
    # 只有當指標 > 0 時計算倍數，否則設為 NaN 隨後填充，確保倍數恆正
    df['raw_mult'] = np.where(df['val_smooth'] > 1e-4, df['Close'] / df['val_smooth'], np.nan)
    df['mult_filled'] = df['raw_mult'].ffill().bfill()

    # --- 4. Winsorization (縮尾處理)：確保 Rolling Mean 不被污染 ---
    # 動態計算該股票自身的 15% 與 85% 分位數作為邊界
    q_low = df['mult_filled'].quantile(0.15)
    q_high = df['mult_filled'].quantile(0.85)
    df['mult_capped'] = df['mult_filled'].clip(lower=q_low, upper=q_high)

    results = {}
    avgs = {}

    for label, window in WINDOWS.items():
        # --- 5. Rolling Mean 計算 (應要求取代 Median) ---
        # min_periods=1 確保從第一天開始就有數據，消滅 NaN
        m_col = df['mult_capped'].rolling(window=window, min_periods=1).mean().ffill().bfill()
        s_col = df['mult_capped'].rolling(window=window, min_periods=1).std().fillna(0).ffill().bfill()
        
        # 限制估值帶標準差範圍 (Volatility Cap)
        s_col = np.minimum(s_col, m_col * 0.2)

        res = pd.DataFrame(index=df.index)
        # 確保指標基準為正，防止 mean 變負
        v_base = df['val_smooth'].clip(lower=0.01)
        
        # --- 6. 生成最後結果 ---
        res['mean'] = m_col * v_base
        res['up1'] = (m_col + s_col) * v_base
        res['up2'] = (m_col + 2 * s_col) * v_base
        res['down1'] = (m_col - s_col) * v_base
        res['down2'] = (m_col - 2 * s_col) * v_base
        
        # 格式化輸出
        final_df = res.clip(lower=0.01).round(2)
        results[label] = final_df.replace([np.inf, -np.inf], 0).fillna(0)
        avgs[label] = round(float(m_col.iloc[-1]), 2)

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