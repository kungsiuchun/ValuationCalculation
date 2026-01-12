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

# --- 3. 核心估值邏輯 (Senior Analyst Hybrid Version) ---
def calculate_bands(ticker, prices_df, metrics_df, col_name):
    # 日期標準化與全時間軸合併
    prices_df.index = pd.to_datetime(prices_df.index).tz_localize(None).normalize()
    metrics_df.index = pd.to_datetime(metrics_df.index).tz_localize(None).normalize()
    
    all_dates = prices_df.index.union(metrics_df.index).sort_values()
    df = pd.DataFrame(index=all_dates).join(prices_df)
    df['metric_raw'] = metrics_df[col_name]

    # 處理拆分調整因子 (即使 yfinance 調整過，此處仍保留邏輯以防萬一)
    df['adj_ratio'] = (df['Adj Close'] / df['Close'].replace(0, np.nan)).ffill().bfill()
    df['metric_adj'] = df['metric_raw'] * df['adj_ratio']
    df['metric_final'] = df['metric_adj'].interpolate(method='time').ffill().bfill()

    # 計算倍數：排除負值
    df['multiple'] = df['Adj Close'] / df['metric_final']
    df.loc[df['metric_final'] <= 0, 'multiple'] = np.nan

    # --- 策略選擇邏輯 ---
    # 如果負值或極端值比例過高 (如 AMZN)，自動切換至 Median
    null_ratio = df['multiple'].isna().mean()
    use_median = True if (ticker == "AMZN" or null_ratio > 0.1) else False
    
    # 倍數剪枝 (Winsorization)
    upper_limit = 150 if 'eps' in col_name else 120
    df['multiple'] = df['multiple'].clip(0, upper_limit)

    results = {}
    avgs = {}

    for label, window in WINDOWS.items():
        # Hybrid 滾動計算
        if use_median:
            m_col = df['multiple'].rolling(window=window, min_periods=60).median()
        else:
            m_col = df['multiple'].rolling(window=window, min_periods=60).mean()
            
        s_col = df['multiple'].rolling(window=window, min_periods=60).std().fillna(0)
        
        # 防止標準差過大導致 Band 炸開 (上限設為均值的 60%)
        s_col = s_col.clip(upper=m_col * 0.6)

        res = pd.DataFrame(index=df.index)
        res['mean'] = m_col * df['metric_final']
        res['up1'] = (m_col + s_col) * df['metric_final']
        res['up2'] = (m_col + 2 * s_col) * df['metric_final']
        res['down1'] = (m_col - s_col) * df['metric_final']
        res['down2'] = (m_col - 2 * s_col) * df['metric_final']

        results[label] = res.loc[prices_df.index].clip(lower=0).ffill().bfill().round(2)
        
        last_val = m_col.dropna().iloc[-1] if not m_col.dropna().empty else 0
        avgs[label] = round(float(last_val), 2)

    return results, avgs



# --- 5. 主程序 ---
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 呼叫 Debug
    ## debug_valuation("AAPL")

    for ticker in DOW_30:
        print(f"\n🏗️  Pipeline Starting: {ticker}")
        prices = yf.Ticker(ticker).history(period="8y", auto_adjust=False)
        prices.index = prices.index.tz_localize(None)

        prices_df = prices[['Close', 'Adj Close']].copy()

        eps_ttm, fcf_ttm = build_quarterly_ttm(ticker)
        if eps_ttm is None: continue

        pe_res, pe_avgs = calculate_bands(ticker, prices_df, eps_ttm, 'eps_ttm')
        fcf_res, fcf_avgs = calculate_bands(ticker, prices_df, fcf_ttm, 'fcf_ps_ttm')

        history = []

        for date, row in prices[prices.index >= '2021-01-01'].iterrows():
            if date not in pe_res["1Y"].index: continue
            history.append({
                "date": date.strftime("%Y-%m-%d"),
                "price": round(float(row['Adj Close']), 2),
                "valuation": {
                    lb: {
                        "pe": pe_res[lb].loc[date].round(2).to_dict(),
                        "fcf": fcf_res[lb].loc[date].round(2).to_dict()
                    } for lb in WINDOWS
                }
            })
        # --- 更新 JSON 結構，加入 last_updated ---
        output_data = {
            "ticker": ticker.upper(), 
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # 加入這行
            "averages": {
                "pe": pe_avgs, 
                "fcf": fcf_avgs
            }, 
            "data": history
        }

        # 最後結果也存入 ticker 資料夾
        final_dir = os.path.join(OUTPUT_DIR, "results", ticker.upper())
        os.makedirs(final_dir, exist_ok=True)
        
        with open(os.path.join(final_dir, "valuation_summary.json"), "w") as f:
            json.dump(output_data, f, indent=4)
        print(f"✨ [Success] {ticker} pipeline execution completed. Folder: {final_dir} {len(history)} points generated.")

if __name__ == "__main__":
    main()