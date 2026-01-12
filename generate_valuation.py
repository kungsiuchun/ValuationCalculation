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
    優化後的估值軌道計算：
    1. 使用 Adjusted Price (已調整拆分與分紅的股價) 作為基準
    2. 自動修正財務指標，使其與現行股價量級對齊
    3. 移除複雜的 raw_prices 邏輯，確保軌道平滑
    """
    # 確保索引為 tz-naive
    if prices_adj.index.tz is not None:
        prices_adj.index = prices_adj.index.tz_localize(None)
    if metrics_df.index.tz is not None:
        metrics_df.index = metrics_df.index.tz_localize(None)

    # 1. 獲取調整因子 (Cumulative Adjustment Factor)
    # yfinance 的 adj_ratio = adj_close / close
    tk = yf.Ticker(ticker)
    hist_all = tk.history(period="7y", auto_adjust=False) # 獲取原始與調整價格
    hist_all.index = hist_all.index.tz_localize(None)
    
    # 計算每一天的調整比例 (這反映了拆分與分紅的累積影響)
    # 我們將這個比例應用到財務指標上，讓「歷史指標」與「現今股價」對齊
    adj_factors = hist_all['Close'] / hist_all['Adj Close'] # 注意：這裡反過來算，用於縮小/放大指標
    
    # 2. 數據合併
    df = pd.DataFrame(index=prices_adj.index)
    df['price'] = prices_adj
    df = df.join(metrics_df, how='left')
    
    # 3. 處理指標：先插值，再修正拆分影響
    # 使用 time linear interpolate 填充季度間的空白
    df['metric_raw'] = df[col_name].interpolate(method='time').ffill().bfill()
    
    # 【關鍵步驟】修正指標量級
    # 如果 2022 年拆分了 1:20，那之前的 EPS 應該除以 20，才能跟現在的股價匹配
    # 我們利用價格的 adj_factor 來反推這個比例
    df = df.join(adj_factors.rename('adj_f'), how='left').ffill()
    df['metric_adj'] = df['metric_raw'] / df['adj_f']

    # 4. 計算倍數 (P/E 或 P/FCF)
    # 此時 price 是 adj_close, metric_adj 是經過調整的指標，兩者量級一致
    df['multiple'] = df['price'] / df['metric_adj'].replace(0, np.nan)
    
    results = {}
    avgs = {}

    for label, window in WINDOWS.items():
        # 計算滾動平均倍數
        # 使用 min_periods 確保早期也有數據，不至於出現大量空值
        m_col = df['multiple'].rolling(window=window, min_periods=60).mean()
        s_col = df['multiple'].rolling(window=window, min_periods=60).std().fillna(0)

        # 5. 生成軌道 (Valuation Bands)
        # 軌道 = 滾動倍數 * 當前(調整後)指標
        res = pd.DataFrame(index=df.index)
        res['mean'] = m_col * df['metric_adj']
        res['up1'] = (m_col + s_col) * df['metric_adj']
        res['up2'] = (m_col + 2 * s_col) * df['metric_adj']
        res['down1'] = (m_col - s_col) * df['metric_adj']
        res['down2'] = (m_col - 2 * s_col) * df['metric_adj']

        results[label] = res.ffill().bfill().round(2)
        
        # 獲取最新的一個有效倍數作為平均值參考
        current_m = m_col.dropna().iloc[-1] if not m_col.dropna().empty else 0
        avgs[label] = round(float(current_m), 2)

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