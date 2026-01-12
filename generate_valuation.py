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

def calculate_bands(ticker, prices_df, metrics_df, col_name):
    """
    接收 prices_df (包含 Close 和 Adj Close)，不再內部呼叫 yfinance
    """
    # 強制對齊日期格式
    prices_df.index = pd.to_datetime(prices_df.index).tz_localize(None).normalize()
    metrics_df.index = pd.to_datetime(metrics_df.index).tz_localize(None).normalize()

    # 去重
    prices_df = prices_df[~prices_df.index.duplicated(keep='first')]
    metrics_df = metrics_df[~metrics_df.index.duplicated(keep='first')]

    # 建立全時間軸容器
    all_dates = prices_df.index.union(metrics_df.index).sort_values()
    df = pd.DataFrame(index=all_dates)
    
    # 合併價格與指標
    df = df.join(prices_df)  # 包含 Close 和 Adj Close
    df['metric_raw'] = metrics_df[col_name]

    # --- 核心邏輯：計算拆分調整因子 ---
    # 這是為了讓 AMZN 2022 年的 1:20 拆分前後數據對齊
    # adj_ratio = Adj Close / Close
    df['adj_ratio'] = (df['Adj Close'] / df['Close'].replace(0, np.nan)).ffill().bfill()
    
    # 修正指標量級：讓歷史 EPS 追隨股價的調整
    df['metric_adj'] = df['metric_raw'] * df['adj_ratio']
    
    # 時間插值填補 (解決 AAPL 週六財報問題)
    df['metric_final'] = df['metric_adj'].interpolate(method='time').ffill().bfill()

    # 計算 PE/PFCF 倍數 (兩邊都已經是 Adjusted 量級，算出來的倍數才是平滑的)
    df['multiple'] = df['Adj Close'] / df['metric_final'].apply(lambda x: x if x > 0 else np.nan)
    
    # 資深分析師修正：剪枝極端值 (AMZN 案例)
    upper_limit = 150 if 'eps' in col_name else 100
    df['multiple'] = df['multiple'].clip(0, upper_limit).ffill().bfill()

    # 回切到交易日
    df = df.loc[prices_df.index].copy()

    results = {}
    avgs = {}

    for label, window in WINDOWS.items():
        m_col = df['multiple'].rolling(window=window, min_periods=min(window, 60)).mean()
        s_col = df['multiple'].rolling(window=window, min_periods=min(window, 60)).std().fillna(0)

        res = pd.DataFrame(index=df.index)
        res['mean'] = m_col * df['metric_final']
        res['up1'] = (m_col + s_col) * df['metric_final']
        res['up2'] = (m_col + 2 * s_col) * df['metric_final']
        res['down1'] = (m_col - s_col) * df['metric_final']
        res['down2'] = (m_col - 2 * s_col) * df['metric_final']

        results[label] = res.clip(lower=0).ffill().bfill().round(2)
        
        valid_m = m_col.dropna()
        avgs[label] = round(float(valid_m.iloc[-1]), 2) if not valid_m.empty else 0

    return results, avgs

def debug_valuation(ticker):
    print(f"\n🔍 --- Deep Dive Debug: {ticker} ---")
    
    # 1. 獲取價格
    tk = yf.Ticker(ticker)
    hist = tk.history(period="7y", auto_adjust=False)
    # yfinance 默認返回的可能是 Adj Close 作為 Close，我們強制拿這兩個
    df_prices = hist[['Close', 'Adj Close']].copy()
    df_prices.index = pd.to_datetime(df_prices.index).tz_localize(None).normalize()
    df_prices = df_prices[~df_prices.index.duplicated(keep='first')]

    # 2. 獲取指標 (從你的 build_quarterly_ttm)
    eps_ttm, _ = build_quarterly_ttm(ticker)
    if eps_ttm is None:
        print("❌ Error: eps_ttm is None")
        return
    
    eps_df = eps_ttm.copy()
    eps_df.index = pd.to_datetime(eps_df.index).tz_localize(None).normalize()

    # 3. 合併觀察
    df = df_prices.join(eps_df, how='left')
    
    print("\n[Table 1: 原始數據合併情況 (前 5 行)]")
    # 檢查 eps_ttm 是否成功 join 進來，還是全是 NaN
    print(df[['Close', 'Adj Close', 'eps_ttm']].head(5))

    # 4. 模擬插值
    df['eps_filled'] = df['eps_ttm'].interpolate(method='time').ffill()
    
    # 5. 計算關鍵比例 (這是為了避開拆分)
    # AAPL 2020年 1:4 拆分，那時的 Adj Close / Close 應該約等於 0.25
    df['adj_ratio'] = df['Adj Close'] / df['Close']
    df['eps_final'] = df['eps_filled'] * df['adj_ratio']
    
    print("\n[Table 2: 拆分調整檢查 (2020年8月拆分前後)]")
    # 找出 2020-08-31 附近的數據，看看 adj_ratio 有沒有起作用
    split_date = '2020-08-31'
    if split_date in df.index:
        loc = df.index.get_loc(split_date)
        print(df[['Close', 'Adj Close', 'adj_ratio', 'eps_final']].iloc[loc-2:loc+3])
    else:
        print(df[['Close', 'Adj Close', 'adj_ratio', 'eps_final']].tail(5))

    # 6. 計算倍數
    df['pe_ratio'] = df['Adj Close'] / df['eps_final'].replace(0, np.nan)
    
    print("\n[Table 3: 最終 PE 計算結果]")
    print(df[['Adj Close', 'eps_final', 'pe_ratio']].tail(10))

    if df['pe_ratio'].isna().all():
        print("\n❌ 警報：PE Ratio 全係 NaN！")
        print(f"原因檢查：\n- eps_final 是否全為 0? { (df['eps_final']==0).all() }")
        print(f"- eps_ttm 是否根本沒對齊日期? { eps_df.index.isin(df_prices.index).sum() } 個日期對齊")



# --- 5. 主程序 ---
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 呼叫 Debug
    ## debug_valuation("AAPL")

    for ticker in DOW_30:
        print(f"\n🏗️  Pipeline Starting: {ticker}")
        prices = yf.Ticker(ticker).history(period="8y", auto_adjust=False)

        prices_df = prices[['Close', 'Adj Close']].copy()
        ##prices.index = prices.index.tz_localize(None)

        eps_ttm, fcf_ttm = build_quarterly_ttm(ticker)
        if eps_ttm is None: continue

        pe_res, pe_avgs = calculate_bands(ticker, prices_df, eps_ttm, 'eps_ttm')
        fcf_res, fcf_avgs = calculate_bands(ticker, prices_df, fcf_ttm, 'fcf_ps_ttm')

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