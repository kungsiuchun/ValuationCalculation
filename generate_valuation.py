import requests
import pandas as pd
import numpy as np
import yfinance as yf
import json
import os
from datetime import datetime, timedelta

# --- 1. 初始化與配置 ---
FMP_API_KEY = "F9dROu64FwpDqETGsu1relweBEoTcpID"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "data")
DOW_30 = ["AMZN"]
## ["AAPL", "MSFT", "WMT", "GOOGL", "AMZN"] 

# 定義滾動週期（以交易日計算，一年約 252 天）
WINDOWS = {
    "1Y": 252,
    "2Y": 504,
    "3Y": 756,
    "5Y": 1260
}

def load_local_data(ticker):
    """ 檢查本地是否已有 JSON 檔，用於增量判斷 """
    file_path = os.path.join(OUTPUT_DIR, f"{ticker}_valuation.json")
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"  ⚠️  Warning: Local JSON for {ticker} exists but is corrupted: {e}")
            return None
    return None

def get_income_statement(ticker):
    """ 從 FMP 獲取利潤表 (EPS 數據) """
    url = f"https://financialmodelingprep.com/stable/income-statement?symbol={ticker}&apikey={FMP_API_KEY}"
    try:
        res = requests.get(url).json()
        if not res or "Error" in str(res): 
            print(f"  ❌ FMP API Error (Income Statement): {res}")
            return None
        df = pd.DataFrame(res)[['date', 'eps']]
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        return df.set_index('date').sort_index()
    except Exception as e:
        print(f"  ❌ Failed to fetch Income Statement for {ticker}: {e}")
        return None

def get_cash_flow_statement(ticker):
    """ 從 FMP 獲取現金流量表與歷史股數，計算每股 FCF """
    url_cf = f"https://financialmodelingprep.com/stable/cash-flow-statement?symbol={ticker}&apikey={FMP_API_KEY}"
    url_ev = f"https://financialmodelingprep.com/stable/enterprise-values/?symbol={ticker}&apikey={FMP_API_KEY}"
    try:
        # 抓取現金流
        res_cf = requests.get(url_cf).json()
        df_cf = pd.DataFrame(res_cf)[['date', 'freeCashFlow']]
        df_cf['date'] = pd.to_datetime(df_cf['date']).dt.tz_localize(None)
        df_cf = df_cf.set_index('date').sort_index()

        # 抓取股數 (用於計算 Per Share 數據)
        res_ev = requests.get(url_ev).json()
        df_ev = pd.DataFrame(res_ev)[['date', 'numberOfShares']]
        df_ev['date'] = pd.to_datetime(df_ev['date']).dt.tz_localize(None)
        df_ev = df_ev.set_index('date').sort_index()

        # 將兩份報表對齊
        df_merge = pd.merge_asof(df_cf, df_ev, left_index=True, right_index=True, direction='nearest', tolerance=pd.Timedelta(days=30))
        df_merge['fcf_ps'] = df_merge['freeCashFlow'] / df_merge['numberOfShares']
        return df_merge[['fcf_ps']]
    except Exception as e:
        print(f"  ❌ Failed to fetch Cash Flow Statement for {ticker}: {e}")
        return None

def calculate_multi_period_bands(ticker, price_series, metric_series, metric_name):
    """ 
    修正版核心算法：
    1. 獲取股票拆分歷史並調整財務數據 (解決 AMZN/GOOGL 拆分導致的估值斷層)
    2. 將股價與調整後的財務數據對齊並線性插值
    3. 計算歷史滾動 PE/FCF 倍數 (Multiple)
    4. 生成 1Y, 2Y, 3Y, 5Y 的 5 條估值通道線
    """
    # --- Step A: 股票拆分調整 (保持不變) ---
    tk = yf.Ticker(ticker)
    splits = tk.splits
    adjusted_metric = metric_series.copy()
    if not splits.empty:
        for split_date, ratio in splits.items():
            split_dt = split_date.tz_localize(None)
            adjusted_metric.loc[adjusted_metric.index < split_dt] /= ratio

    # --- Step B: 數據對齊 ---
    combined = pd.concat([price_series, adjusted_metric], axis=1).sort_index()
    combined[f'{metric_name}_smooth'] = combined[metric_name].interpolate(method='time').ffill().bfill()
    df = combined.dropna(subset=['Close']).copy()
    
    # --- Step C: 計算倍數 (修正負數問題) ---
    # 如果財務指標為負(如 FCF < 0)，該天的 Multiple 設為 NaN，不參與滾動平均計算
    df['multiple'] = df['Close'] / df[f'{metric_name}_smooth']
    df.loc[df[f'{metric_name}_smooth'] <= 0, 'multiple'] = np.nan 

    period_results = {}
    current_averages = {}

    for label, window_size in WINDOWS.items():
        # 計算滾動均值，跳過 NaN (即跳過負 FCF 的時期)
        # 增加 min_periods 要求，例如至少要有該窗口 20% 的有效數據，否則不顯示，避免數據剛開始時過度重合
        df[f'mean_{label}'] = df['multiple'].rolling(window=window_size, min_periods=max(1, int(window_size*0.1))).mean()
        df[f'std_{label}'] = df['multiple'].rolling(window=window_size, min_periods=max(1, int(window_size*0.1))).std().fillna(0)

        bands = pd.DataFrame(index=df.index)
        m_col = df[f'mean_{label}']
        s_col = df[f'std_{label}']
        val_col = df[f'{metric_name}_smooth']

        # 生成估值線 (注意：即使 Multiple 是 NaN，我們還是會根據最後的平均值畫線)
        # 使用 ffill() 確保如果當前 FCF 是負的，它會延用最近一個正數的平均倍數
        bands['mean'] = m_col.ffill() * val_col
        bands['up1'] = (m_col.ffill() + s_col.ffill()) * val_col
        bands['up2'] = (m_col.ffill() + 2 * s_col.ffill()) * val_col
        bands['down1'] = (m_col.ffill() - s_col.ffill()) * val_col
        bands['down2'] = (m_col.ffill() - 2 * s_col.ffill()) * val_col
        
        period_results[label] = bands
        
        # 獲取最後一個非空值作為當前平均值
        last_valid_mean = m_col.dropna().iloc[-1] if not m_col.dropna().empty else 0
        current_averages[label] = round(last_valid_mean, 2)

    return period_results, current_averages

def process_pipeline():
    """ 執行主程序 """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for ticker in DOW_30:
        print(f"\n🔍 [Step 1/5] Checking {ticker} status...")
        
        local_json = load_local_data(ticker)
        if local_json and len(local_json.get('data', [])) > 0:
            last_date_str = local_json['data'][-1]['date']
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
            # 判斷是否需要更新 (如果最後數據日期是昨天或更早)
            if last_date.date() >= datetime.now().date() - timedelta(days=1):
                print(f"  ✅ {ticker} is already up to date (Last: {last_date_str}). Skipping calculation.")
                continue

        print(f"  📈 {ticker} needs update. Fetching 7-year price history from Yahoo Finance...")
        try:
            # 獲取 7 年數據確保 5Y 滾動窗格在起始點是滿的
            full_price_df = yf.Ticker(ticker).history(period="7y")[['Close']]
            if full_price_df.empty:
                print(f"  ❌ No price data found for {ticker}")
                continue
            full_price_df.index = full_price_df.index.tz_localize(None)
        except Exception as e:
            print(f"  ❌ Yahoo Finance fetch failed: {e}")
            continue

        print(f"🧪 [Step 2/5] Fetching financial statements from FMP...")
        eps_df = get_income_statement(ticker)
        fcf_df = get_cash_flow_statement(ticker)

        # 計算估值帶
        print(f"🧮 [Step 3/5] Calculating Multi-Period Valuation Bands (1Y, 2Y, 3Y, 5Y)...")
        # 修改呼叫行
        pe_results, pe_avgs = calculate_multi_period_bands(ticker, full_price_df['Close'], eps_df['eps'], 'eps')
        fcf_results, fcf_avgs = calculate_multi_period_bands(ticker, full_price_df['Close'], fcf_df['fcf_ps'], 'fcf_ps')

        # 整理歷史紀錄至 JSON 格式
        print(f"📦 [Step 4/5] Packing historical data (Starting from 2021)...")
        history = []
        start_date = datetime(2021, 1, 1)
        output_df = full_price_df[full_price_df.index >= start_date]

        for date, row in output_df.iterrows():
            date_str = date.strftime("%Y-%m-%d")
            record = {
                "date": date_str,
                "price": round(row['Close'], 2),
                "valuation": {}
            }

            # 遍歷所有時間窗格填寫數據
            for label in WINDOWS.keys():
                record["valuation"][label] = {}
                
                # PE 模型
                if label in pe_results and date in pe_results[label].index:
                    b = pe_results[label].loc[date]
                    record["valuation"][label]["pe"] = {k: round(v, 2) for k, v in b.to_dict().items()}
                
                # FCF 模型
                if label in fcf_results and date in fcf_results[label].index:
                    b = fcf_results[label].loc[date]
                    record["valuation"][label]["fcf"] = {k: round(v, 2) for k, v in b.to_dict().items()}
            
            history.append(record)

        # 最終 JSON 封裝
        output = {
            "ticker": ticker,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "averages": {
                "pe": pe_avgs,
                "fcf": fcf_avgs
            },
            "data": history
        }

        print(f"💾 [Step 5/5] Saving results to {ticker}_valuation.json...")
        with open(os.path.join(OUTPUT_DIR, f"{ticker}_valuation.json"), "w") as f:
            json.dump(output, f)
        
        print(f"✨ [Success] {ticker} pipeline completed.")

if __name__ == "__main__":
    print(f"🚀 Starting Valuation Data Pipeline at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    process_pipeline()
    print(f"\n🏁 All tickers processed. Terminal standby.")