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
DOW_30 = ["AAPL", "MSFT", "WMT", "GOOGL", "AMZN"] 

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

def calculate_multi_period_bands(price_series, metric_series, metric_name):
    """ 
    核心算法：
    1. 將股價與財務數據對齊
    2. 線性插值填補報表間隙
    3. 計算歷史滾動 PE/FCF 倍數
    4. 生成 5 條估值通道線
    """
    # 對齊與插值
    combined = pd.concat([price_series, metric_series], axis=1).sort_index()
    combined[f'{metric_name}_smooth'] = combined[metric_name].interpolate(method='time').ffill().bfill()
    df = combined.dropna(subset=['Close']).copy()
    
    # 計算估值倍數 (Price / Value)
    df['multiple'] = df['Close'] / df[f'{metric_name}_smooth']

    period_results = {}
    current_averages = {}

    for label, window_size in WINDOWS.items():
        # 計算滾動均值與標準差 (Rolling Mean & Std)
        df[f'mean_{label}'] = df['multiple'].rolling(window=window_size, min_periods=1).mean()
        df[f'std_{label}'] = df['multiple'].rolling(window=window_size, min_periods=1).std().fillna(0)

        # 套用公式生成 5 條線: Mean, ±1σ, ±2σ
        bands = pd.DataFrame(index=df.index)
        m_col = df[f'mean_{label}']
        s_col = df[f'std_{label}']
        val_col = df[f'{metric_name}_smooth']

        bands['mean'] = m_col * val_col
        bands['up1'] = (m_col + s_col) * val_col
        bands['up2'] = (m_col + 2 * s_col) * val_col
        bands['down1'] = (m_col - s_col) * val_col
        bands['down2'] = (m_col - 2 * s_col) * val_col
        
        period_results[label] = bands
        current_averages[label] = round(m_col.iloc[-1], 2)

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
        pe_results, pe_avgs = calculate_multi_period_bands(full_price_df['Close'], eps_df['eps'], 'eps') if eps_df is not None else ({}, {})
        fcf_results, fcf_avgs = calculate_multi_period_bands(full_price_df['Close'], fcf_df['fcf_ps'], 'fcf_ps') if fcf_df is not None else ({}, {})

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