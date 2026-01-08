import requests
import pandas as pd
import numpy as np
import yfinance as yf
import json
import os
import time
from datetime import datetime

FMP_API_KEY = "F9dROu64FwpDqETGsu1relweBEoTcpID"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "data")

DOW_30 = ["AAPL"] 

def get_financials(ticker):
    print(f"  [1/3] Fetching Stable FMP Income Statement for {ticker}...")
    # 完全保留你的原始 URL
    url = f"https://financialmodelingprep.com/stable/income-statement/?symbol={ticker}&apikey={FMP_API_KEY}"

    try:
        response = requests.get(url)
        inc_data = response.json()

        if not inc_data or "Error Message" in str(inc_data):
            print(f"  ❌ FMP API Error: {inc_data}")
            return None

        df_inc = pd.DataFrame(inc_data)
        required_cols = ['date', 'eps'] # 核心需要這兩個
        df_inc = df_inc[required_cols].copy()
        df_inc['date'] = pd.to_datetime(df_inc['date']).dt.tz_localize(None)
        
        df_inc = df_inc.set_index('date').sort_index()
        print(f"  ✅ Financials found: {len(df_inc)} rows")
        return df_inc
        
    except Exception as e:
        print(f"  ❌ Error fetching financials: {e}")
        return None

def process_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for ticker in DOW_30:
        print(f"\n🚀 Starting Pipeline for {ticker}...")
        
        financials = get_financials(ticker)
        if financials is None or financials.empty: 
            print("  ❌ No financials found, skipping...")
            continue

        print(f"  [2/3] Fetching yfinance prices...")
        stock = yf.Ticker(ticker)
        price_df = stock.history(period="10y")[['Close']]
        price_df.index = price_df.index.tz_localize(None)

        print(f"  [3/3] Transforming Data (Safe Interpolation)...")
        
        # --- 核心邏輯修復：使用 Concat 確保週末的財報也不會丟失 ---
        # 創建一個包含所有日期的 DataFrame
        combined = pd.concat([price_df, financials], axis=0).sort_index()
        
        # 在聯集時間軸上插值 (線性插值讓階梯變曲線)
        combined['eps_smooth'] = combined['eps'].interpolate(method='time')
        
        # 填充首尾可能的空值
        combined['eps_smooth'] = combined['eps_smooth'].ffill().bfill()
        
        # 現在只保留有股價的日期 (交易日)
        merged = combined.dropna(subset=['Close']).copy()
        
        # 計算每日 PE
        merged['daily_pe'] = merged['Close'] / merged['eps_smooth']
        
        # 計算滾動統計 (min_periods=1 確保不會 NaN)
        window = 504
        merged['rolling_mean'] = merged['daily_pe'].rolling(window=window, min_periods=1).mean()
        merged['rolling_std'] = merged['daily_pe'].rolling(window=window, min_periods=1).std().fillna(0)

        # 生成通道價格
        merged['band_mean'] = merged['rolling_mean'] * merged['eps_smooth']
        merged['band_up2'] = (merged['rolling_mean'] + 2 * merged['rolling_std']) * merged['eps_smooth']
        merged['band_down2'] = (merged['rolling_mean'] - 2 * merged['rolling_std']) * merged['eps_smooth']

        # --- 封裝 JSON ---
        final_df = merged[merged.index >= '2021-01-01']
        
        history = []
        for date, row in final_df.iterrows():
            # 確保寫入 JSON 前轉換為原生的 Python float，避免 NaN
            history.append({
                "date": date.strftime("%Y-%m-%d"),
                "price": float(round(row['Close'], 2)),
                "bands": {
                    "mean": float(round(row['band_mean'], 2)),
                    "up2": float(round(row['band_up2'], 2)),
                    "down2": float(round(row['band_down2'], 2))
                }
            })

        final_output = {
            "ticker": ticker,
            "last_updated": datetime.now().strftime("%Y-%m-%d"),
            "summary": {
                "rolling_avg_pe": float(round(merged['rolling_mean'].iloc[-1], 2)),
                "current_pe": float(round(merged['daily_pe'].iloc[-1], 2))
            },
            "data": history
        }

        file_path = os.path.join(OUTPUT_DIR, f"{ticker}.json")
        with open(file_path, "w") as f:
            json.dump(final_output, f)
        
        print(f"  💾 SUCCESS: {ticker}.json generated with {len(history)} data points.")
        time.sleep(1)

if __name__ == "__main__":
    process_pipeline()