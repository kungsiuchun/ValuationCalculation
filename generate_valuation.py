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
    url = f"https://financialmodelingprep.com/stable/income-statement/?symbol={ticker}&apikey={FMP_API_KEY}"

    try:
        response = requests.get(url)
        inc_data = response.json()

        if not inc_data or "Error Message" in str(inc_data):
            print(f"  ❌ FMP API Error: {inc_data}")
            return None

        df_inc = pd.DataFrame(inc_data)
        # 打印原始資料長度
        print(f"  📊 Raw FMP data rows: {len(df_inc)}")
        
        required_cols = ['date', 'eps', 'revenue', 'weightedAverageShsOut']
        df_inc = df_inc[required_cols]
        df_inc['date'] = pd.to_datetime(df_inc['date'])
        
        df_inc = df_inc.set_index('date').sort_index()
        return df_inc
        
    except Exception as e:
        print(f"  ❌ Error fetching financials: {e}")
        return None

def process_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for ticker in DOW_30:
        print(f"\n🚀 Starting Pipeline for {ticker}...")
        
        financials = get_financials(ticker)
        if financials is None: continue

        print(f"  [2/3] Fetching yfinance prices...")
        stock = yf.Ticker(ticker)
        # 獲取 10 年，確保有足夠數據計算 Rolling
        price_df = stock.history(period="10y")[['Close']]
        price_df.index = price_df.index.tz_localize(None)
        print(f"  📊 Price data rows: {len(price_df)}")

        print(f"  [3/3] Transforming Data...")
        
        # 1. Merge
        merged = price_df.sort_index().copy()
        merged = merged.join(financials[['eps']], how='left')
        
        # 2. 關鍵修復：先填充 EPS 再計算。
        # 如果 FMP 只有最近幾年，我們需要確保插值能運作
        merged['eps_smooth'] = merged['eps'].interpolate(method='linear')
        merged['eps_smooth'] = merged['eps_smooth'].bfill().ffill() # 雙向填充避免首尾出現 NaN

        # 3. 計算每日 PE
        merged['daily_pe'] = merged['Close'] / merged['eps_smooth']
        
        # 4. 滾動計算：將 min_periods 設為 1，徹底解決 NaN 問題
        window = 504
        merged['rolling_mean'] = merged['daily_pe'].rolling(window=window, min_periods=1).mean()
        merged['rolling_std'] = merged['daily_pe'].rolling(window=window, min_periods=1).std().fillna(0)

        # 5. 生成通道
        merged['band_mean'] = merged['rolling_mean'] * merged['eps_smooth']
        merged['band_up2'] = (merged['rolling_mean'] + 2 * merged['rolling_std']) * merged['eps_smooth']
        merged['band_down2'] = (merged['rolling_mean'] - 2 * merged['rolling_std']) * merged['eps_smooth']

        # 6. 最終過濾 (只要有 band_mean 且在 2021 之後)
        final_df = merged[merged.index >= '2021-01-01'].copy()
        print(f"  📊 Final processed rows (since 2021): {len(final_df)}")
        
        if final_df.empty:
            print(f"  ⚠️ Warning: final_df is empty for {ticker}!")
            continue

        history = []
        for date, row in final_df.iterrows():
            history.append({
                "date": date.strftime("%Y-%m-%d"),
                "price": round(row['Close'], 2),
                "bands": {
                    "mean": round(row['band_mean'], 2),
                    "up2": round(row['band_up2'], 2),
                    "down2": round(row['band_down2'], 2)
                }
            })

        final_output = {
            "ticker": ticker,
            "last_updated": datetime.now().strftime("%Y-%m-%d"),
            "summary": {
                "rolling_avg_pe": round(float(merged['rolling_mean'].iloc[-1]), 2),
                "current_pe": round(float(merged['daily_pe'].iloc[-1]), 2)
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