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

DOW_30 = ["AAPL"] # 你可以在此增加其他代碼

def get_financials(ticker):
    print(f"  [1/3] Fetching Stable FMP Income Statement for {ticker}...")
    # 使用你提供的新版 URL 格式
    url = f"https://financialmodelingprep.com/stable/income-statement/?symbol={ticker}&apikey={FMP_API_KEY}"

    try:
        response = requests.get(url)
        print(f"json response: {response.json}")
        inc_data = response.json()

        if not inc_data or "Error Message" in str(inc_data):
            print(f"  ❌ FMP API Error: {inc_data}")
            return None

        # 根據你提供的 JSON 結構提取數據
        # 我們取 eps, revenue, weightedAverageShsOut, date
        df_inc = pd.DataFrame(inc_data)
        
        # 確保數據包含必要欄位
        required_cols = ['date', 'eps', 'revenue', 'weightedAverageShsOut']
        df_inc = df_inc[required_cols]
        
        # 轉換日期並過濾 2021 年以後的數據
        df_inc['date'] = pd.to_datetime(df_inc['date'])
        df_inc = df_inc[df_inc['date'].dt.year >= 2021]
        
        # 設定索引
        df_inc = df_inc.set_index('date')
        
        # 計算 SPS (Revenue / Shares) 以備後續擴展 P/S
        df_inc['sps'] = df_inc['revenue'] / df_inc['weightedAverageShsOut']
        
        print(f"  ✅ Financials loaded. Records from 2021: {len(df_inc)}")
        return df_inc.sort_index()
        
    except Exception as e:
        print(f"  ❌ Error fetching financials: {e}")
        return None

def process_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for ticker in DOW_30:
        print(f"\n🚀 Starting Pipeline for {ticker}...")
        
        # 1. 抓取財報 (Income Statement Only)
        financials = get_financials(ticker)
        if financials is None: continue

        # 2. 抓取股價
        print(f"  [2/3] Fetching yfinance prices...")
        stock = yf.Ticker(ticker)
        price_df = stock.history(period="10y")[['Close']]
        price_df.index = price_df.index.tz_localize(None)

        # 3. 數據對齊
        print(f"  [3/3] Aligning Data & Generating Bands...")
        merged = pd.merge_asof(
            price_df.sort_index(), 
            financials.sort_index(), 
            left_index=True, 
            right_index=True, 
            direction='backward'
        ).dropna(subset=['eps'])

        # 4. 計算 P/E 估值通道
        # 我們計算從 2021 至今的平均 P/E 作為基準
        merged['PE_Ratio'] = merged['Close'] / merged['eps']
        valid_pe = merged[merged['PE_Ratio'] > 0]['PE_Ratio']
        
        mean_pe = valid_pe.mean()
        std_pe = valid_pe.std()
        
        print(f"  📊 Analysis Result: Mean PE = {round(mean_pe, 2)}, STD = {round(std_pe, 2)}")

        # 5. 封裝 JSON
        history = []
        # 為了網頁效能，我們只取 2021 年後的歷史
        final_df = merged[merged.index >= '2021-01-01']
        
        for date, row in final_df.iterrows():
            history.append({
                "date": date.strftime("%Y-%m-%d"),
                "price": round(row['Close'], 2),
                "eps": round(row['eps'], 2),
                "bands": {
                    "mean": round(mean_pe * row['eps'], 2),
                    "up1": round((mean_pe + std_pe) * row['eps'], 2),
                    "up2": round((mean_pe + 2*std_pe) * row['eps'], 2),
                    "down1": round((mean_pe - std_pe) * row['eps'], 2),
                    "down2": round((mean_pe - 2*std_pe) * row['eps'], 2)
                }
            })

        final_output = {
            "ticker": ticker,
            "last_updated": datetime.now().strftime("%Y-%m-%d"),
            "summary": {
                "mean_pe": round(mean_pe, 2),
                "std_pe": round(std_pe, 2)
            },
            "data": history
        }

        # 寫入檔案
        file_path = os.path.join(OUTPUT_DIR, f"{ticker}.json")
        with open(file_path, "w") as f:
            json.dump(final_output, f)
        
        print(f"  💾 SUCCESS: Saved valuation to {file_path}")
        time.sleep(1)

if __name__ == "__main__":
    process_pipeline()