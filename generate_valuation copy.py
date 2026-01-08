import requests
import pandas as pd
import numpy as np
import yfinance as yf
import json
import os
import time
from datetime import datetime

FMP_API_KEY = "F9dROu64FwpDqETGsu1relweBEoTcpID"
# 確保路徑是相對腳本位置的
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "data")

# 先測試 3 支就好，確認成功再擴大
DOW_30 = ["AAPL"]

def get_financials(ticker):
    print(f"  [1/3] Fetching FMP financials for {ticker}...")
    urls = {
        "income": f"https://financialmodelingprep.com/stable/income-statement/?symbol={ticker}?limit=40&apikey={FMP_API_KEY}"
    }

    try:
        inc_data = requests.get(urls["income"]).json()
        cf_data = requests.get(urls["cashflow"]).json()
        met_data = requests.get(urls["metrics"]).json()

        if not inc_data or "Error Message" in str(inc_data):
            print(f"  ❌ FMP API Error: {inc_data}")
            return None

        df_inc = pd.DataFrame(inc_data)[['date', 'eps', 'revenue']].set_index('date')
        df_cf = pd.DataFrame(cf_data)[['date', 'freeCashFlow']].set_index('date')
        df_met = pd.DataFrame(met_data)[['date', 'weightedAverageSharesOutstanding']].set_index('date')

        financials = pd.concat([df_inc, df_cf, df_met], axis=1)
        financials.index = pd.to_datetime(financials.index)
        
        financials['sps'] = financials['revenue'] / financials['weightedAverageSharesOutstanding']
        financials['fcfps'] = financials['freeCashFlow'] / financials['weightedAverageSharesOutstanding']
        
        print(f"  ✅ Financials loaded. Rows: {len(financials)}")
        return financials.sort_index()
    except Exception as e:
        print(f"  ❌ Error fetching financials: {e}")
        return None

def process_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"📂 Output directory: {OUTPUT_DIR}")
    
    for ticker in DOW_30:
        print(f"\n🚀 Processing {ticker}...")
        
        # 1. 抓取財報
        financials = get_financials(ticker)
        if financials is None: continue

        # 2. 抓取股價
        print(f"  [2/3] Fetching yfinance prices...")
        price_df = yf.Ticker(ticker).history(period="max")[['Close']]
        if price_df.empty:
            print(f"  ❌ No price data found for {ticker}")
            continue
        price_df.index = price_df.index.tz_localize(None)
        print(f"  ✅ Price data loaded. Rows: {len(price_df)}")

        # 3. 數據對齊 (最容易出錯的地方)
        print(f"  [3/3] Aligning data and calculating bands...")
        merged = pd.merge_asof(
            price_df.sort_index(), 
            financials.sort_index(), 
            left_index=True, 
            right_index=True, 
            direction='backward'
        )
        
        # 檢查對齊後是否還有數據
        initial_count = len(merged)
        merged = merged.dropna(subset=['eps', 'sps', 'fcfps'])
        if len(merged) == 0:
            print(f"  ❌ Alignment failed: No matching data after dropna. (Original rows: {initial_count})")
            continue
        
        print(f"  ✅ Alignment success. Valid rows: {len(merged)}")

        final_output = {
            "ticker": ticker,
            "last_updated": datetime.now().strftime("%Y-%m-%d"),
            "valuations": {}
        }

        ratios = {"P/E": "eps", "P/S": "sps", "P/FCF": "fcfps"}
        
        valid_json = False
        for label, col in ratios.items():
            # 計算通道邏輯
            temp_df = merged.copy()
            temp_df['Ratio'] = temp_df['Close'] / temp_df[col]
            valid_ratios = temp_df[temp_df['Ratio'] > 0]['Ratio']
            
            if valid_ratios.empty:
                print(f"  ⚠️  No positive ratios for {label}, skipping this metric.")
                continue

            mean_val, std_val = valid_ratios.mean(), valid_ratios.std()
            
            # 轉換為前端格式 (取 2018 年後以節省空間)
            recent_merged = merged[merged.index >= '2018-01-01']
            history = []
            for date, row in recent_merged.iterrows():
                history.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "price": round(row['Close'], 2),
                    "bands": {
                        "mean": round(mean_val * row[col], 2),
                        "up2": round((mean_val + 2*std_val) * row[col], 2),
                        "down2": round((mean_val - 2*std_val) * row[col], 2)
                    }
                })
            
            final_output["valuations"][label] = {
                "avg_ratio": round(mean_val, 2),
                "history": history
            }
            valid_json = True

        if valid_json:
            file_path = os.path.join(OUTPUT_DIR, f"{ticker}.json")
            with open(file_path, "w") as f:
                json.dump(final_output, f)
            print(f"  💾 SUCCESS: Saved to {file_path}")
        else:
            print(f"  ❌ FAILED: No valuation data generated for {ticker}")

        time.sleep(1)

if __name__ == "__main__":
    process_pipeline()