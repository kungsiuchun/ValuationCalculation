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
            return None

        df_inc = pd.DataFrame(inc_data)
        df_inc = df_inc[['date', 'eps']].copy()
        df_inc['date'] = pd.to_datetime(df_inc['date']).dt.tz_localize(None)
        return df_inc.set_index('date').sort_index()
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None

def process_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for ticker in DOW_30:
        print(f"\n🚀 Generating 5-Line Bands for {ticker}...")
        
        financials = get_financials(ticker)
        if financials is None: continue

        # 獲取價格
        price_df = yf.Ticker(ticker).history(period="10y")[['Close']]
        price_df.index = price_df.index.tz_localize(None)

        # 1. 數據對齊與平滑 (Interpolation)
        combined = pd.concat([price_df, financials], axis=0).sort_index()
        combined['eps_smooth'] = combined['eps'].interpolate(method='time').ffill().bfill()
        
        # 2. 回到交易日索引
        df = combined.dropna(subset=['Close']).copy()
        
        # 3. 計算動態 P/E 統計 (Rolling Window: 2 years / 504 days)
        df['pe'] = df['Close'] / df['eps_smooth']
        df['m_pe'] = df['pe'].rolling(window=504, min_periods=1).mean()
        df['s_pe'] = df['pe'].rolling(window=504, min_periods=1).std().fillna(0)

        # 4. 計算 5 條通道線
        # Price = PE * EPS
        df['b_m'] = df['m_pe'] * df['eps_smooth']
        df['b_u1'] = (df['m_pe'] + df['s_pe']) * df['eps_smooth']
        df['b_u2'] = (df['m_pe'] + 2 * df['s_pe']) * df['eps_smooth']
        df['b_d1'] = (df['m_pe'] - df['s_pe']) * df['eps_smooth']
        df['b_d2'] = (df['m_pe'] - 2 * df['s_pe']) * df['eps_smooth']

        # 5. 封裝數據 (2021年起)
        final_df = df[df.index >= '2021-01-01']
        history = []
        for date, row in final_df.iterrows():
            history.append({
                "date": date.strftime("%Y-%m-%d"),
                "price": round(float(row['Close']), 2),
                "bands": {
                    "up2": round(float(row['b_u2']), 2),
                    "up1": round(float(row['b_u1']), 2),
                    "mean": round(float(row['b_m']), 2),
                    "down1": round(float(row['b_d1']), 2),
                    "down2": round(float(row['b_d2']), 2)
                }
            })

        output = {
            "ticker": ticker,
            "summary": {"avg_pe": round(float(df['m_pe'].iloc[-1]), 2)},
            "data": history
        }

        with open(os.path.join(OUTPUT_DIR, f"{ticker}.json"), "w") as f:
            json.dump(output, f)
        
        print(f"  ✅ Done: 5-line chart data ready for {ticker}")

if __name__ == "__main__":
    process_pipeline()