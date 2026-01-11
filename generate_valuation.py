import requests
import pandas as pd
import numpy as np
import yfinance as yf
import json
import os
import time

FMP_API_KEY = "F9dROu64FwpDqETGsu1relweBEoTcpID"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "data")

# 測試用 DOW 30 成分股
DOW_30 = ["AAPL", "MSFT", "WMT"] 

def get_income_statement(ticker):
    """ 獲取 EPS 數據 (用於 P/E 模型) """
    url = f"https://financialmodelingprep.com/stable/income-statement?symbol={ticker}&apikey={FMP_API_KEY}"
    try:
        res = requests.get(url).json()
        if not res or "Error" in str(res): return None
        df = pd.DataFrame(res)[['date', 'eps']]
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        return df.set_index('date').sort_index()
    except:
        return None

def get_cash_flow_statement(ticker):
    """ 獲取 Free Cash Flow 數據 (用於 P/FCF 模型) """
    # 這裡直接拿 freeCashFlow，還需要流通股數來算 Per Share
    url_cf = f"https://financialmodelingprep.com/stable/cash-flow-statement?symbol={ticker}&apikey={FMP_API_KEY}"
    # 我們還需要流通股數 (Shares Outstanding) 來計算 FCF per Share
    # FMP 的 enterprise-values 接口通常有最準確的歷史股數
    url_ev = f"https://financialmodelingprep.com/stable/enterprise-values/?symbol={ticker}&apikey={FMP_API_KEY}"

    try:
        # 1. 獲取 FCF
        res_cf = requests.get(url_cf).json()
        df_cf = pd.DataFrame(res_cf)[['date', 'freeCashFlow']]
        df_cf['date'] = pd.to_datetime(df_cf['date']).dt.tz_localize(None)
        df_cf = df_cf.set_index('date').sort_index()

        # 2. 獲取歷史股數 (numberOfShares)
        res_ev = requests.get(url_ev).json()
        df_ev = pd.DataFrame(res_ev)[['date', 'numberOfShares']]
        df_ev['date'] = pd.to_datetime(df_ev['date']).dt.tz_localize(None)
        df_ev = df_ev.set_index('date').sort_index()

        # 3. 合併算出 FCF per Share
        # 使用 merge_asof 或者是簡單的 reindex 對齊 (年報日期通常接近)
        # 這裡簡化處理：將兩者都按年度對齊
        df_merge = pd.merge_asof(df_cf, df_ev, left_index=True, right_index=True, direction='nearest', tolerance=pd.Timedelta(days=30))
        
        # 計算每股 FCF
        df_merge['fcf_ps'] = df_merge['freeCashFlow'] / df_merge['numberOfShares']
        return df_merge[['fcf_ps']]
    
    except Exception as e:
        print(f"  ❌ FCF Error: {e}")
        return None

def calculate_bands(price_series, metric_series, metric_name):
    """ 通用的 5 線計算邏輯 """
    # 1. 數據對齊
    combined = pd.concat([price_series, metric_series], axis=1).sort_index()
    
    # 2. 線性插值填補季報/年報之間的空隙
    combined[f'{metric_name}_smooth'] = combined[metric_name].interpolate(method='time').ffill().bfill()
    
    df = combined.dropna(subset=['Close']).copy()

    # 3. 計算估值倍數 (Price / Metric)
    # P/E = Price / EPS
    # P/FCF = Price / FCF_Per_Share
    df['multiple'] = df['Close'] / df[f'{metric_name}_smooth']

    # 4. 滾動統計 (2年 / 504天)
    df['mean_mul'] = df['multiple'].rolling(window=504, min_periods=1).mean()
    df['std_mul'] = df['multiple'].rolling(window=504, min_periods=1).std().fillna(0)

    # 5. 計算 5 條軌道
    bands = pd.DataFrame(index=df.index)
    bands['mean'] = df['mean_mul'] * df[f'{metric_name}_smooth']
    bands['up1'] = (df['mean_mul'] + df['std_mul']) * df[f'{metric_name}_smooth']
    bands['up2'] = (df['mean_mul'] + 2 * df['std_mul']) * df[f'{metric_name}_smooth']
    bands['down1'] = (df['mean_mul'] - df['std_mul']) * df[f'{metric_name}_smooth']
    bands['down2'] = (df['mean_mul'] - 2 * df['std_mul']) * df[f'{metric_name}_smooth']
    
    return bands, df['mean_mul'].iloc[-1]

def process_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for ticker in DOW_30:
        print(f"\n🚀 Analyzing {ticker}...")
        
        # 1. 獲取股價
        try:
            price_df = yf.Ticker(ticker).history(period="5y")[['Close']]
            price_df.index = price_df.index.tz_localize(None)
        except:
            print(f"  ❌ Price fetch failed for {ticker}")
            continue

        # 2. 獲取並計算 P/E Bands
        eps_df = get_income_statement(ticker)
        pe_bands, current_pe = (None, None)
        if eps_df is not None:
            pe_bands, current_pe = calculate_bands(price_df['Close'], eps_df['eps'], 'eps')

        # 3. 獲取並計算 P/FCF Bands
        fcf_df = get_cash_flow_statement(ticker)
        fcf_bands, current_pfcf = (None, None)
        if fcf_df is not None:
            fcf_bands, current_pfcf = calculate_bands(price_df['Close'], fcf_df['fcf_ps'], 'fcf_ps')

        # 4. 封裝數據
        # 我們只取最近 5 年的數據來顯示，減少 JSON 大小
        start_date = '2021-01-01'
        final_df = price_df[price_df.index >= start_date].copy()
        
        history = []
        for date, row in final_df.iterrows():
            date_str = date.strftime("%Y-%m-%d")
            
            record = {
                "date": date_str,
                "price": round(row['Close'], 2),
                "valuation": {} # 這裡存放兩種模型的線
            }

            # 填入 P/E 數據
            if pe_bands is not None and date in pe_bands.index:
                b = pe_bands.loc[date]
                record["valuation"]["pe"] = {
                    "mean": round(b['mean'], 2),
                    "up1": round(b['up1'], 2), 
                    "up2": round(b['up2'], 2),
                    "down1": round(b['down1'], 2),
                    "down2": round(b['down2'], 2)
                }

            # 填入 P/FCF 數據
            if fcf_bands is not None and date in fcf_bands.index:
                b = fcf_bands.loc[date]
                record["valuation"]["fcf"] = {
                    "mean": round(b['mean'], 2),
                    "up1": round(b['up1'], 2),
                    "up2": round(b['up2'], 2),
                    "down1": round(b['down1'], 2),
                    "down2": round(b['down2'], 2)
                }
            
            history.append(record)

        # 5. 輸出 JSON
        output = {
            "ticker": ticker,
            "metrics": {
                "current_pe_avg": round(current_pe, 2) if current_pe else None,
                "current_pfcf_avg": round(current_pfcf, 2) if current_pfcf else None
            },
            "data": history
        }

        with open(os.path.join(OUTPUT_DIR, f"{ticker}_valuation.json"), "w") as f:
            json.dump(output, f)
        
        print(f"  ✅ Saved: {ticker}_valuation.json (Dual Model)")

if __name__ == "__main__":
    process_pipeline()