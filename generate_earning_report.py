import os
import json
import pandas as pd
from pathlib import Path

# Configuration
DATA_DIR = Path('data/fmp_cache')
OUTPUT_DIR = Path('data/processed')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_and_normalize_data(ticker_path):
    """
    Reads all JSON files, handles lists correctly, and grabs all dates found.
    """
    data_store = {
        'income-statement': [],
        'cash-flow-statement': [],
        'enterprise-values': [] # Added support for this file type
    }
    
    print(f"--- Scanning files for {ticker_path.name} ---")

    for file_path in ticker_path.glob('*.json'):
        filename = file_path.name
        
        # Identify Statement Type
        if 'income-statement' in filename:
            st_type = 'income-statement'
        elif 'cash-flow' in filename:
            st_type = 'cash-flow-statement'
        elif 'enterprise-values' in filename:
            st_type = 'enterprise-values'
        else:
            print(f"⚠️  Skipping unknown file type: {filename}")
            continue

        try:
            with open(file_path, 'r') as f:
                content = json.load(f)
                
                # FIX: Handle both Lists and Single Objects
                if isinstance(content, list):
                    items = content # Take ALL items, not just [0]
                else:
                    items = [content]

                # Append valid items
                count_added = 0
                for item in items:
                    if item and 'date' in item:
                        data_store[st_type].append(item)
                        count_added += 1
                
                # Debug: Show user what dates were found in this file
                if count_added > 0:
                    found_dates = [x['date'] for x in items if 'date' in x]
                    print(f"   📄 {filename}: Found {count_added} records. Dates: {found_dates}")
                else:
                    print(f"   ⚠️ {filename}: Valid JSON but no 'date' field found.")

        except Exception as e:
            print(f"   ❌ Error reading {filename}: {e}")

    return data_store

def calculate_growth_metrics(df):
    """
    Calculates YoY and QoQ growth for key financial metrics.
    Assumes data is sorted by date ascending for shift operations.
    """
    # Sort ascending for time-series calculations
    df = df.sort_values('date', ascending=True)
    
    # Define which columns we want to track growth for
    kpis = ['revenue', 'netIncome', 'operatingCashFlow', 'eps']
    
    for kpi in kpis:
        if kpi in df.columns:
            # QoQ: Compare to the row immediately above (previous quarter)
            df[f'{kpi}_qoq'] = df[kpi].pct_change(periods=1) * 100
            
            # YoY: Compare to 4 rows above (same quarter last year)
            # This assumes your data is consistently quarterly with no gaps
            df[f'{kpi}_yoy'] = df[kpi].pct_change(periods=4) * 100
            
    # Return to descending order (newest first) for the JSON export
    return df.sort_values('date', ascending=False)

def combine_statements(data_store):
    dfs = []
    
    for st_type, records in data_store.items():
        if not records:
            continue
        
        # Create DataFrame and Remove Duplicates (in case multiple files have same date)
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'])
        
        # Deduplicate based on date (keep the last one found if duplicates exist)
        df = df.drop_duplicates(subset=['date'], keep='last')
        
        df = df.set_index('date')
        
        # Optional: Add suffix to columns if you want to distinguish source (e.g., _inc, _bs)
        # For simplicity in visuals, we often keep original names, but be careful of overlapping keys like 'symbol'
        dfs.append(df)

    if not dfs:
        return None

    # Merge all DataFrames on 'date' index
    combined_df = dfs[0]
    for i in range(1, len(dfs)):
        combined_df = combined_df.join(dfs[i], how='outer', rsuffix=f'_{i}') 
        # Note: rsuffix handles colliding column names (like 'symbol' or 'period')

    combined_df = combined_df.sort_index(ascending=False).reset_index()
    
    # --- NEW: Calculate Growth ---
    combined_df = calculate_growth_metrics(combined_df)

    combined_df['date'] = combined_df['date'].dt.strftime('%Y-%m-%d')
    
    return combined_df

def main():
    if not DATA_DIR.exists():
        print(f"Directory {DATA_DIR} not found.")
        return

    tickers = [d for d in DATA_DIR.iterdir() if d.is_dir()]

    for ticker_path in tickers:
        print(f"\nPROCESSING TICKER: {ticker_path.name}")
        
        data_store = load_and_normalize_data(ticker_path)
        combined_df = combine_statements(data_store)
        
        if combined_df is not None and not combined_df.empty:
            output_file = OUTPUT_DIR / f"{ticker_path.name}_combined.json"
            combined_df.to_json(output_file, orient='records', indent=4)
            print(f"✅ Success! Combined data saved to {output_file}")
            print(f"   Total Rows (Quarters): {len(combined_df)}")
            print(f"   Date Range: {combined_df['date'].min()} to {combined_df['date'].max()}")
        else:
            print(f"❌ No combineable data found for {ticker_path.name}")

if __name__ == "__main__":
    main()