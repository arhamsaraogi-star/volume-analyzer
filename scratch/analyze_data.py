import pandas as pd
from pathlib import Path

def analyze_cache():
    p = Path('screener_cache/bhavcopy_history.parquet')
    if not p.exists():
        print("Cache not found.")
        return
    
    df = pd.read_parquet(p)
    if df.empty:
        print("Cache is empty.")
        return
    
    print(f"Total Rows: {len(df)}")
    print(f"Unique Symbols: {df['SYMBOL'].nunique()}")
    print(f"Unique Dates: {df['TRADE_DATE'].nunique()}")
    print(f"Date Range: {df['TRADE_DATE'].min()} to {df['TRADE_DATE'].max()}")
    
    # Check for sectors
    n500_path = Path('screener_cache/nifty500.json')
    if n500_path.exists():
        import json
        with open(n500_path) as f:
            n500 = json.load(f)
            print(f"N500 Sectors available: {len(n500.get('sector', {}))}")
    else:
        print("N500 metadata not found.")

if __name__ == "__main__":
    analyze_cache()
