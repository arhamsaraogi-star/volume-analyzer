import os
import time
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
import pandas as pd
from nsepython import get_bhavcopy

# ── Config ────────────────────────────────────────────────────────────────────
DIR = Path(__file__).parent.absolute()
CACHE_DIR = DIR / "screener_cache"
BHAV_PARQUET = CACHE_DIR / "bhavcopy_history.parquet"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

COL_MAP = {
    "SYMBOL": "SYMBOL", "SERIES": "SERIES", "DATE1": "DATE",
    "PREV_CLOSE": "PREV_CLOSE", "OPEN_PRICE": "OPEN", "HIGH_PRICE": "HIGH",
    "LOW_PRICE": "LOW", "LAST_PRICE": "LAST", "CLOSE_PRICE": "CLOSE",
    "AVG_PRICE": "AVG_PRICE", "TTL_TRD_QNTY": "TOTTRDQTY",
    "TURNOVER_LACS": "TURNOVER_LACS", "NO_OF_TRADES": "TRADES",
    "DELIV_QTY": "DELIV_QTY", "DELIV_PER": "DELIV_PER",
}

def normalise(raw: pd.DataFrame, trade_date: date) -> pd.DataFrame:
    df = raw.copy()
    if isinstance(df, list): return pd.DataFrame() # Some nsepython errors
    df.columns = df.columns.str.strip()
    df = df.rename(columns=COL_MAP)
    if "SERIES" not in df.columns or "SYMBOL" not in df.columns:
        return pd.DataFrame()
    df = df[df["SERIES"].str.strip() == "EQ"].copy()
    df["SYMBOL"] = df["SYMBOL"].str.strip()
    df["TRADE_DATE"] = pd.Timestamp(trade_date)
    for c in ["CLOSE", "PREV_CLOSE", "OPEN", "HIGH", "LOW",
              "TOTTRDQTY", "DELIV_QTY", "DELIV_PER", "TURNOVER_LACS"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["RETURN_PCT"] = ((df["CLOSE"] - df["PREV_CLOSE"]) / df["PREV_CLOSE"] * 100).round(4)
    return df.reset_index(drop=True)

def fetch_one_bhav(d: date, retries=2) -> pd.DataFrame | None:
    date_str = d.strftime("%d-%m-%Y")
    for attempt in range(retries):
        try:
            raw = get_bhavcopy(date_str)
            if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty) or isinstance(raw, list):
                return None
            normed = normalise(raw, d)
            if not normed.empty:
                return normed
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
    return None

def get_trading_days(start_date: date, end_date: date) -> list[date]:
    days = []
    curr = start_date
    while curr <= end_date:
        if curr.weekday() < 5: # Mon-Fri
            days.append(curr)
        curr += timedelta(days=1)
    return days

def backfill(days_to_fetch=250):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    end_date = date.today()
    start_date = end_date - timedelta(days=int(days_to_fetch * 1.5)) # Buffer for weekends/holidays
    
    target_days = get_trading_days(start_date, end_date)
    log.info(f"Targeting {len(target_days)} potential trading days...")
    
    cache = pd.DataFrame()
    if BHAV_PARQUET.exists():
        cache = pd.read_parquet(BHAV_PARQUET)
        cached_dates = set(pd.to_datetime(cache["TRADE_DATE"]).dt.date.unique())
        log.info(f"Found {len(cached_dates)} days already in cache.")
        target_days = [d for d in target_days if d not in cached_dates]
    
    if not target_days:
        log.info("Cache is already up to date.")
        return

    log.info(f"Fetching {len(target_days)} missing days...")
    
    success_count = 0
    frames = []
    
    for i, d in enumerate(target_days):
        df = fetch_one_bhav(d)
        if df is not None:
            frames.append(df)
            success_count += 1
            log.info(f"[{i+1}/{len(target_days)}] Success: {d}")
        else:
            log.info(f"[{i+1}/{len(target_days)}] No data (maybe holiday): {d}")
        
        # Save incrementally every 20 days to avoid data loss
        if (i+1) % 20 == 0 and frames:
            temp_df = pd.concat(frames, ignore_index=True)
            if not cache.empty:
                cache = pd.concat([cache, temp_df], ignore_index=True)
            else:
                cache = temp_df
            cache = cache.drop_duplicates(subset=["SYMBOL", "TRADE_DATE"]).sort_values("TRADE_DATE")
            cache.to_parquet(BHAV_PARQUET, index=False)
            frames = []
            log.info(f"--- Checkpoint saved. Cache now has {cache['TRADE_DATE'].nunique()} days. ---")
        
        time.sleep(0.3) # Avoid hitting NSE too hard

    if frames:
        temp_df = pd.concat(frames, ignore_index=True)
        if not cache.empty:
            cache = pd.concat([cache, temp_df], ignore_index=True)
        else:
            cache = temp_df
        cache = cache.drop_duplicates(subset=["SYMBOL", "TRADE_DATE"]).sort_values("TRADE_DATE")
        cache.to_parquet(BHAV_PARQUET, index=False)
    
    log.info(f"Backfill complete. Total unique days in cache: {cache['TRADE_DATE'].nunique()}")

if __name__ == "__main__":
    backfill(300) # Aim for 300 to be safe for 200rd DMA
