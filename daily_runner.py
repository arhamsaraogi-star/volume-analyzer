import os
import sys
import json
import time
import re
import io
import logging
from pathlib import Path
from datetime import datetime, timedelta, date

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from nsepython import get_bhavcopy, nse_eq

# ── Config ────────────────────────────────────────────────────────────────────
DIR = Path(__file__).parent.absolute()
CACHE_DIR = DIR / "screener_cache"
BHAV_PARQUET = CACHE_DIR / "bhavcopy_history.parquet"
WATCHLIST_CACHE = CACHE_DIR / "watchlists.json"
BSE2000_JSON = CACHE_DIR / "bse2000_universe.json"
EXCEL_SOURCE = DIR / "6_0_BSE_1000_Sector_Allocation___Results_Schedule.xlsx"

# Credentials for watchlists (if still used)
SCREENER_USER = os.getenv("SCREENER_USERNAME", "asutosh@ashikagroup.com")
SCREENER_PASS = os.getenv("SCREENER_PASSWORD", "Dilipsir@1234")

WATCHLIST_IDS = ["10259781", "10259808"]
MA_WINDOW = 20
BACKFILL_DAYS = 250

SCREENER_HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

NSE_HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://www.nseindia.com/",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── Setup ─────────────────────────────────────────────────────────────────────

def ensure_dirs():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ── BSE 2000 Universe Loader ──────────────────────────────────────────────────

def load_bse2000_universe():
    """
    Parses the 6.0 Excel file (BSE2000 sheet) for Industry Subgroups and NSE Symbols.
    """
    if not EXCEL_SOURCE.exists():
        log.error(f"Excel source {EXCEL_SOURCE} not found!")
        return {"sector": {}, "symbols": []}

    log.info(f"Parsing BSE 2000 Universe from {EXCEL_SOURCE.name}...")
    try:
        # User confirmed: Col J (index 9) = Subgroup, Col L (index 11) = NSE Symbol
        # Start reading from row 3 (skip header noise)
        df = pd.read_excel(EXCEL_SOURCE, sheet_name='BSE2000', header=None, skiprows=2)
        
        # Mapping: {Symbol: Subgroup}
        # Symbols are in index 11, Subgroups in index 9
        mapping = {}
        valid_symbols = []
        
        for _, row in df.iterrows():
            sym = str(row[11]).strip().upper()
            subgroup = str(row[9]).strip()
            
            if sym and sym != 'NAN' and subgroup and subgroup != 'NAN':
                mapping[sym] = subgroup
                valid_symbols.append(sym)
                
        data = {
            "fetched": date.today().isoformat(),
            "sector": mapping,
            "symbols": valid_symbols
        }
        
        with open(BSE2000_JSON, "w") as f:
            json.dump(data, f)
            
        log.info(f"BSE 2000 Universe loaded: {len(valid_symbols)} companies mapped.")
        return data
    except Exception as e:
        log.error(f"Failed to parse Excel: {e}")
        return {"sector": {}, "symbols": []}

# ── Screener Watchlist Scraping (Optional Overlay) ────────────────────────────

def fetch_watchlists():
    # If user wants watchlists as well, we keep them, but BSE2000 is the primary universe now
    if not SCREENER_USER or not SCREENER_PASS: return {}
    # (Existing logic omitted for brevity in snippet, assume it works or return {})
    return {}

# ── Metrics & Engine ──────────────────────────────────────────────────────────

def compute_advanced_metrics(cache_df: pd.DataFrame, target_date: date, target_symbols: list) -> pd.DataFrame:
    ts = pd.Timestamp(target_date)
    log.info(f"Computing sustainable analytics for {len(target_symbols)} universe symbols...")
    rows = []
    
    # Pre-filter for the universe to optimize
    universe_df = cache_df[cache_df["SYMBOL"].isin(target_symbols)]
    
    for sym, grp in universe_df.groupby("SYMBOL"):
        grp = grp.sort_values("TRADE_DATE")
        today = grp[grp["TRADE_DATE"] == ts]
        if today.empty: continue
        
        grp['VOL_MA20'] = grp['TOTTRDQTY'].rolling(20).mean()
        grp['VOL_STD20'] = grp['TOTTRDQTY'].rolling(20).std()
        grp['VOL_Z'] = (grp['TOTTRDQTY'] - grp['VOL_MA20']) / grp['VOL_STD20']
        
        grp['DELIV_MA20'] = grp['DELIV_QTY'].rolling(20).mean()
        grp['DELIV_STD20'] = grp['DELIV_QTY'].rolling(20).std()
        grp['DELIV_Z'] = (grp['DELIV_QTY'] - grp['DELIV_MA20']) / grp['DELIV_STD20']
        
        grp['VOL_Z_5D'] = grp['VOL_Z'].rolling(5).mean()
        grp['VOL_Z_125D'] = grp['VOL_Z'].rolling(125).mean()
        
        cur = grp.loc[today.index[0]]
        vol_z = float(cur['VOL_Z']) if not pd.isna(cur['VOL_Z']) else 0
        deliv_z = float(cur['DELIV_Z']) if not pd.isna(cur['DELIV_Z']) else 0
        vol_z_5d = float(cur['VOL_Z_5D']) if not pd.isna(cur['VOL_Z_5D']) else vol_z
        vol_z_125d = float(cur['VOL_Z_125D']) if not pd.isna(cur['VOL_Z_125D']) else 0
        
        close = float(cur['CLOSE'])
        dma20 = grp['CLOSE'].tail(20).mean()
        dma50 = grp['CLOSE'].tail(50).mean() if len(grp) >= 50 else dma20
        dma200 = grp['CLOSE'].tail(200).mean() if len(grp) >= 200 else dma20
        
        vz_s = min(max(vol_z * 20, 0), 100)
        dz_s = min(max(deliv_z * 20, 0), 100)
        price_trend = 100 if (close > dma20 and cur['RETURN_PCT'] > 0) else 50
        conv_score = (vz_s * 0.4) + (dz_s * 0.4) + (price_trend * 0.2)
        
        rows.append({
            "SYMBOL": sym,
            "DMA50": float(dma50),
            "DMA200": float(dma200),
            "VOL_Z": vol_z,
            "DELIV_Z": deliv_z,
            "VOL_Z_5D": vol_z_5d,
            "VOL_Z_125D": vol_z_125d,
            "SUSTAINABLE_SCORE": vol_z_5d,
            "DELIV_MA20": float(cur['DELIV_MA20']),
            "HIGH52": float(grp['CLOSE'].tail(250).max()),
            "CONVICTION_SCORE": float(conv_score)
        })
    return pd.DataFrame(rows)

# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_pipeline(trade_date: date):
    ensure_dirs()
    log.info(f"--- Pipeline Execution (BSE 2000 Universe): {trade_date} ---")
    
    # 1. Load Universal Classifications and Symbols
    universe = load_bse2000_universe()
    target_symbols = universe["symbols"]
    sector_map = universe["sector"]
    
    if not BHAV_PARQUET.exists():
        log.error("Historical data missing.")
        return False
        
    cache = pd.read_parquet(BHAV_PARQUET)
    ts = pd.Timestamp(trade_date)
    
    if trade_date not in pd.to_datetime(cache["TRADE_DATE"]).dt.date.unique():
        from backfill_history import fetch_one_bhav
        df_today = fetch_one_bhav(trade_date)
        if df_today is not None:
            cache = pd.concat([cache, df_today], ignore_index=True).drop_duplicates(["SYMBOL", "TRADE_DATE"])
            cache.to_parquet(BHAV_PARQUET, index=False)
        else: return False

    # 2. Advanced Metrics for Universe
    metrics_df = compute_advanced_metrics(cache, trade_date, target_symbols)
    
    today = cache[cache["TRADE_DATE"] == ts].copy()
    today = today[today["SYMBOL"].isin(target_symbols)]
    today = today.merge(metrics_df, on="SYMBOL", how="inner")
    
    # 3. Enrichment
    today["SECTOR"] = today["SYMBOL"].map(sector_map).fillna("Unknown")
    
    # 4. Sector Aggregation
    sector_stats = {}
    for sec, grp in today.groupby("SECTOR"):
        sector_stats[sec] = {
            "avg_vol_z": float(grp["VOL_Z"].mean()),
            "avg_vol_z_5d": float(grp["VOL_Z_5D"].mean()),
            "avg_vol_z_125d": float(grp["VOL_Z_125D"].mean()),
            "avg_conv": float(grp["CONVICTION_SCORE"].mean()),
            "avg_return": float(grp["RETURN_PCT"].mean()),
            "count": int(len(grp))

        }
    
    generate_dashboards(trade_date, today, sector_stats)
    return True

def generate_dashboards(trade_date: date, df: pd.DataFrame, sector_stats: dict):
    df = df.copy()
    if "TRADE_DATE" in df.columns:
        df["TRADE_DATE"] = df["TRADE_DATE"].dt.strftime("%Y-%m-%d")
    
    df["ABOVE_MA"] = df["DELIV_QTY"] > df["DELIV_MA20"]
    df["DELIV_VS_MA_PCT"] = ((df["DELIV_QTY"] - df["DELIV_MA20"]) / df["DELIV_MA20"] * 100).round(1).fillna(0)
    
    # Dashboard Quadrants (Restricted to Universe)
    quads = {
        "Q1": df[(df["RETURN_PCT"] > 0) & df["ABOVE_MA"]].to_dict(orient="records"),
        "Q2": df[(df["RETURN_PCT"] < 0) & df["ABOVE_MA"]].to_dict(orient="records"),
        "Q3": df[(df["RETURN_PCT"] > 0) & ~df["ABOVE_MA"]].to_dict(orient="records"),
        "Q4": df[(df["RETURN_PCT"] < 0) & ~df["ABOVE_MA"]].to_dict(orient="records"),
    }
    
    payload = {
        "updated_at": datetime.now().strftime("%d %b %Y, %H:%M"),
        "trade_date": trade_date.strftime("%d %b %Y"),
        "quadrants": {q: len(v) for q, v in quads.items()},
        "records": df.to_dict(orient="records"),
        "sectors": sector_stats
    }
    
    render("template.html", "index.html", payload)
    render("analytics_template.html", "analytics.html", payload)
    render("sector_template.html", "sector.html", payload)
    log.info("Dashboards generated.")

def render(template_name, output_name, data):
    tp = DIR / template_name
    if not tp.exists(): return
    with open(tp, "r", encoding="utf-8") as f: html = f.read()
    html = html.replace("__DATA__", json.dumps(data))
    with open(DIR / output_name, "w", encoding="utf-8") as f: f.write(html)

if __name__ == "__main__":
    t_date = date.today()
    if t_date.weekday() >= 5: t_date -= timedelta(days=t_date.weekday() - 4)
    if not run_pipeline(t_date): run_pipeline(t_date - timedelta(days=1))
