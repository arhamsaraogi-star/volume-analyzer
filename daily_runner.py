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
N500_JSON = CACHE_DIR / "nifty500.json"
SECTOR_MAPPING_CACHE = CACHE_DIR / "extra_sectors.json"

# Credentials
SCREENER_USER = os.getenv("SCREENER_USERNAME", "asutosh@ashikagroup.com")
SCREENER_PASS = os.getenv("SCREENER_PASSWORD", "Dilipsir@1234")

WATCHLIST_IDS = ["10259781", "10259808"]
MA_WINDOW = 20
BACKFILL_DAYS = 250 # Support 200rd DMA and 125rd Sustainability score

SCREENER_HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Referer": "https://www.screener.in/",
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

# ── Screener Watchlist Scraping ───────────────────────────────────────────────

def get_screener_session():
    session = requests.Session()
    session.headers.update(SCREENER_HDRS)
    try:
        r = session.get("https://www.screener.in/login/", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
        if not csrf_input: return None
        csrf_token = csrf_input["value"]
        payload = {"username": SCREENER_USER, "password": SCREENER_PASS, "csrfmiddlewaretoken": csrf_token, "next": "/"}
        r = session.post("https://www.screener.in/login/", data=payload, headers={"Referer": "https://www.screener.in/login/"}, timeout=15)
        if "logout" in r.text.lower() or r.status_code == 200: return session
    except: pass
    return None

def fetch_watchlists():
    session = get_screener_session()
    if not session: return {}
    all_symbols = {}
    for wid in WATCHLIST_IDS:
        url = f"https://www.screener.in/watchlist/{wid}/"
        try:
            r = session.get(url, timeout=15)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table")
            if not table: continue
            symbols = []
            for row in table.find_all("tr")[1:]:
                link = row.find("a")
                if link and "/company/" in link["href"]:
                    symbols.append(link["href"].split("/")[2].upper())
            all_symbols[wid] = symbols
        except: pass
    with open(WATCHLIST_CACHE, "w") as f: json.dump(all_symbols, f)
    return all_symbols

# ── Nifty 500 & Market Cap ───────────────────────────────────────────────────

def scrape_n500_mcap(session=None) -> dict[str, float]:
    s = session or requests.Session()
    if not session: s.headers.update(SCREENER_HDRS)
    result = {}
    for page in range(1, 11):
        url = f"https://www.screener.in/company/CNX500/?sort=market+capitalization&order=desc&limit=50&page={page}"
        try:
            r = s.get(url, timeout=15)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table")
            if not table: continue
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) < 5: continue
                link = cols[1].find("a")
                if not link: continue
                m = re.search(r"/company/([^/]+)/", link.get("href", ""))
                if not m: continue
                sym = m.group(1).strip().upper()
                try: mcap = float(cols[4].get_text(strip=True).replace(",", ""))
                except: mcap = None
                result[sym] = mcap
            time.sleep(0.3)
        except: pass
    return result

def fetch_sector_constituents(url: str) -> list[str]:
    try:
        r = requests.get(url, headers=NSE_HDRS, timeout=15)
        if r.status_code == 200:
            df = pd.read_csv(io.StringIO(r.text))
            df.columns = df.columns.str.strip()
            sym_col = next((c for c in df.columns if "symbol" in c.lower()), None)
            if sym_col:
                return df[sym_col].str.strip().str.upper().tolist()
    except Exception as e:
        log.warning(f"Failed to fetch sectoral CSV {url}: {e}")
    return []

def get_nifty500_data():
    if N500_JSON.exists():
        try:
            with open(N500_JSON) as f:
                cache = json.load(f)
                if cache.get("fetched") == date.today().isoformat(): return cache
        except: pass
    log.info("Refreshing Nifty 500 universe...")
    mcap = scrape_n500_mcap()
    
    url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
    sector = {}
    try:
        r = requests.get(url, headers=NSE_HDRS, timeout=15)
        if r.status_code == 200:
            df = pd.read_csv(io.StringIO(r.text))
            df.columns = df.columns.str.strip()
            sym_col = next((c for c in df.columns if "symbol" in c.lower()), None)
            ind_col = next((c for c in df.columns if "industry" in c.lower()), None)
            if sym_col and ind_col:
                sector = dict(zip(df[sym_col].str.strip().str.upper(), df[ind_col].str.strip()))
    except: pass
    
    symbols = sorted(set(mcap) | set(sector))
    data = {"fetched": date.today().isoformat(), "mcap": mcap, "sector": sector, "symbols": symbols}
    with open(N500_JSON, "w") as f: json.dump(data, f)
    return data

def get_extra_sector(symbol, cache, enrich_all=False):
    if symbol in cache: return cache[symbol]
    if not enrich_all: return "Other Markets"
    
    try:
        log.info(f"Fetching sector for {symbol} via nsepython...")
        info = nse_eq(symbol)
        if info and "metadata" in info and "industry" in info["metadata"]:
            ind = info["metadata"]["industry"]
            cache[symbol] = ind
            return ind
    except: pass
    return "Unknown"

# ── Metrics & Engine ──────────────────────────────────────────────────────────

def compute_advanced_metrics(cache_df: pd.DataFrame, target_date: date) -> pd.DataFrame:
    ts = pd.Timestamp(target_date)
    today_all = cache_df[cache_df["TRADE_DATE"] == ts]
    # Process ALL symbols present in today's bhavcopy
    target_symbols = today_all["SYMBOL"].unique()
    
    log.info(f"Computing sustainable analytics for FULL MARKET ({len(target_symbols)} symbols)...")
    rows = []
    
    # Pre-sort and pre-calculate columns to speed up processing
    for sym, grp in cache_df[cache_df["SYMBOL"].isin(target_symbols)].groupby("SYMBOL"):
        grp = grp.sort_values("TRADE_DATE")
        today = grp[grp["TRADE_DATE"] == ts]
        if today.empty: continue
        
        # 1. Volatility & Intensity Baselines
        grp['VOL_MA20'] = grp['TOTTRDQTY'].rolling(20).mean()
        grp['VOL_STD20'] = grp['TOTTRDQTY'].rolling(20).std()
        grp['VOL_Z'] = (grp['TOTTRDQTY'] - grp['VOL_MA20']) / grp['VOL_STD20']
        
        grp['DELIV_MA20'] = grp['DELIV_QTY'].rolling(20).mean()
        grp['DELIV_STD20'] = grp['DELIV_QTY'].rolling(20).std()
        grp['DELIV_Z'] = (grp['DELIV_QTY'] - grp['DELIV_MA20']) / grp['DELIV_STD20']
        
        # 2. Sustainability Metrics (Rollups of Intensity)
        grp['VOL_Z_5D'] = grp['VOL_Z'].rolling(5).mean()
        grp['DELIV_Z_5D'] = grp['DELIV_Z'].rolling(5).mean()
        grp['VOL_Z_125D'] = grp['VOL_Z'].rolling(125).mean()
        grp['DELIV_Z_125D'] = grp['DELIV_Z'].rolling(125).mean()
        
        idx = today.index[0]
        cur = grp.loc[idx]
        
        vol_z = float(cur['VOL_Z']) if not pd.isna(cur['VOL_Z']) else 0
        deliv_z = float(cur['DELIV_Z']) if not pd.isna(cur['DELIV_Z']) else 0
        vol_z_5d = float(cur['VOL_Z_5D']) if not pd.isna(cur['VOL_Z_5D']) else vol_z
        vol_z_125d = float(cur['VOL_Z_125D']) if not pd.isna(cur['VOL_Z_125D']) else 0
        
        close = float(cur['CLOSE'])
        dma20 = grp['CLOSE'].tail(20).mean()
        dma50 = grp['CLOSE'].tail(50).mean() if len(grp) >= 50 else dma20
        dma200 = grp['CLOSE'].tail(200).mean() if len(grp) >= 200 else dma200
        
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
    log.info(f"--- Pipeline Execution (Multi-Page): {trade_date} ---")
    
    n500 = get_nifty500_data()
    watchlists = fetch_watchlists()
    watchlist_syms = set()
    for s in watchlists.values(): watchlist_syms.update(s)
    target_enrichment_syms = set(n500["symbols"]) | watchlist_syms
    
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

    psu_banks = set(fetch_sector_constituents("https://archives.nseindia.com/content/indices/ind_niftypsubanklist.csv"))
    pvt_banks = set(fetch_sector_constituents("https://archives.nseindia.com/content/indices/ind_niftyprivatebanklist.csv"))
    
    today = cache[cache["TRADE_DATE"] == ts].copy()
    
    extra_cache = {}
    if SECTOR_MAPPING_CACHE.exists():
        with open(SECTOR_MAPPING_CACHE) as f: extra_cache = json.load(f)
    
    sectors = n500["sector"]
    def resolve_sector(s):
        if s in psu_banks: return "Public Sector Bank"
        if s in pvt_banks: return "Private Sector Bank"
        if s in sectors: return sectors[s]
        return get_extra_sector(s, extra_cache, enrich_all=(s in target_enrichment_syms))
    
    today["SECTOR"] = today["SYMBOL"].apply(resolve_sector)
    with open(SECTOR_MAPPING_CACHE, "w") as f: json.dump(extra_cache, f)
    
    today["IN_N500"] = today["SYMBOL"].isin(n500["symbols"])
    today["MARKET_CAP_CR"] = today["SYMBOL"].map(n500["mcap"])
    
    metrics_df = compute_advanced_metrics(cache, trade_date)
    today = today.merge(metrics_df, on="SYMBOL", how="left")
    
    sector_stats = {}
    for sec, grp in today.groupby("SECTOR"):
        sector_stats[sec] = {
            "avg_vol_z": float(grp["VOL_Z"].mean()),
            "avg_vol_z_5d": float(grp["VOL_Z_5D"].mean()),
            "avg_vol_z_125d": float(grp["VOL_Z_125D"].mean()),
            "avg_conv": float(grp["CONVICTION_SCORE"].mean()),
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
    
    quads = {
        "Q1": df[(df["RETURN_PCT"] > 0) & df["ABOVE_MA"] & df["IN_N500"]].to_dict(orient="records"),
        "Q2": df[(df["RETURN_PCT"] < 0) & df["ABOVE_MA"] & df["IN_N500"]].to_dict(orient="records"),
        "Q3": df[(df["RETURN_PCT"] > 0) & ~df["ABOVE_MA"] & df["IN_N500"]].to_dict(orient="records"),
        "Q4": df[(df["RETURN_PCT"] < 0) & ~df["ABOVE_MA"] & df["IN_N500"]].to_dict(orient="records"),
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
