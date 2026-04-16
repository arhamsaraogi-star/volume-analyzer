import os
import sys
import json
import time
import re
import io
import logging
import threading
import warnings
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, date

import pandas as pd
import requests
from bs4 import BeautifulSoup
from nsepython import get_bhavcopy

# ── Config ────────────────────────────────────────────────────────────────────
DIR = Path(__file__).parent.absolute()
CACHE_DIR = DIR / "screener_cache"
PUBLIC_SITE_DIR = DIR / "public_site"
BHAV_PARQUET = CACHE_DIR / "bhavcopy_history.parquet"
WATCHLIST_CACHE = CACHE_DIR / "watchlists.json"
N500_JSON = CACHE_DIR / "nifty500.json"
HIST_CSV = CACHE_DIR / "quadrant_history.csv"

# Credentials
SCREENER_USER = "asutosh@ashikagroup.com"
SCREENER_PASS = "Dilipsir@1234"

# Watchlists
WATCHLIST_IDS = ["10259781", "10259808"]

MA_WINDOW = 20
BACKFILL_DAYS = 30
SPIKE_THRESH = 200.0

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
    PUBLIC_SITE_DIR.mkdir(parents=True, exist_ok=True)

# ── Screener Watchlist Scraping ───────────────────────────────────────────────

def get_screener_session():
    session = requests.Session()
    session.headers.update(SCREENER_HDRS)
    
    # Get CSRF
    try:
        r = session.get("https://www.screener.in/login/", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
        if not csrf_input:
            log.error("Failed to find CSRF token on Screener")
            return None
        
        csrf_token = csrf_input["value"]
        
        # Login
        payload = {
            "username": SCREENER_USER,
            "password": SCREENER_PASS,
            "csrfmiddlewaretoken": csrf_token,
            "next": "/"
        }
        r = session.post("https://www.screener.in/login/", data=payload, headers={"Referer": "https://www.screener.in/login/"}, timeout=15)
        
        if "logout" in r.text.lower() or r.status_code == 200:
            log.info("Screener login successful")
            return session
        else:
            log.error("Screener login failed")
            return None
    except Exception as e:
        log.error(f"Screener session creation failed: {e}")
        return None

def fetch_watchlists():
    """Fetches symbols from the configured watchlists."""
    session = get_screener_session()
    if not session:
        return {}

    all_symbols = {}
    for wid in WATCHLIST_IDS:
        url = f"https://www.screener.in/watchlist/{wid}/"
        try:
            r = session.get(url, timeout=15)
            if r.status_code != 200:
                log.warning(f"Failed to fetch watchlist {wid}: HTTP {r.status_code}")
                continue
            
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table")
            if not table:
                log.warning(f"No table found in watchlist {wid}")
                continue
            
            symbols = []
            for row in table.find_all("tr")[1:]:
                link = row.find("a")
                if link and "/company/" in link["href"]:
                    sym = link["href"].split("/")[2].upper()
                    symbols.append(sym)
            
            all_symbols[wid] = symbols
            log.info(f"Fetched {len(symbols)} symbols from watchlist {wid}")
        except Exception as e:
            log.error(f"Error fetching watchlist {wid}: {e}")
    
    # Save to cache
    with open(WATCHLIST_CACHE, "w") as f:
        json.dump(all_symbols, f)
    
    return all_symbols

# ── Nifty 500 & Market Cap ───────────────────────────────────────────────────

def scrape_n500_mcap(session=None) -> dict[str, float]:
    """10 requests to screener for N500 market caps."""
    s = session or requests.Session()
    if not session:
        s.headers.update(SCREENER_HDRS)
    
    result = {}
    for page in range(1, 11):
        url = (f"https://www.screener.in/company/CNX500/"
               f"?sort=market+capitalization&order=desc&limit=50&page={page}")
        try:
            r = s.get(url, timeout=15)
            if r.status_code != 200:
                log.warning(f"  MCap page {page}: HTTP {r.status_code}")
                continue
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
                try:
                    mcap = float(cols[4].get_text(strip=True).replace(",", ""))
                except ValueError:
                    mcap = None
                result[sym] = mcap
            log.info(f"  N500 MCap page {page}/10: {len(result)} symbols")
            time.sleep(0.3)
        except Exception as e:
            log.warning(f"  N500 MCap page {page} failed: {e}")
    return result

def fetch_n500_sectors() -> dict[str, str]:
    url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
    try:
        r = requests.get(url, headers=NSE_HDRS, timeout=15)
        if r.status_code == 200:
            df = pd.read_csv(io.StringIO(r.text))
            df.columns = df.columns.str.strip()
            sym_col = next((c for c in df.columns if "symbol" in c.lower()), None)
            ind_col = next((c for c in df.columns if "industry" in c.lower()), None)
            if sym_col and ind_col:
                return dict(zip(df[sym_col].str.strip().str.upper(), df[ind_col].str.strip()))
    except Exception as e:
        log.warning(f"NSE sector CSV fetch failed: {e}")
    return {}

def get_nifty500_data():
    if N500_JSON.exists():
        try:
            with open(N500_JSON) as f:
                cache = json.load(f)
                # Check for all required keys
                if (cache.get("fetched") == date.today().isoformat() and 
                    all(k in cache for k in ["mcap", "sector", "symbols"])):
                    log.info(f"N500 cache hit: {len(cache['symbols'])} symbols")
                    return cache
        except Exception as e:
            log.warning(f"Failed to load N500 cache: {e}")
    
    log.info("Refreshing Nifty 500 universe (MCap + Sectors)...")
    mcap = scrape_n500_mcap()
    sector = fetch_n500_sectors()
    symbols = sorted(set(mcap) | set(sector))
    
    data = {"fetched": date.today().isoformat(), "mcap": mcap, "sector": sector, "symbols": symbols}
    with open(N500_JSON, "w") as f:
        json.dump(data, f)
    return data


# ── Date helpers ──────────────────────────────────────────────────────────────

def last_trading_day(ref=None):
    d = ref or date.today()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d

def prev_n_trading_days(end: date, n: int) -> list[date]:
    days, d = [], end - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return list(reversed(days))

# ── Bhavcopy ──────────────────────────────────────────────────────────────────

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

def fetch_one_bhav(d: date, retries=3) -> pd.DataFrame | None:
    date_str = d.strftime("%d-%m-%Y")
    for attempt in range(retries):
        try:
            raw = get_bhavcopy(date_str)
            if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
                return None
            normed = normalise(raw, d)
            if not normed.empty:
                log.info(f"  {d}: {len(normed)} EQ rows")
                return normed
        except Exception as e:
            log.warning(f"  {d} attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(2)
    return None

def update_bhav_cache(target: date) -> pd.DataFrame:
    cache = pd.DataFrame()
    if BHAV_PARQUET.exists():
        try:
            cache = pd.read_parquet(BHAV_PARQUET)
        except:
            pass
    
    cached: set[date] = set()
    if not cache.empty and "TRADE_DATE" in cache.columns:
        cached = {pd.Timestamp(t).date() for t in cache["TRADE_DATE"].unique()}

    needed = prev_n_trading_days(target, BACKFILL_DAYS) + [target]
    missing = [d for d in needed if d not in cached]

    if not missing:
        log.info(f"Bhav cache complete — {len(needed)} days present")
        return cache

    log.info(f"Fetching {len(missing)} missing bhavcopies …")
    frames = []
    for d in missing:
        df = fetch_one_bhav(d)
        if df is not None:
            frames.append(df)
        time.sleep(0.5)

    if frames:
        new_data = pd.concat(frames, ignore_index=True)
        cache = pd.concat([cache, new_data], ignore_index=True) if not cache.empty else new_data
        cache = cache.drop_duplicates(subset=["SYMBOL", "TRADE_DATE"], keep="last")
        cache = cache.sort_values(["TRADE_DATE", "SYMBOL"]).reset_index(drop=True)
        cache.to_parquet(BHAV_PARQUET, index=False)
    return cache

# ── Logic ─────────────────────────────────────────────────────────────────────

def build_symbol_map(universe_symbols: list[str], bhav_symbols: set[str]) -> dict[str, str]:
    mapping = {}
    for sym in universe_symbols:
        if sym in bhav_symbols:
            mapping[sym] = sym
        else:
            alt = sym.replace("&", "").replace("-", "")
            if alt in bhav_symbols:
                mapping[sym] = alt
    return mapping

def compute_ma(cache_df: pd.DataFrame, target: date) -> pd.DataFrame:
    ts = pd.Timestamp(target)
    hist = cache_df[cache_df["TRADE_DATE"] < ts]
    rows = []
    for sym, grp in hist.groupby("SYMBOL"):
        vals = grp.sort_values("TRADE_DATE").tail(MA_WINDOW)["DELIV_QTY"].dropna()
        if len(vals) >= 3:
            rows.append({"SYMBOL": sym, "DELIV_MA20": float(vals.mean())})
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["SYMBOL", "DELIV_MA20"])

def classify(df: pd.DataFrame) -> dict:
    df = df.copy()
    df["ABOVE_MA"] = df["DELIV_QTY"] > df["DELIV_MA20"]
    df["DELIV_VS_MA_PCT"] = ((df["DELIV_QTY"] - df["DELIV_MA20"]) / df["DELIV_MA20"] * 100).round(1)
    
    masks = {
        "Q1": (df["RETURN_PCT"] > 0) & df["ABOVE_MA"],
        "Q2": (df["RETURN_PCT"] < 0) & df["ABOVE_MA"],
        "Q3": (df["RETURN_PCT"] > 0) & ~df["ABOVE_MA"],
        "Q4": (df["RETURN_PCT"] < 0) & ~df["ABOVE_MA"],
    }
    
    results = {}
    for q, mask in masks.items():
        sub = df[mask].copy()
        results[q] = sub.to_dict(orient="records")
    return results

# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_pipeline(trade_date: date) -> bool:
    ensure_dirs()
    log.info(f"Starting Volume Analyzer Pipeline for {trade_date}")
    
    # 1. Fetch Universe (N500 + Watchlists)
    n500 = get_nifty500_data()
    watchlists = fetch_watchlists()
    
    watchlist_syms = set()
    for syms in watchlists.values():
        watchlist_syms.update(syms)
    
    # Universal set
    all_target_syms = sorted(set(n500["symbols"]) | watchlist_syms)
    log.info(f"Total target universe: {len(all_target_syms)} symbols")
    
    # 2. Update Bhavcopy Cache
    cache = update_bhav_cache(trade_date)
    if cache.empty:
        log.error("Failed to load bhavcopy data")
        return False
    
    # 3. Today's Slice
    ts = pd.Timestamp(trade_date)
    today = cache[cache["TRADE_DATE"] == ts].copy()
    if today.empty:
        log.warning(f"No data for {trade_date} (may not be out yet)")
        return False
    
    # 4. Filter & Enrichment
    bhav_syms = set(today["SYMBOL"])
    sym_map = build_symbol_map(all_target_syms, bhav_syms)
    rev_map = {v: k for k, v in sym_map.items()}
    
    today = today[today["SYMBOL"].isin(sym_map.values())].copy()
    today["TARGET_SYM"] = today["SYMBOL"].map(rev_map).fillna(today["SYMBOL"])
    today["MARKET_CAP_CR"] = today["TARGET_SYM"].map(n500["mcap"])
    today["SECTOR"] = today["TARGET_SYM"].map(n500["sector"]).fillna("Unknown")
    
    # Tagging
    today["IN_N500"] = today["TARGET_SYM"].isin(n500["symbols"])
    today["IN_WATCHLIST"] = today["TARGET_SYM"].isin(watchlist_syms)
    
    # 5. Metrics
    ma_df = compute_ma(cache, trade_date)
    today = today.merge(ma_df, on="SYMBOL", how="left")
    today["DELIV_MA20"] = today["DELIV_MA20"].fillna(today["DELIV_QTY"])
    today["DELIV_VS_MA_PCT"] = ((today["DELIV_QTY"] - today["DELIV_MA20"]) / today["DELIV_MA20"] * 100).round(1)

    # 6. Classify
    quadrants = classify(today)
    
    # 7. Generate Dashboard
    generate_dashboard(trade_date, quadrants, today)
    return True


def generate_dashboard(trade_date: date, quadrants: dict, all_data: pd.DataFrame):
    log.info("Generating Final Dashboard...")
    
    # Prepare records for the table
    records = all_data.drop(columns=["TRADE_DATE", "TARGET_SYM"]).to_dict(orient="records")
    
    data_payload = {
        "updated_at": datetime.now().strftime("%d %b %Y, %H:%M"),
        "trade_date": trade_date.strftime("%d %b %Y"),
        "quadrants": {q: len(v) for q, v in quadrants.items()},
        "records": records
    }
    
    # Load template
    template_path = PUBLIC_SITE_DIR / "index.html"
    if not template_path.exists():
        log.error(f"Template not found at {template_path}")
        return
        
    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()
    
    # Inject data
    json_data = json.dumps(data_payload)
    html = html.replace("__DATA__", json_data)
    
    # Save final index.html (overwrite)
    with open(template_path, "w", encoding="utf-8") as f:
        f.write(html)
    
    log.info(f"Pipeline successful. Dashboard updated: {template_path}")


if __name__ == "__main__":
    t_date = last_trading_day()
    
    if not run_pipeline(t_date):
        log.info("Pipeline failed for latest day. Falling back to previous trading day...")
        prev_date = prev_n_trading_days(t_date, 1)[0]
        run_pipeline(prev_date)



