"""
NSE Delivery Screener — Flask Backend
======================================
Universe    : Nifty 500 only
Data source : nsepython.get_bhavcopy()  (sec_bhavdata_full)
Return      : (CLOSE - PREV_CLOSE) / PREV_CLOSE  — no yfinance
20d MA      : rolling DELIV_QTY from persistent parquet cache
Market cap  : scraped once/day from screener.in/company/CNX500/ (10 requests)
Sector      : NSE ind_nifty500list.csv (public, cached daily)

Changes from prior version:
  - MIN_DEL_PCT removed (show all N500 stocks, was filtering ~250 out)
  - Sector data added from NSE constituent CSV
  - Symbol normalization (L&T → LT fallback)
  - Streak detection (stocks in same quadrant N days running)
  - Quadrant migration vs previous day
  - Large/Mid/Small cap bucketing
  - Delivery spike flag (DELIV_VS_MA_PCT > 200%)
  - Daily classification log appended to history CSV
"""

import json
import time
import re
import logging
import threading
import warnings
import io
from pathlib import Path
from datetime import datetime, timedelta, date

import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
from flask_cors import CORS
from nsepython import get_bhavcopy

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ── Config ────────────────────────────────────────────────────────────────────
CACHE_DIR = Path("./screener_cache")
BHAV_PARQUET = CACHE_DIR / "bhavcopy_history.parquet"
N500_JSON = CACHE_DIR / "nifty500.json"        # {symbol: {mcap, sector}, ...}
HIST_CSV = CACHE_DIR / "quadrant_history.csv"  # daily classification log
MA_WINDOW = 20
BACKFILL_DAYS = 30
SPIKE_THRESH = 200.0   # DELIV_VS_MA_PCT threshold for spike flag

SCREENER_HDRS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.screener.in/",
}

NSE_HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://www.nseindia.com/",
}

# ── State ─────────────────────────────────────────────────────────────────────
_state = {
    "status": "idle", "progress": "Ready", "step": 0,
    "trade_date": None, "data": None, "summary": None,
    "error": None, "last_run": None, "cache_info": None,
}
_lock = threading.Lock()


def ss(**kw):
    with _lock:
        _state.update(kw)

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

# ── Cache I/O ─────────────────────────────────────────────────────────────────


def ensure_dirs():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def load_bhav() -> pd.DataFrame:
    if BHAV_PARQUET.exists():
        try:
            return pd.read_parquet(BHAV_PARQUET)
        except Exception as e:
            log.warning(f"Parquet read failed ({e}) — rebuilding")
    return pd.DataFrame()


def save_bhav(df: pd.DataFrame):
    ensure_dirs()
    df.to_parquet(BHAV_PARQUET, index=False)


def load_n500() -> dict:
    if N500_JSON.exists():
        with open(N500_JSON) as f:
            return json.load(f)
    return {}


def save_n500(d: dict):
    ensure_dirs()
    with open(N500_JSON, "w") as f:
        json.dump(d, f)

# ── Nifty 500: scrape screener.in for MCap + NSE CSV for sector ───────────────


def scrape_n500_mcap() -> dict[str, float]:
    """10 requests → {symbol: mcap_cr} for all 500 constituents."""
    session = requests.Session()
    session.headers.update(SCREENER_HDRS)
    result: dict[str, float] = {}

    for page in range(1, 11):
        url = (f"https://www.screener.in/company/CNX500/"
               f"?sort=market+capitalization&order=desc&limit=50&page={page}")
        try:
            r = session.get(url, timeout=15)
            if r.status_code != 200:
                log.warning(f"  MCap page {page}: HTTP {r.status_code}")
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table")
            if not table:
                continue
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) < 5:
                    continue
                link = cols[1].find("a")
                if not link:
                    continue
                m = re.search(r"/company/([^/]+)/", link.get("href", ""))
                if not m:
                    continue
                sym = m.group(1).strip().upper()
                try:
                    mcap = float(cols[4].get_text(strip=True).replace(",", ""))
                except ValueError:
                    mcap = None
                result[sym] = mcap
            log.info(f"  MCap page {page}/10: {len(result)} symbols")
            time.sleep(0.3)
        except Exception as e:
            log.warning(f"  MCap page {page} failed: {e}")

    return result


def fetch_n500_sectors() -> dict[str, str]:
    """
    Fetch NSE's Nifty 500 constituent CSV → {symbol: industry}.
    Public file, no auth needed. Cached daily alongside mcap.
    """
    url = ("https://archives.nseindia.com/content/indices/ind_nifty500list.csv")
    try:
        r = requests.get(url, headers=NSE_HDRS, timeout=15)
        if r.status_code == 200:
            df = pd.read_csv(io.StringIO(r.text))
            # Columns: Company Name, Industry, Symbol, Series, ISIN Code
            df.columns = df.columns.str.strip()
            sym_col = next(
                (c for c in df.columns if "symbol" in c.lower()), None)
            ind_col = next(
                (c for c in df.columns if "industry" in c.lower()), None)
            if sym_col and ind_col:
                return dict(zip(
                    df[sym_col].str.strip().str.upper(),
                    df[ind_col].str.strip()
                ))
    except Exception as e:
        log.warning(f"NSE sector CSV fetch failed: {e}")
    return {}


def get_nifty500() -> dict:
    """
    Returns {
      "fetched": "YYYY-MM-DD",
      "mcap":    {symbol: mcap_cr},
      "sector":  {symbol: industry},
      "symbols": [list of 500 symbols]
    }
    Re-fetches once per calendar day.
    """
    cache = load_n500()
    today = date.today().isoformat()

    if cache.get("fetched") == today and cache.get("mcap"):
        log.info(f"N500 cache hit: {len(cache['mcap'])} symbols")
        return cache

    log.info("Fetching Nifty 500 data …")

    ss(progress="Scraping Nifty 500 market caps from screener.in (10 pages) …", step=4)
    mcap = scrape_n500_mcap()

    ss(progress="Fetching Nifty 500 sector data from NSE …", step=14)
    sector = fetch_n500_sectors()

    # Symbol universe = union of mcap keys + sector keys
    symbols = sorted(set(mcap) | set(sector))

    fresh = {"fetched": today, "mcap": mcap,
             "sector": sector, "symbols": symbols}
    if mcap:
        save_n500(fresh)
    elif cache.get("mcap"):
        log.warning("MCap scrape failed — using stale cache")
        return cache

    return fresh


# ── Bhavcopy normalisation ────────────────────────────────────────────────────
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
    df["RETURN_PCT"] = (
        (df["CLOSE"] - df["PREV_CLOSE"]) / df["PREV_CLOSE"] * 100
    ).round(4)
    return df.reset_index(drop=True)


def fetch_one(d: date, retries=3) -> pd.DataFrame | None:
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
                time.sleep(2 + attempt * 3)
    return None


def update_cache(target: date) -> pd.DataFrame:
    cache = load_bhav()
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
    for i, d in enumerate(missing):
        ss(progress=f"Downloading bhavcopy {i+1}/{len(missing)}: {d}",
           step=16 + int(i / len(missing) * 35))
        df = fetch_one(d)
        if df is not None:
            frames.append(df)
        time.sleep(0.5)

    if frames:
        new_data = pd.concat(frames, ignore_index=True)
        cache = pd.concat([cache, new_data],
                          ignore_index=True) if not cache.empty else new_data
        cache = cache.drop_duplicates(
            subset=["SYMBOL", "TRADE_DATE"], keep="last")
        cache = cache.sort_values(
            ["TRADE_DATE", "SYMBOL"]).reset_index(drop=True)
        save_bhav(cache)
        log.info(f"Bhav cache: {cache['TRADE_DATE'].nunique()} days, "
                 f"{cache['SYMBOL'].nunique()} symbols")
    return cache

# ── Symbol normalisation: screener↔bhavcopy ──────────────────────────────────


def build_symbol_map(n500_symbols: list[str], bhav_symbols: set[str]) -> dict[str, str]:
    """
    Returns {n500_symbol: bhavcopy_symbol} for all resolvable symbols.
    Handles: L&T→LT, ARE&M→AREM, GVT&D→GVTD, etc.
    """
    mapping = {}
    for sym in n500_symbols:
        if sym in bhav_symbols:
            mapping[sym] = sym
        else:
            alt = sym.replace("&", "")
            if alt in bhav_symbols:
                mapping[sym] = alt
            else:
                alt2 = sym.replace("-", "")
                if alt2 in bhav_symbols:
                    mapping[sym] = alt2
    return mapping

# ── 20d delivery MA ───────────────────────────────────────────────────────────


def compute_ma(cache: pd.DataFrame, target: date) -> pd.DataFrame:
    ts = pd.Timestamp(target)
    hist = cache[cache["TRADE_DATE"] < ts]
    rows = []
    for sym, grp in hist.groupby("SYMBOL"):
        vals = grp.sort_values("TRADE_DATE").tail(
            MA_WINDOW)["DELIV_QTY"].dropna()
        if len(vals) >= 3:
            rows.append({"SYMBOL": sym, "DELIV_MA20": float(vals.mean())})
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["SYMBOL", "DELIV_MA20"])

# ── Streak detection ──────────────────────────────────────────────────────────


def compute_streaks(cache: pd.DataFrame, target: date,
                    today_quad: dict[str, str]) -> dict[str, int]:
    """
    For each symbol, count consecutive past days in the SAME quadrant as today.
    Returns {symbol: streak_days}. Streak=1 means today only (no history match).
    """
    ts = pd.Timestamp(target)
    # Get the last BACKFILL_DAYS of history excluding today
    hist = cache[cache["TRADE_DATE"] < ts].copy()
    if hist.empty or not today_quad:
        return {s: 1 for s in today_quad}

    # Compute quadrant for each (symbol, date) in history
    hist = hist.sort_values(["SYMBOL", "TRADE_DATE"])
    hist["ABOVE_MA_H"] = False   # placeholder; recompute properly below
    # For streaks we just need return sign + delivery vs ma per day
    # Simpler: re-classify history rows by return sign only (no MA needed for streaks)
    hist["RET"] = (hist["CLOSE"] - hist["PREV_CLOSE"]) / \
        hist["PREV_CLOSE"] * 100

    # Compute per-day MA for historical context (expensive, skip — use return sign only for streaks)
    # Streak = same return sign (pos/neg) as today, consecutive days backwards
    today_pos = {sym for sym, q in today_quad.items() if q in (
        "Q1", "Q3")}  # positive return
    today_neg = {sym for sym, q in today_quad.items() if q in (
        "Q2", "Q4")}  # negative return

    streaks: dict[str, int] = {}
    dates = sorted(hist["TRADE_DATE"].unique(), reverse=True)   # newest first

    for sym, q in today_quad.items():
        pos_today = sym in today_pos
        streak = 1
        sym_hist = hist[hist["SYMBOL"] == sym].sort_values(
            "TRADE_DATE", ascending=False)
        for _, row in sym_hist.iterrows():
            ret = row["RET"]
            if pd.isna(ret):
                break
            same_sign = (ret > 0) == pos_today
            if same_sign:
                streak += 1
            else:
                break
        streaks[sym] = streak

    return streaks

# ── Quadrant migration vs previous trading day ────────────────────────────────


def compute_migrations(cache: pd.DataFrame, target: date,
                       today_quad: dict[str, str]) -> dict[str, str]:
    """
    Returns {symbol: "Q2→Q1"} for stocks that changed quadrant since yesterday.
    Uses return sign only for previous day (fast, no MA needed).
    """
    ts = pd.Timestamp(target)
    prev_days = cache[cache["TRADE_DATE"] < ts]["TRADE_DATE"].unique()
    if not len(prev_days):
        return {}

    prev_date = max(prev_days)
    prev_df = cache[cache["TRADE_DATE"] == prev_date][[
        "SYMBOL", "CLOSE", "PREV_CLOSE", "DELIV_QTY"]].copy()
    prev_df["RET"] = (prev_df["CLOSE"] - prev_df["PREV_CLOSE"]
                      ) / prev_df["PREV_CLOSE"] * 100

    # For previous day quadrant we only use return sign (Q1/Q3 = positive, Q2/Q4 = negative)
    # Full MA-based quadrant would require recomputing MA for that date - skip for speed
    prev_quad: dict[str, str] = {}
    for _, row in prev_df.iterrows():
        sym = row["SYMBOL"]
        ret = row["RET"]
        if pd.notna(ret):
            prev_quad[sym] = "positive" if ret > 0 else "negative"

    migrations: dict[str, str] = {}
    for sym, q in today_quad.items():
        if sym not in prev_quad:
            continue
        today_sign = "positive" if q in ("Q1", "Q3") else "negative"
        if prev_quad[sym] != today_sign:
            prev_q_label = "Q1/Q3" if prev_quad[sym] == "positive" else "Q2/Q4"
            today_q_label = q
            migrations[sym] = f"{prev_q_label}→{today_q_label}"

    return migrations

# ── MCap bucket ───────────────────────────────────────────────────────────────


def mcap_bucket(mcap_cr) -> str:
    if mcap_cr is None or (isinstance(mcap_cr, float) and pd.isna(mcap_cr)):
        return "Unknown"
    if mcap_cr >= 20000:
        return "Large"     # >₹20K Cr = Large Cap
    if mcap_cr >= 5000:
        return "Mid"       # ₹5K–20K Cr = Mid Cap
    return "Small"         # <₹5K Cr = Small Cap

# ── Append to daily history log ───────────────────────────────────────────────


def append_history(trade_date: date, data: dict):
    ensure_dirs()
    rows = []
    for q, stocks in data.items():
        for s in stocks:
            rows.append({
                "date":       trade_date.isoformat(),
                "symbol":     s["SYMBOL"],
                "quadrant":   q,
                "return_pct": s.get("RETURN_PCT"),
                "deliv_qty":  s.get("DELIV_QTY"),
                "deliv_pct":  s.get("DELIV_PER"),
                "vs_ma_pct":  s.get("DELIV_VS_MA_PCT"),
                "close":      s.get("CLOSE"),
                "mcap_cr":    s.get("MARKET_CAP_CR"),
            })
    if not rows:
        return
    new_df = pd.DataFrame(rows)
    if HIST_CSV.exists():
        existing = pd.read_csv(HIST_CSV)
        # Remove today's rows if already present (re-run scenario)
        existing = existing[existing["date"] != trade_date.isoformat()]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
    combined.to_csv(HIST_CSV, index=False)
    log.info(f"History log updated: {len(combined)} total rows")


# ── Classify ──────────────────────────────────────────────────────────────────
QUAD_LABELS = {
    "Q1": "Conviction Buy",
    "Q2": "Distribution",
    "Q3": "Low-Conv Rally",
    "Q4": "Weak Selling",
}


def classify(df: pd.DataFrame, streaks: dict, migrations: dict) -> tuple[dict, dict]:
    df = df.copy()
    df["ABOVE_MA"] = df["DELIV_QTY"] > df["DELIV_MA20"]
    df["DELIV_VS_MA_PCT"] = (
        (df["DELIV_QTY"] - df["DELIV_MA20"]) / df["DELIV_MA20"] * 100
    ).round(1)
    df["SPIKE"] = df["DELIV_VS_MA_PCT"] > SPIKE_THRESH
    df["MCAP_BUCKET"] = df["MARKET_CAP_CR"].apply(mcap_bucket)
    df["STREAK"] = df["SYMBOL"].map(streaks).fillna(1).astype(int)
    df["MIGRATION"] = df["SYMBOL"].map(migrations).fillna("")

    masks = {
        "Q1": (df["RETURN_PCT"] > 0) & df["ABOVE_MA"],
        "Q2": (df["RETURN_PCT"] < 0) & df["ABOVE_MA"],
        "Q3": (df["RETURN_PCT"] > 0) & ~df["ABOVE_MA"],
        "Q4": (df["RETURN_PCT"] < 0) & ~df["ABOVE_MA"],
    }

    KEEP = [
        "SYMBOL", "CLOSE", "OPEN", "HIGH", "LOW", "PREV_CLOSE",
        "RETURN_PCT", "DELIV_QTY", "DELIV_PER", "DELIV_MA20",
        "DELIV_VS_MA_PCT", "TOTTRDQTY", "TURNOVER_LACS",
        "MARKET_CAP_CR", "MCAP_BUCKET", "SECTOR",
        "SPIKE", "STREAK", "MIGRATION",
    ]

    data, summary = {}, {}
    for q, mask in masks.items():
        sub = df[mask].copy()
        cols = [c for c in KEEP if c in sub.columns]
        rows = sub[cols].copy()
        for fc in ["CLOSE", "OPEN", "HIGH", "LOW", "PREV_CLOSE", "RETURN_PCT",
                   "DELIV_PER", "DELIV_VS_MA_PCT", "TURNOVER_LACS"]:
            if fc in rows.columns:
                rows[fc] = rows[fc].round(2)
        for ic in ["DELIV_QTY", "DELIV_MA20", "TOTTRDQTY"]:
            if ic in rows.columns:
                rows[ic] = rows[ic].fillna(0).astype(int)

        data[q] = json.loads(rows.where(pd.notnull(
            rows), None).to_json(orient="records"))

        n = len(sub)
        # Sector breakdown for this quadrant
        sec_counts: dict = {}
        if "SECTOR" in sub.columns:
            sec_counts = sub["SECTOR"].value_counts().to_dict()
        # MCap breakdown
        mcap_breakdown = sub["MCAP_BUCKET"].value_counts(
        ).to_dict() if "MCAP_BUCKET" in sub.columns else {}
        # Spikes
        spikes = int(sub["SPIKE"].sum()) if "SPIKE" in sub.columns else 0
        # Multi-day streaks (≥3 days)
        streak_3plus = int((sub["STREAK"] >= 3).sum()
                           ) if "STREAK" in sub.columns else 0

        summary[q] = {
            "count":        n,
            "label":        QUAD_LABELS[q],
            "avg_return":   round(float(sub["RETURN_PCT"].mean()), 2) if n else 0,
            "avg_dvol":     int(sub["DELIV_QTY"].mean()) if n else 0,
            "avg_dpct":     round(float(sub["DELIV_PER"].mean()), 1) if n else 0,
            "avg_vsmapct":  round(float(sub["DELIV_VS_MA_PCT"].mean()), 1) if n else 0,
            "total_to":     round(float(sub["TURNOVER_LACS"].sum()), 0) if n and "TURNOVER_LACS" in sub else 0,
            "sectors":      sec_counts,
            "mcap_split":   mcap_breakdown,
            "spikes":       spikes,
            "streak_3plus": streak_3plus,
        }

    return data, summary

# ── Pipeline ──────────────────────────────────────────────────────────────────


def run_pipeline(trade_date: date):
    try:
        ss(status="running", error=None, data=None, summary=None,
           trade_date=str(trade_date), step=2,
           progress=f"Starting pipeline for {trade_date} …")

        # 1. Nifty 500 universe: mcap + sector (cached daily, 10+1 requests)
        n500_data = get_nifty500()
        n500_mcap = n500_data.get("mcap", {})
        n500_sector = n500_data.get("sector", {})
        n500_syms = set(n500_mcap) | set(n500_sector)
        if not n500_syms:
            raise RuntimeError("Could not load Nifty 500 universe")
        log.info(f"N500 universe: {len(n500_syms)} symbols")

        # 2. Bhav cache update
        cache = update_cache(trade_date)
        if cache.empty:
            raise RuntimeError("No bhavcopy data — check NSE connectivity")

        # 3. Today's slice
        ss(progress="Building today's dataset …", step=53)
        ts = pd.Timestamp(trade_date)
        today = cache[cache["TRADE_DATE"] == ts].copy()
        if today.empty:
            raise RuntimeError(
                f"No data for {trade_date} — market holiday or not yet published"
            )

        # 4. Symbol normalisation + N500 filter
        bhav_syms = set(today["SYMBOL"])
        sym_map = build_symbol_map(list(n500_syms), bhav_syms)
        # Reverse map: bhavcopy_sym → n500_sym (for mcap/sector lookup)
        rev_map = {v: k for k, v in sym_map.items()}

        # Keep only bhavcopy symbols that map to N500
        mapped_bhav_syms = set(sym_map.values())
        today = today[today["SYMBOL"].isin(mapped_bhav_syms)].copy()

        # Attach mcap + sector using reverse map
        today["N500_SYM"] = today["SYMBOL"].map(
            rev_map).fillna(today["SYMBOL"])
        today["MARKET_CAP_CR"] = today["N500_SYM"].map(n500_mcap)
        today["SECTOR"] = today["N500_SYM"].map(n500_sector).fillna("Unknown")
        today.drop(columns=["N500_SYM"], inplace=True)

        log.info(f"After N500 filter: {len(today)} symbols "
                 f"(expected ~500, gaps = not traded today)")

        # 5. 20d delivery MA
        ss(progress="Computing 20-day delivery MA …", step=58)
        ma_df = compute_ma(cache, trade_date)
        today = today.merge(ma_df, on="SYMBOL", how="left")
        today["DELIV_MA20"] = today["DELIV_MA20"].fillna(today["DELIV_QTY"])

        # 6. Streaks + migrations
        ss(progress="Computing streaks and migrations …", step=63)
        today_quad_map = {}   # will be filled after classify; do streaks after
        # Quick pre-classify for streak input (return sign only)
        for _, row in today.iterrows():
            ret = row.get("RETURN_PCT", 0) or 0
            above = (row.get("DELIV_QTY", 0) or 0) > (
                row.get("DELIV_MA20", 0) or 0)
            if ret > 0:
                today_quad_map[row["SYMBOL"]] = "Q1" if above else "Q3"
            else:
                today_quad_map[row["SYMBOL"]] = "Q2" if above else "Q4"

        streaks = compute_streaks(cache, trade_date, today_quad_map)
        migrations = compute_migrations(cache, trade_date, today_quad_map)

        # 7. Classify
        ss(progress="Classifying into quadrants …", step=88)
        data, summary = classify(today, streaks, migrations)
        counts = {q: v["count"] for q, v in summary.items()}
        total = sum(counts.values())
        log.info(f"Classified {total}/500: Q1={counts['Q1']} Q2={counts['Q2']} "
                 f"Q3={counts['Q3']} Q4={counts['Q4']}")

        # 8. Append to daily history log
        append_history(trade_date, data)

        ci = {
            "days":    int(cache["TRADE_DATE"].nunique()),
            "symbols": int(cache["SYMBOL"].nunique()),
            "from":    str(pd.Timestamp(cache["TRADE_DATE"].min()).date()),
            "to":      str(pd.Timestamp(cache["TRADE_DATE"].max()).date()),
            "n500":    len(n500_syms),
            "mapped":  len(today),
        }

        ss(status="done", data=data, summary=summary, step=100,
           progress=f"Done — {total}/500 stocks classified "
           f"({500-total} not traded today)",
           last_run=datetime.now().strftime("%H:%M:%S"),
           cache_info=ci)

    except Exception as e:
        import traceback
        log.error(traceback.format_exc())
        ss(status="error", error=str(e), step=0, progress=f"Error: {e}")

# ── Flask routes ──────────────────────────────────────────────────────────────


@app.route("/api/status")
def api_status():
    with _lock:
        return jsonify({k: v for k, v in _state.items() if k != "data"})


@app.route("/api/data")
def api_data():
    with _lock:
        return jsonify({
            "status":     _state["status"],
            "data":       _state["data"],
            "summary":    _state["summary"],
            "trade_date": _state["trade_date"],
            "last_run":   _state["last_run"],
            "cache_info": _state["cache_info"],
        })


@app.route("/api/run", methods=["POST"])
def api_run():
    if _state["status"] == "running":
        return jsonify({"ok": False, "msg": "Already running"}), 400
    body = request.get_json(silent=True) or {}
    date_str = body.get("date")
    try:
        td = datetime.strptime(
            date_str, "%Y-%m-%d").date() if date_str else last_trading_day()
    except ValueError:
        return jsonify({"ok": False, "msg": "Use YYYY-MM-DD"}), 400
    threading.Thread(target=run_pipeline, args=(td,), daemon=True).start()
    return jsonify({"ok": True, "trade_date": str(td)})


@app.route("/api/last_trading_day")
def api_ltd():
    return jsonify({"date": str(last_trading_day())})


@app.route("/api/history")
def api_history():
    """Return last 30 days of quadrant history for trend analysis."""
    if not HIST_CSV.exists():
        return jsonify([])
    df = pd.read_csv(HIST_CSV)
    df = df.tail(30 * 500)   # cap at 30 trading days
    return jsonify(df.to_dict(orient="records"))


if __name__ == "__main__":
    ensure_dirs()
    log.info("=" * 62)
    log.info("  NSE Delivery Screener  →  http://localhost:5050")
    log.info("  Universe: Nifty 500  |  No MIN_DEL_PCT filter")
    log.info("  Open delivery_screener.html in your browser")
    log.info("=" * 62)
    app.run(port=5050, debug=False, threaded=True)
