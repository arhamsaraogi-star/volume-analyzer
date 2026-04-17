import os
import json
import time
import requests
from bs4 import BeautifulSoup

DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(DIR, "indices_lookup.json")

SCREENER_EMAIL = "asutosh@ashikagroup.com"
SCREENER_PASSWORD = "Dilipsir@1234"
BASE_URL = "https://www.screener.in"

INDICES = {
    "Nifty 50": "NIFTY",
    "Nifty Midcap 150": "NMIDCAP150",
    "Nifty Smallcap 250": "SMALLCA250",
    "Nifty 500": "CNX500"
}

def main():
    print("=" * 50)
    print("Updating Market Indices from Screener.in")
    print("=" * 50)

    session = requests.Session()
    # 1. Login
    print("Logging into Screener...")
    try:
        r = session.get(f"{BASE_URL}/login/", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        csrf_tag = soup.find('input', {'name': 'csrfmiddlewaretoken'})
        if not csrf_tag:
            print("Failed to find CSRF token.")
            return

        payload = {
            "csrfmiddlewaretoken": csrf_tag['value'],
            "username": SCREENER_EMAIL,
            "password": SCREENER_PASSWORD
        }
        res = session.post(f"{BASE_URL}/login/?", data=payload, headers={"Referer": f"{BASE_URL}/login/"}, timeout=15)
        if "Logout" not in res.text and "Dashboard" not in res.text and "account" not in res.url:
            print("Login failed! Status code:", res.status_code)
            return
        print("Logged in successfully.")
    except Exception as e:
        print(f"Login error: {e}")
        return

    # Map: ticker -> list of index names
    # e.g. "TCS" -> ["Nifty 50", "Nifty 500"]
    ticker_indices = {}

    for index_name, screen_id in INDICES.items():
        print(f"\nScraping {index_name} ({screen_id})...")
        page = 1
        last_tickers = set()
        total_found = 0

        while True:
            url = f"{BASE_URL}/company/{screen_id}/?page={page}"
            try:
                r = session.get(url, timeout=15)
                soup = BeautifulSoup(r.text, "html.parser")
                
                # Find the Constituents section
                section = soup.find(id="constituents")
                if not section:
                    break
                
                table = section.find("table", class_="data-table")
                if not table:
                    break
                
                current_tickers = set()
                
                # Fetch rows that represent companies
                for tr in table.find_all("tr"):
                    if tr.has_attr("data-row-company-id"):
                        a_tag = tr.find("a")
                        if a_tag:
                            href = a_tag.get("href", "")
                            if href.startswith("/company/"):
                                parts = href.strip("/").split("/")
                                if len(parts) >= 2:
                                    current_tickers.add(parts[1].upper())
                
                # Screener returns the last valid page if you overflow the page number.
                # So if current is exactly the same as last, we have over-paginated.
                if not current_tickers or current_tickers == last_tickers:
                    break
                
                for t in current_tickers:
                    if t not in ticker_indices:
                        ticker_indices[t] = []
                    if index_name not in ticker_indices[t]:
                        ticker_indices[t].append(index_name)
                    total_found += 1
                
                last_tickers = current_tickers
                print(f"  Page {page}: found {len(current_tickers)} constituents (Total: {total_found})")
                
                page += 1
                time.sleep(1)  # polite throttle
                
            except Exception as e:
                print(f"  Failed on page {page}: {e}")
                break

    # Save to JSON
    if ticker_indices:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(ticker_indices, f, indent=2)
        print(f"\n[OK] Saved {len(ticker_indices)} unique companies to {CACHE_FILE}")
    else:
        print("\n[WARNING] No companies scraped!")

if __name__ == "__main__":
    main()
