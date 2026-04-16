import requests
from bs4 import BeautifulSoup
import re
import json

URL_LOGIN = "https://www.screener.in/login/"
URL_WATCHLIST_1 = "https://www.screener.in/watchlist/10259781/"
URL_DASHBOARD_1 = "https://www.screener.in/dash/10259808/"

# Credentials from user
USER_ID = "asutosh@ashikagroup.com"
PASS = "Dilipsir@1234"

def test_screener_scraping():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    })

    # Get CSRF token
    r = session.get(URL_LOGIN)
    soup = BeautifulSoup(r.text, "html.parser")
    csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
    if not csrf_input:
        print("Failed to get CSRF token")
        return

    csrf_token = csrf_input["value"]
    print(f"CSRF Token: {csrf_token}")

    # Login
    payload = {
        "username": USER_ID,
        "password": PASS,
        "csrfmiddlewaretoken": csrf_token,
        "next": "/"
    }
    r = session.post(URL_LOGIN, data=payload, headers={"Referer": URL_LOGIN})
    
    if "logout" in r.text.lower() or r.status_code == 200:
        print("Login looks successful")
    else:
        print("Login failed")
        return

    # Try watchlist 1
    r = session.get(URL_WATCHLIST_1)
    print(f"Watchlist 1 Status: {r.status_code}")
    # Extract symbols
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if table:
        symbols = []
        for row in table.find_all("tr")[1:]:
            link = row.find("a")
            if link and "/company/" in link["href"]:
                sym = link["href"].split("/")[2]
                symbols.append(sym.upper())
        print(f"Watchlist 1 Symbols: {len(symbols)} found")
        print(f"First 5: {symbols[:5]}")
    else:
        print("Watchlist 1 table not found")

    # Try dashboard 1
    r = session.get(URL_DASHBOARD_1)
    print(f"Dashboard 1 Status: {r.status_code}")
    # Extract symbols
    soup = BeautifulSoup(r.text, "html.parser")
    # Look for any tables or links
    tables = soup.find_all("table")
    print(f"Dashboard 1 Tables: {len(tables)}")
    if tables:
        for idx, table in enumerate(tables):
            symbols = []
            for row in table.find_all("tr")[1:]:
                link = row.find("a")
                if link and "/company/" in link["href"]:
                    sym = link["href"].split("/")[2]
                    symbols.append(sym.upper())
            print(f"Table {idx} Symbols: {len(symbols)} found")
            if symbols:
                print(f"First 5: {symbols[:5]}")
    else:
        # Maybe it's a list of watchlists?
        links = soup.find_all("a", href=re.compile(r"/watchlist/"))
        print(f"Watchlist links in dashboard: {[l['href'] for l in links]}")


if __name__ == "__main__":
    test_screener_scraping()
