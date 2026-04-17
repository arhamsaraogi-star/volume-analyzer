"""
BSE Board Meetings Scraper
---------------------------
Scrapes upcoming board meetings (next 45 days) from BSE India:
  https://www.bseindia.com/corporates/board_meeting.aspx

Uses Selenium in headless mode to navigate the ASP.NET page,
set the date range, submit, and scrape all rows.

Output: Board_Meetings.csv (overwrites)

Usage:
  py bse_scraper.py             # next 45 days from today
  py bse_scraper.py 60          # next 60 days from today
"""

import os
import sys
import csv
import time
import re
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

DIR = os.path.dirname(os.path.abspath(__file__))
CSV_OUT = os.path.join(DIR, "Board_Meetings.csv")
BSE_URL = "https://www.bseindia.com/corporates/board_meeting.aspx"


def get_chrome_driver():
    """Create a headless Chrome driver."""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36")
    try:
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except Exception:
        # Fallback: try default Chrome
        return webdriver.Chrome(options=opts)


def clear_and_type(driver, element_id, value):
    """Clear a field and type a new value."""
    el = driver.find_element(By.ID, element_id)
    el.clear()
    # Use JavaScript to set value for ASP.NET date pickers
    driver.execute_script(
        f"document.getElementById('{element_id}').value = '{value}';")
    return el


def scrape_board_meetings(days_ahead=45):
    """
    Scrape BSE India board meetings for the next `days_ahead` days.
    Returns list of dicts.
    """
    from_date = datetime.today()
    to_date = from_date + timedelta(days=days_ahead)

    from_str = from_date.strftime("%d/%m/%Y")
    to_str = to_date.strftime("%d/%m/%Y")

    print(f"  BSE Scraper: {from_str} -> {to_str} ({days_ahead} days)")

    driver = get_chrome_driver()
    meetings = []

    try:
        driver.get(BSE_URL)
        wait = WebDriverWait(driver, 15)

        # Wait for the page to load
        time.sleep(3)

        # Try to find and set the date fields
        # BSE uses various input IDs - try common patterns
        date_from_ids = [
            "ContentPlaceHolder1_txtFromDate",
            "txtFromDate",
            "ctl00_ContentPlaceHolder1_txtFromDate",
        ]
        date_to_ids = [
            "ContentPlaceHolder1_txtToDate",
            "txtToDate",
            "ctl00_ContentPlaceHolder1_txtToDate",
        ]

        # Find the actual from-date field
        from_el = None
        for fid in date_from_ids:
            try:
                from_el = driver.find_element(By.ID, fid)
                print(f"    Found from-date: #{fid}")
                break
            except Exception:
                continue

        if not from_el:
            # Try finding by CSS
            try:
                inputs = driver.find_elements(By.CSS_SELECTOR,
                                              "input[type='text']")
                # Usually first date input is from, second is to
                if len(inputs) >= 2:
                    from_el = inputs[0]
                    print(f"    Found from-date by CSS fallback")
            except Exception:
                pass

        if from_el:
            from_el_id = from_el.get_attribute("id")
            driver.execute_script(
                f"document.getElementById('{from_el_id}').value = '{from_str}';")

        # Find the to-date field
        to_el = None
        for tid in date_to_ids:
            try:
                to_el = driver.find_element(By.ID, tid)
                print(f"    Found to-date: #{tid}")
                break
            except Exception:
                continue

        if not to_el:
            try:
                inputs = driver.find_elements(By.CSS_SELECTOR,
                                              "input[type='text']")
                if len(inputs) >= 2:
                    to_el = inputs[1]
                    print(f"    Found to-date by CSS fallback")
            except Exception:
                pass

        if to_el:
            to_el_id = to_el.get_attribute("id")
            driver.execute_script(
                f"document.getElementById('{to_el_id}').value = '{to_str}';")

        # Click submit button
        submit_ids = [
            "ContentPlaceHolder1_btnSubmit",
            "btnSubmit",
            "ctl00_ContentPlaceHolder1_btnSubmit",
        ]
        submitted = False
        for sid in submit_ids:
            try:
                btn = driver.find_element(By.ID, sid)
                btn.click()
                submitted = True
                print(f"    Clicked submit: #{sid}")
                break
            except Exception:
                continue

        if not submitted:
            # Try finding by value/text
            try:
                btns = driver.find_elements(By.CSS_SELECTOR,
                                            "input[type='submit']")
                for btn in btns:
                    if "submit" in (btn.get_attribute("value") or "").lower():
                        btn.click()
                        submitted = True
                        print(f"    Clicked submit by value fallback")
                        break
            except Exception:
                pass

        if not submitted:
            print("    WARNING: Could not find submit button")

        # Wait for results to load
        time.sleep(5)

        # Now scrape the results table
        meetings = _scrape_table(driver)

        # Check for pagination — BSE uses ASP.NET WebForms __doPostBack
        page = 1
        while True:
            try:
                page += 1
                # ASP.NET GridView pagination: find any link matching 'Page$N'
                # Links look like: href="javascript:__doPostBack('...gvData','Page$2')"
                page_links = driver.find_elements(
                    By.XPATH,
                    f"//a[contains(@href,'Page${page}') or contains(@href,'page${page}')]"
                )
                if not page_links:
                    # Also try by link text (some BSE pages show num links)
                    page_links = driver.find_elements(
                        By.XPATH,
                        f"//tr[contains(@class,'pgr') or contains(@class,'pager') or contains(@class,'Pager')]//a[text()='{page}']"
                    )
                if not page_links:
                    # Try generic: any table-footer link with that page number
                    page_links = driver.find_elements(
                        By.XPATH,
                        f"//table//td[contains(@colspan,'')]//a[normalize-space(text())='{page}']"
                    )
                if not page_links:
                    break  # No more pages

                page_links[0].click()
                time.sleep(3)
                page_meetings = _scrape_table(driver)
                if not page_meetings:
                    break
                meetings.extend(page_meetings)
                print(f"    Page {page}: +{len(page_meetings)} meetings (total {len(meetings)})")
                if page > 50:  # Safety cap
                    break
            except Exception:
                break

    except Exception as e:
        print(f"  ERROR in BSE scraper: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()

    print(f"  BSE Scraper: Total {len(meetings)} meetings found")
    return meetings


def _scrape_table(driver):
    """Extract rows from the currently visible table."""
    rows = []
    try:
        # Try common table selectors
        table_selectors = [
            "#ContentPlaceHolder1_gvData",
            "#ctl00_ContentPlaceHolder1_gvData",
            "table.mGrid",
            "table.table",
            "#divData table",
            "table[id*='gv']",
        ]

        table = None
        for sel in table_selectors:
            try:
                table = driver.find_element(By.CSS_SELECTOR, sel)
                break
            except Exception:
                continue

        if not table:
            # Last resort: find any data table
            tables = driver.find_elements(By.TAG_NAME, "table")
            for t in tables:
                trs = t.find_elements(By.TAG_NAME, "tr")
                if len(trs) > 3:  # Likely a data table
                    table = t
                    break

        if not table:
            print("    WARNING: No data table found")
            return rows

        trs = table.find_elements(By.TAG_NAME, "tr")
        headers = []

        for i, tr in enumerate(trs):
            tds = tr.find_elements(By.TAG_NAME, "td")
            ths = tr.find_elements(By.TAG_NAME, "th")

            if ths and not headers:
                headers = [th.text.strip() for th in ths]
                continue

            if not tds:
                continue

            vals = [td.text.strip() for td in tds]

            # Try to map to our expected columns
            rec = _map_row(vals, headers)
            if rec and rec.get("Security Code"):
                rows.append(rec)

    except Exception as e:
        print(f"    WARNING: Table scraping error: {e}")

    return rows


def _map_row(vals, headers):
    """Map raw row values to our column format."""
    rec = {}

    if headers:
        # Map by header names
        for i, h in enumerate(headers):
            if i >= len(vals):
                break
            hl = h.lower().strip()
            v = vals[i].strip()

            if "security code" in hl or "scrip" in hl or "code" in hl:
                rec["Security Code"] = v
            elif "company" in hl or "name" in hl:
                rec["Company name"] = v
            elif "industry" in hl or "sector" in hl:
                rec["Industry"] = v
            elif "purpose" in hl:
                rec["Purpose"] = v
            elif "meeting" in hl and "date" in hl:
                rec["Meeting Date"] = _normalize_date(v)
            elif "announcement" in hl and "date" in hl:
                rec["Announcement Date"] = _normalize_date_slash(v)
    else:
        # Positional fallback (BSE typical: Code, Name, Industry, Purpose, MeetDate, AnnDate)
        if len(vals) >= 6:
            rec["Security Code"] = vals[0]
            rec["Company name"] = vals[1]
            rec["Industry"] = vals[2]
            rec["Purpose"] = vals[3]
            rec["Meeting Date"] = _normalize_date(vals[4])
            rec["Announcement Date"] = _normalize_date_slash(vals[5])

    return rec


def _normalize_date(s):
    """Normalize date to 'DD Mon YYYY' format."""
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %b %Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d %b %Y")
        except Exception:
            continue
    return s


def _normalize_date_slash(s):
    """Normalize date to 'DD/MM/YYYY' format."""
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %b %Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
        except Exception:
            continue
    return s


def save_meetings_csv(meetings, path=CSV_OUT):
    """
    Merge scraped meetings with existing CSV and write back.
    Keyed on (Security Code, Meeting Date) — never loses existing data.
    """
    cols = ["Security Code", "Company name", "Industry", "Purpose",
            "Meeting Date", "Announcement Date"]

    # Load existing CSV if present
    existing = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = (row.get("Security Code", "").strip(),
                           row.get("Meeting Date", "").strip())
                    if key[0]:  # valid row
                        existing[key] = {k.strip(): v.strip() for k, v in row.items()}
            print(f"  Loaded {len(existing)} existing meetings from CSV")
        except Exception as e:
            print(f"  WARNING: Could not read existing CSV: {e}")

    # Merge new meetings (new data takes priority for same key)
    added = 0
    for m in meetings:
        key = (m.get("Security Code", "").strip(),
               m.get("Meeting Date", "").strip())
        if key[0]:
            if key not in existing:
                added += 1
            existing[key] = m

    if not existing:
        print("  No meetings to save.")
        return

    # Write merged result sorted by meeting date
    merged = sorted(existing.values(),
                    key=lambda x: x.get("Meeting Date", ""))

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        for m in merged:
            writer.writerow(m)

    print(f"  [OK] Saved {len(merged)} meetings ({added} new) -> {path}")


def main():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 45
    print(f"\nBSE Board Meetings Scraper")
    print(f"{'=' * 50}")
    meetings = scrape_board_meetings(days)
    save_meetings_csv(meetings)
    print(f"\n[OK] Done. {len(meetings)} meetings saved.")


if __name__ == "__main__":
    main()
