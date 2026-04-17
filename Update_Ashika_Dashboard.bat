@echo off
TITLE Ashika Dashboard - One-Click Update
cd /d "%~dp0"
echo ==================================================
echo   🚀 Ashika Dashboard - Fetching Latest Data
echo ==================================================
echo.
echo   - Bhavcopy: Fetching today's market data...
echo   - Results: Scraping today's corporate filings...
echo   - Analytics: Regenerating dashboards...
echo.
py daily_ashika_runner.py || python daily_ashika_runner.py
echo.
echo ==================================================
echo   ✅ Update Complete!
echo   Opening Ashika Dashboard...
echo ==================================================
start "" "index.html"
exit
