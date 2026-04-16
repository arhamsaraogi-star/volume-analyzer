@echo off
TITLE Volume Analyzer Pipeline
cd /d "%~dp0"
echo ==================================================
echo   🚀 NSE Volume Analyzer - Daily Update
echo ==================================================
echo.
python daily_runner.py
echo.
echo ==================================================
echo   ✅ Update Complete!
echo   Opening dashboard...
echo ==================================================
start "" "public_site\index.html"
pause
