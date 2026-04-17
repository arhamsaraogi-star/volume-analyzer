@echo off
TITLE Ashika Dashboard Pipeline
cd /d "%~dp0"
echo ==================================================
echo   🚀 Ashika Dashboard - Daily Mega Update
echo ==================================================
echo.
python daily_ashika_runner.py
echo.
echo ==================================================
echo   ✅ Update Complete!
echo   Opening Ashika Dashboard Hub...
echo ==================================================
start "" "index.html"
pause
