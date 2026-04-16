@echo off
REM NSE Delivery Screener Launcher
REM Place this .bat file in the same folder as screener_server.py and delivery_screener.html

cd /d "%~dp0"

REM Kill any existing instance on port 5050
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :5050 ^| findstr LISTENING') do (
    taskkill /PID %%a /F >nul 2>&1
)

REM Start Flask server in background (minimised window)
start /min "NSE Screener Server" py screener_server.py

REM Wait 2 seconds for server to start
timeout /t 2 /nobreak >nul

REM Open dashboard in default browser
start "" delivery_screener.html

echo NSE Delivery Screener launched.
echo Server running at http://localhost:5050
echo Dashboard opened in browser.
echo Close this window to stop the server.
pause
