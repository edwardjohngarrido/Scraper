@echo off
echo 🛠 Installing dependencies for TikTok/Instagram Scrapers...
echo.

REM ✅ Ensure Node.js is installed
node -v >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo ❌ Node.js is not installed.
    echo 🔗 Download from: https://nodejs.org/
    pause
    exit /b
) ELSE (
    echo ✅ Node.js is already installed: %~nx0
)

REM ✅ Change to scraper folder
cd /d "%~dp0"

REM ✅ Create node_modules and install all required packages
echo 🔄 Running: npm install...
call npm install puppeteer puppeteer-extra puppeteer-extra-plugin-stealth proxy-chain googleapis axios node-fetch

REM ✅ Optional: install nodemon for auto-restarts during dev
REM call npm install -g nodemon

echo.
echo ✅ All dependencies installed successfully!
pause
