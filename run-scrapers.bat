@echo off
setlocal enabledelayedexpansion

:: Go to the scraper directory
cd /d C:\Users\edwardjohngarrido\Desktop\Scraper

:: Generate timestamp
for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd_HH-mm-ss\""') do set "datetime=%%a"

:: Ensure logs directory exists
if not exist logs mkdir logs

:: Define log filenames
set "ttlog=logs\tiktok-log_!datetime!.txt"
set "iglog=logs\instagram-log_!datetime!.txt"
set "vwlog=logs\view-log_!datetime!.txt"

:: Run Instagram scraper (currently disabled)
echo ============================
echo Running Instagram Scraper...
echo ============================
:: powershell -Command "node tbIGScraper.js 2>&1 | Tee-Object -FilePath '!iglog!'"
:: echo.

:: Run TikTok scraper
echo ============================
echo Running TikTok Scraper...
echo ============================
powershell -Command "node tbScraper.js 2>&1 | Tee-Object -FilePath '!ttlog!'"
echo.

:: Run View scraper
echo ============================
echo Running View Scraper...
echo ============================
powershell -Command "node viewScraper.js 2>&1 | Tee-Object -FilePath '!vwlog!'"
echo.

echo ============================
echo All scraping complete.
echo ============================
