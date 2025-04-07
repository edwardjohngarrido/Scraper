@echo off
setlocal enabledelayedexpansion

:: Define script directory
cd /d C:\Users\edwar\OneDrive\Documents\Tiktok-Scraper

:: Fetch latest changes
echo ============================
echo Checking for updates...
echo ============================
git fetch

:: Check if there are new commits
for /f %%i in ('git rev-parse HEAD') do set "LOCAL=%%i"
for /f %%i in ('git rev-parse origin/main') do set "REMOTE=%%i"

if "!LOCAL!"=="!REMOTE!" (
    echo No updates found. Skipping scraper run.
    goto :eof
)

:: Pull the latest changes
git pull https://github.com/edwardjohngarrido/Scraper main

:: Generate timestamp for log filenames
for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd_HH-mm-ss\""') do set "datetime=%%a"

:: Ensure logs directory exists
if not exist logs mkdir logs

:: Define log filenames
set "ttlog=logs\tiktok-log_!datetime!.txt"
set "iglog=logs\instagram-log_!datetime!.txt"

:: Run TikTok scraper
echo ============================
echo Running TikTok Scraper...
echo ============================
powershell -Command "node tbScraper.js 2>&1 | Tee-Object -FilePath '!ttlog!'"
echo.

:: Run Instagram scraper
echo ============================
echo Running Instagram Scraper...
echo ============================
powershell -Command "node tbIGScraper.js 2>&1 | Tee-Object -FilePath '!iglog!'"
echo.

echo ============================
echo All scraping complete.
echo ============================
