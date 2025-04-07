@echo off
setlocal enabledelayedexpansion

:: Go to the scraper directory
cd /d C:\Users\edwardjohngarrido\Desktop\Scraper

:: Ensure remote is correctly set to GitHub
git remote set-url origin https://github.com/edwardjohngarrido/Scraper

:: Fetch latest changes
echo ============================
echo Checking for updates...
echo ============================
git fetch origin main

:: Compare local HEAD and remote HEAD
for /f %%i in ('git rev-parse HEAD') do set "LOCAL=%%i"
for /f %%i in ('git rev-parse origin/main') do set "REMOTE=%%i"

set "updated=false"

if not "!LOCAL!"=="!REMOTE!" (
    echo Update available. Pulling latest changes...
    git pull origin main
    set "updated=true"
    echo Git repo updated.
) else (
    echo No updates found.
)

:: Generate timestamp
for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd_HH-mm-ss\""') do set "datetime=%%a"

:: Ensure logs directory exists
if not exist logs mkdir logs

:: Define log filenames
set "ttlog=logs\tiktok-log_!datetime!.txt"
set "iglog=logs\instagram-log_!datetime!.txt"

:: Optional: log update info
if "!updated!"=="true" (
    echo Repo updated on !datetime! > logs\update-log_!datetime!.txt
)

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
