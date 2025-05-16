@echo off
setlocal enabledelayedexpansion

:: Go to the scraper directory
cd /d C:\Users\edwardjohngarrido\Desktop\Scraper

:: Get timestamp
for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd_HH-mm-ss\""') do set "datetime=%%a"

:: Ensure Git remote is correct
git remote set-url origin https://github.com/edwardjohngarrido/Scraper

:: Fetch latest changes
echo ============================
echo Checking for updates...
echo ============================
git fetch origin main

:: Compare current and remote commits
for /f %%i in ('git rev-parse HEAD') do set "LOCAL=%%i"
for /f %%i in ('git rev-parse origin/main') do set "REMOTE=%%i"

if not "!LOCAL!"=="!REMOTE!" (
    echo Update available.

    :: Create backup folder
    set "backupFolder=old code\!datetime!"
    mkdir "!backupFolder!"

    :: Copy important files to backup
    echo Backing up current files...
    copy run-scrapers.bat "!backupFolder!">nul
    copy tbScraper.js "!backupFolder!">nul
    copy viewScraper.js "!backupFolder!">nul
    if exist tbIGScraper.js copy tbIGScraper.js "!backupFolder!">nul

    echo Pulling latest changes...
    git pull origin main
    echo ✅ Update complete.
) else (
    echo No updates found.
)

:: Run the latest scraper batch
echo ============================
echo Running run-scrapers.bat...
echo ============================
call run-scrapers.bat
