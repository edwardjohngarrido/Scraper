@echo off
setlocal enableextensions enabledelayedexpansion

:: --- config ---
set "ROOT=C:\Users\edwardjohngarrido\Desktop\Scraper"
set "NODE_EXE=node"  ^&^& where node >nul 2>&1 || set "NODE_EXE=C:\Program Files\nodejs\node.exe"

pushd "%ROOT%" || ( echo Failed to cd into "%ROOT%". & pause & exit /b 1 )

:: Timestamp
for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd_HH-mm-ss\""') do set "datetime=%%a"

:: Ensure Git remote & check updates
git remote set-url origin https://github.com/edwardjohngarrido/Scraper >nul 2>&1

echo ============================
echo Checking for updates before launching Master Scraper...
echo ============================
git fetch origin main

for /f %%i in ('git rev-parse HEAD') do set "LOCAL=%%i"
for /f %%i in ('git rev-parse origin/main') do set "REMOTE=%%i"

if not "%LOCAL%"=="%REMOTE%" (
  echo Update available.

  set "backupFolder=old code\%datetime%"
  mkdir "%backupFolder%" >nul 2>&1

  echo Backing up current files...
  copy /y "masterScraper.js" "%backupFolder%\masterScraper.js" >nul
  if exist ".env"             copy /y ".env" "%backupFolder%\.env" >nul
  if exist "credentials.json" copy /y "credentials.json" "%backupFolder%\credentials.json" >nul

  echo Pulling latest changes...
  git reset --hard >nul
  git pull --rebase origin main
  echo ✅ Update complete.
) else (
  echo No updates found.
)

if not exist "masterlogs" mkdir "masterlogs"
set "masterlog=masterlogs\masterlog_%datetime%.txt"

echo ============================
echo Running masterScraper.js...
echo ============================

:: Run Node, tee to log, and propagate Node's exit code
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Stop';" ^
  "$env:PSModulePath=$env:PSModulePath;" ^
  "$node = '%NODE_EXE%';" ^
  "& $node 'masterScraper.js' 2>&1 | Tee-Object -FilePath '%masterlog%';" ^
  "if ($null -ne $LASTEXITCODE) { exit $LASTEXITCODE } else { exit 0 }"

set "rc=%ERRORLEVEL%"
echo Exit code: %rc%

:: Keep window open on failure and show the tail of the log
if not "%rc%"=="0" (
  echo.
  echo ======= LAST 80 LINES OF %masterlog% =======
  powershell -NoProfile -Command "Get-Content -Path '%masterlog%' -Tail 80"
  echo.
  pause
)

popd
exit /b %rc%
