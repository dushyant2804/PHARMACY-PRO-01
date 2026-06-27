@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "APP_URL=http://127.0.0.1:8000"
set "HEALTH_URL=http://127.0.0.1:8000/api/health"
set "CHROME_EXE="
set "HEALTH_CHECK_FILE=%TEMP%\pharmacyos-health-check.tmp"
set "MAX_ATTEMPTS=60"

echo Starting backend...
start "PharmacyOS Backend" /MIN cmd /c ""D:\pharmacy-app-v2\backend\backend-run.bat""

echo Waiting for backend health...

for /L %%I in (1,1,%MAX_ATTEMPTS%) do (
    set "HTTP_STATUS="
    for /F %%S in ('curl -s -o "%HEALTH_CHECK_FILE%" -w "%%{http_code}" "%HEALTH_URL%" 2^>nul') do set "HTTP_STATUS=%%S"

    if "!HTTP_STATUS!"=="200" (
        powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $json = Get-Content -Raw '%HEALTH_CHECK_FILE%' | ConvertFrom-Json; if ($json.status -eq 'ok') { exit 0 } exit 1 } catch { exit 1 }" >nul 2>nul
        if not errorlevel 1 goto OPEN
    )

    if not "%%I"=="%MAX_ATTEMPTS%" timeout /t 2 /nobreak >nul
)

echo Backend health did not report status ok within 120 seconds. Browser was not opened.
if exist "%HEALTH_CHECK_FILE%" del "%HEALTH_CHECK_FILE%" >nul 2>nul
exit /b 1

:OPEN
echo Opening PharmacyOS...
if exist "%HEALTH_CHECK_FILE%" del "%HEALTH_CHECK_FILE%" >nul 2>nul

if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%ProgramFiles%\Google\Chrome\Application\chrome.exe"
if not defined CHROME_EXE if exist "%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"
if not defined CHROME_EXE if exist "%LocalAppData%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%LocalAppData%\Google\Chrome\Application\chrome.exe"

if defined CHROME_EXE (
    start "" "%CHROME_EXE%" --app="%APP_URL%" --new-window
) else (
    start "" "%APP_URL%"
)

exit /b 0
