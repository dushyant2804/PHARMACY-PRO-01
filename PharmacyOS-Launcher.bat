@echo off
setlocal EnableExtensions

set "APP_URL=http://127.0.0.1:8000"
set "HEALTH_URL=http://127.0.0.1:8000/api/health"
set "CHROME_EXE="
set "HEALTH_CHECK_FILE=%TEMP%\pharmacyos-health-check.tmp"
set "MAX_ATTEMPTS=150"

echo Starting backend...
start "PharmacyOS Backend" /MIN cmd /c ""D:\pharmacy-app-v2\backend\backend-run.bat""

echo Waiting for backend health at %HEALTH_URL%...

for /L %%I in (1,1,%MAX_ATTEMPTS%) do (
    curl -fs "%HEALTH_URL%" -o "%HEALTH_CHECK_FILE%" 2>nul
    if not errorlevel 1 goto BACKEND_READY

    if not "%%I"=="%MAX_ATTEMPTS%" timeout /t 2 /nobreak >nul
)

echo Backend health did not respond within 5 minutes. Opening PharmacyOS anyway.
goto OPEN_UI

:BACKEND_READY
echo Backend ready. Opening PharmacyOS...

:OPEN_UI
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
