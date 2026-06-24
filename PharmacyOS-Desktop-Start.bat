@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM PharmacyOS quiet Windows desktop launcher.
REM Intended to be started by PharmacyOS-Launch.vbs so no console window is shown.
REM PharmacyOS-Start.bat remains the visible troubleshooting fallback.
set "BASE_DIR=%~dp0"
if "%BASE_DIR:~-1%"=="\" set "BASE_DIR=%BASE_DIR:~0,-1%"
cd /d "%BASE_DIR%"

set "BACKEND_DIR=D:\pharmacy-app-v2\backend"
set "APP_DIR=D:\pharmacy-app-v2"
set "PHARMACYOS_MODE=LOCAL_MODE"
set "DATA_DIR=%APP_DIR%\data"
set "LOCAL_DATA_DIR=%APP_DIR%\local_data"
set "LOCAL_DB_PATH=%LOCAL_DATA_DIR%\pharmacyos.sqlite3"
set "BACKUP_DIR=%APP_DIR%\backups"
set "UPLOAD_DIR=%APP_DIR%\uploads"
set "LOG_DIR=%APP_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\pharmacyos-local.log"
set "BACKEND_VBS_FILE=%BASE_DIR%\PharmacyOS-Backend-Hidden.vbs"
set "BACKEND_BAT_FILE=%BASE_DIR%\PharmacyOS-Backend-Hidden.bat"
set "BACKEND_OUTPUT_LOG=%LOG_DIR%\pharmacyos-backend-output.log"
set "HEALTH_URL=http://127.0.0.1:8000/api/health"
set "APP_URL=http://127.0.0.1:8000"

if not exist "%LOCAL_DATA_DIR%" mkdir "%LOCAL_DATA_DIR%"
if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"
if not exist "%UPLOAD_DIR%" mkdir "%UPLOAD_DIR%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

>>"%LOG_FILE%" echo.
>>"%LOG_FILE%" echo ===============================================
>>"%LOG_FILE%" echo [%date% %time%] PharmacyOS quiet launcher starting.
>>"%LOG_FILE%" echo [%date% %time%] Mode=LOCAL_MODE
>>"%LOG_FILE%" echo [%date% %time%] Database=%LOCAL_DB_PATH% Backups=%BACKUP_DIR% Uploads=%UPLOAD_DIR%
>>"%LOG_FILE%" echo [%date% %time%] Backend dir=%BACKEND_DIR%
>>"%LOG_FILE%" echo [%date% %time%] Backend VBS=%BACKEND_VBS_FILE%
>>"%LOG_FILE%" echo [%date% %time%] Backend BAT=%BACKEND_BAT_FILE%
>>"%LOG_FILE%" echo [%date% %time%] Backend output log=%BACKEND_OUTPUT_LOG%
>>"%LOG_FILE%" echo [%date% %time%] Health URL=%HEALTH_URL%

if not exist "%BACKEND_VBS_FILE%" (
    >>"%LOG_FILE%" echo [%date% %time%] ERROR: Missing backend hidden launcher: %BACKEND_VBS_FILE%
    exit /b 1
)
if not exist "%BACKEND_BAT_FILE%" (
    >>"%LOG_FILE%" echo [%date% %time%] ERROR: Missing backend hidden command: %BACKEND_BAT_FILE%
    exit /b 1
)

call :CHECK_HEALTH
if errorlevel 2 (
    >>"%LOG_FILE%" echo [%date% %time%] Quiet launcher stopped: health endpoint reports CLOUD_MODE.
    exit /b 2
)
if errorlevel 1 (
    >>"%LOG_FILE%" echo [%date% %time%] Backend not healthy yet. Starting permanent hidden backend launcher.
    wscript.exe "%BACKEND_VBS_FILE%"
    if errorlevel 1 (
        >>"%LOG_FILE%" echo [%date% %time%] ERROR: Backend hidden launcher failed with errorlevel !errorlevel!.
        exit /b 1
    )
    >>"%LOG_FILE%" echo [%date% %time%] Hidden backend start command issued.
) else (
    >>"%LOG_FILE%" echo [%date% %time%] Backend already running; opening app.
    goto :OPEN_APP
)

for /L %%I in (1,1,60) do (
    call :CHECK_HEALTH
    if errorlevel 2 (
        >>"%LOG_FILE%" echo [%date% %time%] Quiet launcher stopped: health endpoint reports CLOUD_MODE after backend start.
        exit /b 2
    )
    if not errorlevel 1 goto :OPEN_APP
    >>"%LOG_FILE%" echo [%date% %time%] Waiting for health check %%I/60.
    timeout /t 2 /nobreak >nul
)

>>"%LOG_FILE%" echo [%date% %time%] ERROR: PharmacyOS did not become healthy within 2 minutes.
exit /b 1

:OPEN_APP
>>"%LOG_FILE%" echo [%date% %time%] Health check success. Opening PharmacyOS app window.
set "CHROME_EXE="
if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%ProgramFiles%\Google\Chrome\Application\chrome.exe"
if not defined CHROME_EXE if exist "%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"
if not defined CHROME_EXE if exist "%LocalAppData%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%LocalAppData%\Google\Chrome\Application\chrome.exe"

if defined CHROME_EXE (
    start "PharmacyOS" "%CHROME_EXE%" --app="%APP_URL%" --new-window
) else (
    >>"%LOG_FILE%" echo [%date% %time%] Chrome not found in standard locations. Opening default browser.
    start "PharmacyOS" "%APP_URL%"
)

>>"%LOG_FILE%" echo [%date% %time%] Quiet launcher finished successfully.
exit /b 0

:CHECK_HEALTH
set "HEALTH_STDOUT=%TEMP%\pharmacyos-health-stdout-%RANDOM%.log"
set "HEALTH_STDERR=%TEMP%\pharmacyos-health-stderr-%RANDOM%.log"
python -c "import json, sys, urllib.request; url = sys.argv[1]; r = urllib.request.urlopen(url, timeout=2); body = r.read().decode('utf-8'); data = json.loads(body); mode = data.get('runtime_mode'); ok = r.getcode() in range(200, 300) and data.get('status') == 'ok' and mode == 'LOCAL_MODE' and data.get('local_mode') is True and data.get('local_database_connected') is True; sys.exit(0 if ok else (2 if mode == 'CLOUD_MODE' else 1))" "%HEALTH_URL%" > "%HEALTH_STDOUT%" 2> "%HEALTH_STDERR%"
set "HEALTH_EXIT=%errorlevel%"
>>"%LOG_FILE%" echo [%date% %time%] Health-check exit code: %HEALTH_EXIT%
if exist "%HEALTH_STDOUT%" type "%HEALTH_STDOUT%" >> "%LOG_FILE%"
if exist "%HEALTH_STDERR%" type "%HEALTH_STDERR%" >> "%LOG_FILE%"
if exist "%HEALTH_STDOUT%" del "%HEALTH_STDOUT%" >nul 2>nul
if exist "%HEALTH_STDERR%" del "%HEALTH_STDERR%" >nul 2>nul
exit /b %HEALTH_EXIT%
