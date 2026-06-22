@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM PharmacyOS Windows desktop launcher.
REM Starts the existing local backend/database, waits for health, then opens Chrome app mode.
set "BASE_DIR=%~dp0"
if "%BASE_DIR:~-1%"=="\" set "BASE_DIR=%BASE_DIR:~0,-1%"
cd /d "%BASE_DIR%"

set "APP_DIR=%BASE_DIR%"
set "PHARMACYOS_MODE=LOCAL_MODE"
set "DATA_DIR=%BASE_DIR%\data"
set "LOCAL_DATA_DIR=%BASE_DIR%\local_data"
set "LOCAL_DB_PATH=%LOCAL_DATA_DIR%\pharmacyos.sqlite3"
set "BACKUP_DIR=%BASE_DIR%\backups"
set "UPLOAD_DIR=%BASE_DIR%\uploads"
set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\pharmacyos-local.log"
set "HEALTH_URL=http://localhost:8000/api/health"
set "APP_URL=http://localhost:8000"

if not exist "%LOCAL_DATA_DIR%" mkdir "%LOCAL_DATA_DIR%"
if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"
if not exist "%UPLOAD_DIR%" mkdir "%UPLOAD_DIR%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo. 
echo ===============================================
echo  PharmacyOS Local Desktop Launcher
echo ===============================================
echo Mode: LOCAL_MODE
echo Database: %LOCAL_DB_PATH%
echo Backups:  %BACKUP_DIR%
echo Uploads:  %UPLOAD_DIR%
echo Log file: %LOG_FILE%
echo.
echo [%date% %time%] PharmacyOS launcher starting.>>"%LOG_FILE%"
echo [%date% %time%] Log file=%LOG_FILE%>>"%LOG_FILE%"
echo [%date% %time%] Database=%LOCAL_DB_PATH% Backups=%BACKUP_DIR% Uploads=%UPLOAD_DIR%>>"%LOG_FILE%"

powershell -NoProfile -ExecutionPolicy Bypass -Command "try { Invoke-WebRequest -UseBasicParsing '%HEALTH_URL%' -TimeoutSec 2 | Out-Null; exit 0 } catch { exit 1 }" >nul 2>nul
if errorlevel 1 (
    echo Starting local backend on http://localhost:8000 ...
    echo [%date% %time%] Starting backend.>>"%LOG_FILE%"
    start "PharmacyOS Local Backend" /min cmd /c ""cd /d "%APP_DIR%" && python -m uvicorn server:app --host 127.0.0.1 --port 8000 >> "%LOG_FILE%" 2>>&1""
) else (
    echo Local backend is already running.
    echo [%date% %time%] Backend already running.>>"%LOG_FILE%"
)

echo Waiting for PharmacyOS health check: %HEALTH_URL%
set "READY="
for /L %%I in (1,1,60) do (
    powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing '%HEALTH_URL%' -TimeoutSec 2; if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) { exit 0 } else { exit 1 } } catch { exit 1 }" >nul 2>nul
    if not errorlevel 1 (
        set "READY=1"
        goto :OPEN_APP
    )
    echo   Waiting... %%I/60
    timeout /t 2 /nobreak >nul
)

echo.
echo ERROR: PharmacyOS did not become healthy within 2 minutes.
echo Check %LOG_FILE% for details. No data was deleted or changed by this launcher.
echo [%date% %time%] Health check timeout.>>"%LOG_FILE%"
pause
exit /b 1

:OPEN_APP
echo PharmacyOS local backend is healthy.
echo [%date% %time%] Backend healthy. Opening app window.>>"%LOG_FILE%"

set "CHROME_EXE="
if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%ProgramFiles%\Google\Chrome\Application\chrome.exe"
if not defined CHROME_EXE if exist "%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"
if not defined CHROME_EXE if exist "%LocalAppData%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%LocalAppData%\Google\Chrome\Application\chrome.exe"

if defined CHROME_EXE (
    echo Opening PharmacyOS in Chrome app window...
    start "PharmacyOS" "%CHROME_EXE%" --app="%APP_URL%" --new-window
) else (
    echo Chrome was not found in the standard locations. Opening default browser instead.
    start "" "%APP_URL%"
)

echo.
echo PharmacyOS is ready.
echo Keep the backend window running while using PharmacyOS.
echo To stop safely, double click PharmacyOS-Stop.bat.
echo.
pause
