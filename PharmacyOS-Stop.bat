@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Safe local stop: request app-exit backup first, then stop the local backend process.
cd /d "%~dp0"

set "LOG_DIR=%~dp0logs"
set "LOG_FILE=%LOG_DIR%\pharmacyos-local.log"
set "HEALTH_URL=http://localhost:8000/api/health"
set "EXIT_BACKUP_URL=http://localhost:8000/api/local/app-exit"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo.
echo ===============================================
echo  PharmacyOS Safe Local Stop
echo ===============================================
echo [%date% %time%] Stop requested.>>"%LOG_FILE%"

echo Checking local backend...
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { Invoke-WebRequest -UseBasicParsing '%HEALTH_URL%' -TimeoutSec 2 | Out-Null; exit 0 } catch { exit 1 }" >nul 2>nul
if errorlevel 1 (
    echo No running PharmacyOS backend was found on http://localhost:8000.
    echo [%date% %time%] Backend not reachable.>>"%LOG_FILE%"
) else (
    echo Backend is running. Requesting app-exit backup...
    echo [%date% %time%] Requesting app-exit backup.>>"%LOG_FILE%"
    powershell -NoProfile -ExecutionPolicy Bypass -Command "try { Invoke-RestMethod -Method Post -Uri '%EXIT_BACKUP_URL%' -TimeoutSec 60 | ConvertTo-Json -Compress | Out-File -Append -Encoding utf8 '%LOG_FILE%'; exit 0 } catch { ('App-exit backup request failed: ' + $_.Exception.Message) | Out-File -Append -Encoding utf8 '%LOG_FILE%'; exit 1 }"
    if errorlevel 1 (
        echo Backup request could not be confirmed. Continuing with safe stop attempt; no data will be deleted.
    ) else (
        echo App-exit backup request completed.
    )
)

echo Stopping PharmacyOS local backend on port 8000...
set "FOUND_PID="
for /f "tokens=5" %%P in ('netstat -ano ^| findstr ":8000" ^| findstr "LISTENING"') do (
    set "FOUND_PID=%%P"
    taskkill /PID %%P /F >nul 2>nul
)

if defined FOUND_PID (
    echo PharmacyOS local backend stopped.
    echo [%date% %time%] Backend stopped PID=!FOUND_PID!.>>"%LOG_FILE%"
) else (
    echo No PharmacyOS local backend process was found on port 8000.
    echo [%date% %time%] No backend PID found.>>"%LOG_FILE%"
)

echo No data, backups, uploads, or database files were deleted.
echo.
pause
