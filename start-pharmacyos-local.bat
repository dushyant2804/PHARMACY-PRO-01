@echo off
setlocal

REM Start PharmacyOS in local Windows desktop mode.
REM Keep all paths beside this launcher so cloud mode and cloud settings are unchanged.
cd /d "%~dp0"

set "PHARMACYOS_MODE=LOCAL_MODE"
set "LOCAL_DB_PATH=%~dp0local_data\pharmacyos.sqlite3"
set "BACKUP_DIR=%~dp0backups"
set "UPLOAD_DIR=%~dp0uploads"

if not exist "%~dp0local_data" mkdir "%~dp0local_data"
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"
if not exist "%UPLOAD_DIR%" mkdir "%UPLOAD_DIR%"

echo.
echo Starting PharmacyOS local server...
echo Local database: %LOCAL_DB_PATH%
echo Backups: %BACKUP_DIR%
echo Uploads: %UPLOAD_DIR%
echo.
echo Local PharmacyOS server running at http://localhost:8000
echo Keep this window open while using PharmacyOS.
echo To stop the server, close this window or double click stop-pharmacyos-local.bat.
echo.

python -m uvicorn server:app --host 127.0.0.1 --port 8000

echo.
echo PharmacyOS local server stopped.
pause
