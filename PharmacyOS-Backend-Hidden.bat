@echo off
setlocal EnableExtensions

REM Permanent hidden backend worker for PharmacyOS Windows desktop launcher.
REM This file is run hidden by PharmacyOS-Backend-Hidden.vbs.
set "BASE_DIR=%~dp0"
if "%BASE_DIR:~-1%"=="\" set "BASE_DIR=%BASE_DIR:~0,-1%"

set "BACKEND_DIR=D:\pharmacy-app-v2\backend"
set "APP_DIR=D:\pharmacy-app-v2"
set "PHARMACYOS_MODE=LOCAL_MODE"
set "DATA_DIR=%APP_DIR%\data"
set "LOCAL_DATA_DIR=%APP_DIR%\local_data"
set "LOCAL_DB_PATH=%LOCAL_DATA_DIR%\pharmacyos.sqlite3"
set "BACKUP_DIR=%APP_DIR%\backups"
set "UPLOAD_DIR=%APP_DIR%\uploads"
set "LOG_DIR=%APP_DIR%\logs"
set "BACKEND_OUTPUT_LOG=%LOG_DIR%\pharmacyos-backend-output.log"

if not exist "%LOCAL_DATA_DIR%" mkdir "%LOCAL_DATA_DIR%"
if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"
if not exist "%UPLOAD_DIR%" mkdir "%UPLOAD_DIR%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BACKEND_DIR%"
>> "%BACKEND_OUTPUT_LOG%" echo.
>> "%BACKEND_OUTPUT_LOG%" echo ===============================================
>> "%BACKEND_OUTPUT_LOG%" echo [%date% %time%] Hidden backend starting in %CD%.
>> "%BACKEND_OUTPUT_LOG%" echo [%date% %time%] Command: python -m uvicorn server:app --host 127.0.0.1 --port 8000
python -m uvicorn server:app --host 127.0.0.1 --port 8000 >> D:\pharmacy-app-v2\logs\pharmacyos-backend-output.log 2>&1
set "BACKEND_EXIT=%errorlevel%"
>> "%BACKEND_OUTPUT_LOG%" echo [%date% %time%] Hidden backend exited with errorlevel %BACKEND_EXIT%.
exit /b %BACKEND_EXIT%
