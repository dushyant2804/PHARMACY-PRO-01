@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM PharmacyOS Windows desktop launcher.
REM Starts the existing local backend/database, waits for health, then opens Chrome app mode.
set "BASE_DIR=%~dp0"
if "%BASE_DIR:~-1%"=="\" set "BASE_DIR=%BASE_DIR:~0,-1%"
cd /d "%BASE_DIR%"

set "APP_DIR=%BASE_DIR%"
set "PHARMACYOS_MODE=LOCAL_MODE"
set "LOCAL_DB_PATH=%APP_DIR%local_data\pharmacyos.sqlite3"
set "BACKUP_DIR=%APP_DIR%backups"
set "UPLOAD_DIR=%APP_DIR%uploads"
set "LOG_DIR=%APP_DIR%logs"
set "LOG_FILE=%LOG_DIR%pharmacyos-local.log"
set "BACKEND_CMD_FILE=%LOG_DIR%pharmacyos-backend.cmd"
set "UVICORN_CMD=python -m uvicorn server:app --host 127.0.0.1 --port 8000"
set "HEALTH_URL=http://127.0.0.1:8000/api/health"
set "APP_URL=http://127.0.0.1:8000"
set "DATA_DIR=%BASE_DIR%\data"
set "LOCAL_DATA_DIR=%BASE_DIR%\local_data"
set "LOCAL_DB_PATH=%LOCAL_DATA_DIR%\pharmacyos.sqlite3"
set "BACKUP_DIR=%BASE_DIR%\backups"
set "UPLOAD_DIR=%BASE_DIR%\uploads"
set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\pharmacyos-local.log"
set "BACKEND_CMD_FILE=%LOG_DIR%\pharmacyos-backend.cmd"
set "BACKEND_OUTPUT_LOG=%LOG_DIR%\pharmacyos-backend-output.log"
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
echo Backend output log: %BACKEND_OUTPUT_LOG%
echo.
echo [%date% %time%] PharmacyOS launcher starting.>>"%LOG_FILE%"
echo [%date% %time%] Log file=%LOG_FILE%>>"%LOG_FILE%"
echo [%date% %time%] Backend output log=%BACKEND_OUTPUT_LOG%>>"%LOG_FILE%"
echo [%date% %time%] Database=%LOCAL_DB_PATH% Backups=%BACKUP_DIR% Uploads=%UPLOAD_DIR%>>"%LOG_FILE%"
echo [%date% %time%] Health URL=%HEALTH_URL%>>"%LOG_FILE%"
echo [%date% %time%] Launch command: %UVICORN_CMD%>>"%LOG_FILE%"

call :CHECK_HEALTH >nul 2>nul
if errorlevel 1 (
    echo Starting local backend on http://127.0.0.1:8000 ...
    echo Launch command:
    echo %UVICORN_CMD%
    echo [%date% %time%] Starting backend.>>"%LOG_FILE%"
    echo Backend output log: %BACKEND_OUTPUT_LOG%
    echo @echo off>"%BACKEND_CMD_FILE%"
    echo cd /d "%BASE_DIR%">>"%BACKEND_CMD_FILE%"
    echo set PHARMACYOS_MODE=LOCAL_MODE>>"%BACKEND_CMD_FILE%"
    echo set LOCAL_DB_PATH=%LOCAL_DB_PATH%>>"%BACKEND_CMD_FILE%"
    echo set BACKUP_DIR=%BACKUP_DIR%>>"%BACKEND_CMD_FILE%"
    echo set UPLOAD_DIR=%UPLOAD_DIR%>>"%BACKEND_CMD_FILE%"
    echo %UVICORN_CMD% ^>^> "%BACKEND_OUTPUT_LOG%" 2^>^&1>>"%BACKEND_CMD_FILE%"
    wmic process call create "cmd.exe /c ""%BACKEND_CMD_FILE%""" > "%TEMP%\pharmacyos-wmic.txt" 2>nul
    set "BACKEND_PID="
    for /F "tokens=2 delims=;=" %%P in ('find "ProcessId" ^< "%TEMP%\pharmacyos-wmic.txt"') do set "BACKEND_PID=%%P"
    if defined BACKEND_PID (
        set "BACKEND_PID=!BACKEND_PID: =!"
        set "BACKEND_PID=!BACKEND_PID:.=!"
        echo Backend launcher process id: !BACKEND_PID!
        echo [%date% %time%] Backend launcher process id: !BACKEND_PID!>>"%LOG_FILE%"
    ) else (
        echo Backend process id unavailable.
        echo [%date% %time%] Backend process id unavailable.>>"%LOG_FILE%"
    )
) else (
    echo Local backend is already running.
    echo [%date% %time%] Backend already running; health check succeeded before launch.>>"%LOG_FILE%"
)

echo Waiting for PharmacyOS health check: %HEALTH_URL%
set "READY="
for /L %%I in (1,1,60) do (
    call :CHECK_HEALTH >nul 2>nul
    if not errorlevel 1 (
        set "READY=1"
        goto :OPEN_APP
    )
    echo   Waiting... %%I/60
    timeout /t 2 /nobreak >nul
)

echo.
echo ERROR: PharmacyOS did not become healthy within 2 minutes.
echo Check %LOG_FILE% for launcher details. No data was deleted or changed by this launcher.
echo Backend output log: %BACKEND_OUTPUT_LOG%
if exist "%BACKEND_OUTPUT_LOG%" (
    echo.
    echo Last backend output log lines:
    powershell -NoProfile -Command "Get-Content -LiteralPath '%BACKEND_OUTPUT_LOG%' -Tail 40"
) else (
    echo Backend output log was not created.
)
echo [%date% %time%] Health check failure: timeout waiting for %HEALTH_URL%.>>"%LOG_FILE%"
pause
exit /b 1

:OPEN_APP
echo PharmacyOS local backend is healthy.
echo [%date% %time%] Health check success: %HEALTH_URL%. Opening app window.>>"%LOG_FILE%"

set "CHROME_EXE="
if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%ProgramFiles%\Google\Chrome\Application\chrome.exe"
if not defined CHROME_EXE if exist "%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"
if not defined CHROME_EXE if exist "%LocalAppData%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%LocalAppData%\Google\Chrome\Application\chrome.exe"

if defined CHROME_EXE (
    echo Opening PharmacyOS in Chrome app window...
    start "PharmacyOS" "%CHROME_EXE%" --app="%APP_URL%" --new-window
) else (
    echo Chrome was not found in the standard locations. Opening default browser instead.
    start "PharmacyOS" "%APP_URL%"
)

echo.
goto :DONE

:CHECK_HEALTH
python -c "import sys, urllib.request; r = urllib.request.urlopen(sys.argv[1], timeout=2); sys.exit(0 if 200 <= r.getcode() ^< 500 else 1)" "%HEALTH_URL%"
exit /b %errorlevel%

:DONE
echo PharmacyOS is ready.
echo Keep the backend window running while using PharmacyOS.
echo To stop safely, double click PharmacyOS-Stop.bat.
echo.
pause
