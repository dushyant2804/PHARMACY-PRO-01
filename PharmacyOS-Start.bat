@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM PharmacyOS Windows desktop launcher.
REM Starts the existing local backend/database, waits for health, then opens Chrome app mode.
set "BASE_DIR=%~dp0"
if "%BASE_DIR:~-1%"=="\" set "BASE_DIR=%BASE_DIR:~0,-1%"
cd /d "%BASE_DIR%"

set "APP_DIR=%BASE_DIR%"
set "PHARMACYOS_MODE=LOCAL_MODE"
set "DATA_DIR=%APP_DIR%\data"
set "LOCAL_DATA_DIR=%APP_DIR%\local_data"
set "LOCAL_DB_PATH=%LOCAL_DATA_DIR%\pharmacyos.sqlite3"
set "BACKUP_DIR=%APP_DIR%\backups"
set "UPLOAD_DIR=%APP_DIR%\uploads"
set "LOG_DIR=%APP_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\pharmacyos-local.log"
set "BACKEND_CMD_FILE=%LOG_DIR%\pharmacyos-backend.cmd"
set "BACKEND_OUTPUT_LOG=%LOG_DIR%\pharmacyos-backend-output.log"
set "UVICORN_CMD=python -m uvicorn server:app --host 127.0.0.1 --port 8000"
set "HEALTH_URL=http://127.0.0.1:8000/api/health"
set "APP_URL=http://127.0.0.1:8000"

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
echo [%date% %time%] Backend command file=%BACKEND_CMD_FILE%>>"%LOG_FILE%"
echo [%date% %time%] Database=%LOCAL_DB_PATH% Backups=%BACKUP_DIR% Uploads=%UPLOAD_DIR%>>"%LOG_FILE%"
echo [%date% %time%] Health URL=%HEALTH_URL%>>"%LOG_FILE%"
echo [%date% %time%] Launch command: %UVICORN_CMD%>>"%LOG_FILE%"

call :CHECK_HEALTH >nul 2>nul
if errorlevel 1 (
    echo Starting local backend on http://127.0.0.1:8000 ...
    echo Launch command:
    echo %UVICORN_CMD%
    echo [%date% %time%] Starting backend with Windows start command.>>"%LOG_FILE%"
    echo Backend output log: %BACKEND_OUTPUT_LOG%
    echo Backend command file: %BACKEND_CMD_FILE%
    call :WRITE_BACKEND_CMD
    if errorlevel 1 (
        echo ERROR: Could not create backend command file: %BACKEND_CMD_FILE%
        echo [%date% %time%] Failed to create backend command file.>>"%LOG_FILE%"
        pause
        exit /b 1
    )
    echo Backend command file created: yes
    start "PharmacyOS Backend" /MIN cmd.exe /c ""%BACKEND_CMD_FILE%""
    if errorlevel 1 (
        echo ERROR: Windows could not start the backend process.
        echo [%date% %time%] start command failed with errorlevel !errorlevel!.>>"%LOG_FILE%"
        pause
        exit /b 1
    )
    echo [%date% %time%] Backend start command issued.>>"%LOG_FILE%"
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
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-Content -LiteralPath '%BACKEND_OUTPUT_LOG%' -Tail 40"
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

:WRITE_BACKEND_CMD
(
    echo @echo off
    echo cd /d "%BASE_DIR%"
    echo set "PHARMACYOS_MODE=LOCAL_MODE"
    echo set "LOCAL_DB_PATH=%LOCAL_DB_PATH%"
    echo set "BACKUP_DIR=%BACKUP_DIR%"
    echo set "UPLOAD_DIR=%UPLOAD_DIR%"
    echo echo [%%date%% %%time%%] Backend command starting. ^>^> "%BACKEND_OUTPUT_LOG%"
    echo echo Command: %UVICORN_CMD% ^>^> "%BACKEND_OUTPUT_LOG%"
    echo %UVICORN_CMD% ^>^> "%BACKEND_OUTPUT_LOG%" 2^>^&1
    echo echo [%%date%% %%time%%] Backend command exited with errorlevel %%errorlevel%%. ^>^> "%BACKEND_OUTPUT_LOG%"
) > "%BACKEND_CMD_FILE%"
set "BACKEND_CMD_CREATED=no"
set "BACKEND_CMD_SIZE="
if exist "%BACKEND_CMD_FILE%" (
    set "BACKEND_CMD_CREATED=yes"
    for %%A in ("%BACKEND_CMD_FILE%") do set "BACKEND_CMD_SIZE=%%~zA"
)
echo [%date% %time%] Backend command file path=%BACKEND_CMD_FILE%>>"%LOG_FILE%"
echo [%date% %time%] Backend command file created=!BACKEND_CMD_CREATED!>>"%LOG_FILE%"
if defined BACKEND_CMD_SIZE echo [%date% %time%] Backend command file size=!BACKEND_CMD_SIZE! bytes>>"%LOG_FILE%"
if exist "%BACKEND_CMD_FILE%" (
    if defined BACKEND_CMD_SIZE (
        if !BACKEND_CMD_SIZE! GTR 0 exit /b 0
    )
)
exit /b 1

:CHECK_HEALTH
python -c "import json, sys, urllib.request; r = urllib.request.urlopen(sys.argv[1], timeout=2); data = json.loads(r.read().decode('utf-8')); sys.exit(0 if 200 <= r.getcode() ^< 300 and data.get('status') == 'ok' else 1)" "%HEALTH_URL%"
exit /b %errorlevel%

:DONE
echo PharmacyOS is ready.
echo Keep the backend running while using PharmacyOS.
echo To stop safely, double click PharmacyOS-Stop.bat.
echo.
pause
