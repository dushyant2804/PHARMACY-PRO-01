@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM PharmacyOS Windows desktop launcher.
REM Starts the existing local backend/database, waits for health, then opens Chrome app mode.
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
echo Backend dir: %BACKEND_DIR%
echo Log file: %LOG_FILE%
echo.
echo [%date% %time%] PharmacyOS launcher starting.>>"%LOG_FILE%"
echo [%date% %time%] Log file=%LOG_FILE%>>"%LOG_FILE%"
echo [%date% %time%] Backend command file=%BACKEND_CMD_FILE%>>"%LOG_FILE%"
echo [%date% %time%] Backend dir=%BACKEND_DIR%>>"%LOG_FILE%"
echo [%date% %time%] Database=%LOCAL_DB_PATH% Backups=%BACKUP_DIR% Uploads=%UPLOAD_DIR%>>"%LOG_FILE%"
echo [%date% %time%] Health URL=%HEALTH_URL%>>"%LOG_FILE%"
echo [%date% %time%] Launch command: %UVICORN_CMD%>>"%LOG_FILE%"

call :CHECK_HEALTH
if errorlevel 2 (
    echo Local backend started but PHARMACYOS_MODE was not applied.
    echo [%date% %time%] Health check failed because runtime_mode was CLOUD_MODE.>>"%LOG_FILE%"
    pause
    exit /b 1
)
if errorlevel 1 (
    echo Starting local backend on http://127.0.0.1:8000 ...
    echo Launch command:
    echo %UVICORN_CMD%
    echo [%date% %time%] Starting backend with Windows start command.>>"%LOG_FILE%"
    echo Backend command file: %BACKEND_CMD_FILE%
    call :WRITE_BACKEND_CMD
    if errorlevel 1 (
        echo ERROR: Could not create backend command file: %BACKEND_CMD_FILE%
        echo [%date% %time%] Failed to create backend command file.>>"%LOG_FILE%"
        pause
        exit /b 1
    )
    echo Backend command file created: yes
    start "PharmacyOS Backend" /MIN cmd.exe /k ""%BACKEND_CMD_FILE%""
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
    goto :OPEN_APP
)

echo Waiting for PharmacyOS health check: %HEALTH_URL%
set "READY="
for /L %%I in (1,1,60) do (
    call :CHECK_HEALTH
    if errorlevel 2 (
        echo Local backend started but PHARMACYOS_MODE was not applied.
        echo [%date% %time%] Health check failed because runtime_mode was CLOUD_MODE.>>"%LOG_FILE%"
        pause
        exit /b 1
    )
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
echo The backend window was started visible/minimized. Restore it to review backend errors.
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
    echo cd /d "%BACKEND_DIR%"
    echo set "PHARMACYOS_MODE=LOCAL_MODE"
    echo set "LOCAL_DB_PATH=%LOCAL_DB_PATH%"
    echo set "BACKUP_DIR=%BACKUP_DIR%"
    echo set "UPLOAD_DIR=%UPLOAD_DIR%"
    echo echo [%%date%% %%time%%] Backend command starting in %%CD%%.
    echo echo Command: %UVICORN_CMD%
    echo %UVICORN_CMD%
    echo echo [%%date%% %%time%%] Backend command exited with errorlevel %%errorlevel%%.
    echo echo Leave this window open to inspect backend errors.
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
set "HEALTH_STDOUT=%TEMP%\pharmacyos-health-stdout-%RANDOM%.log"
set "HEALTH_STDERR=%TEMP%\pharmacyos-health-stderr-%RANDOM%.log"
echo Health-check command: python -c "import json, sys, urllib.request; url = sys.argv[1]; r = urllib.request.urlopen(url, timeout=2); body = r.read().decode('utf-8'); print('HTTP status: %%s' %% r.getcode()); print('Response body: %%s' %% body); data = json.loads(body); sys.exit(0 if r.getcode() in range(200, 300) and data.get('status') == 'ok' and data.get('runtime_mode') == 'LOCAL_MODE' and data.get('local_mode') is True and data.get('local_database_connected') is True else (2 if data.get('runtime_mode') == 'CLOUD_MODE' else 1))" "%HEALTH_URL%"
echo [%date% %time%] Health-check command: python -c "import json, sys, urllib.request; url = sys.argv[1]; r = urllib.request.urlopen(url, timeout=2); body = r.read().decode('utf-8'); print('HTTP status: %%s' %% r.getcode()); print('Response body: %%s' %% body); data = json.loads(body); sys.exit(0 if r.getcode() in range(200, 300) and data.get('status') == 'ok' and data.get('runtime_mode') == 'LOCAL_MODE' and data.get('local_mode') is True and data.get('local_database_connected') is True else (2 if data.get('runtime_mode') == 'CLOUD_MODE' else 1))" "%HEALTH_URL%">>"%LOG_FILE%"
python -c "import json, sys, urllib.request; url = sys.argv[1]; r = urllib.request.urlopen(url, timeout=2); body = r.read().decode('utf-8'); print('HTTP status: %%s' %% r.getcode()); print('Response body: %%s' %% body); data = json.loads(body); mode = data.get('runtime_mode'); ok = r.getcode() in range(200, 300) and data.get('status') == 'ok' and mode == 'LOCAL_MODE' and data.get('local_mode') is True and data.get('local_database_connected') is True; sys.exit(0 if ok else (2 if mode == 'CLOUD_MODE' else 1))" "%HEALTH_URL%" > "%HEALTH_STDOUT%" 2> "%HEALTH_STDERR%"
set "HEALTH_EXIT=%errorlevel%"
echo Health-check exit code: %HEALTH_EXIT%
echo [%date% %time%] Health-check exit code: %HEALTH_EXIT%>>"%LOG_FILE%"
echo Health-check stdout:
echo [%date% %time%] Health-check stdout:>>"%LOG_FILE%"
if exist "%HEALTH_STDOUT%" (
    type "%HEALTH_STDOUT%"
    type "%HEALTH_STDOUT%" >> "%LOG_FILE%"
) else (
    echo ^<stdout file missing^>
    echo ^<stdout file missing^>>>"%LOG_FILE%"
)
echo Health-check stderr:
echo [%date% %time%] Health-check stderr:>>"%LOG_FILE%"
if exist "%HEALTH_STDERR%" (
    type "%HEALTH_STDERR%"
    type "%HEALTH_STDERR%" >> "%LOG_FILE%"
) else (
    echo ^<stderr file missing^>
    echo ^<stderr file missing^>>>"%LOG_FILE%"
)
if exist "%HEALTH_STDOUT%" del "%HEALTH_STDOUT%" >nul 2>nul
if exist "%HEALTH_STDERR%" del "%HEALTH_STDERR%" >nul 2>nul
exit /b %HEALTH_EXIT%

:DONE
echo PharmacyOS is ready.
echo Keep the backend running while using PharmacyOS.
echo To stop safely, double click PharmacyOS-Stop.bat.
echo.
pause
