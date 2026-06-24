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
set "HEALTH_CHECK_PY=%BASE_DIR%\PharmacyOS-Health-Check.py"
set "HEALTH_URL=http://127.0.0.1:8000/api/health"
set "DOCS_URL=http://127.0.0.1:8000/docs"
set "APP_URL=http://127.0.0.1:8000"
set "FINAL_EXIT_CODE=1"

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
>>"%LOG_FILE%" echo [%date% %time%] Health-check helper=%HEALTH_CHECK_PY%
>>"%LOG_FILE%" echo [%date% %time%] Health URL=%HEALTH_URL%

if not exist "%BACKEND_VBS_FILE%" (
    set "FINAL_EXIT_CODE=1"
    >>"%LOG_FILE%" echo [%date% %time%] ERROR: Missing backend hidden launcher: %BACKEND_VBS_FILE%
    >>"%LOG_FILE%" echo [%date% %time%] Final decision: failure; final exit code !FINAL_EXIT_CODE!.
    exit /b !FINAL_EXIT_CODE!
)
if not exist "%BACKEND_BAT_FILE%" (
    set "FINAL_EXIT_CODE=1"
    >>"%LOG_FILE%" echo [%date% %time%] ERROR: Missing backend hidden command: %BACKEND_BAT_FILE%
    >>"%LOG_FILE%" echo [%date% %time%] Final decision: failure; final exit code !FINAL_EXIT_CODE!.
    exit /b !FINAL_EXIT_CODE!
)
if not exist "%HEALTH_CHECK_PY%" (
    set "FINAL_EXIT_CODE=1"
    >>"%LOG_FILE%" echo [%date% %time%] ERROR: Missing health-check helper: %HEALTH_CHECK_PY%
    >>"%LOG_FILE%" echo [%date% %time%] Final decision: failure; final exit code !FINAL_EXIT_CODE!.
    exit /b !FINAL_EXIT_CODE!
)

call :CHECK_HEALTH 0
if not errorlevel 1 (
    >>"%LOG_FILE%" echo [%date% %time%] Backend already running; opening app.
    goto :OPEN_APP
)

>>"%LOG_FILE%" echo [%date% %time%] Backend not healthy yet. Starting permanent hidden backend launcher.
wscript.exe "%BACKEND_VBS_FILE%"
if errorlevel 1 (
    set "FINAL_EXIT_CODE=1"
    >>"%LOG_FILE%" echo [%date% %time%] ERROR: Backend hidden launcher failed with errorlevel !errorlevel!.
    >>"%LOG_FILE%" echo [%date% %time%] Final decision: failure; final exit code !FINAL_EXIT_CODE!.
    exit /b !FINAL_EXIT_CODE!
)
>>"%LOG_FILE%" echo [%date% %time%] Hidden backend start command issued.

for /L %%I in (1,1,60) do (
    call :CHECK_HEALTH %%I
    if not errorlevel 1 goto :OPEN_APP
    >>"%LOG_FILE%" echo [%date% %time%] Waiting for health check %%I/60.
    timeout /t 2 /nobreak >nul
)

set "FINAL_EXIT_CODE=1"
>>"%LOG_FILE%" echo [%date% %time%] ERROR: PharmacyOS did not become reachable at %HEALTH_URL% within 2 minutes.
>>"%LOG_FILE%" echo [%date% %time%] Final decision: failure; backend never became reachable after full timeout; final exit code %FINAL_EXIT_CODE%.
exit /b %FINAL_EXIT_CODE%

:OPEN_APP
set "FINAL_EXIT_CODE=0"
>>"%LOG_FILE%" echo [%date% %time%] Final decision: success; %HEALTH_URL% returned HTTP 200; opening PharmacyOS app window; final exit code %FINAL_EXIT_CODE%.
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

>>"%LOG_FILE%" echo [%date% %time%] Quiet launcher finished successfully with final exit code %FINAL_EXIT_CODE%.
exit /b %FINAL_EXIT_CODE%

:CHECK_HEALTH
set "HEALTH_ATTEMPT=%~1"
if "%HEALTH_ATTEMPT%"=="" set "HEALTH_ATTEMPT=0"
set "HEALTH_RESULT=%TEMP%\pharmacyos-health-result-%RANDOM%.log"
python "%HEALTH_CHECK_PY%" "%HEALTH_URL%" "%DOCS_URL%" "%HEALTH_RESULT%" >nul 2>&1
set "HEALTH_EXIT=%errorlevel%"
>>"%LOG_FILE%" echo [%date% %time%] Health-check attempt %HEALTH_ATTEMPT%; URL=%HEALTH_URL%
if exist "%HEALTH_RESULT%" type "%HEALTH_RESULT%" >> "%LOG_FILE%"
if not exist "%HEALTH_RESULT%" >>"%LOG_FILE%" echo Health-check helper failed before writing result.
if exist "%HEALTH_RESULT%" del "%HEALTH_RESULT%" >nul 2>nul
exit /b %HEALTH_EXIT%
