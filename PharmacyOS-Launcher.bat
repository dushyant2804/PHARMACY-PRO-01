@echo off
setlocal EnableExtensions

echo Starting PharmacyOS...
start "PharmacyOS Backend" /MIN cmd /c ""D:\pharmacy-app-v2\backend\backend-run.bat""

echo Waiting for backend...

:WAIT_FOR_BACKEND
curl -fs "http://127.0.0.1:8000/api/health" 2>nul | findstr /I /C:"\"status\":\"ok\"" /C:"\"status\": \"ok\"" >nul
if errorlevel 1 (
    timeout /t 2 /nobreak >nul
    goto WAIT_FOR_BACKEND
)

echo Backend ready. Opening app...
start "" "http://127.0.0.1:8000"
exit /b 0
