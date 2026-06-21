@echo off
setlocal

echo Stopping PharmacyOS local server on http://localhost:8000 ...

set "FOUND_PID="
for /f "tokens=5" %%P in ('netstat -ano ^| findstr ":8000" ^| findstr "LISTENING"') do (
    set "FOUND_PID=%%P"
    taskkill /PID %%P /F >nul 2>nul
)

if defined FOUND_PID (
    echo PharmacyOS local server stopped.
) else (
    echo No PharmacyOS local server was found on port 8000.
)

pause
