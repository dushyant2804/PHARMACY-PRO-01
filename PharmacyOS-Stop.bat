@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Optional PharmacyOS local backend stop script for Windows 7.
REM Stops the process listening on 127.0.0.1:8000 without touching data files.

set "FOUND_PID="
for /f "tokens=5" %%P in ('netstat -ano ^| findstr ":8000" ^| findstr "LISTENING"') do (
    set "FOUND_PID=%%P"
    taskkill /PID %%P /F >nul 2>nul
)

if defined FOUND_PID (
    echo PharmacyOS local backend stopped.
) else (
    echo No PharmacyOS local backend process was found on port 8000.
)

exit /b 0
