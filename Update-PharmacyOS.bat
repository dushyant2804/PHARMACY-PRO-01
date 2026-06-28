@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ================================================================
REM PharmacyOS Windows auto-updater
REM - Pulls backend and frontend source from GitHub
REM - Downloads the latest frontend dist artifact through GitHub REST API
REM - Replaces only the frontend dist folder after backing it up
REM - Restarts PharmacyOS with the existing launcher
REM ================================================================

set "APP_ROOT=D:\pharmacy-app-v2"
set "BACKEND_DIR=%APP_ROOT%\backend"
set "FRONTEND_DIR=%APP_ROOT%\frontend"
set "DIST_DIR=%FRONTEND_DIR%\dist"
set "DIST_BACKUP=%FRONTEND_DIR%\dist_backup"
set "DIST_TEMP=%FRONTEND_DIR%\dist_temp"

REM Optional override for the GitHub owner or organization that stores the UI build artifact.
REM If this is blank, the updater automatically detects the owner from the frontend
REM git remote after pulling the frontend source repository.
REM Example: set "GITHUB_OWNER=your-org-or-user"
set "GITHUB_OWNER=%GITHUB_OWNER%"
set "GITHUB_REPO=pharmacyos-frontend-dist"
set "ARTIFACT_NAME=pharmacyos-frontend-dist"

REM Optional for private repositories or higher API limits:
REM set "GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx"

echo.
echo ================================================================
echo PharmacyOS automatic updater
echo ================================================================
echo.

where git >nul 2>nul
if errorlevel 1 (
    echo ERROR: git was not found in PATH. Update stopped safely.
    pause
    exit /b 1
)

echo [1/5] Updating backend code...
if not exist "%BACKEND_DIR%" (
    echo ERROR: Backend folder not found: %BACKEND_DIR%
    echo Update stopped safely.
    pause
    exit /b 1
)
cd /d "%BACKEND_DIR%" || goto :GitFail
git pull origin main
if errorlevel 1 goto :GitFail
echo Backend updated successfully
echo.

echo [2/5] Updating frontend source code...
if not exist "%FRONTEND_DIR%" (
    echo ERROR: Frontend folder not found: %FRONTEND_DIR%
    echo Update stopped safely.
    pause
    exit /b 1
)
cd /d "%FRONTEND_DIR%" || goto :GitFail
git pull origin main
if errorlevel 1 goto :GitFail
echo Frontend updated successfully
echo.

if "%GITHUB_OWNER%"=="" call :DetectGitHubOwner
if "%GITHUB_OWNER%"=="" (
    echo ERROR: GitHub owner could not be detected for the UI artifact repository.
    echo Edit Update-PharmacyOS.bat and set GITHUB_OWNER to the owner of %GITHUB_REPO%.
    pause
    exit /b 1
)

echo [3/5] Downloading latest UI build...
call :DownloadAndInstallUi
if errorlevel 1 (
    echo Frontend update failed, using existing UI
) else (
    echo UI updated successfully
)
echo.

echo [4/5] Safety check complete. Local data, SQLite database, backups,
echo       logs, backend-run.bat, and launcher files were not modified.
echo.

echo [5/5] PharmacyOS restarting...
call :RestartPharmacyOS
if errorlevel 1 (
    echo WARNING: PharmacyOS launcher was not found. Please start PharmacyOS manually.
    pause
    exit /b 1
)

echo PharmacyOS restarting...
echo.
pause
exit /b 0

:GitFail
echo.
echo ERROR: git pull failed. Update stopped safely.
echo No UI files were replaced.
pause
exit /b 1

:DetectGitHubOwner
for /f "usebackq tokens=*" %%R in (`git config --get remote.origin.url`) do set "FRONTEND_REMOTE=%%R"
if "%FRONTEND_REMOTE%"=="" exit /b 0
for /f "usebackq tokens=*" %%O in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$u=$env:FRONTEND_REMOTE; if ($u -match 'github.com[:/]([^/]+)/') { $matches[1] }"`) do set "GITHUB_OWNER=%%O"
if not "%GITHUB_OWNER%"=="" echo Detected GitHub owner: %GITHUB_OWNER%
exit /b 0

:DownloadAndInstallUi
set "PS_SCRIPT=%TEMP%\pharmacyos_update_ui_%RANDOM%%RANDOM%.ps1"

> "%PS_SCRIPT%" echo $ErrorActionPreference = 'Stop'
>> "%PS_SCRIPT%" echo try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch { }
>> "%PS_SCRIPT%" echo $owner = $env:GITHUB_OWNER
>> "%PS_SCRIPT%" echo $repo = $env:GITHUB_REPO
>> "%PS_SCRIPT%" echo $artifactName = $env:ARTIFACT_NAME
>> "%PS_SCRIPT%" echo $frontendDir = $env:FRONTEND_DIR
>> "%PS_SCRIPT%" echo $distDir = $env:DIST_DIR
>> "%PS_SCRIPT%" echo $backupDir = $env:DIST_BACKUP
>> "%PS_SCRIPT%" echo $tempDir = $env:DIST_TEMP
>> "%PS_SCRIPT%" echo $zipPath = Join-Path $env:TEMP 'pharmacyos-frontend-dist.zip'
>> "%PS_SCRIPT%" echo function New-Client { $wc = New-Object Net.WebClient; $wc.Headers.Add('User-Agent','PharmacyOS-Updater'); $wc.Headers.Add('Accept','application/vnd.github+json'); if ($env:GITHUB_TOKEN) { $wc.Headers.Add('Authorization','Bearer ' + $env:GITHUB_TOKEN) }; return $wc }
>> "%PS_SCRIPT%" echo $api = 'https://api.github.com/repos/' + $owner + '/' + $repo + '/actions/artifacts?per_page=100'
>> "%PS_SCRIPT%" echo $json = (New-Client).DownloadString($api)
>> "%PS_SCRIPT%" echo Add-Type -AssemblyName System.Web.Extensions
>> "%PS_SCRIPT%" echo $serializer = New-Object System.Web.Script.Serialization.JavaScriptSerializer
>> "%PS_SCRIPT%" echo $data = $serializer.DeserializeObject($json)
>> "%PS_SCRIPT%" echo $artifact = $null
>> "%PS_SCRIPT%" echo foreach ($a in $data['artifacts']) { if ($a['name'] -eq $artifactName -and -not $a['expired']) { $ok = $true; if ($a.ContainsKey('workflow_run') -and $a['workflow_run'] -and $a['workflow_run'].ContainsKey('conclusion') -and $a['workflow_run']['conclusion']) { $ok = ($a['workflow_run']['conclusion'] -eq 'success') }; if ($ok) { $artifact = $a; break } } }
>> "%PS_SCRIPT%" echo if (-not $artifact) { throw 'No non-expired successful artifact named ' + $artifactName + ' was found.' }
>> "%PS_SCRIPT%" echo if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
>> "%PS_SCRIPT%" echo (New-Client).DownloadFile($artifact['archive_download_url'], $zipPath)
>> "%PS_SCRIPT%" echo if (Test-Path $tempDir) { Remove-Item $tempDir -Recurse -Force }
>> "%PS_SCRIPT%" echo New-Item -ItemType Directory -Path $tempDir ^| Out-Null
>> "%PS_SCRIPT%" echo $shell = New-Object -ComObject Shell.Application
>> "%PS_SCRIPT%" echo $zip = $shell.NameSpace($zipPath)
>> "%PS_SCRIPT%" echo $dest = $shell.NameSpace($tempDir)
>> "%PS_SCRIPT%" echo if (-not $zip -or -not $dest) { throw 'Unable to open artifact zip.' }
>> "%PS_SCRIPT%" echo $dest.CopyHere($zip.Items(), 16)
>> "%PS_SCRIPT%" echo Start-Sleep -Seconds 3
>> "%PS_SCRIPT%" echo if ((Get-ChildItem -Path $tempDir -Force ^| Measure-Object).Count -eq 0) { throw 'Extracted artifact is empty.' }
>> "%PS_SCRIPT%" echo if (Test-Path $backupDir) { Remove-Item $backupDir -Recurse -Force }
>> "%PS_SCRIPT%" echo if (Test-Path $distDir) { Move-Item $distDir $backupDir }
>> "%PS_SCRIPT%" echo Move-Item $tempDir $distDir
>> "%PS_SCRIPT%" echo if (Test-Path $zipPath) { Remove-Item $zipPath -Force }

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS_SCRIPT%"
set "PS_EXIT=%ERRORLEVEL%"
if exist "%PS_SCRIPT%" del "%PS_SCRIPT%" >nul 2>nul
exit /b %PS_EXIT%

:RestartPharmacyOS
cd /d "%APP_ROOT%" >nul 2>nul
if exist "%APP_ROOT%\PharmacyOS-Launch.vbs" (
    start "" wscript.exe "%APP_ROOT%\PharmacyOS-Launch.vbs"
    exit /b 0
)
if exist "%APP_ROOT%\PharmacyOS-Launcher.bat" (
    start "" "%APP_ROOT%\PharmacyOS-Launcher.bat"
    exit /b 0
)
exit /b 1
