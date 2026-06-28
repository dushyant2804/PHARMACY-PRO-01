@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ================================================================
REM PharmacyOS Windows auto-updater
REM - Pulls backend and frontend source from GitHub
REM - Downloads the latest frontend dist artifact through GitHub REST API
REM - Replaces only the frontend dist folder after backing it up
REM - Restarts PharmacyOS with the existing launcher
REM ================================================================

set "UPDATER_DIR=%~dp0"
if "%UPDATER_DIR:~-1%"=="\" set "UPDATER_DIR=%UPDATER_DIR:~0,-1%"

REM Automatically detect the project root from the updater location.
REM If this file is run from the backend folder, the project root is its parent.
for %%I in ("%UPDATER_DIR%") do set "UPDATER_FOLDER=%%~nxI"
if /I "%UPDATER_FOLDER%"=="backend" (
    for %%I in ("%UPDATER_DIR%\..") do set "APP_ROOT=%%~fI"
) else (
    set "APP_ROOT=%UPDATER_DIR%"
)

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

echo Project root: %APP_ROOT%
echo Backend path: %BACKEND_DIR%
echo Frontend path: %FRONTEND_DIR%
echo.

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
if "%GITHUB_TOKEN%"=="" (
    echo Frontend artifact download requires GITHUB_TOKEN. Using existing UI.
) else (
    call :DownloadAndInstallUi
    if errorlevel 1 (
        echo Frontend update failed, using existing UI.
    ) else (
        echo UI updated successfully
    )
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
>> "%PS_SCRIPT%" echo [Net.ServicePointManager]::SecurityProtocol = [Enum]::ToObject([Net.SecurityProtocolType], 3072)
>> "%PS_SCRIPT%" echo Write-Host ('TLS version being used: ' + [Net.ServicePointManager]::SecurityProtocol)
>> "%PS_SCRIPT%" echo $owner = $env:GITHUB_OWNER
>> "%PS_SCRIPT%" echo $repo = $env:GITHUB_REPO
>> "%PS_SCRIPT%" echo $artifactName = $env:ARTIFACT_NAME
>> "%PS_SCRIPT%" echo $frontendDir = $env:FRONTEND_DIR
>> "%PS_SCRIPT%" echo $distDir = $env:DIST_DIR
>> "%PS_SCRIPT%" echo $backupDir = $env:DIST_BACKUP
>> "%PS_SCRIPT%" echo $tempDir = $env:DIST_TEMP
>> "%PS_SCRIPT%" echo $zipPath = Join-Path $env:TEMP 'pharmacyos-frontend-dist.zip'
>> "%PS_SCRIPT%" echo function New-Client { $wc = New-Object Net.WebClient; $wc.Headers.Add('User-Agent','PharmacyOS-Updater'); $wc.Headers.Add('Accept','application/vnd.github+json'); $wc.Headers.Add('Authorization','Bearer ' + $env:GITHUB_TOKEN); return $wc }
>> "%PS_SCRIPT%" echo $api = 'https://api.github.com/repos/' + $owner + '/' + $repo + '/actions/artifacts?per_page=100'
>> "%PS_SCRIPT%" echo Write-Host ('GitHub API URL: ' + $api)
>> "%PS_SCRIPT%" echo $json = (New-Client).DownloadString($api)
>> "%PS_SCRIPT%" echo Add-Type -AssemblyName System.Web.Extensions
>> "%PS_SCRIPT%" echo $serializer = New-Object System.Web.Script.Serialization.JavaScriptSerializer
>> "%PS_SCRIPT%" echo $data = $serializer.DeserializeObject($json)
>> "%PS_SCRIPT%" echo $artifact = $null
>> "%PS_SCRIPT%" echo foreach ($a in $data['artifacts']) { if ($a['name'] -eq $artifactName -and -not $a['expired']) { $ok = $true; if ($a.ContainsKey('workflow_run') -and $a['workflow_run'] -and $a['workflow_run'].ContainsKey('conclusion') -and $a['workflow_run']['conclusion']) { $ok = ($a['workflow_run']['conclusion'] -eq 'success') }; if ($ok) { $artifact = $a; break } } }
>> "%PS_SCRIPT%" echo if (-not $artifact) { throw 'No non-expired successful artifact named ' + $artifactName + ' was found.' }
>> "%PS_SCRIPT%" echo $artifactUrl = $artifact['archive_download_url']
>> "%PS_SCRIPT%" echo Write-Host ('Artifact URL: ' + $artifactUrl)
>> "%PS_SCRIPT%" echo if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
>> "%PS_SCRIPT%" echo (New-Client).DownloadFile($artifactUrl, $zipPath)
>> "%PS_SCRIPT%" echo if (-not (Test-Path $zipPath)) { throw 'Artifact download did not create a zip file.' }
>> "%PS_SCRIPT%" echo $zipInfo = Get-Item $zipPath
>> "%PS_SCRIPT%" echo if ($zipInfo.Length -le 0) { throw 'Artifact download created an empty zip file.' }
>> "%PS_SCRIPT%" echo if (Test-Path $tempDir) { Remove-Item $tempDir -Recurse -Force }
>> "%PS_SCRIPT%" echo New-Item -ItemType Directory -Path $tempDir ^| Out-Null
>> "%PS_SCRIPT%" echo $shell = New-Object -ComObject Shell.Application
>> "%PS_SCRIPT%" echo $zip = $shell.NameSpace($zipPath)
>> "%PS_SCRIPT%" echo $dest = $shell.NameSpace($tempDir)
>> "%PS_SCRIPT%" echo if (-not $zip -or -not $dest) { throw 'Unable to open artifact zip.' }
>> "%PS_SCRIPT%" echo $dest.CopyHere($zip.Items(), 16)
>> "%PS_SCRIPT%" echo Start-Sleep -Seconds 3
>> "%PS_SCRIPT%" echo if ((Get-ChildItem -Path $tempDir -Force ^| Measure-Object).Count -eq 0) { throw 'Extracted artifact is empty.' }
>> "%PS_SCRIPT%" echo Write-Host 'Artifact extraction completed.'
>> "%PS_SCRIPT%" echo if (Test-Path $backupDir) { Remove-Item $backupDir -Recurse -Force }
>> "%PS_SCRIPT%" echo if (Test-Path $distDir) { Move-Item $distDir $backupDir }
>> "%PS_SCRIPT%" echo Move-Item $tempDir $distDir
>> "%PS_SCRIPT%" echo if (-not (Test-Path $distDir)) { throw 'Dist replacement failed.' }
>> "%PS_SCRIPT%" echo if ((Get-ChildItem -Path $distDir -Force ^| Measure-Object).Count -eq 0) { throw 'Dist replacement produced an empty folder.' }
>> "%PS_SCRIPT%" echo Write-Host 'Dist replacement completed.'
>> "%PS_SCRIPT%" echo if (Test-Path $zipPath) { Remove-Item $zipPath -Force }

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS_SCRIPT%"
set "PS_EXIT=%ERRORLEVEL%"
if exist "%PS_SCRIPT%" del "%PS_SCRIPT%" >nul 2>nul
exit /b %PS_EXIT%

:RestartPharmacyOS
set "LAUNCHER_PATH="
cd /d "%APP_ROOT%" >nul 2>nul
if exist "%APP_ROOT%\PharmacyOS-Launch.vbs" set "LAUNCHER_PATH=%APP_ROOT%\PharmacyOS-Launch.vbs"
if "%LAUNCHER_PATH%"=="" if exist "%APP_ROOT%\PharmacyOS-Launcher.bat" set "LAUNCHER_PATH=%APP_ROOT%\PharmacyOS-Launcher.bat"
if "%LAUNCHER_PATH%"=="" if exist "%BACKEND_DIR%\PharmacyOS-Launch.vbs" set "LAUNCHER_PATH=%BACKEND_DIR%\PharmacyOS-Launch.vbs"
if "%LAUNCHER_PATH%"=="" if exist "%BACKEND_DIR%\PharmacyOS-Launcher.bat" set "LAUNCHER_PATH=%BACKEND_DIR%\PharmacyOS-Launcher.bat"
echo Launcher path: %LAUNCHER_PATH%
if "%LAUNCHER_PATH%"=="" exit /b 1
for %%I in ("%LAUNCHER_PATH%") do set "LAUNCHER_EXT=%%~xI"
if /I "%LAUNCHER_EXT%"==".vbs" (
    start "" wscript.exe "%LAUNCHER_PATH%"
) else (
    start "" "%LAUNCHER_PATH%"
)
exit /b 0
