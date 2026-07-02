@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ================================================================
REM PharmacyOS Windows auto-updater
REM - Pulls backend and frontend source from GitHub
REM - Downloads the GitHub Actions-built frontend dist artifact
REM - Verifies index.html, static\, and version.json before replacement
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
set "DIST_STAGE=%FRONTEND_DIR%\dist_stage"

REM Optional override for the GitHub owner or organization that stores the UI build artifact.
REM If this is blank, the updater automatically detects the owner from the frontend
REM git remote after pulling the frontend source repository.
REM Example: set "GITHUB_OWNER=your-org-or-user"
set "GITHUB_OWNER=%GITHUB_OWNER%"
set "GITHUB_REPO=pharmacyos-frontend-dist"
set "ARTIFACT_NAME=pharmacyos-frontend-dist"

REM Optional for private repositories or higher API limits:
REM set "GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx"
REM Optional manifest URL. If set, its artifact URL is preferred over latest artifact lookup.
REM set "PHARMACYOS_UPDATE_MANIFEST_URL=https://example.com/manifest.json"

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

where powershell >nul 2>nul
if errorlevel 1 (
    echo ERROR: PowerShell was not found in PATH. Update stopped safely.
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
echo Frontend source updated successfully
echo No local npm build will run on this machine. The updater uses the GitHub-built frontend artifact.
echo.

if "%GITHUB_OWNER%"=="" call :DetectGitHubOwner

echo [3/5] Downloading, extracting, and verifying frontend artifact...
call :DownloadAndInstallUi
if errorlevel 1 (
    echo ERROR: Frontend artifact update failed. Existing frontend/dist was not overwritten.
    pause
    exit /b 1
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
>> "%PS_SCRIPT%" echo function Log($message) { Write-Host ('[frontend artifact] ' + $message) }
>> "%PS_SCRIPT%" echo function New-Client($accept) { $wc = New-Object Net.WebClient; $wc.Headers.Add('User-Agent','PharmacyOS-Updater'); if ($accept) { $wc.Headers.Add('Accept',$accept) }; if ($env:GITHUB_TOKEN) { $wc.Headers.Add('Authorization','Bearer ' + $env:GITHUB_TOKEN) }; return $wc }
>> "%PS_SCRIPT%" echo function Add-JsonType { Add-Type -AssemblyName System.Web.Extensions -ErrorAction SilentlyContinue }
>> "%PS_SCRIPT%" echo function Read-JsonFile($path) { Add-JsonType; $serializer = New-Object System.Web.Script.Serialization.JavaScriptSerializer; return $serializer.DeserializeObject([IO.File]::ReadAllText($path)) }
>> "%PS_SCRIPT%" echo function Read-JsonUrl($url) { Add-JsonType; $serializer = New-Object System.Web.Script.Serialization.JavaScriptSerializer; return $serializer.DeserializeObject((New-Client 'application/json').DownloadString($url)) }
>> "%PS_SCRIPT%" echo function First-Text($map, [string[]] $names) { foreach ($name in $names) { if ($map -and $map.ContainsKey($name) -and $map[$name]) { return [string]$map[$name] } }; return '' }
>> "%PS_SCRIPT%" echo function Test-FrontendDist($path) { return ((Test-Path -LiteralPath (Join-Path $path 'index.html') -PathType Leaf) -and (Test-Path -LiteralPath (Join-Path $path 'static') -PathType Container) -and (Test-Path -LiteralPath (Join-Path $path 'version.json') -PathType Leaf)) }
>> "%PS_SCRIPT%" echo function Find-ArtifactRoot($path) { $roots = @(Get-Item -LiteralPath $path) + @(Get-ChildItem -LiteralPath $path -Directory -Recurse); foreach ($root in $roots) { if (Test-FrontendDist $root.FullName) { return $root.FullName } }; return $null }
>> "%PS_SCRIPT%" echo function Copy-DirectoryContents($source, $dest) { if (Test-Path -LiteralPath $dest) { Remove-Item -LiteralPath $dest -Recurse -Force }; New-Item -ItemType Directory -Path $dest ^| Out-Null; Get-ChildItem -LiteralPath $source -Force ^| Copy-Item -Destination $dest -Recurse -Force }
>> "%PS_SCRIPT%" echo $owner = $env:GITHUB_OWNER
>> "%PS_SCRIPT%" echo $repo = $env:GITHUB_REPO
>> "%PS_SCRIPT%" echo $artifactName = $env:ARTIFACT_NAME
>> "%PS_SCRIPT%" echo $appRoot = $env:APP_ROOT
>> "%PS_SCRIPT%" echo $frontendDir = $env:FRONTEND_DIR
>> "%PS_SCRIPT%" echo $distDir = $env:DIST_DIR
>> "%PS_SCRIPT%" echo $backupDir = $env:DIST_BACKUP
>> "%PS_SCRIPT%" echo $tempDir = $env:DIST_TEMP
>> "%PS_SCRIPT%" echo $stageDir = $env:DIST_STAGE
>> "%PS_SCRIPT%" echo $zipPath = Join-Path $env:TEMP 'pharmacyos-frontend-dist.zip'
>> "%PS_SCRIPT%" echo $manifest = $null
>> "%PS_SCRIPT%" echo $manifestUrl = $env:PHARMACYOS_UPDATE_MANIFEST_URL
>> "%PS_SCRIPT%" echo if ($manifestUrl) { Log ('reading manifest.json from ' + $manifestUrl); $manifest = Read-JsonUrl $manifestUrl }
>> "%PS_SCRIPT%" echo if (-not $manifest) { foreach ($p in @((Join-Path $appRoot 'manifest.json'), (Join-Path $frontendDir 'manifest.json'), (Join-Path $appRoot 'update-manifest.json'))) { if (Test-Path $p) { Log ('reading manifest.json from ' + $p); $manifest = Read-JsonFile $p; break } } }
>> "%PS_SCRIPT%" echo $artifactUrl = First-Text $manifest @('frontend_artifact_url','artifact_url','artifactUrl','download_url')
>> "%PS_SCRIPT%" echo if ($artifactUrl) { Log ('downloading artifact from manifest URL'); if (Test-Path $zipPath) { Remove-Item $zipPath -Force }; (New-Client 'application/zip').DownloadFile($artifactUrl, $zipPath) } else { if (-not $owner) { throw 'GitHub owner is not set and no artifact URL was found in manifest.json.' }; $api = 'https://api.github.com/repos/' + $owner + '/' + $repo + '/actions/artifacts?per_page=100'; Log ('downloading latest artifact metadata from ' + $api); $data = Read-JsonUrl $api; $artifact = $null; foreach ($a in $data['artifacts']) { if ($a['name'] -eq $artifactName -and -not $a['expired']) { $artifact = $a; break } }; if (-not $artifact) { throw 'No non-expired artifact named ' + $artifactName + ' was found.' }; $artifactUrl = $artifact['archive_download_url']; Log ('downloading artifact ' + $artifactUrl); if (Test-Path $zipPath) { Remove-Item $zipPath -Force }; (New-Client 'application/vnd.github+json').DownloadFile($artifactUrl, $zipPath) }
>> "%PS_SCRIPT%" echo if (-not (Test-Path $zipPath)) { throw 'Artifact download did not create a zip file.' }
>> "%PS_SCRIPT%" echo $zipInfo = Get-Item $zipPath
>> "%PS_SCRIPT%" echo if ($zipInfo.Length -le 0) { throw 'Artifact download created an empty zip file.' }
>> "%PS_SCRIPT%" echo Log ('extracting artifact to ' + $tempDir)
>> "%PS_SCRIPT%" echo if (Test-Path $tempDir) { Remove-Item $tempDir -Recurse -Force }
>> "%PS_SCRIPT%" echo if (Test-Path $stageDir) { Remove-Item $stageDir -Recurse -Force }
>> "%PS_SCRIPT%" echo New-Item -ItemType Directory -Path $tempDir ^| Out-Null
>> "%PS_SCRIPT%" echo $shell = New-Object -ComObject Shell.Application
>> "%PS_SCRIPT%" echo $zip = $shell.NameSpace($zipPath)
>> "%PS_SCRIPT%" echo $dest = $shell.NameSpace($tempDir)
>> "%PS_SCRIPT%" echo if (-not $zip -or -not $dest) { throw 'Unable to open artifact zip.' }
>> "%PS_SCRIPT%" echo $dest.CopyHere($zip.Items(), 16)
>> "%PS_SCRIPT%" echo Start-Sleep -Seconds 3
>> "%PS_SCRIPT%" echo Log 'verifying artifact before touching frontend/dist'
>> "%PS_SCRIPT%" echo $artifactRoot = Find-ArtifactRoot $tempDir
>> "%PS_SCRIPT%" echo if (-not $artifactRoot) { throw 'Artifact verification failed. Update stopped before touching frontend/dist because the extracted artifact does not contain a folder with index.html, static/, and version.json.' }
>> "%PS_SCRIPT%" echo Log ('verified artifact root: ' + $artifactRoot)
>> "%PS_SCRIPT%" echo Copy-DirectoryContents $artifactRoot $stageDir
>> "%PS_SCRIPT%" echo if (-not (Test-FrontendDist $stageDir)) { if (Test-Path -LiteralPath $stageDir) { Remove-Item -LiteralPath $stageDir -Recurse -Force }; throw 'Staged artifact verification failed. Existing frontend/dist was left untouched.' }
>> "%PS_SCRIPT%" echo Log ('validated staged artifact; replacing frontend/dist: ' + $distDir)
>> "%PS_SCRIPT%" echo if (Test-Path -LiteralPath $backupDir) { Remove-Item -LiteralPath $backupDir -Recurse -Force }
>> "%PS_SCRIPT%" echo if (Test-Path -LiteralPath $distDir) { Move-Item -LiteralPath $distDir -Destination $backupDir }
>> "%PS_SCRIPT%" echo Move-Item -LiteralPath $stageDir -Destination $distDir
>> "%PS_SCRIPT%" echo if (-not (Test-FrontendDist $distDir)) { if (Test-Path -LiteralPath $distDir) { Remove-Item -LiteralPath $distDir -Recurse -Force }; if (Test-Path -LiteralPath $backupDir) { Move-Item -LiteralPath $backupDir -Destination $distDir }; throw 'Dist replacement verification failed; restored previous frontend/dist.' }
>> "%PS_SCRIPT%" echo Log 'update complete'
>> "%PS_SCRIPT%" echo if (Test-Path $tempDir) { Remove-Item $tempDir -Recurse -Force }
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
