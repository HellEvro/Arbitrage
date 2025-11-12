Param(
    [switch]$ForceVenv,
    [switch]$ForceReinstall,
    [string]$PythonPath
)

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Arbitrage Bot Installer - Windows" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

function Resolve-Python {
    param([string]$Preferred)

    if ($Preferred) {
        if (Test-Path $Preferred) {
            return $Preferred
        }
        Write-Error "Provided PythonPath '$Preferred' does not exist."
    }

    $python = Get-Command python -ErrorAction SilentlyContinue
    if (-not $python) {
        Write-Error "Python 3.11+ not found. Install from https://www.python.org/downloads/windows/ and re-run this script."
    }
    return $python.Source
}

$pythonExe = Resolve-Python -Preferred $PythonPath

$projectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $projectRoot

Write-Host "Project root: $projectRoot"
Write-Host "Python executable: $pythonExe"

$pythonVersion = & $pythonExe -c "import sys; print('.'.join(map(str, sys.version_info[:3])))"

if (-not $pythonVersion) {
    Write-Error "Unable to determine Python version."
}

$majorMinor = $pythonVersion.Split(".")[0..1] -join "."
if ([version]$majorMinor -lt [version]"3.11") {
    Write-Error "Python $pythonVersion detected. Python 3.11 or newer is required."
}

$venvPath = Join-Path $projectRoot ".venv"
$venvPython = Join-Path $venvPath "Scripts\python.exe"

if ((-not (Test-Path $venvPython)) -or $ForceVenv) {
    if (Test-Path $venvPath) {
        Write-Host "Removing existing virtual environment (.venv)..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force $venvPath
    }
    Write-Host "Creating virtual environment (.venv)..." -ForegroundColor Green
    & $pythonExe -m venv ".venv"
}

if (-not (Test-Path $venvPython)) {
    Write-Error "Virtual environment creation failed. Check Python installation."
}

Write-Host "Upgrading pip/setuptools/wheel..." -ForegroundColor Green
& $venvPython -m pip install --upgrade pip setuptools wheel

if ($ForceReinstall) {
    Write-Host "Forcing reinstallation of dependencies..." -ForegroundColor Yellow
    & $venvPython -m pip install --force-reinstall -r requirements.txt
} else {
    Write-Host "Installing/updating project dependencies..." -ForegroundColor Green
    & $venvPython -m pip install -r requirements.txt
}

$configTemplate = Join-Path $projectRoot "config\config.example.yaml"
$configFile = Join-Path $projectRoot "config\config.yaml"

Write-Host ""
Write-Host "Preparing configuration..." -ForegroundColor Cyan
if (Test-Path $configFile) {
    Write-Host "Config already exists: $configFile" -ForegroundColor Gray
} elseif (Test-Path $configTemplate) {
    Copy-Item $configTemplate $configFile
    Write-Host "Created config from template: $configFile" -ForegroundColor Green
} else {
    Write-Warning "Config template not found: $configTemplate"
}

$launcherCmd = Join-Path $projectRoot "launcher\start_launcher.cmd"
if (-not (Test-Path $launcherCmd)) {
    Write-Warning "Launcher script not found at launcher\start_launcher.cmd"
    Write-Host "You can run the launcher manually via: $venvPython launcher\arbitrage_launcher.py"
}

Write-Host ""
Write-Host "Installation complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Activate the venv: .\.venv\Scripts\activate"
Write-Host "  2. Run the bot: python main.py"
Write-Host "     or use the launcher: launcher\start_launcher.cmd"
if (Test-Path "$projectRoot\requirements-dev.txt") {
    Write-Host "  3. (optional) Install dev deps: $venvPython -m pip install -r requirements-dev.txt"
}
Write-Host ""