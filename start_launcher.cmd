@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%"
set "MANAGER_PY=%SCRIPT_DIR%launcher\arbitrage_manager.py"

if exist "%PROJECT_ROOT%.venv\Scripts\python.exe" (
    set "PYTHON_BIN=%PROJECT_ROOT%.venv\Scripts\python.exe"
) else if exist "%SystemRoot%\py.exe" (
    set "PYTHON_BIN=py -3"
) else (
    set "PYTHON_BIN=python"
)

if "%PYTHON_BIN%" == "py -3" (
    %PYTHON_BIN% "%MANAGER_PY%" %*
) else (
    "%PYTHON_BIN%" "%MANAGER_PY%" %*
)

endlocal


