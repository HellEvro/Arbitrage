#!/usr/bin/env bash

set -euo pipefail

echo "================================================================"
echo "  Arbitrage Bot Installer (macOS)"
echo "================================================================"
echo

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${PROJECT_ROOT}/.venv"
REQUIREMENTS_FILE="${PROJECT_ROOT}/requirements.txt"
CONFIG_TEMPLATE="${PROJECT_ROOT}/config/config.example.yaml"
CONFIG_FILE="${PROJECT_ROOT}/config/config.yaml"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "[ERROR] Python executable '${PYTHON_BIN}' not found."
  echo "        Install Python 3.11+ from https://www.python.org/downloads/macos/ and re-run."
  exit 1
fi

PY_VERSION="$(${PYTHON_BIN} -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"
if ! "${PYTHON_BIN}" -c "import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)"; then
  echo "[ERROR] Python 3.11+ required (found ${PY_VERSION})."
  exit 1
fi

echo "[INFO] Using Python ${PY_VERSION}"
echo

if [[ -d "${VENV_DIR}" ]]; then
  echo "[INFO] Virtual environment already exists at ${VENV_DIR}"
else
  echo "[INFO] Creating virtual environment at ${VENV_DIR}"
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

VENV_PYTHON="${VENV_DIR}/bin/python"

echo
echo "[INFO] Upgrading pip/setuptools/wheel"
"${VENV_PYTHON}" -m pip install --upgrade pip setuptools wheel

echo "[INFO] Installing project dependencies"
"${VENV_PYTHON}" -m pip install -r "${REQUIREMENTS_FILE}"

if [[ -f "${PROJECT_ROOT}/requirements-dev.txt" ]]; then
  echo "[HINT] To install development dependencies run:"
  echo "       ${VENV_PYTHON} -m pip install -r requirements-dev.txt"
fi

echo
echo "[INFO] Preparing configuration"
if [[ -f "${CONFIG_FILE}" ]]; then
  echo "       Config already exists: ${CONFIG_FILE}"
elif [[ -f "${CONFIG_TEMPLATE}" ]]; then
  cp "${CONFIG_TEMPLATE}" "${CONFIG_FILE}"
  echo "       Created config from template: ${CONFIG_FILE}"
else
  echo "       [WARN] Config template not found: ${CONFIG_TEMPLATE}"
fi

echo
echo "[SUCCESS] Installation complete!"
echo "Next steps:"
echo "  1. source ${VENV_DIR}/bin/activate"
echo "  2. python main.py"
echo "     or use the launcher: ./launcher/start_launcher.sh"
echo

