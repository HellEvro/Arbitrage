#!/bin/bash
# Скрипт установки для Linux/Mac

set -e

echo "========================================"
echo "Arbitrage Bot - Установка зависимостей"
echo "========================================"
echo ""

# Проверка Python
if ! command -v python3 &> /dev/null; then
    echo "[ОШИБКА] Python3 не найден! Установите Python 3.11+"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
REQUIRED_VERSION="3.11"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    echo "[ОШИБКА] Требуется Python 3.11+, найден: $PYTHON_VERSION"
    exit 1
fi

echo "[1/4] Создание виртуального окружения..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo "[OK] Виртуальное окружение создано"
else
    echo "[OK] Виртуальное окружение уже существует"
fi

echo ""
echo "[2/4] Активация виртуального окружения..."
source .venv/bin/activate

echo ""
echo "[3/4] Установка зависимостей..."
pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "[4/4] Настройка конфигурации..."
if [ ! -f "config/config.yaml" ]; then
    cp config/config.example.yaml config/config.yaml
    echo "[OK] Создан файл config/config.yaml"
    echo "[INFO] Отредактируйте config/config.yaml для настройки Telegram (опционально)"
else
    echo "[OK] Файл config/config.yaml уже существует"
fi

echo ""
echo "========================================"
echo "Установка завершена успешно!"
echo "========================================"
echo ""
echo "Для запуска бота выполните:"
echo "  source .venv/bin/activate"
echo "  python main.py"
echo ""

