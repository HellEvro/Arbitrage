@echo off
REM Скрипт установки для Windows
echo ========================================
echo Arbitrage Bot - Установка зависимостей
echo ========================================
echo.

REM Проверка Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ОШИБКА] Python не найден! Установите Python 3.11+ с python.org
    pause
    exit /b 1
)

echo [1/4] Создание виртуального окружения...
if not exist .venv (
    python -m venv .venv
    if errorlevel 1 (
        echo [ОШИБКА] Не удалось создать виртуальное окружение
        pause
        exit /b 1
    )
    echo [OK] Виртуальное окружение создано
) else (
    echo [OK] Виртуальное окружение уже существует
)

echo.
echo [2/4] Активация виртуального окружения...
call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo [ОШИБКА] Не удалось активировать виртуальное окружение
    pause
    exit /b 1
)

echo.
echo [3/4] Установка зависимостей...
pip install --upgrade pip
pip install -r requirements.txt
if errorlevel 1 (
    echo [ОШИБКА] Не удалось установить зависимости
    pause
    exit /b 1
)

echo.
echo [4/4] Настройка конфигурации...
if not exist config\config.yaml (
    copy config\config.example.yaml config\config.yaml >nul
    echo [OK] Создан файл config\config.yaml
    echo [INFO] Отредактируйте config\config.yaml для настройки Telegram (опционально)
) else (
    echo [OK] Файл config\config.yaml уже существует
)

echo.
echo ========================================
echo Установка завершена успешно!
echo ========================================
echo.
echo Для запуска бота выполните:
echo   .venv\Scripts\activate
echo   python main.py
echo.
pause

