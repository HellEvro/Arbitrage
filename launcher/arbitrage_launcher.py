#!/usr/bin/env python
"""
Arbitrage Launcher - cross-platform management utility for the Arbitrage Bot project.

This script provides a small interactive menu as well as direct CLI commands to help
with common project tasks:
    * bootstrap a virtual environment
    * install runtime and development dependencies
    * launch the bot backend
    * inspect logs and configuration files
    * perform quick health checks

The launcher intentionally avoids third-party dependencies so it can run on a fresh
Python installation (3.11+) without any preparation.
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Iterable, List, Optional


class CommandError(RuntimeError):
    """Raised when a subprocess exits with a non-zero return code."""


class ArbitrageLauncher:
    """Utility responsible for orchestrating common project workflows."""

    def __init__(self, project_root: Path | None = None) -> None:
        self.project_root = project_root or Path(__file__).resolve().parents[1]
        self.venv_path = self.project_root / ".venv"
        self.requirements = self.project_root / "requirements.txt"
        self.dev_requirements = self.project_root / "requirements-dev.txt"
        self.pyproject = self.project_root / "pyproject.toml"
        self.config_example = self.project_root / "config" / "config.example.yaml"
        self.config_file = self.project_root / "config" / "config.yaml"
        self.logs_dir = self.project_root / "logs"

    # ------------------------------------------------------------------
    # Paths & environment
    # ------------------------------------------------------------------

    def python_in_venv(self) -> Path:
        """Return the path to the Python interpreter inside the virtualenv."""
        if os.name == "nt":
            path = self.venv_path / "Scripts" / "python.exe"
        else:
            path = self.venv_path / "bin" / "python"
        return path

    def venv_exists(self) -> bool:
        """Return True if the virtual environment has already been created."""
        return self.python_in_venv().exists()

    @staticmethod
    def format_path(path: Path) -> str:
        """Pretty string representation with forward slashes on all platforms."""
        return path.as_posix()

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    def run(self, args: Iterable[str], *, cwd: Optional[Path] = None, env: Optional[dict] = None) -> None:
        """Run a subprocess and raise CommandError upon failure."""
        cmd = list(args)
        try:
            subprocess.run(cmd, check=True, cwd=cwd or self.project_root, env=env)
        except subprocess.CalledProcessError as exc:
            raise CommandError(f"Command failed with exit code {exc.returncode}: {' '.join(cmd)}") from exc

    def safe_copy(self, src: Path, dst: Path) -> None:
        """Copy a file if the destination does not exist yet."""
        if not src.exists():
            raise FileNotFoundError(self.format_path(src))
        if dst.exists():
            return
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    @staticmethod
    def print_header(title: str) -> None:
        print("=" * 72)
        print(title)
        print("=" * 72)

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def check_python_version(self) -> None:
        """Ensure that the running interpreter satisfies the minimum requirement."""
        major, minor = sys.version_info[:2]
        required = (3, 11)
        if (major, minor) < required:
            raise RuntimeError(
                f"Python {required[0]}.{required[1]}+ is required. "
                f"Current interpreter: Python {major}.{minor} ({sys.executable})"
            )

    def create_virtualenv(self, *, recreate: bool = False) -> None:
        """Create (or recreate) the virtual environment."""
        self.check_python_version()
        if recreate and self.venv_path.exists():
            print(f"Removing existing virtual environment: {self.format_path(self.venv_path)}")
            shutil.rmtree(self.venv_path)
        if self.venv_exists():
            print(f"Virtual environment already exists at {self.format_path(self.venv_path)}")
            return
        self.print_header("Creating virtual environment")
        self.run([sys.executable, "-m", "venv", str(self.venv_path)])
        print(f"Virtual environment created at {self.format_path(self.venv_path)}")

    def upgrade_pip(self) -> None:
        """Upgrade pip inside the virtual environment."""
        if not self.venv_exists():
            self.create_virtualenv()
        python = self.python_in_venv()
        self.print_header("Upgrading pip & build tools")
        self.run([str(python), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])

    def install_dependencies(self, *, dev: bool = False) -> None:
        """Install project dependencies inside the virtual environment."""
        if not self.venv_exists():
            self.create_virtualenv()
        python = self.python_in_venv()
        self.upgrade_pip()
        self.print_header("Installing runtime dependencies")
        self.run([str(python), "-m", "pip", "install", "-r", str(self.requirements)])
        if dev and self.dev_requirements.exists():
            self.print_header("Installing development dependencies")
            self.run([str(python), "-m", "pip", "install", "-r", str(self.dev_requirements)])
        elif dev:
            print(f"Development requirements file not found: {self.format_path(self.dev_requirements)}")

    def install_editable(self) -> None:
        """Install the project in editable mode using pyproject."""
        if not self.venv_exists():
            self.create_virtualenv()
        if not self.pyproject.exists():
            raise FileNotFoundError("pyproject.toml not found. Editable install is unavailable.")
        python = self.python_in_venv()
        self.upgrade_pip()
        self.print_header("Installing project in editable mode")
        self.run([str(python), "-m", "pip", "install", "-e", ".[dev]"])

    def ensure_config(self) -> None:
        """Create a user config from the example if needed."""
        if not self.config_example.exists():
            raise FileNotFoundError(f"Config example not found at {self.format_path(self.config_example)}")
        if self.config_file.exists():
            print(f"Config already present at {self.format_path(self.config_file)}")
            return
        self.safe_copy(self.config_example, self.config_file)
        print(f"Created config from template: {self.format_path(self.config_file)}")

    def open_config(self) -> None:
        """Open the main configuration file in the default editor."""
        if not self.config_file.exists():
            print("Config file is missing. Creating it from template...")
            self.ensure_config()
        path = self.config_file
        print(f"Opening config file: {self.format_path(path)}")
        self._open_path(path)

    def open_logs_dir(self) -> None:
        """Open the logs directory in the file explorer."""
        if not self.logs_dir.exists():
            print("Logs directory does not exist yet. It will be created on first launch.")
            return
        print(f"Opening logs directory: {self.format_path(self.logs_dir)}")
        self._open_path(self.logs_dir)

    def tail_logs(self, filename: str = "system.log", lines: int = 50) -> None:
        """Print the last N lines of a log file."""
        log_path = self.logs_dir / filename
        if not log_path.exists():
            print(f"Log file not found: {self.format_path(log_path)}")
            return
        print(f"--- Last {lines} lines of {self.format_path(log_path)} ---")
        with log_path.open("r", encoding="utf-8", errors="replace") as fh:
            buffer: List[str] = fh.readlines()
        for line in buffer[-lines:]:
            print(line.rstrip("\n"))

    def launch_bot(self, extra_args: Optional[List[str]] = None) -> None:
        """Run the main arbitrage bot entry point within the virtual environment."""
        if not self.venv_exists():
            print("Virtual environment not found. Initializing...")
            self.install_dependencies()
        python = self.python_in_venv()
        self.ensure_config()
        cmd = [str(python), "main.py"]
        if extra_args:
            cmd.extend(extra_args)
        self.print_header("Starting Arbitrage Bot")
        print("Press Ctrl+C to stop the bot and return to the launcher.")
        try:
            self.run(cmd, cwd=self.project_root)
        except CommandError as exc:
            print(f"[ERROR] Failed to start the bot: {exc}")

    def run_tests(self) -> None:
        """Execute the project's test suite using pytest."""
        if not self.venv_exists():
            print("Virtual environment not found. Initializing...")
            self.install_dependencies(dev=True)
        python = self.python_in_venv()
        self.print_header("Running test suite (pytest)")
        try:
            self.run([str(python), "-m", "pytest"])
        except CommandError as exc:
            print(f"[ERROR] Tests failed: {exc}")

    def show_status(self) -> None:
        """Display diagnostic information about the current setup."""
        python_version = platform.python_version()
        venv_python = self.python_in_venv()
        print("Project root:", self.format_path(self.project_root))
        print("Platform:", platform.platform())
        print("Launcher Python:", f"{python_version} ({sys.executable})")
        print("Virtualenv exists:", "yes" if self.venv_exists() else "no")
        if self.venv_exists():
            print("Virtualenv interpreter:", self.format_path(venv_python))
            try:
                output = subprocess.check_output(
                    [str(venv_python), "-m", "pip", "list"],
                    cwd=self.project_root,
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                lines = [line.strip() for line in output.splitlines() if line.strip()]
                summary = [line for line in lines if "Package" in line or "----" in line]
                if summary:
                    print("pip list (first 5):")
                    header_index = lines.index(summary[-1]) if summary else 0
                    snippet = lines[header_index : header_index + 6]
                    for entry in snippet:
                        print(" ", entry)
            except (subprocess.CalledProcessError, FileNotFoundError):
                pass
        print("Config exists:", "yes" if self.config_file.exists() else "no")
        print("Logs directory:", self.format_path(self.logs_dir))

    def _open_path(self, path: Path) -> None:
        """Open a file or directory using the default OS-specific handler."""
        if os.name == "nt":
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])

    # ------------------------------------------------------------------
    # Interactive menu
    # ------------------------------------------------------------------

    def interactive_menu(self) -> None:
        """Launch an interactive text menu for common operations."""
        options = {
            "1": ("Создать/обновить виртуальное окружение", self.install_dependencies),
            "2": ("Установить зависимости для разработки", lambda: self.install_dependencies(dev=True)),
            "3": ("Установить проект в editable-режиме", self.install_editable),
            "4": ("Запустить Arbitrage Bot", self.launch_bot),
            "5": ("Показать статус окружения", self.show_status),
            "6": ("Открыть конфигурацию", self.open_config),
            "7": ("Открыть директорию логов", self.open_logs_dir),
            "8": ("Показать хвост system.log", self.tail_logs),
            "9": ("Запустить тесты (pytest)", self.run_tests),
            "0": ("Выход", None),
        }
        while True:
            print()
            self.print_header("Arbitrage Launcher")
            print("Выберите действие:")
            for key, (title, _) in options.items():
                print(f"  {key}. {title}")
            choice = input("\nВведите номер и нажмите Enter: ").strip()
            if choice == "0":
                print("До встречи! 👋")
                return
            action = options.get(choice)
            if not action:
                print(f"Неизвестный выбор: {choice}")
                continue
            title, callback = action
            print()
            self.print_header(title)
            try:
                if callback is self.launch_bot:
                    callback(extra_args=None)  # type: ignore[misc]
                elif callback is self.tail_logs:
                    filename = input("Введите имя лог-файла (по умолчанию system.log): ").strip() or "system.log"
                    lines_input = input("Сколько последних строк показать? [50]: ").strip()
                    lines = int(lines_input) if lines_input else 50
                    callback(filename=filename, lines=lines)  # type: ignore[misc]
                elif callback is self.install_dependencies:
                    callback(dev=False)  # type: ignore[misc]
                else:
                    callback()  # type: ignore[misc]
            except CommandError as exc:
                print(f"[ERROR] {exc}")
            except Exception as exc:  # noqa: BLE001
                print(f"[UNEXPECTED ERROR] {exc}")
            print()
            input("Нажмите Enter чтобы продолжить...")


def build_parser() -> argparse.ArgumentParser:
    description = """\
    Arbitrage Launcher — вспомогательный инструмент для обслуживания проекта.

    Если командные аргументы не указаны, будет запущено интерактивное меню.
    Примеры использования:

        python launcher/arbitrage_launcher.py setup
        python launcher/arbitrage_launcher.py run -- --min-profit 1.5
        python launcher/arbitrage_launcher.py status
    """
    parser = argparse.ArgumentParser(
        description=textwrap.dedent(description),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("setup", help="Создать виртуальное окружение и установить зависимости")
    subparsers.add_parser("setup-dev", help="Установить runtime и dev-зависимости")
    subparsers.add_parser("editable", help="Установить проект в editable-режиме (pip install -e)")
    subparsers.add_parser("run", help="Запустить Arbitrage Bot (main.py). Дополнительные аргументы передаются после --")
    subparsers.add_parser("status", help="Показать информацию о текущем состоянии окружения")
    subparsers.add_parser("config", help="Открыть config/config.yaml")
    subparsers.add_parser("logs", help="Открыть директорию логов")
    tail_parser = subparsers.add_parser("tail", help="Показать хвост указанного лог-файла")
    tail_parser.add_argument("filename", nargs="?", default="system.log", help="Имя лог-файла (по умолчанию system.log)")
    tail_parser.add_argument("-n", "--lines", type=int, default=50, help="Количество строк для отображения")
    subparsers.add_parser("test", help="Запустить pytest в виртуальном окружении")

    return parser


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_parser()
    args, extra = parser.parse_known_args(argv)
    launcher = ArbitrageLauncher()

    if not args.command:
        launcher.interactive_menu()
        return

    try:
        if args.command == "setup":
            launcher.install_dependencies(dev=False)
            launcher.ensure_config()
        elif args.command == "setup-dev":
            launcher.install_dependencies(dev=True)
            launcher.ensure_config()
        elif args.command == "editable":
            launcher.install_editable()
            launcher.ensure_config()
        elif args.command == "run":
            launcher.launch_bot(extra_args=extra)
        elif args.command == "status":
            launcher.show_status()
        elif args.command == "config":
            launcher.open_config()
        elif args.command == "logs":
            launcher.open_logs_dir()
        elif args.command == "tail":
            launcher.tail_logs(filename=args.filename, lines=args.lines)
        elif args.command == "test":
            launcher.run_tests()
        else:
            parser.print_help()
    except CommandError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
    except RuntimeError as exc:
        print(f"[FATAL] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()


