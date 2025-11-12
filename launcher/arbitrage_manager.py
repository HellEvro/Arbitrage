#!/usr/bin/env python
"""
Arbitrage Manager — графический интерфейс обслуживания проекта Arbitrage Bot.
Интерфейс повторяет знакомый менеджер с шагами 1-7, отображает статусы и позволяет
выполнять команды без терминала.
"""

from __future__ import annotations

import json
import os
import platform
import shlex
import shutil
import subprocess
import sys
import threading
from pathlib import Path
import webbrowser
from typing import Callable, Dict, Iterable, List, Optional

try:
    import tkinter as tk
    from tkinter import messagebox, ttk
except ImportError as exc:  # pragma: no cover
    raise SystemExit("tkinter не установлен. Установите полноценную версию Python с tkinter.") from exc


CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from arbitrage_launcher import ArbitrageLauncher  # noqa: E402


def quote(arg: str) -> str:
    if not arg:
        return '""'
    if any(ch.isspace() for ch in arg) or any(ch in arg for ch in '"\''):
        return shlex.quote(arg)
    return arg


REPO_URL = "https://github.com/HellEvro/Arbitrage.git"


class ArbitrageManagerApp:
    def __init__(self) -> None:
        self.launcher = ArbitrageLauncher()
        self.project_root = self.launcher.project_root

        self.state_path = self.project_root / ".arbitrage_manager_state.json"

        self.root = tk.Tk()
        self.root.title("Arbitrage Manager")
        self.root.minsize(940, 680)
        self._restore_window_geometry()

        self.status_var = tk.StringVar(value="Готово")
        self.venv_status_var = tk.StringVar()
        self.deps_hint_var = tk.StringVar()
        self.git_status_var = tk.StringVar()
        self.config_status_var = tk.StringVar()
        self.license_status_var = tk.StringVar(value="Лицензия не требуется")

        self.service_states: Dict[str, tk.StringVar] = {}
        self.processes: Dict[str, subprocess.Popen[str]] = {}

        self.interactive_widgets: List[tk.Widget] = []
        self.step2_controls: List[tk.Widget] = []
        self.current_task_lock = threading.Lock()
        self._venv_bootstrap_done = False

        self._build_ui()
        self._refresh_all()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ------------------------------------------------------------------
    # UI Construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        main = ttk.Frame(self.root, padding=12)
        main.pack(fill=tk.BOTH, expand=True)

        status_frame = ttk.Frame(main)
        status_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(status_frame, text="Статус операций:").pack(side=tk.LEFT)
        ttk.Label(status_frame, textvariable=self.status_var, foreground="#146c43").pack(side=tk.LEFT, padx=(6, 0))
        self.progress_bar = ttk.Progressbar(status_frame, mode="indeterminate", length=180)
        self.progress_bar.stop()
        self.progress_bar["value"] = 0
        self._progress_visible = False

        self._build_section_env(main)
        self._build_section_dependencies(main)
        self._build_section_git(main)
        self._build_section_license(main)
        self._build_section_services(main)
        self._build_section_docs(main)
        self._build_section_logs(main)
        self._build_footer(main)

        self.root.after(100, self._start_repo_sync)

    def _build_section_env(self, parent: tk.Widget) -> None:
        frame = ttk.LabelFrame(
            parent,
            text="1. Виртуальное окружение (рекомендуется, вместо прямой установки в системный Python в п.2)",
        )
        frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame, textvariable=self.venv_status_var).grid(row=0, column=0, padx=10, pady=8, sticky="w")

        btn_create = ttk.Button(
            frame,
            text="Создать/обновить окружение (.venv)",
            command=lambda: self._run_async(
                self._create_or_update_venv,
                "Создание/обновление окружения",
                on_start=lambda: self.venv_status_var.set("Создание/обновление окружения..."),
            ),
        )
        self._register_widget(btn_create)
        btn_create.grid(row=0, column=1, padx=6, pady=8)

        btn_delete = ttk.Button(
            frame,
            text="Удалить окружение (venv)",
            command=lambda: self._run_async(
                self._remove_venv,
                "Удаление окружения",
                on_start=lambda: self.venv_status_var.set("Удаление окружения..."),
            ),
        )
        self._register_widget(btn_delete)
        btn_delete.grid(row=0, column=2, padx=6, pady=8)

        frame.columnconfigure(0, weight=1)

    def _build_section_dependencies(self, parent: tk.Widget) -> None:
        frame = ttk.LabelFrame(
            parent,
            text="2. Установка зависимостей напрямую (опционально, изменяет системный Python)",
        )
        frame.pack(fill=tk.X, pady=(0, 10))

        btn_install = ttk.Button(
            frame,
            text="Установить/обновить зависимости (pip install -r requirements.txt)",
            command=lambda: self._run_async(
                self._install_system_dependencies,
                "Установка зависимостей в систему",
                on_start=lambda: self.deps_hint_var.set("Установка зависимостей..."),
            ),
        )
        btn_install.grid(row=0, column=0, padx=10, pady=8, sticky="w")
        self.step2_controls.append(btn_install)
        self._register_widget(btn_install)

        btn_open_project = ttk.Button(
            frame,
            text="Открыть каталог проекта",
            command=lambda: self._open_path(self.project_root),
        )
        self._register_widget(btn_open_project)
        btn_open_project.grid(row=0, column=1, padx=6, pady=8, sticky="w")

        ttk.Label(frame, textvariable=self.deps_hint_var, foreground="#888888").grid(
            row=1, column=0, columnspan=2, padx=10, pady=(0, 8), sticky="w"
        )

        frame.columnconfigure(0, weight=1)

    def _build_section_git(self, parent: tk.Widget) -> None:
        frame = ttk.LabelFrame(parent, text="3. Обновления из Git")
        frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame, text="Статус репозитория:").grid(row=0, column=0, padx=10, pady=8, sticky="w")
        ttk.Label(frame, textvariable=self.git_status_var).grid(row=0, column=1, padx=4, pady=8, sticky="w")

        btn_git = ttk.Button(
            frame,
            text="Получить обновления (fetch + reset)",
            command=lambda: self._run_async(
                self._git_fetch_and_reset,
                "Получение обновлений",
                on_start=lambda: self.git_status_var.set("Получение обновлений..."),
            ),
        )
        self._register_widget(btn_git)
        btn_git.grid(row=0, column=2, padx=6, pady=8)

    def _build_section_license(self, parent: tk.Widget) -> None:
        frame = ttk.LabelFrame(parent, text="4. Лицензия и ключ (опционально)")
        frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame, textvariable=self.license_status_var).grid(row=0, column=0, padx=10, pady=6, sticky="w")
        ttk.Label(frame, textvariable=self.config_status_var).grid(row=1, column=0, padx=10, pady=(0, 8), sticky="w")

        btn_open_config = ttk.Button(
            frame,
            text="Редактировать config (config/config.yaml)",
            command=lambda: self._open_path(self.launcher.config_file),
        )
        self._register_widget(btn_open_config)
        btn_open_config.grid(row=2, column=0, padx=10, pady=(0, 8), sticky="w")

        btn_open_example = ttk.Button(
            frame,
            text="Открыть config.example.yaml",
            command=lambda: self._open_path(self.launcher.config_example),
        )
        self._register_widget(btn_open_example)
        btn_open_example.grid(row=2, column=1, padx=10, pady=(0, 8), sticky="w")

        frame.columnconfigure(0, weight=1)

    def _build_section_services(self, parent: tk.Widget) -> None:
        frame = ttk.LabelFrame(parent, text="5. Запуск сервисов")
        frame.pack(fill=tk.X, pady=(0, 10))

        frame.grid_columnconfigure(1, weight=1)

        self.services_config = [
            {
                "id": "web",
                "title": "Web UI (main.py, порт 5152)",
                "command": ["{python}", "main.py"],
            },
        ]

        for idx, service in enumerate(self.services_config, start=1):
            row = idx
            var = tk.StringVar(value="Не запущен")
            self.service_states[service["id"]] = var

            ttk.Label(frame, text=service["title"]).grid(row=row, column=0, padx=10, pady=6, sticky="w")
            ttk.Label(frame, textvariable=var).grid(row=row, column=1, padx=6, pady=6, sticky="w")

            btn_start = ttk.Button(
                frame,
                text="Запустить",
                command=lambda sid=service["id"], title=service["title"]: self._run_async(
                    lambda sid=sid: self._start_service(sid),
                    f"Запуск {title}",
                    on_start=lambda sid=sid: self.service_states[sid].set("Запуск..."),
                ),
            )
            self._register_widget(btn_start)
            btn_start.grid(row=row, column=2, padx=4, pady=6)

            btn_stop = ttk.Button(
                frame,
                text="Остановить",
                command=lambda sid=service["id"], title=service["title"]: self._run_async(
                    lambda sid=sid: self._stop_service(sid),
                    f"Остановка {title}",
                    on_start=lambda sid=sid: self.service_states[sid].set("Остановка..."),
                ),
            )
            self._register_widget(btn_stop)
            btn_stop.grid(row=row, column=3, padx=4, pady=6)

        frame.columnconfigure(1, weight=1)

    def _build_section_docs(self, parent: tk.Widget) -> None:
        frame = ttk.LabelFrame(parent, text="6. Документация и файлы")
        frame.pack(fill=tk.X, pady=(0, 10))

        btn_readme = ttk.Button(
            frame,
            text="Открыть README",
            command=lambda: self._open_path(self.project_root / "README.md"),
        )
        self._register_widget(btn_readme)
        btn_readme.grid(row=0, column=0, padx=10, pady=6, sticky="w")

        btn_log = ttk.Button(
            frame,
            text="Открыть лог бота",
            command=lambda: self._open_path(self.launcher.logs_dir / "system.log"),
        )
        self._register_widget(btn_log)
        btn_log.grid(row=0, column=1, padx=10, pady=6, sticky="w")

        btn_project_dir = ttk.Button(
            frame,
            text="Открыть каталог проекта",
            command=lambda: self._open_path(self.project_root),
        )
        self._register_widget(btn_project_dir)
        btn_project_dir.grid(row=0, column=2, padx=10, pady=6, sticky="w")

        frame.columnconfigure(0, weight=1)

    def _build_section_logs(self, parent: tk.Widget) -> None:
        frame = ttk.LabelFrame(parent, text="7. Логи и вывод команд")
        frame.pack(fill=tk.BOTH, expand=True)

        self.log_widget = tk.Text(frame, wrap=tk.WORD, state=tk.DISABLED, font=("Consolas", 10))
        scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.log_widget.yview)
        self.log_widget.configure(yscrollcommand=scrollbar.set)

        self.log_widget.grid(row=0, column=0, padx=(10, 0), pady=10, sticky="nsew")
        scrollbar.grid(row=0, column=1, padx=(0, 10), pady=10, sticky="ns")

        buttons = ttk.Frame(frame)
        buttons.grid(row=1, column=0, columnspan=2, padx=10, pady=(0, 10), sticky="ew")
        btn_copy_log = ttk.Button(buttons, text="Скопировать лог", command=self._copy_log)
        self._register_widget(btn_copy_log)
        btn_copy_log.pack(side=tk.LEFT)
        btn_clear_log = ttk.Button(buttons, text="Очистить лог", command=self._clear_log)
        self._register_widget(btn_clear_log)
        btn_clear_log.pack(side=tk.LEFT, padx=6)

        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _append_log(self, _channel: str, message: str) -> None:
        def _write() -> None:
            self.log_widget.configure(state=tk.NORMAL)
            self.log_widget.insert(tk.END, message + "\n")
            self.log_widget.see(tk.END)
            self.log_widget.configure(state=tk.DISABLED)

        self.root.after(0, _write)

    def _copy_log(self) -> None:
        content = self.log_widget.get("1.0", tk.END).strip()
        self.root.clipboard_clear()
        self.root.clipboard_append(content)
        self.status_var.set("Лог скопирован в буфер обмена")

    def _clear_log(self) -> None:
        self.log_widget.configure(state=tk.NORMAL)
        self.log_widget.delete("1.0", tk.END)
        self.log_widget.configure(state=tk.DISABLED)

    def _build_footer(self, parent: tk.Widget) -> None:
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.X, pady=(6, 0))

        ttk.Label(frame, text="Проект:").pack(side=tk.LEFT, padx=(0, 4))
        self._create_link(frame, "github.com/HellEvro/Arbitrage", "https://github.com/HellEvro/Arbitrage")

        ttk.Label(frame, text="   Telegram:").pack(side=tk.LEFT, padx=(12, 4))
        self._create_link(frame, "h3113vr0", "https://t.me/h3113vr0")

        ttk.Label(frame, text="   Email:").pack(side=tk.LEFT, padx=(12, 4))
        self._create_link(frame, "gci.company.ou@gmail.com", "mailto:gci.company.ou@gmail.com")

        frame.pack_propagate(False)

    def _register_widget(self, widget: tk.Widget) -> None:
        self.interactive_widgets.append(widget)

    def _create_link(self, parent: tk.Widget, text: str, url: str) -> None:
        label = ttk.Label(parent, text=text, foreground="#0a66c2", cursor="hand2")
        label.pack(side=tk.LEFT)
        label.bind("<Button-1>", lambda _event: self._open_url(url))

    def _set_busy(self, description: str) -> None:
        self.status_var.set(f"{description}...")
        self._show_progress()
        self.progress_bar.start(12)
        for widget in self.interactive_widgets:
            widget.configure(state=tk.DISABLED)

    def _clear_busy(self) -> None:
        self.progress_bar.stop()
        self.progress_bar["value"] = 0
        self.status_var.set("Готово")
        self._hide_progress()
        for widget in self.interactive_widgets:
            widget.configure(state=tk.NORMAL)
        self._update_step2_state()

    def _start_task(self, description: str, on_start: Optional[Callable[[], None]]) -> None:
        self._set_busy(description)
        if on_start:
            on_start()

    def _finish_task(self, on_finish: Optional[Callable[[], None]]) -> None:
        if on_finish:
            on_finish()
        self._clear_busy()
        self._refresh_all()

    def _run_async(
        self,
        func: Callable[[], None],
        description: str,
        *,
        on_start: Optional[Callable[[], None]] = None,
        on_finish: Optional[Callable[[], None]] = None,
    ) -> None:
        def worker() -> None:
            with self.current_task_lock:
                self.root.after(0, lambda: self._start_task(description, on_start))
                try:
                    self._append_log("system", f"▶ {description}")
                    func()
                    self._append_log("system", f"✔ {description}")
                except Exception as exc:  # noqa: BLE001
                    self._append_log("system", f"✖ {description}: {exc}")
                    self.root.after(0, lambda: messagebox.showerror("Ошибка", str(exc)))
                finally:
                    self.root.after(0, lambda: self._finish_task(on_finish))

        threading.Thread(target=worker, daemon=True).start()

    def _run_command(
        self,
        args: Iterable[str],
        channel: str,
        description: str,
        *,
        extra_env: Optional[dict] = None,
    ) -> None:
        cmd = [str(arg) for arg in args]
        self._append_log(channel, "$ " + " ".join(quote(a) for a in cmd))
        env = os.environ.copy()
        env.setdefault("LANG", "C.UTF-8")
        env.setdefault("LC_ALL", "C.UTF-8")
        env.setdefault("PYTHONIOENCODING", "utf-8")
        env.setdefault("GIT_TERMINAL_PROMPT", "0")
        if extra_env:
            env.update(extra_env)
        process = subprocess.Popen(
            cmd,
            cwd=self.project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        )
        assert process.stdout is not None
        for line in process.stdout:
            self._append_log(channel, line.rstrip())
        exit_code = process.wait()
        if exit_code != 0:
            raise RuntimeError(f"{description} завершилось с кодом {exit_code}")

    def _open_path(self, path: Path) -> None:
        path = path.resolve()
        if path.is_dir() and not path.exists():
            path.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            messagebox.showwarning("Путь не найден", str(path))
            return
        try:
            if sys.platform.startswith("win"):
                os.startfile(path)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Ошибка", f"Не удалось открыть {path}:\n{exc}")

    def _open_url(self, url: str) -> None:
        try:
            webbrowser.open(url, new=2)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Ошибка", f"Не удалось открыть ссылку:\n{exc}")

    def _show_progress(self) -> None:
        if not self._progress_visible:
            self.progress_bar.pack(side=tk.RIGHT, padx=(10, 0))
            self._progress_visible = True

    def _hide_progress(self) -> None:
        if self._progress_visible:
            self.progress_bar.pack_forget()
            self._progress_visible = False

    def _venv_python(self) -> str:
        python = self.launcher.python_in_venv()
        if python.exists():
            return str(python)
        return self._system_python()

    def _system_python(self) -> str:
        base_executable = getattr(sys, "_base_executable", None)
        if base_executable:
            return base_executable
        if sys.prefix != getattr(sys, "base_prefix", sys.prefix):
            suffix = "Scripts/python.exe" if os.name == "nt" else "bin/python3"
            candidate = Path(getattr(sys, "base_prefix", sys.prefix)) / suffix
            if candidate.exists():
                return str(candidate)
        return shutil.which("python3") or shutil.which("python") or sys.executable

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------

    def _create_or_update_venv(self) -> None:
        python = self._system_python()
        self._append_log("system", "Создание виртуального окружения (.venv)")
        self._run_command([python, "-m", "venv", str(self.launcher.venv_path)], "system", "Создание окружения")
        self._venv_bootstrap_done = False
        python_version = subprocess.check_output([python, "-c", "import sys; print(sys.version)"], text=True).strip()
        self._append_log("system", f"Используется интерпретатор: {python} (версия: {python_version})")

    def _remove_venv(self) -> None:
        if not self.launcher.venv_path.exists():
            self._append_log("system", "Окружение отсутствует — нечего удалять.")
            return
        if sys.prefix != getattr(sys, "base_prefix", sys.prefix):
            raise RuntimeError("Нельзя удалить .venv пока менеджер запущен из него.")
        shutil.rmtree(self.launcher.venv_path)
        self._venv_bootstrap_done = False

    def _install_system_dependencies(self) -> None:
        python = self._system_python()
        self._run_command(
            [python, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"],
            "system",
            "Обновление pip",
        )
        self._run_command(
            [python, "-m", "pip", "install", "-r", str(self.launcher.requirements)],
            "system",
            "Установка зависимостей в системный Python",
        )

    def _git_fetch_and_reset(self) -> None:
        self._run_command(["git", "fetch", "--all"], "system", "git fetch")
        self._run_command(["git", "reset", "--hard", "origin/main"], "system", "git reset")

    def _start_all_services(self) -> None:
        for service in self.services_config:
            self._start_service(service["id"])

    def _stop_all_services(self) -> None:
        for service in self.services_config:
            self._stop_service(service["id"])

    def _start_service(self, service_id: str) -> None:
        if service_id in self.processes and self.processes[service_id].poll() is None:
            self._append_log("system", f"{service_id}: уже запущен.")
            return

        self._ensure_venv_bootstrap()
        self._ensure_config_exists()
        service = next((item for item in self.services_config if item["id"] == service_id), None)
        if not service:
            raise RuntimeError(f"Неизвестный сервис {service_id}")

        command_template = service["command"]
        resolved_command = []
        for item in command_template:
            if item == "{python}":
                resolved_command.append(self._venv_python())
            else:
                resolved_command.append(item)

        self._append_log("system", "$ " + " ".join(quote(item) for item in resolved_command))

        process = subprocess.Popen(
            resolved_command,
            cwd=self.project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.processes[service_id] = process
        self.root.after(0, lambda sid=service_id: self.service_states[sid].set("Запущен"))
        threading.Thread(
            target=self._stream_process_output,
            args=(service_id, process),
            daemon=True,
        ).start()

    def _stop_service(self, service_id: str) -> None:
        process = self.processes.get(service_id)
        if not process or process.poll() is not None:
            self._append_log("system", f"{service_id}: не запущен.")
            self.root.after(0, lambda sid=service_id: self.service_states[sid].set("Не запущен"))
            return
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        self.root.after(0, lambda sid=service_id: self.service_states[sid].set("Не запущен"))
        self._append_log("system", f"{service_id}: остановлен.")

    def _stream_process_output(self, service_id: str, process: subprocess.Popen[str]) -> None:
        assert process.stdout is not None
        for line in process.stdout:
            self._append_log("system", line.rstrip())
        exit_code = process.wait()
        self._append_log("system", f"[{service_id}] завершился с кодом {exit_code}")
        self.root.after(0, lambda sid=service_id: self.service_states[sid].set("Не запущен"))

    def _start_repo_sync(self) -> None:
        threading.Thread(target=self._initial_repo_sync, daemon=True).start()

    def _initial_repo_sync(self) -> None:
        if shutil.which("git") is None:
            self._append_log("system", "Git не найден в PATH. Пропускаю авто-синхронизацию репозитория.")
            return

        git_dir = self.project_root / ".git"
        if not git_dir.exists():
            self._append_log("system", "Git репозиторий не найден. Инициализирую новый...")
            if not self._git_cmd(["init"], "git init"):
                return
            self._git_cmd(["remote", "add", "origin", REPO_URL], "git remote add origin", allow_fail=True)
            self._git_cmd(["fetch", "origin"], "git fetch origin", allow_fail=True)
            self._git_cmd(["checkout", "-B", "main", "origin/main"], "git checkout -B main origin/main", allow_fail=True)
            self._git_cmd(["pull", "origin", "main"], "git pull origin main", allow_fail=True)
        else:
            # Существующий репозиторий — только обновим информацию о remote, не трогая локальные изменения.
            if not self._git_cmd(["remote", "get-url", "origin"], "git remote get-url origin", allow_fail=True):
                self._git_cmd(["remote", "add", "origin", REPO_URL], "git remote add origin", allow_fail=True)
            self._git_cmd(["fetch", "origin"], "git fetch origin", allow_fail=True)

        self.root.after(0, self._refresh_all)

    def _git_cmd(self, args: List[str], description: str, allow_fail: bool = False) -> bool:
        cmd = ["git"] + args
        try:
            self._run_command(cmd, "system", description)
            return True
        except RuntimeError:
            self._append_log("system", f"Ошибка при выполнении: {' '.join(cmd)}")
            return allow_fail

    def _ensure_venv_bootstrap(self) -> None:
        if not self.launcher.venv_exists():
            self._create_or_update_venv()
            self._venv_bootstrap_done = False
        if self._venv_bootstrap_done:
            return
        venv_python = self._venv_python()
        self._append_log("system", "Обновление pip/setuptools/wheel в .venv")
        self._run_command(
            [venv_python, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"],
            "system",
            "Обновление pip в .venv",
        )
        self._append_log("system", "Установка зависимостей в .venv (requirements.txt)")
        self._run_command(
            [venv_python, "-m", "pip", "install", "-r", str(self.launcher.requirements)],
            "system",
            "Установка зависимостей в .venv",
        )
        self._venv_bootstrap_done = True

    # ------------------------------------------------------------------
    # Status updates
    # ------------------------------------------------------------------

    def _refresh_all(self) -> None:
        self._update_env_status()
        self._update_config_status()
        self._update_license_status()
        self._update_git_status()
        self._update_step2_state()

    def _update_env_status(self) -> None:
        if self.launcher.venv_exists():
            self.venv_status_var.set("Виртуальное окружение создано (используется .venv)")
        else:
            self.venv_status_var.set("Виртуальное окружение не создано (используется системный Python)")

    def _update_config_status(self) -> None:
        exists = self.launcher.config_file.exists()
        status = "Найден config.yaml" if exists else "config.yaml отсутствует"
        self.config_status_var.set(status)
        self.deps_hint_var.set(
            "Создайте .venv в пункте 1, чтобы этот шаг стал неактивным." if self.launcher.venv_exists() else ""
        )

    def _update_git_status(self) -> None:
        try:
            output = subprocess.check_output(
                ["git", "status", "-sb"],
                cwd=self.project_root,
                stderr=subprocess.STDOUT,
                text=True,
            )
            first_line = output.strip().splitlines()[0] if output.strip() else "нет данных"
            self.git_status_var.set(first_line)
        except FileNotFoundError:
            self.git_status_var.set("git недоступен")
        except subprocess.CalledProcessError as exc:
            self.git_status_var.set(exc.output.strip() or "Ошибка git status")

    def _update_step2_state(self) -> None:
        state = tk.DISABLED if self.launcher.venv_exists() else tk.NORMAL
        for widget in self.step2_controls:
            widget.configure(state=state)

    def _update_license_status(self) -> None:
        lic_files = sorted(self.project_root.glob("*.lic"))
        if lic_files:
            self.license_status_var.set(f"Статус лицензии: найден {lic_files[0].name}")
        else:
            self.license_status_var.set("Статус лицензии: файл лицензии не найден")

    def _ensure_config_exists(self) -> None:
        if self.launcher.config_file.exists():
            return
        if not self.launcher.config_example.exists():
            raise FileNotFoundError(f"Пример конфигурации не найден: {self.launcher.config_example}")
        self.launcher.config_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.launcher.config_example, self.launcher.config_file)

    # ------------------------------------------------------------------
    # Window state
    # ------------------------------------------------------------------

    def _restore_window_geometry(self) -> None:
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            size = data.get("size")
            if size:
                self.root.geometry(size)
        except Exception:  # noqa: BLE001
            pass

    def _save_window_geometry(self) -> None:
        try:
            width = self.root.winfo_width()
            height = self.root.winfo_height()
            data = {"size": f"{width}x{height}"}
            self.state_path.write_text(json.dumps(data), encoding="utf-8")
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def _on_close(self) -> None:
        running = [sid for sid, proc in self.processes.items() if proc.poll() is None]
        if running:
            if not messagebox.askyesno(
                "Подтверждение",
                f"Активны процессы: {', '.join(running)}. Остановить их и выйти?",
            ):
                return
            for sid in running:
                self._stop_service(sid)
        self._save_window_geometry()
        self.root.destroy()

    # ------------------------------------------------------------------
    # Entrypoint
    # ------------------------------------------------------------------

    def run(self) -> None:
        self._append_log("system", f"Arbitrage Manager — {platform.system()} {platform.release()}")
        self._append_log("system", f"Рабочая директория: {self.project_root}")
        self.root.mainloop()


def main() -> None:
    ArbitrageManagerApp().run()


if __name__ == "__main__":
    main()
