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


def _system_python_executable() -> str:
    base_executable = getattr(sys, "_base_executable", None)
    if base_executable:
        return base_executable

    base_prefix = getattr(sys, "base_prefix", sys.prefix)
    if sys.prefix != base_prefix:
        suffix = "Scripts\\python.exe" if os.name == "nt" else "bin/python3"
        candidate = Path(base_prefix) / suffix
        if candidate.exists():
            return str(candidate)

    return shutil.which("python3") or shutil.which("python") or sys.executable


def _ensure_running_outside_venv() -> None:
    base_prefix = getattr(sys, "base_prefix", sys.prefix)
    if sys.prefix == base_prefix:
        return

    if os.environ.get("ARBITRAGE_MANAGER_REEXEC") == "1":
        return

    base_python = _system_python_executable()
    env = os.environ.copy()
    env["ARBITRAGE_MANAGER_REEXEC"] = "1"
    script_path = Path(__file__).resolve()
    os.execve(base_python, [base_python, str(script_path), *sys.argv[1:]], env)


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
        self.git_status_var = tk.StringVar()
        self.config_status_var = tk.StringVar()

        self.service_state_var = tk.StringVar(value="Остановлен")
        self.processes: Dict[str, subprocess.Popen[str]] = {}

        self.interactive_widgets: List[tk.Widget] = []
        self.current_task_lock = threading.Lock()
        self._venv_bootstrap_done = False
        self._controls_enabled = False

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

        self._build_status_section(main)
        self._build_control_section(main)
        self._build_docs_section(main)
        self._build_logs_section(main)
        self._build_footer(main)

        self._update_controls_enabled(False)
        threading.Thread(target=self._bootstrap_environment, daemon=True).start()

    def _build_status_section(self, parent: tk.Widget) -> None:
        frame = ttk.LabelFrame(parent, text="Состояние окружения")
        frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame, text="Git:").grid(row=0, column=0, padx=10, pady=6, sticky="w")
        links = ttk.Frame(frame)
        links.grid(row=0, column=1, padx=4, pady=6, sticky="w")
        self._create_link(links, "github.com/HellEvro/Arbitrage", "https://github.com/HellEvro/Arbitrage")
        ttk.Label(links, text="   Telegram:").pack(side=tk.LEFT, padx=(12, 4))
        self._create_link(links, "t.me/h3113vr0", "https://t.me/h3113vr0")
        ttk.Label(links, text="   Email:").pack(side=tk.LEFT, padx=(12, 4))
        self._create_link(links, "gci.company.ou@gmail.com", "mailto:gci.company.ou@gmail.com")

        ttk.Label(frame, text="Статус Git:").grid(row=1, column=0, padx=10, pady=6, sticky="w")
        ttk.Label(frame, textvariable=self.git_status_var).grid(row=1, column=1, padx=4, pady=6, sticky="w")

        ttk.Label(frame, text="Виртуальное окружение:").grid(row=2, column=0, padx=10, pady=6, sticky="w")
        ttk.Label(frame, textvariable=self.venv_status_var).grid(row=2, column=1, padx=4, pady=6, sticky="w")

        ttk.Label(frame, text="config.yaml:").grid(row=3, column=0, padx=10, pady=6, sticky="w")
        ttk.Label(frame, textvariable=self.config_status_var).grid(row=3, column=1, padx=4, pady=6, sticky="w")

        frame.columnconfigure(1, weight=1)

    def _build_control_section(self, parent: tk.Widget) -> None:
        frame = ttk.LabelFrame(parent, text="Управление запуском")
        frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame, text="Web UI (main.py, порт 5152)").grid(row=0, column=0, padx=10, pady=6, sticky="w")
        ttk.Label(frame, textvariable=self.service_state_var).grid(row=0, column=1, padx=6, pady=6, sticky="w")

        self.start_button = ttk.Button(
            frame,
            text="Запустить",
            command=lambda: self._run_async(
                lambda: self._start_service("web"),
                "Запуск Web UI",
                on_start=lambda: self.service_state_var.set("Запуск..."),
            ),
        )
        self._register_widget(self.start_button)
        self.start_button.grid(row=0, column=2, padx=4, pady=6)

        self.stop_button = ttk.Button(
            frame,
            text="Остановить",
            command=lambda: self._run_async(
                lambda: self._stop_service("web"),
                "Остановка Web UI",
                on_start=lambda: self.service_state_var.set("Остановка..."),
            ),
        )
        self._register_widget(self.stop_button)
        self.stop_button.grid(row=0, column=3, padx=4, pady=6)

        self.config_button = ttk.Button(
            frame,
            text="Открыть config.yaml",
            command=lambda: self._open_path(self.launcher.config_file),
        )
        self._register_widget(self.config_button)
        self.config_button.grid(row=0, column=4, padx=10, pady=6)

        frame.columnconfigure(1, weight=1)

    def _update_controls_enabled(self, enabled: bool) -> None:
        self._controls_enabled = enabled
        state = tk.NORMAL if enabled else tk.DISABLED
        for btn in (getattr(self, "start_button", None), getattr(self, "stop_button", None)):
            if btn is not None:
                btn.configure(state=state)
        if enabled:
            if "web" in self.processes and self.processes["web"].poll() is None:
                self.service_state_var.set("Запущен")
            else:
                self.service_state_var.set("Остановлен")
        else:
            self.service_state_var.set("Недоступно")

    def _build_docs_section(self, parent: tk.Widget) -> None:
        frame = ttk.LabelFrame(parent, text="Документация и файлы")
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
            text="Открыть лог приложения",
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

    def _build_logs_section(self, parent: tk.Widget) -> None:
        frame = ttk.LabelFrame(parent, text="Логи и вывод команд")
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
        self._update_controls_enabled(self._controls_enabled)

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
        log_prefix: str = "",
    ) -> None:
        cmd = [str(arg) for arg in args]
        self._append_log(channel, f"{log_prefix}$ " + " ".join(quote(a) for a in cmd))
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
            self._append_log(channel, f"{log_prefix}{line.rstrip()}")
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
        return _system_python_executable()

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------

    def _create_or_update_venv(self) -> None:
        python = _system_python_executable()
        self._append_log("system", "Создание виртуального окружения (.venv)")
        try:
            self._run_command([python, "-m", "venv", str(self.launcher.venv_path)], "system", "Создание окружения")
        except RuntimeError as exc:
            if self.launcher.venv_path.exists():
                self._append_log(
                    "system",
                    "Создание окружения не удалось. Удаляю повреждённую папку .venv и пробую ещё раз...",
                )
                shutil.rmtree(self.launcher.venv_path, ignore_errors=True)
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

    def _start_service(self, service_id: str) -> None:
        if service_id in self.processes and self.processes[service_id].poll() is None:
            self._append_log("system", f"{service_id}: уже запущен.")
            return

        self._ensure_venv_bootstrap()
        self._ensure_config_exists()
        if service_id != "web":
            raise RuntimeError(f"Неизвестный сервис {service_id}")

        command = [self._venv_python(), "main.py"]
        self._append_log("system", "$ " + " ".join(quote(item) for item in command))

        process = subprocess.Popen(
            command,
            cwd=self.project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.processes[service_id] = process
        self.root.after(0, lambda: self.service_state_var.set("Запущен"))
        threading.Thread(target=self._stream_process_output, args=(service_id, process), daemon=True).start()

    def _stop_service(self, service_id: str) -> None:
        process = self.processes.get(service_id)
        if not process or process.poll() is not None:
            self._append_log("system", f"{service_id}: не запущен.")
            self.root.after(0, lambda: self.service_state_var.set("Остановлен"))
            return
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        self.root.after(0, lambda: self.service_state_var.set("Остановлен"))
        self._append_log("system", f"{service_id}: остановлен.")

    def _stream_process_output(self, service_id: str, process: subprocess.Popen[str]) -> None:
        assert process.stdout is not None
        for line in process.stdout:
            self._append_log("system", line.rstrip())
        exit_code = process.wait()
        self._append_log("system", f"[{service_id}] завершился с кодом {exit_code}")
        self.root.after(0, lambda: self.service_state_var.set("Остановлен"))

    def _bootstrap_environment(self) -> None:
        ready = False
        try:
            self.status_var.set("Подготовка окружения...")
            self._append_log("system", "Проверяю проект...")
            repo_ready = self._ensure_repo_ready()
            venv_ready = self._ensure_venv_bootstrap() if repo_ready else False
            ready = repo_ready and venv_ready
            if ready:
                self._append_log("system", "Проект готов к запуску.")
        except Exception as exc:  # noqa: BLE001
            self._append_log("system", f"Ошибка подготовки: {exc}")
            self.status_var.set("Ошибка — подробности в логе")
        else:
            self.status_var.set("Готово" if ready else "Требуется ручная проверка")
        finally:
            self.root.after(0, self._refresh_all)
            self.root.after(0, lambda: self._update_controls_enabled(ready))

    def _ensure_repo_ready(self) -> bool:
        if shutil.which("git") is None:
            self._append_log("system", "Git не найден в PATH. Пропускаю авто-синхронизацию репозитория.")
            return True

        git_dir = self.project_root / ".git"
        if not git_dir.exists():
            self._append_log("system", "Git репозиторий не найден. Инициализирую новый...")
            if not self._git_cmd(["init"], "git init"):
                return False
            self._git_cmd(["remote", "add", "origin", REPO_URL], "git remote add origin", allow_fail=True)
            if not self._git_cmd(["fetch", "origin"], "git fetch origin"):
                return False
            backup_dir = self.project_root / ".backup_before_git"
            self._append_log("system", f"Создаю резервную копию текущего каталога в {backup_dir}...")
            if backup_dir.exists():
                shutil.rmtree(backup_dir, ignore_errors=True)
            backup_dir.mkdir(exist_ok=True)
            for entry in self.project_root.iterdir():
                if entry.name in {".git", ".backup_before_git"}:
                    continue
                try:
                    shutil.move(str(entry), str(backup_dir / entry.name))
                except Exception as exc:  # noqa: BLE001
                    self._append_log("system", f"Не удалось переместить {entry}: {exc}")
            try:
                if self._git_cmd(["checkout", "-f", "origin/main"], "git checkout origin/main"):
                    self._git_cmd(["branch", "-M", "main"], "git branch -M main", allow_fail=True)
                    self._git_cmd(["pull", "--ff-only", "origin", "main"], "git pull --ff-only origin main", allow_fail=True)
                    self._append_log(
                        "system",
                        f"Репозиторий загружен. Резервная копия исходных файлов сохранена в {backup_dir} (удалите вручную, если не нужна).",
                    )
            except Exception as exc:  # noqa: BLE001
                self._append_log("system", f"Ошибка инициализации репозитория: {exc}")
                self._append_log("system", "Восстанавливаю исходное состояние...")
                for entry in backup_dir.iterdir():
                    shutil.move(str(entry), str(self.project_root / entry.name))
                shutil.rmtree(backup_dir, ignore_errors=True)
                raise
        else:
            # Существующий репозиторий — обновим информацию о remote и мягко подтянем изменения.
            if not self._git_cmd(["remote", "get-url", "origin"], "git remote get-url origin", allow_fail=True):
                self._git_cmd(["remote", "add", "origin", REPO_URL], "git remote add origin", allow_fail=True)
            if self._git_cmd(["fetch", "origin"], "git fetch origin", allow_fail=True):
                result = self._git_cmd(["pull", "--ff-only", "origin", "main"], "git pull --ff-only origin main", allow_fail=True)
                if not result:
                    self._append_log(
                        "system",
                        "Не удалось автоматически обновить ветку (возможно, есть локальные изменения). "
                        "Оставляю локальное состояние без изменений.",
                    )
        return True

    def _git_cmd(self, args: List[str], description: str, allow_fail: bool = False) -> bool:
        cmd = ["git"] + args
        try:
            self._run_command(cmd, "system", description)
            return True
        except RuntimeError:
            self._append_log("system", f"Ошибка при выполнении: {' '.join(cmd)}")
            return allow_fail

    def _ensure_venv_bootstrap(self) -> bool:
        if not self.launcher.venv_exists():
            self._create_or_update_venv()
            self._venv_bootstrap_done = False
        if self._venv_bootstrap_done:
            return True
        venv_python = self._venv_python()
        self._append_log("system", "Обновление pip/setuptools/wheel в .venv")
        self._run_command(
            [venv_python, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"],
            "system",
            "Обновление pip в .venv",
            log_prefix="[venv] ",
        )
        self._append_log("system", "Установка зависимостей в .venv (requirements.txt)")
        self._run_command(
            [venv_python, "-m", "pip", "install", "-r", str(self.launcher.requirements)],
            "system",
            "Установка зависимостей в .venv",
            log_prefix="[venv] ",
        )
        self._venv_bootstrap_done = True
        return True

    # ------------------------------------------------------------------
    # Status updates
    # ------------------------------------------------------------------

    def _refresh_all(self) -> None:
        self._update_env_status()
        self._update_config_status()
        self._update_git_status()

    def _update_env_status(self) -> None:
        if self.launcher.venv_exists():
            self.venv_status_var.set("Виртуальное окружение создано (используется .venv)")
        else:
            self.venv_status_var.set("Виртуальное окружение не создано (используется системный Python)")

    def _update_config_status(self) -> None:
        exists = self.launcher.config_file.exists()
        status = "Найден config.yaml" if exists else "config.yaml отсутствует"
        self.config_status_var.set(status)

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
    _ensure_running_outside_venv()
    ArbitrageManagerApp().run()


if __name__ == "__main__":
    main()
