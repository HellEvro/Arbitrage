from __future__ import annotations

import logging
import os
import platform
import subprocess
import time
from pathlib import Path
from typing import Sequence

log = logging.getLogger("arbitrage_bot.system")

# Get current process ID to exclude it from cleanup
_CURRENT_PID = os.getpid()


def find_process_on_port(port: int) -> list[int]:
    """Find process IDs using the specified port.
    
    Returns list of PIDs (process IDs) that are using the port.
    Works on Windows and Unix-like systems.
    
    IMPORTANT: Excludes current process to avoid self-termination.
    """
    system = platform.system()
    pids: list[int] = []
    current_pid = os.getpid()
    
    try:
        if system == "Windows":
            # Use netstat to find processes on port - more reliable method
            result = subprocess.run(
                ["netstat", "-ano"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10.0,
            )
            for line in result.stdout.splitlines():
                # Look for lines like: TCP    0.0.0.0:5152           0.0.0.0:0              LISTENING       12345
                if f":{port}" in line and ("LISTENING" in line or "LISTEN" in line):
                    parts = line.split()
                    if len(parts) >= 5:
                        try:
                            pid = int(parts[-1])
                            # CRITICAL: Skip invalid PIDs (0 = TIME_WAIT connections, negative = invalid)
                            if pid <= 0:
                                continue
                            # CRITICAL: Skip current process
                            if pid == current_pid:
                                log.debug("Skipping current process %d on port %d", pid, port)
                                continue
                            pids.append(pid)
                            log.debug("Found process %d on port %d: %s", pid, port, line.strip())
                        except (ValueError, IndexError):
                            continue
            
            # Also try Get-NetTCPConnection (PowerShell) as fallback
            if not pids:
                try:
                    ps_cmd = f'Get-NetTCPConnection -LocalPort {port} -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess'
                    result = subprocess.run(
                        ["powershell", "-Command", ps_cmd],
                        capture_output=True,
                        text=True,
                        check=False,
                        timeout=10.0,
                    )
                    for pid_str in result.stdout.strip().split():
                        try:
                            pid = int(pid_str)
                            # CRITICAL: Skip invalid PIDs (0 = TIME_WAIT connections, negative = invalid)
                            if pid <= 0:
                                continue
                            # CRITICAL: Skip current process
                            if pid == current_pid:
                                log.debug("Skipping current process %d on port %d (PowerShell)", pid, port)
                                continue
                            if pid not in pids:
                                pids.append(pid)
                                log.debug("Found process %d on port %d via PowerShell", pid, port)
                        except ValueError:
                            continue
                except Exception as e:
                    log.debug("PowerShell method failed: %s", e)
        else:
            # Unix-like systems: use lsof or ss
            try:
                result = subprocess.run(
                    ["lsof", "-ti", f":{port}"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                for pid_str in result.stdout.strip().split():
                    try:
                        pids.append(int(pid_str))
                    except ValueError:
                        continue
            except FileNotFoundError:
                # Try ss as fallback
                result = subprocess.run(
                    ["ss", "-ltnp"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                for line in result.stdout.splitlines():
                    if f":{port}" in line:
                        # Extract PID from line (format varies)
                        parts = line.split()
                        for part in parts:
                            if "pid=" in part:
                                try:
                                    pid = int(part.split("=")[1].split(",")[0])
                                    pids.append(pid)
                                except (ValueError, IndexError):
                                    continue
    except Exception as e:
        log.warning("Failed to find processes on port %d: %s", port, e)
    
    return list(set(pids))  # Remove duplicates


def is_python_process(pid: int) -> bool:
    """Check if process with given PID is a Python process - fast version with timeout."""
    system = platform.system()
    
    try:
        if system == "Windows":
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV"],
                capture_output=True,
                text=True,
                check=False,
                timeout=2.0,  # Add timeout
            )
            output_lower = result.stdout.lower()
            return "python.exe" in output_lower or "pythonw.exe" in output_lower
        else:
            # Unix-like: check /proc/PID/cmdline - fastest method
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as f:
                    cmdline = f.read().decode("utf-8", errors="ignore")
                    return "python" in cmdline.lower()
            except (FileNotFoundError, PermissionError):
                return False  # If we can't check, assume not Python
    except subprocess.TimeoutExpired:
        return False  # If timeout, assume not Python
    except Exception:
        return False  # If check fails, assume not Python


def kill_process(pid: int, timeout: float = 10.0) -> bool:
    """Kill a process by PID - aggressive approach.
    
    Returns True if process was killed successfully, False otherwise.
    
    CRITICAL: Never kills current process (os.getpid()).
    """
    system = platform.system()
    current_pid = os.getpid()
    
    # CRITICAL: Never kill ourselves
    if pid == current_pid:
        log.error("CRITICAL: Attempted to kill current process %d! Refusing!", pid)
        return False
    
    # Quick check first
    try:
        if not is_process_running(pid):
            log.debug("Process %d is already not running", pid)
            return True
    except Exception as e:
        log.debug("Error checking if process %d is running: %s", pid, e)
    
    # Проверяем, существует ли процесс перед убийством
    if not is_process_running(pid):
        log.debug("Process %d is already not running", pid)
        return True
    
    try:
        if system == "Windows":
            # Сначала пробуем PowerShell для более агрессивного убийства дерева процессов
            try:
                ps_script = f'''
                function Kill-ProcessTree {{
                    param($ProcessId)
                    $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
                    if ($process) {{
                        $children = Get-CimInstance Win32_Process | Where-Object {{$_.ParentProcessId -eq $ProcessId}}
                        foreach ($child in $children) {{
                            Kill-ProcessTree -ProcessId $child.ProcessId
                        }}
                        try {{
                            Stop-Process -Id $ProcessId -Force -ErrorAction Stop
                            Write-Output "Killed $ProcessId"
                        }} catch {{
                            Write-Output "Failed to kill $ProcessId"
                        }}
                    }}
                }}
                Kill-ProcessTree -ProcessId {pid}
                '''
                log.info("Force killing process %d with PowerShell (recursive tree kill)", pid)
                result_ps = subprocess.run(
                    ["powershell", "-Command", ps_script],
                    capture_output=True,
                    text=True,
                    timeout=15.0,
                    shell=False,
                    creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
                )
                # Проверяем результат
                if not is_process_running(pid):
                    log.info("Successfully killed process %d (and children) with PowerShell", pid)
                    return True
                log.warning("PowerShell kill failed for PID %d, trying taskkill", pid)
            except Exception as e:
                log.debug("PowerShell kill failed for PID %d: %s, trying taskkill", pid, e)
            
            # Fallback: Always use force kill (/F) with tree kill (/T) for immediate termination
            # /T убивает все дочерние процессы тоже
            try:
                log.info("Force killing process %d with taskkill /F /T", pid)
                result = subprocess.run(
                    ["taskkill", "/PID", str(pid), "/F", "/T"],  # /T убивает дерево процессов
                    capture_output=True,
                    text=True,
                    timeout=10.0,  # Увеличено до 10 секунд
                    shell=False,
                    creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
                )
                if result.returncode == 0:
                    log.info("Successfully killed process %d (and children)", pid)
                else:
                    # Проверяем, не завершился ли процесс уже
                    if not is_process_running(pid):
                        log.info("Process %d already terminated", pid)
                        return True
                    # Если не удалось убить с /T, пробуем без /T
                    error_msg = result.stderr.strip() if result.stderr else "Unknown error"
                    # Коды ошибок: 128 = процесс не найден, 255 = ошибка доступа
                    if "128" in error_msg or "не найден" in error_msg.lower() or "not found" in error_msg.lower():
                        log.info("Process %d not found (already terminated)", pid)
                        return True
                    log.warning("taskkill /T returned code %d for PID %d, trying without /T: %s", result.returncode, pid, error_msg)
                    result2 = subprocess.run(
                        ["taskkill", "/PID", str(pid), "/F"],
                        capture_output=True,
                        text=True,
                        timeout=5.0,
                        shell=False,
                        creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
                    )
                    if result2.returncode == 0:
                        log.info("Successfully killed process %d (without /T)", pid)
                    else:
                        # Проверяем еще раз
                        if not is_process_running(pid):
                            log.info("Process %d terminated after second attempt", pid)
                            return True
                        error_msg2 = result2.stderr.strip() if result2.stderr else "Unknown error"
                        if "128" in error_msg2 or "не найден" in error_msg2.lower() or "not found" in error_msg2.lower():
                            log.info("Process %d not found (already terminated)", pid)
                            return True
                        log.warning("taskkill failed for PID %d: %s", pid, error_msg2)
            except subprocess.TimeoutExpired:
                log.warning("taskkill timeout for PID %d", pid)
                # Проверяем, завершился ли процесс
                if not is_process_running(pid):
                    log.info("Process %d terminated despite timeout", pid)
                    return True
            except Exception as e:
                log.warning("taskkill exception for PID %d: %s", pid, e)
                # Проверяем, завершился ли процесс
                if not is_process_running(pid):
                    log.info("Process %d terminated despite exception", pid)
                    return True
        else:
            # Send SIGTERM first (graceful)
            subprocess.run(["kill", "-TERM", str(pid)], check=False, capture_output=True, timeout=3.0)
            time.sleep(0.5)
            # Force kill if still running
            if is_process_running(pid):
                subprocess.run(["kill", "-KILL", str(pid)], check=False, capture_output=True, timeout=3.0)
        
        # Wait for process to terminate - simplified approach
        # Check a few times with short intervals, but don't block too long
        start_time = time.time()
        for check_num in range(5):  # Check up to 5 times (1 second total)
            time.sleep(0.2)  # Wait 0.2 seconds between checks
            try:
                if not is_process_running(pid):
                    return True
            except Exception:
                # If check fails, assume terminated
                return True
        
        # Final check - if still running, return False but don't block
        try:
            return not is_process_running(pid)
        except Exception:
            # If we can't check, assume success to avoid blocking
            return True
    except Exception as e:
        log.warning("Failed to kill process %d: %s", pid, e)
        # Try one more time with force - более агрессивно с /T
        try:
            if system == "Windows" and is_process_running(pid):
                log.info("Retry: Force killing process %d with /T flag", pid)
                # Пробуем с /T (дерево процессов)
                result = subprocess.run(
                    ["taskkill", "/PID", str(pid), "/F", "/T"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=10.0
                )
                time.sleep(1.0)  # Даем больше времени на завершение
                if not is_process_running(pid):
                    log.info("Process %d killed on retry", pid)
                    return True
                # Если всё ещё работает, пробуем без /T
                log.warning("Process %d still running after /T, trying without /T", pid)
                subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=False, capture_output=True, timeout=5.0)
                time.sleep(0.5)
                return not is_process_running(pid)
        except Exception as retry_e:
            log.warning("Retry kill failed for PID %d: %s", pid, retry_e)
        return False


def is_process_running(pid: int) -> bool:
    """Check if process with given PID is still running.
    
    Uses fast method with timeout to avoid hanging.
    """
    system = platform.system()
    
    try:
        if system == "Windows":
            # Use tasklist with timeout
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV"],
                capture_output=True,
                text=True,
                check=False,
                timeout=2.0,  # Fast timeout
            )
            # Check if PID appears in output (excluding header)
            output_lines = result.stdout.strip().splitlines()
            if len(output_lines) > 1:  # Has header + data
                for line in output_lines[1:]:  # Skip header
                    if f'"{pid}"' in line or f',{pid},' in line:
                        return True
            return False
        else:
            # Check if /proc/PID exists
            import os
            return os.path.exists(f"/proc/{pid}")
    except subprocess.TimeoutExpired:
        log.debug("Timeout checking process %d, assuming running", pid)
        return True  # Assume running if timeout
    except Exception as e:
        log.debug("Error checking process %d: %s, assuming not running", pid, e)
        return False  # Assume not running on error


def find_python_processes_by_command(port: int) -> list[int]:
    """Find Python processes that might be running our application.
    
    Looks for processes with 'main.py' or 'arbitrage' in command line.
    This is a fallback if port-based detection doesn't work.
    
    IMPORTANT: Excludes current process to avoid self-termination.
    Uses fast PowerShell method with timeout.
    """
    system = platform.system()
    pids: list[int] = []
    current_pid = os.getpid()
    
    try:
        if system == "Windows":
            # Use PowerShell with timeout - faster and more reliable
            # Get all Python processes and check their command lines
            # Use wmic with better formatting
            result = subprocess.run(
                ["wmic", "process", "where", "name='python.exe'", "get", "ProcessId,CommandLine,ExecutablePath"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10.0,
            )
            current_dir = Path.cwd().resolve()
            current_dir_str = str(current_dir).lower()
            
            # Parse wmic output (format is messy, need to handle it carefully)
            lines = result.stdout.splitlines()
            cmdline = ""
            pid = None
            for line in lines:
                line = line.strip()
                if not line or "ProcessId" in line or "CommandLine" in line or "ExecutablePath" in line:
                    continue
                
                # Try to extract PID (usually at the end)
                parts = line.split()
                if parts:
                    # Check if last part is a number (PID)
                    try:
                        potential_pid = int(parts[-1])
                        # If we have a cmdline, this might be the PID
                        if cmdline and potential_pid > 0:
                            pid = potential_pid
                            # Check if it's our application
                            cmdline_lower = cmdline.lower()
                            if ("main.py" in cmdline_lower or "arbitrage" in cmdline_lower) and current_dir_str in cmdline_lower:
                                pids.append(pid)
                                # Don't log here - may cause blocking
                            cmdline = ""
                            pid = None
                        else:
                            # This might be part of command line
                            cmdline = line
                    except ValueError:
                        # Not a PID, part of command line
                        cmdline = line if not cmdline else cmdline + " " + line
            
            # Alternative: Use PowerShell Get-Process with more reliable parsing
            # Always try PowerShell as it's more reliable than wmic
            # Ищем ВСЕ процессы Python, которые могут использовать порт
            try:
                ps_script = f'''
                $port = {port}
                Get-Process python* -ErrorAction SilentlyContinue | ForEach-Object {{
                    $proc = $_
                    try {{
                        $cmdline = (Get-CimInstance Win32_Process -Filter "ProcessId = $($proc.Id)").CommandLine
                        if ($cmdline) {{
                            # Проверяем, содержит ли командная строка main.py, arbitrage, flask, или порт
                            if ($cmdline -match "main.py" -or $cmdline -match "arbitrage" -or $cmdline -match "flask" -or $cmdline -match ":$port") {{
                                Write-Output "$($proc.Id)|$cmdline"
                            }}
                        }}
                    }} catch {{
                    }}
                }}
                '''
                result = subprocess.run(
                    ["powershell", "-Command", ps_script],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=15.0,
                )
                for line in result.stdout.strip().splitlines():
                    if "|" in line:
                        pid_str, cmdline = line.split("|", 1)
                        try:
                            pid = int(pid_str.strip())
                            cmdline_lower = cmdline.lower()
                            # CRITICAL: Skip current process to avoid self-termination
                            if pid == current_pid:
                                log.debug("Skipping current process %d", pid)
                                continue
                            # Более широкий поиск: либо содержит текущую директорию, либо main.py/arbitrage/flask
                            if (current_dir_str in cmdline_lower or 
                                "main.py" in cmdline_lower or 
                                "arbitrage" in cmdline_lower or
                                "flask" in cmdline_lower or
                                f":{port}" in cmdline):
                                if pid not in pids:
                                    pids.append(pid)
                                    log.debug("Found Python process %d by command: %s", pid, cmdline[:100])
                        except ValueError:
                            continue
            except Exception as e:
                log.debug("PowerShell method failed: %s", e)
    except Exception as e:
        log.debug("Failed to find processes by command: %s", e)
    
    return list(set(pids))  # Remove duplicates


def get_process_start_time(pid: int) -> float | None:
    """Get process start time (Unix timestamp).
    
    Returns None if process doesn't exist or can't be queried.
    """
    system = platform.system()
    
    try:
        if system == "Windows":
            # Use PowerShell to get process start time
            ps_script = f'''
            try {{
                $proc = Get-Process -Id {pid} -ErrorAction Stop
                $startTime = $proc.StartTime
                Write-Output $startTime.ToString("yyyy-MM-dd HH:mm:ss.fff")
            }} catch {{
                Write-Output "ERROR"
            }}
            '''
            result = subprocess.run(
                ["powershell", "-Command", ps_script],
                capture_output=True,
                text=True,
                timeout=3.0,
                shell=False,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
            )
            if result.returncode == 0 and "ERROR" not in result.stdout:
                try:
                    from datetime import datetime
                    time_str = result.stdout.strip()
                    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
                    return dt.timestamp()
                except Exception:
                    return None
        else:
            # Unix: use /proc/PID/stat
            try:
                with open(f"/proc/{pid}/stat", "r") as f:
                    stat = f.read().split()
                    # Start time is field 22 (0-indexed: 21)
                    # It's in jiffies (clock ticks), need to convert
                    import os
                    clock_ticks = os.sysconf(os.sysconf_names.get('SC_CLK_TCK', 100))
                    starttime_jiffies = int(stat[21])
                    # Get system uptime to calculate absolute time
                    with open("/proc/uptime", "r") as uptime_file:
                        uptime_seconds = float(uptime_file.read().split()[0])
                    starttime_seconds = uptime_seconds - (starttime_jiffies / clock_ticks)
                    # Get boot time
                    import time
                    boot_time = time.time() - uptime_seconds
                    return boot_time + starttime_seconds
            except (FileNotFoundError, IndexError, ValueError):
                return None
    except Exception:
        return None
    
    return None


def cleanup_port(port: int, wait_timeout: float = 15.0) -> bool:
    """Kill Python processes using the specified port and wait for cleanup.
    
    Простая логика:
    - Если процесс на порту один - это мы сами, ничего не делаем
    - Если процессов больше одного - убиваем все кроме самого молодого (самого недавно запущенного)
    
    Args:
        port: Port number to clean up
        wait_timeout: Maximum time to wait for processes to terminate (seconds)
    
    Returns:
        True if port is now free, False otherwise
    """
    current_pid = os.getpid()
    log.info("Checking for processes on port %d (current PID: %d)", port, current_pid)
    
    system = platform.system()
    
    # Находим ВСЕ процессы на порту (включая текущий)
    all_pids_on_port = []
    
    if system == "Windows":
        # Используем PowerShell для поиска ВСЕХ процессов на порту
        ps_find_script = f'''
        $port = {port}
        $pids = @()
        try {{
            $connections = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
            foreach ($conn in $connections) {{
                $pid = $conn.OwningProcess
                if ($pid -gt 0) {{
                    try {{
                        $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
                        if ($proc -and ($proc.ProcessName -like "*python*" -or $proc.ProcessName -eq "python" -or $proc.ProcessName -eq "pythonw")) {{
                            $pids += $pid
                        }}
                    }} catch {{
                        $pids += $pid
                    }}
                }}
            }}
        }} catch {{
            $netstat = netstat -ano | Select-String ":{port}.*LISTENING"
            foreach ($line in $netstat) {{
                $parts = $line -split '\\s+'
                if ($parts.Length -gt 0) {{
                    try {{
                        $pid = [int]$parts[-1]
                        if ($pid -gt 0) {{
                            $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
                            if ($proc -and ($proc.ProcessName -like "*python*")) {{
                                $pids += $pid
                            }}
                        }}
                    }} catch {{
                    }}
                }}
            }}
        }}
        $pids = $pids | Select-Object -Unique
        foreach ($pid in $pids) {{
            Write-Output $pid
        }}
        '''
        
        try:
            result_ps = subprocess.run(
                ["powershell", "-Command", ps_find_script],
                capture_output=True,
                text=True,
                timeout=10.0,
                shell=False,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
            )
            
            for line in result_ps.stdout.strip().splitlines():
                try:
                    pid = int(line.strip())
                    if pid > 0:
                        all_pids_on_port.append(pid)
                except ValueError:
                    continue
        except Exception as e:
            log.debug("PowerShell find failed: %s, using fallback", e)
    
    # Fallback: используем старый метод
    if not all_pids_on_port:
        pids = []
        for attempt in range(5):
            pids = find_process_on_port(port)
            if pids:
                break
            if attempt < 4:
                time.sleep(0.3)
        
        # Фильтруем только Python процессы
        for pid in pids:
            if pid > 0 and is_python_process(pid):
                if pid not in all_pids_on_port:
                    all_pids_on_port.append(pid)
    
    # Фильтруем только существующие процессы
    existing_pids = []
    for pid in all_pids_on_port:
        if is_process_running(pid):
            existing_pids.append(pid)
    
    if not existing_pids:
        log.info("No running processes found on port %d", port)
        return True
    
    log.info("Found %d running process(es) on port %d: %s", len(existing_pids), port, existing_pids)
    
    # ПРОСТАЯ ЛОГИКА: Если процесс один - это мы сами, ничего не делаем
    if len(existing_pids) == 1:
        if existing_pids[0] == current_pid:
            log.info("Only one process on port %d - this is us (PID %d). No cleanup needed.", port, current_pid)
            return True
        else:
            log.warning("Only one process found (%d) but it's not us (%d) - this shouldn't happen", existing_pids[0], current_pid)
            # Возможно, мы еще не успели занять порт, но другой процесс уже занял
            # В этом случае убиваем его
            existing_pids_to_kill = existing_pids
    else:
        # Если процессов больше одного - находим самый молодой (самый недавно запущенный)
        log.info("Multiple processes found on port %d: %s", port, existing_pids)
        
        # Получаем время запуска для каждого процесса
        process_times = []
        for pid in existing_pids:
            start_time = get_process_start_time(pid)
            if start_time is not None:
                process_times.append((pid, start_time))
                log.debug("Process %d started at %s", pid, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))
            else:
                log.warning("Could not get start time for process %d, will exclude from comparison", pid)
        
        if not process_times:
            log.warning("Could not get start times for any process, killing all except current")
            existing_pids_to_kill = [pid for pid in existing_pids if pid != current_pid]
        else:
            # Находим самый молодой процесс (с самым поздним временем запуска)
            youngest_pid, youngest_time = max(process_times, key=lambda x: x[1])
            log.info("Youngest process (most recently started): PID %d (started at %s)", 
                    youngest_pid, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(youngest_time)))
            
            # Убиваем все процессы кроме самого молодого
            existing_pids_to_kill = [pid for pid in existing_pids if pid != youngest_pid]
            
            if not existing_pids_to_kill:
                log.info("No processes to kill (only youngest process %d remains)", youngest_pid)
                return True
    
    if not existing_pids_to_kill:
        log.info("No processes to kill on port %d", port)
        return True
    
    log.info("Killing %d older process(es) on port %d: %s (keeping youngest)", len(existing_pids_to_kill), port, existing_pids_to_kill)
    
    # Убиваем все старые процессы
    killed = False
    
    if system == "Windows":
        # Убиваем каждый найденный Python процесс по PID с флагом /T (дерево процессов)
        try:
            log.info("Killing %d python.exe process(es) on port %d: %s", len(existing_pids_to_kill), port, existing_pids_to_kill)
            
            # Сначала пробуем убить все процессы одной командой через PowerShell
            if existing_pids_to_kill:
                ps_kill_all_script = f'''
                $pids = @({",".join(map(str, existing_pids_to_kill))})
                foreach ($pid in $pids) {{
                    try {{
                        $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
                        if ($proc) {{
                            Stop-Process -Id $pid -Force -ErrorAction Stop
                            Write-Output "Killed $pid"
                        }}
                    }} catch {{
                        Write-Output "Failed $pid"
                    }}
                }}
                '''
                try:
                    log.info("Attempting to kill all processes at once via PowerShell")
                    result_all = subprocess.run(
                        ["powershell", "-Command", ps_kill_all_script],
                        capture_output=True,
                        text=True,
                        timeout=15.0,
                        shell=False,
                        creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
                    )
                    # Проверяем результаты
                    time.sleep(1.0)  # Даем время на завершение
                    remaining = [pid for pid in existing_pids_to_kill if is_process_running(pid)]
                    if not remaining:
                        log.info("Successfully killed all older processes via PowerShell")
                        killed = True
                    else:
                        log.warning("Some processes still running after PowerShell kill: %s", remaining)
                        existing_pids_to_kill = remaining  # Продолжаем убивать оставшиеся
                except Exception as e:
                    log.debug("PowerShell bulk kill failed: %s, trying individual kills", e)
            
            for pid in existing_pids_to_kill:
                
                # Проверяем, что процесс еще существует и является Python
                # CRITICAL: Проверяем ДО попытки убить, чтобы не тратить время на несуществующие процессы
                if not is_process_running(pid):
                    log.info("Process %d already terminated (not found in process list), skipping kill attempt", pid)
                    killed = True  # Считаем успехом, если процесс уже завершился
                    continue
                
                # Проверяем, что это действительно Python процесс
                if not is_python_process(pid):
                    log.warning("Process %d is not a Python process, but will try to kill anyway", pid)
                
                try:
                    log.info("Killing python.exe process %d (and children) on port %d", pid, port)
                    
                    # Сначала пробуем рекурсивное убийство через PowerShell
                    ps_kill_script = f'''
                    function Kill-ProcessTree {{
                        param($ProcessId)
                        $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
                        if ($process) {{
                            $children = Get-CimInstance Win32_Process | Where-Object {{$_.ParentProcessId -eq $ProcessId}}
                            foreach ($child in $children) {{
                                Kill-ProcessTree -ProcessId $child.ProcessId
                            }}
                            try {{
                                Stop-Process -Id $ProcessId -Force -ErrorAction Stop
                                Write-Output "Killed $ProcessId"
                            }} catch {{
                                Write-Output "Failed to kill $ProcessId"
                            }}
                        }}
                    }}
                    Kill-ProcessTree -ProcessId {pid}
                    '''
                    
                    try:
                        result_ps = subprocess.run(
                            ["powershell", "-Command", ps_kill_script],
                            capture_output=True,
                            text=True,
                            timeout=15.0,
                            shell=False,
                            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
                        )
                        # Проверяем, действительно ли процесс завершился
                        time.sleep(0.3)  # Даем время на завершение
                        if not is_process_running(pid):
                            log.info("Successfully killed python.exe process %d (and children) with PowerShell", pid)
                            killed = True
                            continue
                        else:
                            log.warning("PowerShell kill reported success but process %d still running, trying taskkill", pid)
                    except Exception as e:
                        log.debug("PowerShell kill failed for PID %d: %s, trying taskkill", pid, e)
                    
                    # Fallback: используем taskkill - ПРОСТОЙ ПОДХОД БЕЗ ЗАВИСАНИЙ
                    log.info("Trying taskkill /F /T for PID %d", pid)
                    try:
                        # Используем Popen для неблокирующего выполнения
                        proc = subprocess.Popen(
                            ["taskkill", "/PID", str(pid), "/F", "/T"],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=False,
                            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
                        )
                        # Ждем максимум 5 секунд
                        try:
                            stdout, stderr = proc.communicate(timeout=5.0)
                            returncode = proc.returncode
                        except subprocess.TimeoutExpired:
                            log.warning("taskkill timeout for PID %d, killing subprocess", pid)
                            proc.kill()
                            stdout, stderr = proc.communicate()
                            returncode = -1
                        
                        stdout_str = stdout.decode('utf-8', errors='ignore') if stdout else ""
                        stderr_str = stderr.decode('utf-8', errors='ignore') if stderr else ""
                        
                        # Проверяем результат - даже если returncode != 0, процесс мог завершиться
                        time.sleep(0.3)  # Даем время на завершение
                        if not is_process_running(pid):
                            log.info("Process %d terminated successfully (taskkill returncode: %d)", pid, returncode)
                            killed = True
                        elif returncode == 0:
                            log.info("Successfully killed python.exe process %d (taskkill confirmed)", pid)
                            killed = True
                        else:
                            # taskkill вернул ошибку, но процесс все еще может существовать
                            log.warning("Failed to kill PID %d: returncode=%d, stderr=%s", pid, returncode, stderr_str[:100])
                            # Пробуем еще раз без /T
                            log.info("Retrying kill for PID %d without /T flag", pid)
                            try:
                                proc2 = subprocess.Popen(
                                    ["taskkill", "/PID", str(pid), "/F"],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    shell=False,
                                    creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
                                )
                                stdout2, stderr2 = proc2.communicate(timeout=3.0)
                                if proc2.returncode == 0 or not is_process_running(pid):
                                    log.info("Successfully killed process %d on retry", pid)
                                    killed = True
                                else:
                                    log.warning("Failed to kill PID %d even on retry", pid)
                            except Exception as e2:
                                log.warning("Exception in retry kill for PID %d: %s", pid, e2)
                                if not is_process_running(pid):
                                    log.info("Process %d terminated despite exception", pid)
                                    killed = True
                    except Exception as e:
                        log.warning("Exception running taskkill for PID %d: %s", pid, e)
                        # Проверяем, не завершился ли процесс
                        if not is_process_running(pid):
                            log.info("Process %d terminated despite exception", pid)
                            killed = True
                except Exception as e:
                    log.warning("Exception killing PID %d: %s", pid, e)
                    # Проверяем, не завершился ли процесс
                    if not is_process_running(pid):
                        log.info("Process %d terminated despite exception", pid)
                        killed = True
        except Exception as e:
            log.error("Failed to kill processes on port %d: %s", port, e)
    else:
        # Unix-like systems: убиваем процессы
        try:
            log.info("Killing older processes on port %d (Unix)", port)
            for pid in existing_pids_to_kill:
                try:
                    subprocess.run(["kill", "-9", str(pid)], check=False, capture_output=True, timeout=5.0)
                    killed = True
                except Exception:
                    pass
        except Exception as e:
            log.error("Failed to kill processes on port %d: %s", port, e)
    
    if not killed and existing_pids_to_kill:
        log.warning("Failed to kill some processes on port %d", port)
        # Проверяем, действительно ли процессы еще работают
        time.sleep(0.5)  # Даем время на завершение
        remaining_pids = find_process_on_port(port)
        remaining_valid = [pid for pid in remaining_pids if pid > 0 and is_process_running(pid)]
        if remaining_valid:
            log.warning("Some processes still running on port %d: %s (will continue cleanup)", port, remaining_valid)
            # Не возвращаем False сразу - дадим порту время освободиться в цикле ожидания ниже
        else:
            log.info("Port %d appears to be free despite kill failures (processes may have terminated)", port)
            # Если процессы уже не существуют, считаем это успехом
            killed = True
    
    # Wait for port to be released - simplified approach
    log.info("Waiting for port %d to be released (max %.1f seconds)...", port, wait_timeout)
    start_time = time.time()
    check_interval = 0.5
    max_checks = min(int(wait_timeout / check_interval), 20)  # Max 20 checks (10 seconds)
    
    for check_num in range(max_checks):
        # Check if port is free - проверяем, что остался только один процесс (самый молодой)
        remaining_pids = find_process_on_port(port)
        remaining_running = [pid for pid in remaining_pids if pid > 0 and is_process_running(pid)]
        
        # Если остался только один процесс - это нормально (самый молодой)
        if len(remaining_running) <= 1:
            elapsed = time.time() - start_time
            log.info("Port %d cleanup complete - %d process(es) remaining (took %.1f seconds)", 
                    port, len(remaining_running), elapsed)
            return True
        
        # Log progress every 5 checks (2.5 seconds)
        if check_num > 0 and check_num % 5 == 0:
            elapsed = time.time() - start_time
            log.info("Still waiting... (%.1f seconds, remaining PIDs: %s)", elapsed, remaining_running)
        
        time.sleep(check_interval)
    
    # Final check - проверяем, что остался только один процесс
    remaining_pids = find_process_on_port(port)
    remaining_running = [pid for pid in remaining_pids if pid > 0 and is_process_running(pid)]
    
    if len(remaining_running) > 1:
        log.warning("Port %d still has %d processes after %.1f seconds: %s", 
                   port, len(remaining_running), wait_timeout, remaining_running)
        # Попробуем убить еще раз более агрессивно с увеличенным timeout
        # Но сначала найдем самый молодой процесс
        process_times = []
        for pid in remaining_running:
            start_time_val = get_process_start_time(pid)
            if start_time_val is not None:
                process_times.append((pid, start_time_val))
        
        if process_times:
            youngest_pid, _ = max(process_times, key=lambda x: x[1])
            to_kill = [pid for pid in remaining_running if pid != youngest_pid]
            log.info("Final force kill: killing %d older processes, keeping youngest %d", len(to_kill), youngest_pid)
            for pid in to_kill:
                try:
                    log.info("Final force kill: process %d on port %d", pid, port)
                    if kill_process(pid, timeout=15.0):
                        log.info("Successfully killed process %d in final attempt", pid)
                    else:
                        log.error("FAILED to kill process %d in final attempt", pid)
                except Exception as e:
                    log.error("Exception in final kill for PID %d: %s", pid, e)
        else:
            # Не можем определить самый молодой - убиваем все кроме текущего
            to_kill = [pid for pid in remaining_running if pid != current_pid]
            for pid in to_kill:
                try:
                    log.info("Final force kill: process %d on port %d", pid, port)
                    if kill_process(pid, timeout=15.0):
                        log.info("Successfully killed process %d in final attempt", pid)
                    else:
                        log.error("FAILED to kill process %d in final attempt", pid)
                except Exception as e:
                    log.error("Exception in final kill for PID %d: %s", pid, e)
        
        # Проверим еще раз через небольшую задержку
        time.sleep(1.0)
        final_pids = find_process_on_port(port)
        final_running = [pid for pid in final_pids if pid > 0 and is_process_running(pid)]
        if len(final_running) > 1:
            log.error("Port %d STILL has %d processes after force kill: %s", port, len(final_running), final_running)
            return False
        else:
            log.info("Port %d cleanup complete after force kill - %d process(es) remaining", port, len(final_running))
            return True
    
    log.info("Port %d is now free", port)
    return True

