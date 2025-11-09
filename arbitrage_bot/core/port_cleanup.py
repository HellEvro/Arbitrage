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
    Also excludes PID 0 (system/TIME_WAIT connections) and other invalid PIDs.
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
                            # CRITICAL: Skip invalid PIDs (0 = TIME_WAIT/system, negative = invalid)
                            if pid <= 0:
                                log.debug("Skipping invalid PID %d on port %d (TIME_WAIT/system)", pid, port)
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
                            # CRITICAL: Skip invalid PIDs (0 = TIME_WAIT/system, negative = invalid)
                            if pid <= 0:
                                log.debug("Skipping invalid PID %d on port %d (TIME_WAIT/system)", pid, port)
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
    
    try:
        if system == "Windows":
            # Always use force kill (/F) for immediate termination
            # Fire and forget - don't wait for taskkill
            try:
                # Fire taskkill without waiting - use DEVNULL to avoid blocking
                subprocess.Popen(
                    ["taskkill", "/PID", str(pid), "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    shell=False,
                    creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
                )
                # Don't log here - may cause blocking
            except Exception:
                # Silently ignore errors - process may already be dead
                pass
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
        # Try one more time with force
        try:
            if system == "Windows" and is_process_running(pid):
                subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=False, capture_output=True, timeout=3.0)
                time.sleep(0.5)
                return not is_process_running(pid)
        except:
            pass
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
            try:
                ps_script = '''
                Get-Process python -ErrorAction SilentlyContinue | ForEach-Object {
                    $proc = $_
                    try {
                        $cmdline = (Get-CimInstance Win32_Process -Filter "ProcessId = $($proc.Id)").CommandLine
                        if ($cmdline -and ($cmdline -match "main.py" -or $cmdline -match "arbitrage")) {
                            Write-Output "$($proc.Id)|$cmdline"
                        }
                    } catch {}
                }
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
                            # Check if it's our application - either contains current dir or main.py/arbitrage
                            if current_dir_str in cmdline_lower or ("main.py" in cmdline_lower and "arbitrage" in current_dir_str):
                                if pid not in pids:
                                    pids.append(pid)
                                    # Don't log here - may cause blocking
                        except ValueError:
                            continue
            except Exception as e:
                log.debug("PowerShell method failed: %s", e)
    except Exception as e:
        log.debug("Failed to find processes by command: %s", e)
    
    return list(set(pids))  # Remove duplicates


def cleanup_port(port: int, wait_timeout: float = 15.0) -> bool:
    """Kill Python processes using the specified port and wait for cleanup.
    
    Args:
        port: Port number to clean up
        wait_timeout: Maximum time to wait for processes to terminate (seconds)
    
    Returns:
        True if port is now free, False otherwise
    
    IMPORTANT: Never kills current process (os.getpid()).
    """
    # Get current process ID - CRITICAL for avoiding self-termination
    current_pid = os.getpid()
    log.info("Current process PID: %d, checking for processes on port %d", current_pid, port)
    
    # Try multiple times to find processes (they might be starting up)
    pids = []
    for attempt in range(3):
        pids = find_process_on_port(port)
        if pids:
            break
        if attempt < 2:
            time.sleep(0.5)
    
    # CRITICAL: Filter out invalid PIDs (0, negative) and current process
    pids = [pid for pid in pids if pid > 0 and pid != current_pid]
    
    if not pids:
        log.info("No valid processes found on port %d (only invalid/system PIDs or current PID %d)", port, current_pid)
        return True
    
    log.info("Found %d process(es) on port %d (excluding current PID %d): %s", len(pids), port, current_pid, pids)
    
    # Check which are Python processes - filter out non-Python processes
    python_pids = []
    for pid in pids:
        if pid == current_pid:
            log.warning("CRITICAL: Found current PID %d in list, skipping!", pid)
            continue
        if pid <= 0:
            log.debug("Skipping invalid PID %d", pid)
            continue
        if is_python_process(pid):
            python_pids.append(pid)
            log.debug("Process %d on port %d is a Python process", pid, port)
        else:
            log.debug("Process %d on port %d is NOT a Python process, skipping", pid, port)
    
    if not python_pids:
        # If only non-Python processes (or invalid PIDs like 0), consider port free
        # These are usually TIME_WAIT connections or system processes that don't block the port
        log.info("No Python processes found on port %d (only non-Python/system PIDs: %s), port considered free", port, pids)
        return True
    
    # CRITICAL: Final check - ensure we never kill current process
    current_pid = os.getpid()
    python_pids = [pid for pid in python_pids if pid != current_pid]
    
    if not python_pids:
        log.info("No Python processes to kill (only current PID %d remains)", current_pid)
        return True
    
    log.info("Killing %d Python process(es) on port %d: %s (current PID %d excluded)", 
             len(python_pids), port, python_pids, current_pid)
    
    killed = []
    failed = []
    for pid in python_pids:
        # CRITICAL: Triple-check we're not killing ourselves
        if pid == current_pid:
            log.error("CRITICAL ERROR: Attempted to kill current process %d! Skipping!", pid)
            continue
        
        log.info("Killing Python process %d on port %d...", pid, port)
        try:
            if kill_process(pid, timeout=min(wait_timeout, 3.0)):
                killed.append(pid)
                log.info("Successfully killed process %d", pid)
            else:
                failed.append(pid)
                log.warning("Failed to kill process %d", pid)
        except Exception as e:
            log.warning("Exception killing process %d: %s", pid, e)
            failed.append(pid)
    
    # Retry failed processes - simplified, no logging
    if failed:
        time.sleep(0.5)  # Give system a moment
        for pid in failed[:]:  # Copy list to modify during iteration
            try:
                if kill_process(pid, timeout=2.0):
                    killed.append(pid)
                    failed.remove(pid)
            except Exception:
                pass
    
    if not killed:
        log.error("Failed to kill any processes on port %d (failed: %s)", port, failed)
        if failed:
            return False
    
    if failed:
        log.warning("Some processes failed to terminate: %s, but continuing...", failed)
    
    # Wait for port to be released - simplified approach
    log.info("Waiting for port %d to be released (max %.1f seconds)...", port, wait_timeout)
    start_time = time.time()
    check_interval = 0.5
    max_checks = min(int(wait_timeout / check_interval), 20)  # Max 20 checks (10 seconds)
    
    for check_num in range(max_checks):
        # Check if port is free
        remaining_pids = find_process_on_port(port)
        remaining_python = [pid for pid in remaining_pids if is_python_process(pid) and pid != os.getpid()]
        
        if not remaining_python:
            elapsed = time.time() - start_time
            log.info("Port %d is now free (took %.1f seconds)", port, elapsed)
            return True
        
        # Log progress every 5 checks (2.5 seconds)
        if check_num > 0 and check_num % 5 == 0:
            elapsed = time.time() - start_time
            log.info("Still waiting... (%.1f seconds, remaining PIDs: %s)", elapsed, remaining_python)
        
        time.sleep(check_interval)
    
    # Final check
    remaining_pids = find_process_on_port(port)
    remaining_python = [pid for pid in remaining_pids if is_python_process(pid) and pid != os.getpid()]
    
    if remaining_python:
        log.warning("Port %d still in use by Python processes after %.1f seconds: %s", port, wait_timeout, remaining_python)
        # Don't try to kill again - just return False
        return False
    
    log.info("Port %d is now free", port)
    return True

