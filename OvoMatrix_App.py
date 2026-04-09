import os
import sys
import subprocess
import time
import ctypes
from pathlib import Path

# Explicit imports to force PyInstaller to bundle these modules
import main_backend
import main_ui
import run_dev_broker
import socket
import asyncio

# ── FROZEN-SAFE BASE DIRECTORY ───────────────────────────────────────────────
# We distinguish between:
# 1. BASE_DIR: The directory where the .exe sits (for external .env, .db)
# 2. RESOURCE_DIR: The temp extraction folder (sys._MEIPASS) for internal assets
# ─────────────────────────────────────────────────────────────────────────────
def get_resource_dir() -> Path:
    """Return the temporary extraction folder when frozen, or source folder."""
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        return Path(sys._MEIPASS)
    return Path(__file__).parent

def get_base_dir() -> Path:
    """Return the stable runtime directory (where the .exe is)."""
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).parent
    return Path(__file__).parent

BASE_DIR     = get_base_dir()
RESOURCE_DIR = get_resource_dir()
ENV_FILE     = BASE_DIR / ".env"

# Export for sub-processes
os.environ["OVOMATRIX_BASE_DIR"] = str(BASE_DIR)
os.environ["OVOMATRIX_RESOURCE_DIR"] = str(RESOURCE_DIR)

def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0

def kill_zombie_processes():
    """Ensure no stale instances of the application are running before startup."""
    if sys.platform != "win32":
        return
    import subprocess
    try:
        # Kill any existing backend or ui processes that might be holding the DB open
        # We don't kill ourself because our process name might be different if running from source
        # but if frozen, we need to be careful.
        my_pid = os.getpid()
        cmd = 'tasklist /FI "IMAGENAME eq ovomatrix.exe" /FO CSV /NH'
        output = subprocess.check_output(cmd, shell=True).decode('utf-8', errors='ignore')
        
        import csv
        import io
        reader = csv.reader(io.StringIO(output))
        for row in reader:
            if not row: continue
            pid = int(row[1])
            if pid != my_pid:
                print(f"[OvoLauncher] Killing zombie process PID: {pid}")
                subprocess.run(f"taskkill /F /PID {pid}", shell=True, capture_output=True)
    except Exception as e:
        print(f"[OvoLauncher] Zombie kill warning: {e}")

def main():
    # ── Step 0: Clean environment ─────────────────────────────────────────────
    kill_zombie_processes()
    
    # ── PyInstaller Worker Mode ──────────────────────────────────────────────
    # If running inside the PyInstaller executable with a sub-command
    if len(sys.argv) > 1:
        if sys.argv[1] == '--run-backend':
            # Forward all subsequent arguments (like --db)
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            main_backend.main()
            return
        elif sys.argv[1] == '--run-ui':
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            main_ui.main()
            return
        elif sys.argv[1] == '--run-broker':
            # Run the embedded async broker
            try:
                asyncio.run(run_dev_broker.main("127.0.0.1", 1883))
            except KeyboardInterrupt:
                pass
            return

    # ── Unified Launcher Mode ────────────────────────────────────────────────
    is_frozen = getattr(sys, 'frozen', False)

    db_file = BASE_DIR / "ovomatrix.db"

    # ── Configuration Recovery ───────────────────────────────────────────────
    # If .env is missing (e.g., user moved the .exe alone), generate a safe
    # default configuration instead of crashing.
    if not ENV_FILE.exists():
        print(f"[OvoLauncher] Configuration missing. Generating default .env at {ENV_FILE}")
        defaults = (
            "MQTT_BROKER_HOST=localhost\n"
            "MQTT_BROKER_PORT=1883\n"
            "MQTT_USER=\n"
            "MQTT_PASSWORD=\n"
            "MODBUS_PORT=COM3\n"
            "GSM_PORT=COM4\n"
            "MOCK_HARDWARE=True\n"
            "ALERT_PHONE_NUMBERS=\n"
            "AI_LANGUAGE=English\n"
            "GEMINI_API_KEY=\n"
        )
        try:
            with open(ENV_FILE, "w") as f:
                f.write(defaults)
        except Exception as e:
            print(f"[OvoLauncher] ERROR: Could not create .env: {e}")
    else:
        print(f"[OvoLauncher] Configuration found: {ENV_FILE}")
    
    # Pre-load the correct .env so child_env will inherit these variables.
    # This prevents subprocesses from loading the internal PyInstaller _MEIPASS .env
    try:
        from dotenv import load_dotenv
        load_dotenv(ENV_FILE, override=True)
        print("[OvoLauncher] Environment variables loaded from .env")
    except ImportError:
        pass

    if not db_file.exists():
        print("[OvoLauncher] WARNING: ovomatrix.db not found. A new database will be initialized.")

    # ── Orchestration Commands ───────────────────────────────────────────────
    executable = sys.executable
    db_path_str = str(BASE_DIR / "ovomatrix.db")

    if is_frozen:
        backend_cmd = [executable, '--run-backend', '--db', db_path_str]
        ui_cmd      = [executable, '--run-ui', '--db', db_path_str]
        broker_cmd  = [executable, '--run-broker']
    else:
        # Use python.exe to run the scripts in source mode
        backend_cmd = [executable, str(BASE_DIR / "main_backend.py"), '--db', db_path_str]
        ui_cmd      = [executable, str(BASE_DIR / "main_ui.py"), '--db', db_path_str]
        broker_cmd  = [executable, str(BASE_DIR / "run_dev_broker.py")]

    startupinfo = None
    creationflags = 0

    if sys.platform == "win32":
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        creationflags = subprocess.CREATE_NO_WINDOW   # Completely hidden console

    child_env = os.environ.copy()
    child_env["OVOMATRIX_BASE_DIR"] = str(BASE_DIR)

    # ── Launch Embedded Broker (if needed) ───────────────────────────────────
    broker_proc = None
    if not is_port_in_use(1883):
        print("[OvoLauncher] Port 1883 is idle. Starting embedded MQTT broker...")
        broker_proc = subprocess.Popen(
            broker_cmd,
            startupinfo=startupinfo,
            creationflags=creationflags,
            env=child_env
        )
        time.sleep(1.0) # Small delay to allow broker to bind
    else:
        print("[OvoLauncher] External MQTT broker detected on port 1883. skipping internal.")

    # ── Launch Backend (Headless/Hidden) ─────────────────────────────────────
    print("[OvoLauncher] Starting ovomatrix Backend...")
    backend_proc = subprocess.Popen(
        backend_cmd,
        startupinfo=startupinfo,
        creationflags=creationflags,
        env=child_env
    )

    # Allow backend background threads (Subscriber, UPS, Modbus) time to init
    time.sleep(2.0)

    # ── Launch GUI (Visible) ─────────────────────────────────────────────────
    print("[OvoLauncher] Starting ovomatrix UI...")
    ui_proc = subprocess.Popen(ui_cmd, env=child_env)

    # ── Shutdown Orchestration ───────────────────────────────────────────────
    try:
        ui_proc.wait()
    except KeyboardInterrupt:
        pass
    finally:
        print("[OvoLauncher] GUI closed. Initiating safe shutdown of background threads...")
        # Send SIGTERM to backend so it triggers clean shutdown hook
        backend_proc.terminate()

        try:
            # Reduce wait from 10s to 3s for a snappier exit experience
            backend_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            print("[OvoLauncher] Backend did not exit within 3s. Forcing kill.")
            backend_proc.kill()

        if broker_proc:
            print("[OvoLauncher] Stopping embedded MQTT broker...")
            broker_proc.terminate()
            broker_proc.wait(timeout=1)

        print("[OvoLauncher] ovomatrix Shutdown Complete.")


if __name__ == "__main__":
    import multiprocessing
    # Required to support PyInstaller's multiprocessing on Windows
    multiprocessing.freeze_support()
    main()
